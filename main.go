package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"time"
)

const storageAPI = "https://storage.googleapis.com"
const bucketName = "transitions"

var resultsAPI string
var cliCmdName string
var gcpProjectID string
var subId string
var clientVersion string
var cleanupTempFiles bool

func main() {
	flag.StringVar(&resultsAPI, "results-api", "https://example.com/foobar", "the API endpoint to notify of the transition results, and retrieve signed urls from for output uploading")
	flag.StringVar(&cliCmdName, "cli-cmd", "zcli transition blocks", "change the cli cmd to run transitions with")
	flag.StringVar(&gcpProjectID, "gcp-project-id", "muskoka", "change the google cloud project to connect with pubsub to")
	flag.StringVar(&subId, "sub-id", "poc-v0.8.3", "the pubsub subscription to listen for tasks from")
	flag.StringVar(&clientVersion, "client-version", "eth2team_v0.1.2_v0.3.4_1a2b3c4", "the vendor name, targeted spec version, client version, and git commit hash start. In this order, separated by underscores.")
	flag.BoolVar(&cleanupTempFiles, "cleanup-tmp", true, "if the temporary files should be removed after uploading the results of a transition")
	flag.Parse()

	mainContext, cancel := context.WithCancel(context.Background())

	// Setup pubsub client
	pubsubClient, err := pubsub.NewClient(mainContext, gcpProjectID)
	if err != nil {
		log.Fatalf("Failed to create pubsub client: %v", err)
	}
	sub := pubsubClient.Subscription(subId)
	// check if the subscription exists
	{
		ctx, _ := context.WithTimeout(mainContext, time.Second*15)
		if exists, err := sub.Exists(ctx, ); err != nil {
			log.Fatalf("could not check if pubsub subscription exists")
		} else if !exists {
			log.Fatalf("subscription %s does not exist. Either the worker was misconfigured (try --sub-id) or a new subscription needs to be created and permissioned.", subId)
		}
	}
	// configure pubsub receiver
	sub.ReceiveSettings = pubsub.ReceiveSettings{
		MaxExtension:           0, // triggers default
		MaxOutstandingMessages: 20,
		MaxOutstandingBytes:    1 << 10,
		NumGoroutines:          4,
		Synchronous:            false,
	}
	// try receiving messages
	{
		ctx, _ := context.WithTimeout(mainContext, time.Second*15)
		if err := sub.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			var transitionMsg TransitionMsg
			dec := json.NewDecoder(bytes.NewReader(message.Data))
			if err := dec.Decode(&transitionMsg); err != nil {
				log.Printf("failed to decode message JSON: %v (msg: %s)", err, message.Data)
				message.Nack()
				return
			}
			log.Printf("processing %s (%s)", transitionMsg.Key, transitionMsg.SpecVersion)
			if err := transitionMsg.LoadFromBucket(); err != nil {
				log.Printf("failed to load data from bucket for %s: %v", transitionMsg.Key, err)
				message.Nack()
				return
			}
			if err := transitionMsg.Execute(); err != nil {
				log.Printf("failed to run transition for %s: %v", transitionMsg.Key, err)
				message.Nack()
				return
			}
			log.Printf("successfully processed transition: %s (%s)", transitionMsg.Key, transitionMsg.SpecVersion)
			message.Ack()
		}); err != nil {
			log.Fatalf("failed to receive messages: %v", err)
		}
	}

	c := make(chan os.Signal, 1)
	// Catch SIGINT (Ctrl+C) and shutdown gracefully
	signal.Notify(c, os.Interrupt)
	<-c
	cancel()
	log.Println("shutting down")
	os.Exit(0)
}

type TransitionMsg struct {
	Blocks      int    `json:"blocks"`
	SpecVersion string `json:"spec-version"`
	Key         string `json:"key"`
}

func (tr *TransitionMsg) DirPath() string {
	return path.Join(os.TempDir(), tr.Key)
}

func (tr *TransitionMsg) BucketUrlStart() string {
	return fmt.Sprintf("%s/%s/%s/%s", storageAPI, bucketName, tr.SpecVersion, tr.Key)
}

func (tr *TransitionMsg) LoadFromBucket() error {
	startUrl := tr.BucketUrlStart()
	startFilepath := tr.DirPath()
	if err := os.MkdirAll(startFilepath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory to download files to: %s: %v", startFilepath, err)
	}
	if err := downloadFile(path.Join(startFilepath, "pre.ssz"), startUrl+"/pre.ssz"); err != nil {
		return fmt.Errorf("failed to load pre.ssz for spec version %s task %s: %v", tr.SpecVersion, tr.Key, err)
	}
	for i := 0; i < tr.Blocks; i++ {
		blockName := fmt.Sprintf("block_%d.ssz", i)
		if err := downloadFile(path.Join(startFilepath, blockName), startUrl+"/"+blockName); err != nil {
			return fmt.Errorf("failed to load pre.ssz for spec version %s task %s: %v", tr.SpecVersion, tr.Key, err)
		}
	}
	return nil
}

type ResultMsg struct {
	// if the transition was successful (i.e. no err log)
	Success bool `json:"success"`
	// the flat-hash of the post-state SSZ bytes, for quickly finding different results.
	PostHash string `json:"post-hash"`
	// identifies the client software running the transition
	ClientVersion string `json:"client-version"`
	// identifies the transition task
	Key string `json:"key"`
}

type ResultResponseMsg struct {
	PostStateURL string `json:"post-state"`
	ErrLogURL    string `json:"err-log"`
	OutLogURL    string `json:"out-log"`
}

func (tr *TransitionMsg) Execute() error {
	log.Printf("executing request: %s (%d blocks, spec version %s)\n", tr.Key, tr.Blocks, tr.SpecVersion)
	transitionDirPath := tr.DirPath()
	var args []string
	args = append(args, "--pre", path.Join(transitionDirPath, "pre.ssz"), "--post", path.Join(transitionDirPath, "post.ssz"))
	for i := 0; i < tr.Blocks; i++ {
		args = append(args, path.Join(transitionDirPath, fmt.Sprintf("block_%d.ssz", i)))
	}
	// trigger CLI to run transition in Go routine
	cmd := exec.Command(cliCmdName, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	success := true
	if err != nil {
		log.Printf("transition command failed: %s", err)
		// continue with whatever results the command was able to generate.
		// May be the client resorting to an error-code because of a failed transition, which we still like to upload.
		if exitErr, ok := err.(*exec.ExitError); ok {
			success = exitErr.Success()
		}
	}
	log.Printf("%s\nout:\n%s\nerr:\n%s\n", tr.Key, string(stdout.Bytes()), string(stderr.Bytes()))

	var postHash [32]byte
	postF, err := os.Open(path.Join(transitionDirPath, "post.ssz"))
	if err != nil {
		log.Printf("failed to open post state to compute hash: %v", err)
	} else {
		h := sha256.New()
		_, err := io.Copy(h, postF)
		if err != nil {
			log.Printf("failed to hash post state: %v", err)
		}
		_ = postF.Close()
	}

	var reqBuf bytes.Buffer
	enc := json.NewEncoder(&reqBuf)
	reqMsg := ResultMsg{
		Success:       success,
		PostHash:      fmt.Sprintf("%x", postHash),
		ClientVersion: clientVersion,
		Key:           tr.Key,
	}
	if err := enc.Encode(&reqMsg); err != nil {
		log.Printf("failed to encode result to JSON message.")
		return fmt.Errorf("failed to encode result to JSON message: %v", err)
	}
	client := &http.Client{
		Timeout: time.Second * 15,
	}
	req, err := http.NewRequest("GET", resultsAPI, &reqBuf)
	if err != nil {
		return fmt.Errorf("failed to prepare result API request: %v", err)
	}
	// TODO authenticate request
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make result API request: %v", err)
	}
	var respMsg ResultResponseMsg
	dec := json.NewDecoder(res.Body)
	if err := dec.Decode(&respMsg); err != nil {
		return fmt.Errorf("failed to decode result API response: %v", err)
	}
	{
		// try to upload post state, if it exists
		f, err := os.Open(path.Join(transitionDirPath, "post.ssz"))
		if err != nil {
			log.Printf("cannot open post state to upload to cloud")
		} else {
			if resp, err := http.Post(respMsg.PostStateURL, "application/octet-stream", f); err != nil {
				log.Printf("could not upload post-state: %v", err)
			} else if resp.StatusCode != 200 {
				log.Printf("upload post-state was denied: %v", resp)
			}
			defer f.Close()
		}
	}
	{
		// try to upload out log
		if resp, err := http.Post(respMsg.OutLogURL, "text/plain", &stdout); err != nil {
			log.Printf("could not upload std-out: %v", err)
		} else if resp.StatusCode != 200 {
			log.Printf("upload std-out was denied: %v", resp)
		}
	}
	{
		// try to upload err log
		if resp, err := http.Post(respMsg.ErrLogURL, "text/plain", &stderr); err != nil {
			log.Printf("could not upload std-err: %v", err)
		} else if resp.StatusCode != 200 {
			log.Printf("upload std-err was denied: %v", resp)
		}
	}
	log.Printf("completed execution of %s.", tr.Key)

	if cleanupTempFiles {
		// remove temporary files (blocks, pre, post)
		if err := os.RemoveAll(transitionDirPath); err != nil {
			log.Printf("cannot clean up temporary files of transition %s: %v", tr.Key, err)
		}
	}
	return nil
}

func downloadFile(filepath string, url string) (err error) {
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}
	_, err = io.Copy(out, resp.Body)
	return err
}
