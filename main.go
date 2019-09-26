package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	"strings"
	"time"
)

const storageAPI = "https://storage.googleapis.com"

var bucketName string
var resultsAPI string
var cliCmdName string
var gcpProjectID string
var subId string
var clientVersion string
var clientName string
var cleanupTempFiles bool

func main() {
	flag.StringVar(&bucketName, "bucket-name", "muskoka-transitions", "the name of the storage bucket to download input data from")
	flag.StringVar(&resultsAPI, "results-api", "https://example.com/foobar", "the API endpoint to notify of the transition results, and retrieve signed urls from for output uploading")
	flag.StringVar(&cliCmdName, "cli-cmd", "zcli transition blocks", "change the cli cmd to run transitions with")
	flag.StringVar(&gcpProjectID, "gcp-project-id", "muskoka", "change the google cloud project to connect with pubsub to")
	flag.StringVar(&subId, "sub-id", "poc-v0.8.3", "the pubsub subscription to listen for tasks from")
	flag.StringVar(&clientName, "client-name", "eth2team", "the client name; 'zrnt', 'lighthouse', etc.")
	flag.StringVar(&clientVersion, "client-version", "v0.1.2_1a2b3c4", "the client version, and git commit hash start. In this order, separated by an underscore.")
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
		if exists, err := sub.Exists(ctx); err != nil {
			log.Fatalf("could not check if pubsub subscription exists")
		} else if !exists {
			log.Fatalf("subscription %s does not exist. Either the worker was misconfigured (try --sub-id) or a new subscription needs to be created and permissioned.", subId)
		}
	}
	// configure pubsub receiver
	sub.ReceiveSettings = pubsub.ReceiveSettings{
		MaxExtension:           -1,
		MaxOutstandingMessages: 20,
		MaxOutstandingBytes:    1 << 10,
		NumGoroutines:          4,
		Synchronous:            true,
	}
	// try receiving messages
	{
		if err := sub.Receive(context.Background(), func(ctx context.Context, message *pubsub.Message) {
			var transitionMsg TransitionMsg
			dec := json.NewDecoder(bytes.NewReader(message.Data))
			if err := dec.Decode(&transitionMsg); err != nil {
				log.Printf("failed to decode message JSON: %v (msg: %s)", err, message.Data)
				message.Nack()
				return
			}
			// Give the message a unique ID. Allow for processing of the same message in parallel
			// (if event is fired multiple times, or different workers are processing it on the same host).
			transitionMsg.TmpKey = uniqueID()
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
	TmpKey      string `json:"-"`
}

func (tr *TransitionMsg) DirPath() string {
	return path.Join(os.TempDir(), tr.Key, tr.TmpKey)
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
	// the name of the client; 'zrnt', 'lighthouse', etc.
	ClientName string `json:"client-name"`
	// the version number of the client, may contain a git commit hash
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
	cmdParts := strings.Split(cliCmdName, " ")
	cmdName := cmdParts[0]
	var args []string
	args = append(args, cmdParts[1:]...)
	args = append(args, "--pre", path.Join(transitionDirPath, "pre.ssz"), "--post", path.Join(transitionDirPath, "post.ssz"))
	for i := 0; i < tr.Blocks; i++ {
		args = append(args, path.Join(transitionDirPath, fmt.Sprintf("block_%d.ssz", i)))
	}
	// trigger CLI to run transition in Go routine
	cmd := exec.Command(cmdName, args...)
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
		copy(postHash[:], h.Sum(nil))
	}

	var reqBuf bytes.Buffer
	enc := json.NewEncoder(&reqBuf)
	reqMsg := ResultMsg{
		Success:       success,
		PostHash:      fmt.Sprintf("0x%x", postHash),
		ClientName:    clientName,
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
	if res.StatusCode != 200 {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, res.Body)
		return fmt.Errorf("result API request was denied (status %d): %s", res.StatusCode, string(buf.Bytes()))
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
			req, err := http.NewRequest(http.MethodPut, respMsg.PostStateURL, f)
			if err != nil {
				log.Printf("coult not create request to upload post-state: %v", err)
			} else {
				req.Header.Set("Content-Type", "application/octet-stream")
				if resp, err := client.Do(req); err != nil {
					log.Printf("could not upload post-state: %v", err)
				} else if resp.StatusCode != 200 {
					log.Printf("upload post-state was denied: %v", resp)
				}
			}
			_ = f.Close()
		}
	}
	{
		// try to upload out log
		req, err := http.NewRequest(http.MethodPut, respMsg.OutLogURL, &stdout)
		if err != nil {
			log.Printf("coult not create request to upload std-out: %v", err)
		} else {
			req.Header.Set("Content-Type", "text/plain; charset=utf-8")
			if resp, err := client.Do(req); err != nil {
				log.Printf("could not upload std-out: %v", err)
			} else if resp.StatusCode != 200 {
				log.Printf("upload std-out was denied: %v", resp)
			}
		}
	}
	{
		req, err := http.NewRequest(http.MethodPut, respMsg.ErrLogURL, &stderr)
		if err != nil {
			log.Printf("coult not create request to upload std-out: %v", err)
		} else {
			req.Header.Set("Content-Type", "text/plain; charset=utf-8")
			if resp, err := client.Do(req); err != nil {
				log.Printf("could not upload std-err: %v", err)
			} else if resp.StatusCode != 200 {
				log.Printf("upload std-err was denied: %v", resp)
			}
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

func uniqueID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand.Read error: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
