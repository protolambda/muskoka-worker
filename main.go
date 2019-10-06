package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"time"
)

const storageAPI = "https://storage.googleapis.com"

var inputsBucketName string
var cliCmdName string
var gcpProjectID string
var specVersion string
var specConfig string
var workerID string
var clientVersion string
var clientName string
var resultsBucketName string
var cleanupTempFiles bool

var inputsBucket *storage.BucketHandle
var resultsBucket *storage.BucketHandle
var resultsTopic *pubsub.Topic

func main() {
	flag.StringVar(&inputsBucketName, "inputs-bucket", "muskoka-transitions", "the name of the storage bucket to download input data from")
	flag.StringVar(&specVersion, "spec-version", "v0.8.3", "the spec-version to target")
	flag.StringVar(&specConfig, "spec-config", "minimal", "the config name to target")
	flag.StringVar(&cliCmdName, "cli-cmd", "zcli transition blocks", "change the cli cmd to run transitions with")
	flag.StringVar(&gcpProjectID, "gcp-project-id", "muskoka", "change the google cloud project to connect with pubsub to")
	flag.StringVar(&workerID, "worker-id", "poc", "the name of the worker. Pubsub subscription id is formatted as: <spec version>~<spec config>~<client name>~<worker id> to get a unique subscription name")
	flag.StringVar(&clientName, "client-name", "eth2team", "the client name; 'zrnt', 'lighthouse', etc.")
	flag.StringVar(&resultsBucketName, "results-bucket", "results-eth2team", "the name of the bucket to upload the results to.")
	flag.StringVar(&clientVersion, "client-version", "v0.1.2_1a2b3c4", "the client version, and git commit hash start. In this order, separated by an underscore.")
	flag.BoolVar(&cleanupTempFiles, "cleanup-tmp", true, "if the temporary files should be removed after uploading the results of a transition")
	flag.Parse()

	mainContext, cancel := context.WithCancel(context.Background())

	// storage
	{
		storageClient, err := storage.NewClient(mainContext)
		if err != nil {
			log.Fatalf("Failed to create storage client: %v", err)
		}
		inputsBucket = storageClient.Bucket(inputsBucketName)
		resultsBucket = storageClient.Bucket(resultsBucketName)
	}

	// Setup pubsub client
	pubsubClient, err := pubsub.NewClient(mainContext, gcpProjectID)
	if err != nil {
		log.Fatalf("Failed to create pubsub client: %v", err)
	}

	resultsTopic = pubsubClient.Topic(fmt.Sprintf("results~%s", clientName))
	{
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		ok, err := resultsTopic.Exists(ctx)
		if err != nil {
			log.Fatalf("Could not check if spec version + config is a valid topic: %v", err)
		} else if !ok {
			log.Fatalf("Cannot recognize provided options to find results topic: %s", resultsTopic.ID())
		}
	}

	subId := fmt.Sprintf("%s~%s~%s~%s", specVersion, specConfig, clientName, workerID)
	sub := pubsubClient.Subscription(subId)
	// check if the subscription exists
	{
		ctx, _ := context.WithTimeout(context.Background(), time.Second*15)
		if exists, err := sub.Exists(ctx); err != nil {
			log.Fatalf("could not check if pubsub subscription exists: %v\n", err)
		} else if !exists {
			log.Fatalf("subscription %s does not exist. Either the worker was misconfigured (try --spec-version, --spec-config, --client-name, --worker-id) or a new subscription needs to be created and permissioned.", subId)
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
			if transitionMsg.SpecVersion != specVersion {
				log.Printf("WARNING: received pubsub transition for spec version: %s, but was expecting %s. Ack, but ignoring actual task.", transitionMsg.SpecVersion, specVersion)
				message.Ack()
				return
			}
			if transitionMsg.SpecConfig != specConfig {
				log.Printf("WARNING: received pubsub transition for spec config: %s, but was expecting %s. Ack, but ignoring actual task.", transitionMsg.SpecConfig, specConfig)
				message.Ack()
				return
			}
			// Give the message a unique ID. Allow for processing of the same message in parallel
			// (if event is fired multiple times, or different workers are processing it on the same host).
			transitionMsg.ResultKey = uniqueID()
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
	SpecConfig  string `json:"spec-config"`
	Key         string `json:"key"`
	ResultKey   string `json:"-"`
}

func (tr *TransitionMsg) DirPath() string {
	return path.Join(os.TempDir(), tr.Key, tr.ResultKey)
}

func (tr *TransitionMsg) InputsBucketPathStart() string {
	return fmt.Sprintf("%s/%s/%s", tr.SpecVersion, tr.SpecConfig, tr.Key)
}

func (tr *TransitionMsg) ResultsBucketPathStart() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", tr.SpecVersion, tr.SpecConfig, tr.Key, clientName, clientVersion, tr.ResultKey)
}

func (tr *TransitionMsg) LoadFromBucket() error {
	startFilepath := tr.DirPath()
	if err := os.MkdirAll(startFilepath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to make directory to download files to: %s: %v", startFilepath, err)
	}
	startBucketPath := tr.InputsBucketPathStart()
	if err := downloadInputFile(path.Join(startFilepath, "pre.ssz"), startBucketPath+"/pre.ssz"); err != nil {
		return fmt.Errorf("failed to load pre.ssz for spec version %s task %s: %v", tr.SpecVersion, tr.Key, err)
	}
	for i := 0; i < tr.Blocks; i++ {
		blockName := fmt.Sprintf("block_%d.ssz", i)
		if err := downloadInputFile(path.Join(startFilepath, blockName), startBucketPath+"/"+blockName); err != nil {
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
	// Result files
	Files ResultFilesData `json:"files"`
}

type ResultFilesData struct {
	// urls to the files
	PostState string `json:"post-state"`
	ErrLog    string `json:"err-log"`
	OutLog    string `json:"out-log"`
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

	// upload results
	bucketPathStart := tr.ResultsBucketPathStart()
	resultFiles := ResultFilesData{
		PostState: fmt.Sprintf("%s/%s/%s/post.ssz", storageAPI, resultsBucketName, bucketPathStart),
		ErrLog:    fmt.Sprintf("%s/%s/%s/std_out_log.txt", storageAPI, resultsBucketName, bucketPathStart),
		OutLog:    fmt.Sprintf("%s/%s/%s/std_err_log.txt", storageAPI, resultsBucketName, bucketPathStart),
	}
	{
		{
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			w := resultsBucket.Object(resultFiles.PostState).NewWriter(ctx)
			// try to upload post state, if it exists
			f, err := os.Open(path.Join(transitionDirPath, "post.ssz"))
			if err != nil {
				log.Printf("cannot open post state to upload to cloud")
			} else {
				if _, err := io.Copy(w, f); err != nil {
					log.Printf("could not upload post-state: %v", err)
				}
				_ = f.Close()
			}
			_ = w.Close()
		}
		{
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			w := resultsBucket.Object(resultFiles.OutLog).NewWriter(ctx)
			if _, err := io.Copy(w, &stdout); err != nil {
				log.Printf("could not upload std-out: %v", err)
			}
			_ = w.Close()
		}
		{
			ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
			w := resultsBucket.Object(resultFiles.ErrLog).NewWriter(ctx)
			if _, err := io.Copy(w, &stderr); err != nil {
				log.Printf("could not upload std-err: %v", err)
			}
			_ = w.Close()
		}
	}

	{
		var reqBuf bytes.Buffer
		enc := json.NewEncoder(&reqBuf)
		reqMsg := ResultMsg{
			Success:       success,
			PostHash:      fmt.Sprintf("0x%x", postHash),
			ClientName:    clientName,
			ClientVersion: clientVersion,
			Key:           tr.Key,
			Files:         resultFiles,
		}
		if err := enc.Encode(&reqMsg); err != nil {
			log.Printf("failed to encode result to JSON message.")
			return fmt.Errorf("failed to encode result to JSON message: %v", err)
		}
		ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
		<-resultsTopic.Publish(ctx, &pubsub.Message{
			Data: reqBuf.Bytes(),
		}).Ready()
	}

	if cleanupTempFiles {
		// remove temporary files (blocks, pre, post)
		if err := os.RemoveAll(transitionDirPath); err != nil {
			log.Printf("cannot clean up temporary files of transition %s: %v", tr.Key, err)
		}
	}
	return nil
}

func downloadInputFile(filepath string, bucketpath string) (err error) {
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	r, err := inputsBucket.Object(bucketpath).NewReader(ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	_, err = io.Copy(out, r)
	return err
}

func uniqueID() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand.Read error: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
