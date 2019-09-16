package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"time"
)

var cliCmdName string
var gcpProjectID string
var subId string

func main() {
	flag.StringVar(&cliCmdName, "cli-cmd", "zcli transition blocks", "change the cli cmd to run transitions with")
	flag.StringVar(&gcpProjectID, "gcp-project-id", "muskoka", "change the google cloud project to connect with pubsub to")
	flag.StringVar(&subId, "sub-id", "poc-v0.8.3", "the pubsub subscription to listen for tasks from")
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

func (tr *TransitionMsg) LoadFromBucket() error {
	// TODO
	return nil
}

func (tr *TransitionMsg) Execute() error {
	log.Println("executing request")
	var args []string
	args = append(args, "--pre", path.Join(tr.Key, "pre.ssz"), "--post", path.Join(tr.Key, "post.ssz"))
	for i := 0; i < tr.Blocks; i++ {
		args = append(args, path.Join(tr.Key, fmt.Sprintf("block_%d.ssz", i)))
	}
	// trigger CLI to run transition in Go routine
	cmd := exec.Command(cliCmdName, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Printf("transition command failed: %s", err)
	}
	outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
	fmt.Printf("out:\n%s\nerr:\n%s\n", outStr, errStr)

	// TODO upload transition result to google cloud bucket
	// TODO remove temporary files (blocks, pre, post)
}
