package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"time"
)

var cliCmdName string

func main() {
	var wait time.Duration
	flag.DurationVar(&wait, "graceful-timeout", time.Second * 15, "the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	flag.StringVar(&cliCmdName, "cli-cmd", "zcli transition blocks", "change the cli cmd to run transitions with")
	flag.Parse()

	// TODO: start listening for pubsub events, filtered by spec-version
	// TODO: download transition inputs
	// TODO: run transition
	// TODO: push transition results to API

	c := make(chan os.Signal, 1)
	// Catch SIGINT (Ctrl+C) and shutdown gracefully
	signal.Notify(c, os.Interrupt)
	<-c

	// TODO shutdown pubsub client
	log.Println("shutting down")
	os.Exit(0)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

type TransitionRequest struct {
	// key used in datastore, bucket prefix, etc.
	key string
	// count of blocks, for "block_%d.ssz"
	blocks uint
	// TODO time? other meta?
}

func (tr *TransitionRequest) LoadFromBucket() error {
	// TODO
	return nil
}

func (tr *TransitionRequest) Execute() {
	log.Println("executing request")
	var args []string
	args = append(args, "--pre", path.Join(tr.key, "pre.ssz"), "--post", path.Join(tr.key, "post.ssz"))
	for i := uint(0); i < tr.blocks; i++ {
		args = append(args, path.Join(tr.key, fmt.Sprintf("block_%d.ssz", i)))
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
