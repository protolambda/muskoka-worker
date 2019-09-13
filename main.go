package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	var wait time.Duration
	flag.DurationVar(&wait, "graceful-timeout", time.Second * 15, "the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	flag.Parse()

	r := mux.NewRouter()
	r.Use(loggingMiddleware)
	r.HandleFunc("/transition", TransitionHandler)
	// Add routes as needed

	srv := &http.Server{
		Addr:         "0.0.0.0:8080",
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()

	// TODO: start go-routine that registers this worker every ~30 min. (TBD)
	// This keeps the service reachable passively and automatically, without actively maintaining a register or worrying about deployments.

	c := make(chan os.Signal, 1)
	// Catch SIGINT (Ctrl+C) and shutdown gracefully
	signal.Notify(c, os.Interrupt)
	<-c

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	srv.Shutdown(ctx)
	log.Println("shutting down")
	os.Exit(0)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

// 10 MB
const maxUploadMem = 10 * (1 << 20)

func TransitionHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	if r.Method == "GET" {
		err := r.ParseMultipartForm(maxUploadMem)
		if err != nil {
			log.Println(err)
			return
		}
		// TODO register transition in google datastore

		// todo aggregate parsed files into a struct
		for k, v := range r.MultipartForm.File {
			// todo handleFileUpload
		}
		// TODO: return response after getting files

		// TODO write the struct as a folder into /tmp, with files for each input
		// TODO trigger CLI to run transition in Go routine
		// TODO upload transition result to google cloud bucket

		// TODO after uploading to bucket, call a google cloud function that diffs it against the other currently uploaded states from other clients.
		// TODO upload diffs to cloud bucket or datastore (TBD)
	}
}

func handleFileUpload(r *http.Request, inName string, outPath string) error {
	uploadedFile, handler, err := r.FormFile(inName)
	if err != nil {
		return err
	}
	defer uploadedFile.Close()
	log.Printf("%v", handler.Header)
	outFile, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer outFile.Close()
	_, err = io.Copy(outFile, uploadedFile)
	if err != nil {
		return err
	}
	return nil
}
