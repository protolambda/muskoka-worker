package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"time"

	"github.com/gorilla/mux"
)

func main() {
	var wait time.Duration
	flag.DurationVar(&wait, "graceful-timeout", time.Second * 15, "the duration for which the server gracefully wait for existing connections to finish - e.g. 15s or 1m")
	flag.Parse()

	fs := http.FileServer(http.Dir("static"))

	r := mux.NewRouter()
	r.Use(loggingMiddleware)
	r.HandleFunc("/transition", TransitionHandler)
	r.Handle("/", fs)
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

	//start go-routine that registers this worker every ~30 min. (TBD)
	// This keeps the service reachable passively and automatically,
	// without actively maintaining a register or worrying about deployments.
	stopRegistering := false
	go func() {
		log.Println("starting registering loop")
		for {
			if stopRegistering {
				break
			}
			registerService()
			time.Sleep(time.Second * 5)// todo time.Minute * 30)
		}
		log.Println("stopped registering")
	}()


	c := make(chan os.Signal, 1)
	// Catch SIGINT (Ctrl+C) and shutdown gracefully
	signal.Notify(c, os.Interrupt)
	<-c

	stopRegistering = true

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

func registerService() {
	log.Println("registering")
	// TODO send register data to some API endpoint (CLI version etc.)
}

// 10 MB
const maxUploadMem = 10 * (1 << 20)


func TransitionHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	if r.Method == "POST" {
		tr, err := NewTransitionRequest(r)
		if err != nil {
			log.Printf("Initializing transition request failed, aborting processing. %v", err)
			w.WriteHeader(500)
			return
		}
		if err := tr.Register(); err != nil {
			log.Printf("Registering transition request failed, aborting processing. %v", err)
			w.WriteHeader(500)
			return
		}
		if err := tr.Load(r); err != nil {
			log.Printf("Could not load input data: %v", err)
			w.WriteHeader(500)
			return
		}
		fmt.Fprintln(w, "Received request, started processing.") // TODO: respond with template / redirect to results

		// return request, do processing
		go tr.Execute()
	}
}

type TransitionRequest struct {
	id uuid.UUID
	// Random temporary directory. Always with a "pre.ssz"
	storageDir string
	// "block_%d.ssz"
	blocks uint
	// TODO time? other meta?
}

func NewTransitionRequest(r *http.Request) (*TransitionRequest, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	tr := &TransitionRequest{
		id: id,
		storageDir: path.Join(os.TempDir(), "eth2_test_" + id.String()),
	}
	// TODO add meta from request
	return tr, nil
}

func (tr *TransitionRequest) Register() error {
	// TODO register transition in google datastore
	log.Println("registering task")
	return nil
}

func (tr *TransitionRequest) Load(r *http.Request) error {
	err := r.ParseMultipartForm(maxUploadMem)
	if err != nil {
		return fmt.Errorf("cannot parse multipart upload: %v", err)
	}

	if err := os.Mkdir(tr.storageDir, os.ModePerm); err != nil {
		return fmt.Errorf("cannot create temporary directory for inputs: %v", err)
	}

	// Parse and store pre-state
	if pre, ok := r.MultipartForm.File["pre"]; !ok {
		return errors.New("no pre-state was specified")
	} else {
		if len(pre) != 1 {
			return errors.New("need exactly one pre-state file")
		}
		preUpload := pre[0]
		log.Printf("pre upload header: %v", preUpload.Header)
		outFile, err := os.OpenFile(path.Join(tr.storageDir, "pre.ssz"), os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return fmt.Errorf("cannot create pre file: %v", err)
		}
		defer outFile.Close()
	}
	// parse and store blocks
	if blocks, ok := r.MultipartForm.File["blocks"]; !ok {
		return errors.New("no pre-state was specified")
	} else {
		if len(blocks) > 16 {
			return fmt.Errorf("cannot process high amount of blocks; %v", len(blocks))
		}
		var loopErr error
		for i, b := range blocks {
			log.Printf("block %d upload header: %v", i, b.Header)
			outFile, err := os.OpenFile(path.Join(tr.storageDir, fmt.Sprintf("block_%d.ssz", i)), os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				loopErr = fmt.Errorf("cannot create block %d file: %v", i, err)
				break
			}
			f, err := b.Open()
			if _, err = io.Copy(outFile, f); err != nil {
				return err
			}
			_ = f.Close()
			_ = outFile.Close()
			tr.blocks++
		}
		if loopErr != nil {
			return loopErr
		}
	}
	return nil
}

func (tr *TransitionRequest) Execute() {
	log.Println("executing request")
	// TODO trigger CLI to run transition in Go routine
	// TODO upload transition result to google cloud bucket

	// TODO after uploading to bucket, call a google cloud function that diffs it against the other currently uploaded states from other clients.
	// TODO upload diffs to cloud bucket or datastore (TBD)
	// TODO remove temporary files (blocks, pre, post)
}
