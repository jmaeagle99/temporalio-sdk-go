package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/demos"
)

func newCORSHTTPHandler(web string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", web)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Namespace")

		if r.Method == "OPTIONS" {
			return
		}

		next.ServeHTTP(w, r)
	})
}

// HTTP handler for codecs.
// This remote codec server example supports URLs like: /{namespace}/encode and /{namespace}/decode
// For example, for the default namespace you would hit /default/encode and /default/decode
// It will also accept URLs: /encode and /decode with the X-Namespace set to indicate the namespace.
func newPayloadCodecNamespacesHTTPHandler(encoders map[string][]converter.PayloadCodec) http.Handler {
	mux := http.NewServeMux()

	codecHandlers := make(map[string]http.Handler, len(encoders))
	for namespace, codecChain := range encoders {
		fmt.Printf("Handling namespace: %s\n", namespace)

		handler := converter.NewPayloadCodecHTTPHandler(codecChain...)
		mux.Handle("/"+namespace+"/", handler)

		codecHandlers[namespace] = handler
	}

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace := r.Header.Get("X-Namespace")
		if namespace != "" {
			if handler, ok := codecHandlers[namespace]; ok {
				handler.ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}))

	return mux
}

func newPayloadNamespacesHTTPHandler(encoders map[string][]converter.PayloadCodec) http.Handler {
	mux := http.NewServeMux()

	codecHandlers := make(map[string]http.Handler, len(encoders))
	for namespace, codecChain := range encoders {
		fmt.Printf("Handling namespace: %s\n", namespace)

		handler, err := converter.NewPayloadHTTPHandler(converter.PayloadHTTPHandlerOptions{
			PreStorageCodecs: codecChain,
			ExternalStorage: converter.ExternalStorage{
				Drivers:              []converter.StorageDriver{demos.CreateDriver()},
				PayloadSizeThreshold: 50,
			},
		})
		if err != nil {
			log.Fatal("error creating payload HTTP handler", err)
		}
		mux.Handle("/"+namespace+"/", handler)

		codecHandlers[namespace] = handler
	}

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace := r.Header.Get("X-Namespace")
		if namespace != "" {
			if handler, ok := codecHandlers[namespace]; ok {
				handler.ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}))

	return mux
}

func main() {
	// Set codecs per namespace here.
	// Only handle codecs for the default namespace in this example.
	codecs := map[string][]converter.PayloadCodec{
		"default": {demos.NewPayloadCodec()},
	}

	//handler := newPayloadCodecNamespacesHTTPHandler(codecs)
	handler := newPayloadNamespacesHTTPHandler(codecs)

	handler = newCORSHTTPHandler("*", handler)

	srv := &http.Server{
		Addr:    "0.0.0.0:8081",
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case <-sigCh:
		_ = srv.Close()
	case err := <-errCh:
		if err != http.ErrServerClosed {
			log.Fatal("error from HTTP server", err)
		}
	}
}
