// Command upstream is a tiny HTTP backend used for demos and benchmarks.
// It answers every request with a short body, optionally after a delay.
//
//	upstream -listen :9001 -delay 0
package main

import (
	"flag"
	"log"
	"net/http"
	"time"
)

func main() {
	listen := flag.String("listen", ":9001", "listen address")
	delay := flag.Duration("delay", 0, "artificial processing time per request")
	flag.Parse()

	body := []byte(`{"ok":true}` + "\n")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if *delay > 0 {
			time.Sleep(*delay)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	log.Printf("upstream listening on %s (delay %s)", *listen, *delay)
	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
