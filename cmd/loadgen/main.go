// Command loadgen is a small HTTP load generator used for BENCHMARK.md.
//
// Two modes:
//
//	closed loop (default): -c workers each send requests back to back.
//	    Measures max throughput and latency at a given concurrency.
//	open loop (-rate R):  requests are issued at R per second in total,
//	    regardless of how fast responses come back. Used for the limiter
//	    accuracy test ("offer 3x the limit, how many get 200?").
//
// Examples:
//
//	loadgen -targets http://127.0.0.1:8080/api/x -c 64 -d 10s -keys 1000
//	loadgen -targets http://127.0.0.1:8081/,http://127.0.0.1:8082/ -rate 3000 -d 10s -key client-1
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type options struct {
	targets  []string
	conc     int
	duration time.Duration
	warmup   time.Duration
	rate     float64
	keys     int
	key      string
	jsonOut  bool
	series   bool
}

// Result is what one run reports (also the -json output).
type Result struct {
	Targets       []string       `json:"targets"`
	Concurrency   int            `json:"concurrency"`
	OfferedRate   float64        `json:"offered_rate,omitempty"`
	DurationSec   float64        `json:"duration_sec"`
	Requests      int            `json:"requests"`
	Errors        int            `json:"errors"`
	Status        map[string]int `json:"status"`
	ThroughputRPS float64        `json:"throughput_rps"`
	AllowedRPS    float64        `json:"allowed_rps"` // 2xx per second
	P50Ms         float64        `json:"p50_ms"`
	P90Ms         float64        `json:"p90_ms"`
	P99Ms         float64        `json:"p99_ms"`
	MaxMs         float64        `json:"max_ms"`
	AllowedPerSec []int          `json:"allowed_per_sec,omitempty"`
}

// sample is one completed request.
type sample struct {
	at      time.Duration // since start
	latency time.Duration
	status  int // 0 = transport error
}

func main() {
	var o options
	var targets string
	flag.StringVar(&targets, "targets", "http://127.0.0.1:8080/", "comma-separated URLs, used round-robin")
	flag.IntVar(&o.conc, "c", 64, "concurrent workers (connections)")
	flag.DurationVar(&o.duration, "d", 10*time.Second, "measured duration")
	flag.DurationVar(&o.warmup, "warmup", 2*time.Second, "unmeasured warm-up before the run")
	flag.Float64Var(&o.rate, "rate", 0, "open-loop total requests/second (0 = closed loop)")
	flag.IntVar(&o.keys, "keys", 0, "rotate X-API-Key over this many distinct keys")
	flag.StringVar(&o.key, "key", "", "send this single X-API-Key on every request")
	flag.BoolVar(&o.jsonOut, "json", false, "print the result as JSON")
	flag.BoolVar(&o.series, "series", false, "include 2xx count per second")
	flag.Parse()
	o.targets = strings.Split(targets, ",")

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        o.conc * 2,
			MaxIdleConnsPerHost: o.conc * 2,
			DisableCompression:  true,
		},
	}

	if o.warmup > 0 {
		w := o
		w.duration = o.warmup
		run(client, w)
	}
	samples, elapsed := run(client, o)
	res := summarize(o, samples, elapsed)

	if o.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(res)
		return
	}
	printText(res)
}

// run drives load for o.duration and returns every sample.
func run(client *http.Client, o options) ([]sample, time.Duration) {
	start := time.Now()
	deadline := start.Add(o.duration)

	// In open-loop mode a pacer feeds "tickets"; workers send one request per
	// ticket. In closed-loop mode workers just loop until the deadline.
	var tickets chan struct{}
	if o.rate > 0 {
		tickets = make(chan struct{}, o.conc*4)
		go pace(tickets, o.rate, start, deadline)
	}

	results := make([][]sample, o.conc)
	var wg sync.WaitGroup
	for w := 0; w < o.conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			buf := make([]sample, 0, 1024)
			for i := w; ; i += o.conc {
				if tickets != nil {
					if _, ok := <-tickets; !ok {
						break
					}
				} else if time.Now().After(deadline) {
					break
				}
				buf = append(buf, doRequest(client, o, i, start))
			}
			results[w] = buf
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []sample
	for _, r := range results {
		all = append(all, r...)
	}
	return all, elapsed
}

// pace issues rate tickets per second until deadline, catching up every
// millisecond so high rates don't depend on timer precision.
func pace(tickets chan<- struct{}, rate float64, start, deadline time.Time) {
	defer close(tickets)
	sent := 0
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for now := range ticker.C {
		if now.After(deadline) {
			return
		}
		due := int(now.Sub(start).Seconds() * rate)
		for ; sent < due; sent++ {
			tickets <- struct{}{}
		}
	}
}

func doRequest(client *http.Client, o options, i int, start time.Time) sample {
	req, _ := http.NewRequest(http.MethodGet, o.targets[i%len(o.targets)], nil)
	switch {
	case o.key != "":
		req.Header.Set("X-API-Key", o.key)
	case o.keys > 0:
		req.Header.Set("X-API-Key", "key-"+strconv.Itoa(i%o.keys))
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return sample{at: t0.Sub(start), latency: time.Since(t0)}
	}
	_, _ = io.Copy(io.Discard, resp.Body) // read fully so the connection is reused
	resp.Body.Close()
	return sample{at: t0.Sub(start), latency: time.Since(t0), status: resp.StatusCode}
}

func summarize(o options, samples []sample, elapsed time.Duration) Result {
	res := Result{
		Targets:     o.targets,
		Concurrency: o.conc,
		OfferedRate: o.rate,
		DurationSec: elapsed.Seconds(),
		Requests:    len(samples),
		Status:      map[string]int{},
	}
	lat := make([]time.Duration, 0, len(samples))
	allowed := 0
	if o.series {
		res.AllowedPerSec = make([]int, int(o.duration.Seconds()+0.999))
	}
	for _, s := range samples {
		if s.status == 0 {
			res.Errors++
			continue
		}
		res.Status[strconv.Itoa(s.status)]++
		lat = append(lat, s.latency)
		if s.status >= 200 && s.status < 300 {
			allowed++
			if sec := int(s.at.Seconds()); o.series && sec < len(res.AllowedPerSec) {
				res.AllowedPerSec[sec]++
			}
		}
	}
	res.ThroughputRPS = float64(len(samples)) / elapsed.Seconds()
	res.AllowedRPS = float64(allowed) / elapsed.Seconds()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	res.P50Ms = percentileMs(lat, 0.50)
	res.P90Ms = percentileMs(lat, 0.90)
	res.P99Ms = percentileMs(lat, 0.99)
	if len(lat) > 0 {
		res.MaxMs = ms(lat[len(lat)-1])
	}
	return res
}

// percentileMs uses the nearest-rank method on sorted latencies.
func percentileMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted))*p+0.5) - 1
	idx = max(0, min(idx, len(sorted)-1))
	return ms(sorted[idx])
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func printText(r Result) {
	mode := "closed loop"
	if r.OfferedRate > 0 {
		mode = fmt.Sprintf("open loop @ %.0f req/s", r.OfferedRate)
	}
	fmt.Printf("targets:     %s\n", strings.Join(r.Targets, ", "))
	fmt.Printf("mode:        %s, %d workers, %.2fs\n", mode, r.Concurrency, r.DurationSec)
	fmt.Printf("requests:    %d (errors %d)  status %v\n", r.Requests, r.Errors, r.Status)
	fmt.Printf("throughput:  %.0f req/s   allowed (2xx): %.1f req/s\n", r.ThroughputRPS, r.AllowedRPS)
	fmt.Printf("latency ms:  p50 %.3f  p90 %.3f  p99 %.3f  max %.3f\n", r.P50Ms, r.P90Ms, r.P99Ms, r.MaxMs)
	if len(r.AllowedPerSec) > 0 {
		fmt.Printf("2xx per sec: %v\n", r.AllowedPerSec)
	}
}
