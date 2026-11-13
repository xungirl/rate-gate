package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xungirl/rate-gate/internal/breaker"
	"github.com/xungirl/rate-gate/internal/config"
	"github.com/xungirl/rate-gate/internal/metrics"
)

// upstream is a fake backend that counts hits and echoes the path it saw.
type upstream struct {
	*httptest.Server
	hits atomic.Int64
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		fmt.Fprintf(w, "path=%s xff=%s", r.URL.Path, r.Header.Get("X-Forwarded-For"))
	}))
	t.Cleanup(u.Close)
	return u
}

// testConfig fills a YAML template: {{UP}} becomes the upstream URL.
func testConfig(t *testing.T, yaml string, up *upstream) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(strings.ReplaceAll(yaml, "{{UP}}", up.URL)))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

type instance struct {
	*httptest.Server
	gw      *Gateway
	metrics *metrics.Metrics
}

// startGateway runs one gateway instance. For the redis backend it creates
// its own Redis client and breaker, exactly like a separate process would.
func startGateway(t *testing.T, cfg *config.Config) *instance {
	t.Helper()
	m := metrics.New()
	deps := Deps{Metrics: m}
	if cfg.Backend.Type == "redis" {
		client := redis.NewClient(&redis.Options{Addr: cfg.Backend.Redis.Addr, MaxRetries: -1,
			DialTimeout: 100 * time.Millisecond, PoolSize: 64})
		t.Cleanup(func() { _ = client.Close() })
		deps.Redis = client
		deps.Breaker = breaker.New(cfg.Backend.Breaker.FailureThreshold, cfg.Backend.Breaker.OpenTimeout, nil, nil)
	}
	gw, err := New(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gw.Close)
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("/healthz", gw.HealthHandler())
	mux.Handle("/", gw)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &instance{Server: srv, gw: gw, metrics: m}
}

func get(t *testing.T, url string, headers ...string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

const basicYAML = `
backend: { type: memory }
routes:
  - name: api
    path_prefix: /api/
    upstream: {{UP}}
    rate_limit: { algorithm: fixed_window, key_by: api_key, limit: 3, window: 1h }
`

func TestRateLimitHeadersAnd429(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, basicYAML, up))

	for i := 0; i < 3; i++ {
		resp := get(t, gw.URL+"/api/x", "X-API-Key", "alice")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status %d", i+1, resp.StatusCode)
		}
		if got := resp.Header.Get("X-RateLimit-Limit"); got != "3" {
			t.Errorf("X-RateLimit-Limit=%q want 3", got)
		}
		if got := resp.Header.Get("X-RateLimit-Remaining"); got != strconv.Itoa(2-i) {
			t.Errorf("request %d: X-RateLimit-Remaining=%q want %d", i+1, got, 2-i)
		}
		if reset, _ := strconv.Atoi(resp.Header.Get("X-RateLimit-Reset")); reset < 1 || reset > 3600 {
			t.Errorf("X-RateLimit-Reset=%d, want 1..3600 seconds", reset)
		}
	}

	req, _ := http.NewRequest(http.MethodGet, gw.URL+"/api/x", nil)
	req.Header.Set("X-API-Key", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("4th request: status %d, want 429", resp.StatusCode)
	}
	if ra, _ := strconv.Atoi(resp.Header.Get("Retry-After")); ra < 1 {
		t.Errorf("Retry-After=%q, want >= 1", resp.Header.Get("Retry-After"))
	}
	if resp.Header.Get("X-RateLimit-Remaining") != "0" {
		t.Errorf("Remaining on 429 = %q, want 0", resp.Header.Get("X-RateLimit-Remaining"))
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body["error"] != "rate limit exceeded" {
		t.Errorf("429 body = %v (%v)", body, err)
	}
	if got := up.hits.Load(); got != 3 {
		t.Errorf("upstream saw %d requests, want 3 (429s must not be forwarded)", got)
	}
}

func TestKeysOverridesAndIPFallback(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, `
backend: { type: memory }
routes:
  - name: api
    path_prefix: /
    upstream: {{UP}}
    rate_limit:
      algorithm: token_bucket
      key_by: api_key
      limit: 2
      window: 1h
      overrides: [ { key: vip, limit: 5 } ]
`, up))

	count := func(headers ...string) int {
		ok := 0
		for i := 0; i < 10; i++ {
			if get(t, gw.URL+"/", headers...).StatusCode == http.StatusOK {
				ok++
			}
		}
		return ok
	}
	if got := count("X-API-Key", "a"); got != 2 {
		t.Errorf("key a allowed %d, want 2", got)
	}
	if got := count("X-API-Key", "b"); got != 2 {
		t.Errorf("key b allowed %d, want 2 (independent of a)", got)
	}
	if got := count("X-API-Key", "vip"); got != 5 {
		t.Errorf("vip allowed %d, want 5 (override)", got)
	}
	if got := count(); got != 2 {
		t.Errorf("no key (IP fallback) allowed %d, want 2", got)
	}
}

func TestRoutingAndStripPrefix(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, `
backend: { type: memory }
routes:
  - { name: v1, path_prefix: /v1/, upstream: {{UP}}, strip_prefix: true }
  - { name: v1-admin, path_prefix: /v1/admin/, upstream: {{UP}},
      rate_limit: { key_by: ip, limit: 1, window: 1h } }
`, up))

	resp, err := http.Get(gw.URL + "/v1/users/7")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.HasPrefix(string(body), "path=/users/7 ") {
		t.Errorf("strip_prefix: upstream got %q", body)
	}
	if !strings.Contains(string(body), "xff=127.0.0.1") {
		t.Errorf("X-Forwarded-For not set: %q", body)
	}
	// /v1/admin/ is the longer prefix, so its limit applies.
	get(t, gw.URL+"/v1/admin/x")
	if code := get(t, gw.URL+"/v1/admin/x").StatusCode; code != http.StatusTooManyRequests {
		t.Errorf("longest-prefix route not limited: %d", code)
	}
	if code := get(t, gw.URL+"/v1/other").StatusCode; code != http.StatusOK {
		t.Errorf("unlimited route: %d", code)
	}
	if code := get(t, gw.URL+"/nope").StatusCode; code != http.StatusNotFound {
		t.Errorf("unknown path: %d want 404", code)
	}
}

const redisYAML = `
backend:
  type: redis
  redis: { addr: "%s", timeout: 200ms }
  failure_mode: %s
  breaker: { failure_threshold: 3, open_timeout: 1m }
routes:
  - name: api
    path_prefix: /
    upstream: {{UP}}
    rate_limit: { algorithm: %s, key_by: api_key, limit: %d, window: 1h }
`

// hammer sends total requests spread round-robin over the instances from
// many goroutines and returns how many got 200.
func hammer(t *testing.T, instances []*instance, total int) int64 {
	var ok atomic.Int64
	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				target := instances[i%len(instances)]
				req, _ := http.NewRequest(http.MethodGet, target.URL+"/", nil)
				req.Header.Set("X-API-Key", "shared-client")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Error(err)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					ok.Add(1)
				}
			}
		}()
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return ok.Load()
}

// TestMultiInstanceSharedRedis: three gateway "processes" (separate gateway,
// Redis client and breaker each) share one Redis. One client hits all three
// concurrently; together they must allow exactly the limit. The memory
// backend is run as a contrast: each instance allows the limit on its own.
func TestMultiInstanceSharedRedis(t *testing.T) {
	const limit, total, n = 50, 300, 3

	redisAddrs := map[string]string{"miniredis": miniredis.RunT(t).Addr()}
	if addr := os.Getenv("RATEGATE_REDIS_ADDR"); addr != "" {
		redisAddrs["real-redis"] = addr
	}

	for name, addr := range redisAddrs {
		for _, alg := range []string{"token_bucket", "fixed_window", "sliding_window_log", "sliding_window_counter"} {
			t.Run(name+"/"+alg, func(t *testing.T) {
				up := newUpstream(t)
				// A real Redis keeps keys between test runs: delete this
				// test's key first so every run starts from a clean slate.
				if name == "real-redis" {
					c := redis.NewClient(&redis.Options{Addr: addr})
					c.Del(context.Background(), "rg:api:"+alg+":{key:shared-client}")
					c.Close()
				}
				var instances []*instance
				for i := 0; i < n; i++ {
					cfg := testConfig(t, fmt.Sprintf(redisYAML, addr, "fail_closed", alg, limit), up)
					instances = append(instances, startGateway(t, cfg))
				}
				if got := hammer(t, instances, total); got != limit {
					t.Errorf("%d instances jointly allowed %d, want exactly %d", n, got, limit)
				}
			})
		}
	}

	t.Run("memory backend does NOT share (contrast)", func(t *testing.T) {
		up := newUpstream(t)
		var instances []*instance
		for i := 0; i < n; i++ {
			instances = append(instances, startGateway(t, testConfig(t, fmt.Sprintf(`
backend: { type: memory }
routes:
  - name: api
    path_prefix: /
    upstream: {{UP}}
    rate_limit: { algorithm: fixed_window, key_by: api_key, limit: %d, window: 1h }
`, limit), up)))
		}
		if got := hammer(t, instances, total); got != n*limit {
			t.Errorf("memory instances allowed %d, expected %d (each enforces alone)", got, n*limit)
		}
	})
}

// deadRedisAddr returns an address where nothing is listening.
func deadRedisAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestRedisDownFailureModes(t *testing.T) {
	cases := []struct {
		mode       string
		wantOK     int // out of 10 requests, limit 3
		wantStatus int // status of the non-OK responses
	}{
		{"fail_open", 10, 0},
		{"fail_closed", 0, http.StatusServiceUnavailable},
		{"local_fallback", 3, http.StatusTooManyRequests},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			up := newUpstream(t)
			gw := startGateway(t, testConfig(t, fmt.Sprintf(redisYAML, deadRedisAddr(t), tc.mode, "token_bucket", 3), up))
			ok := 0
			for i := 0; i < 10; i++ {
				resp := get(t, gw.URL+"/", "X-API-Key", "k")
				switch {
				case resp.StatusCode == http.StatusOK:
					ok++
					if resp.Header.Get("X-RateLimit-Degraded") != "true" {
						t.Error("degraded decision not flagged in headers")
					}
				case resp.StatusCode != tc.wantStatus:
					t.Errorf("status %d, want %d", resp.StatusCode, tc.wantStatus)
				}
			}
			if ok != tc.wantOK {
				t.Errorf("allowed %d of 10, want %d", ok, tc.wantOK)
			}
			// Breaker (threshold 3) must have opened, so only 3 Redis calls failed.
			if got := counterValue(t, gw, "rategate_redis_errors_total"); got != 3 {
				t.Errorf("redis errors = %v, want 3 (breaker should stop further calls)", got)
			}
			var health map[string]string
			resp, _ := http.Get(gw.URL + "/healthz")
			_ = json.NewDecoder(resp.Body).Decode(&health)
			resp.Body.Close()
			if health["status"] != "degraded" || health["redis_breaker"] != "open" {
				t.Errorf("healthz = %v, want degraded/open", health)
			}
		})
	}
}

// counterValue scrapes /metrics and returns the value of an unlabelled metric.
func counterValue(t *testing.T, gw *instance, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape(t, gw), "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, _ := strconv.ParseFloat(strings.Fields(line)[1], 64)
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

func scrape(t *testing.T, gw *instance) string {
	resp, err := http.Get(gw.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func TestMetricsEndpoint(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, basicYAML, up))
	for i := 0; i < 5; i++ {
		get(t, gw.URL+"/api/x", "X-API-Key", "m")
	}
	get(t, gw.URL+"/missing")
	out := scrape(t, gw)
	for _, want := range []string{
		`rategate_requests_total{decision="allowed",route="api"} 3`,
		`rategate_requests_total{decision="limited",route="api"} 2`,
		`rategate_requests_total{decision="no_route",route="none"} 1`,
		`rategate_limiter_duration_seconds_count{backend="memory",route="api"} 5`,
		`go_goroutines`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestHotReload(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, basicYAML, up))
	get(t, gw.URL+"/api/x", "X-API-Key", "r")

	// Lower the limit to 1: the next request with a fresh table is allowed
	// once (memory state resets on reload), then limited.
	if err := gw.gw.Reload(testConfig(t, strings.Replace(basicYAML, "limit: 3", "limit: 1", 1), up)); err != nil {
		t.Fatal(err)
	}
	if code := get(t, gw.URL+"/api/x", "X-API-Key", "r").StatusCode; code != http.StatusOK {
		t.Fatalf("status %d after reload", code)
	}
	resp := get(t, gw.URL+"/api/x", "X-API-Key", "r")
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("X-RateLimit-Limit") != "1" {
		t.Errorf("new limit not applied: %d limit=%s", resp.StatusCode, resp.Header.Get("X-RateLimit-Limit"))
	}

	// Changing the backend needs a restart.
	bad := testConfig(t, fmt.Sprintf(redisYAML, "127.0.0.1:1", "fail_open", "token_bucket", 1), up)
	if err := gw.gw.Reload(bad); err == nil {
		t.Error("backend change accepted on reload")
	}
}

// TestReloadUnderLoad swaps configs while requests are in flight; run with
// -race to check the atomic table swap.
func TestReloadUnderLoad(t *testing.T) {
	up := newUpstream(t)
	gw := startGateway(t, testConfig(t, basicYAML, up))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					get(t, gw.URL+"/api/x", "X-API-Key", strconv.Itoa(i))
				}
			}
		}(i)
	}
	for i := 0; i < 20; i++ {
		if err := gw.gw.Reload(testConfig(t, basicYAML, up)); err != nil {
			t.Error(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.9:5555"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.7")
	if got := clientIP(r, false); got != "10.0.0.9" {
		t.Errorf("untrusted: %s", got)
	}
	if got := clientIP(r, true); got != "203.0.113.7" {
		t.Errorf("trusted: %s, want right-most entry", got)
	}
}
