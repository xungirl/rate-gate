// Package gateway is the HTTP layer: it matches a route, asks that route's
// limiter about the request, writes rate-limit headers, and either rejects
// the request (429/503) or forwards it to the upstream.
package gateway

import (
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xungirl/rate-gate/internal/breaker"
	"github.com/xungirl/rate-gate/internal/clock"
	"github.com/xungirl/rate-gate/internal/config"
	"github.com/xungirl/rate-gate/internal/limiter"
	"github.com/xungirl/rate-gate/internal/metrics"
)

// Deps are long-lived shared dependencies created once in main and reused
// across config reloads.
type Deps struct {
	Logger  *slog.Logger
	Metrics *metrics.Metrics
	Clock   clock.Clock      // for in-memory limiters
	Redis   redis.Scripter   // nil when backend.type is memory
	Breaker *breaker.Breaker // shared by every Redis limiter
}

// Gateway is an http.Handler.
type Gateway struct {
	deps      Deps
	transport *http.Transport

	table atomic.Pointer[routeTable] // swapped on hot reload

	access atomic.Bool // per-request access logging on/off

	mu      sync.Mutex // serializes Reload
	backend config.Backend
}

// New builds a gateway from a validated config.
func New(cfg *config.Config, deps Deps) (*Gateway, error) {
	if deps.Clock == nil {
		deps.Clock = clock.Real{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if cfg.Backend.Type == "redis" && (deps.Redis == nil || deps.Breaker == nil) {
		return nil, errors.New("redis backend needs Deps.Redis and Deps.Breaker")
	}
	g := &Gateway{
		deps:      deps,
		transport: newTransport(),
		backend:   cfg.Backend,
	}
	g.access.Store(cfg.Log.Access)
	t, err := buildTable(cfg, deps, g.transport)
	if err != nil {
		return nil, err
	}
	g.table.Store(t)
	return g, nil
}

// newTransport returns an upstream HTTP transport tuned for a proxy. The
// default keeps only 2 idle connections per host, which under load forces a
// new TCP connection for most requests and exhausts ephemeral ports.
func newTransport() *http.Transport {
	return &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
	}
}

// Reload atomically swaps in routes from a new config. Backend settings
// (memory vs redis, Redis address) need a restart and are rejected.
// In-memory limiter state starts fresh after a reload; Redis state survives.
func (g *Gateway) Reload(cfg *config.Config) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !reflect.DeepEqual(cfg.Backend, g.backend) {
		g.deps.Metrics.ConfigReloads.WithLabelValues("rejected").Inc()
		return errors.New("backend settings changed; restart required")
	}
	t, err := buildTable(cfg, g.deps, g.transport)
	if err != nil {
		g.deps.Metrics.ConfigReloads.WithLabelValues("error").Inc()
		return err
	}
	old := g.table.Swap(t)
	g.access.Store(cfg.Log.Access)
	g.deps.Metrics.ConfigReloads.WithLabelValues("ok").Inc()
	// Closing only stops old cleanup goroutines; in-flight requests that
	// still hold the old limiters keep working.
	old.close()
	return nil
}

// HealthHandler serves /healthz. It reports 200 while the process can serve
// traffic; a degraded Redis is reported in the body but is not a failure,
// because the failure policy keeps requests flowing (or deliberately not).
func (g *Gateway) HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := map[string]string{"status": "ok", "backend": g.backend.Type}
		if g.deps.Breaker != nil {
			state := g.deps.Breaker.State()
			body["redis_breaker"] = state.String()
			if state != breaker.Closed {
				body["status"] = "degraded"
			}
		}
		writeJSON(w, http.StatusOK, body)
	})
}

// Close releases background resources.
func (g *Gateway) Close() {
	g.table.Load().close()
	g.transport.CloseIdleConnections()
}

// ServeHTTP is the request path.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rt := g.table.Load().match(r.URL.Path)
	if rt == nil {
		g.deps.Metrics.RequestsTotal.WithLabelValues("none", metrics.DecisionNoRoute).Inc()
		writeJSONError(w, http.StatusNotFound, "no route")
		return
	}

	var rec *statusRecorder
	if g.access.Load() {
		rec = &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		w = rec
		defer func() {
			g.deps.Logger.Info("request", "method", r.Method, "path", r.URL.Path,
				"route", rt.name, "status", rec.status, "duration_ms",
				float64(time.Since(start).Microseconds())/1000)
		}()
	}

	if rt.limit != nil && !g.checkLimit(w, r, rt) {
		return
	}
	rt.allowed.Inc()
	rt.proxy.ServeHTTP(w, r)
}

// checkLimit asks the route's limiter about r, writes rate-limit headers and,
// when the request must not proceed, the error response. It returns true if
// the request may be forwarded.
func (g *Gateway) checkLimit(w http.ResponseWriter, r *http.Request, rt *route) bool {
	key, id := rt.limit.keyFunc(r)
	lim := rt.limit.limiterFor(id)

	t0 := time.Now()
	d, err := lim.Allow(r.Context(), key)
	rt.limit.latency.Observe(time.Since(t0).Seconds())

	switch {
	case errors.Is(err, limiter.ErrLimiterUnavailable):
		rt.unavailable.Inc()
		w.Header().Set("Retry-After", "1")
		writeJSONError(w, http.StatusServiceUnavailable, "rate limiter unavailable")
		return false
	case err != nil:
		// Client went away mid-request; nobody is listening for a response.
		return false
	}

	setRateLimitHeaders(w.Header(), d)
	if !d.Allowed {
		rt.limited.Inc()
		retry := ceilSeconds(d.RetryAfter)
		w.Header().Set("Retry-After", strconv.FormatInt(retry, 10))
		g.deps.Logger.Debug("rate limited", "route", rt.name, "key", key)
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":               "rate limit exceeded",
			"retry_after_seconds": retry,
		})
		return false
	}
	return true
}

// setRateLimitHeaders writes the conventional X-RateLimit-* headers.
// Reset is delta-seconds until the full allowance is back (like the IETF
// RateLimit-Reset draft), not a Unix timestamp, so it is immune to clock
// differences between client and server.
func setRateLimitHeaders(h http.Header, d limiter.Decision) {
	if d.Degraded {
		h.Set("X-RateLimit-Degraded", "true")
	}
	if d.Limit == 0 { // fail-open decision: no real limit information
		return
	}
	h.Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	h.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
}

// ceilSeconds rounds up so a client that waits the advertised time is never
// early. Retry-After must be a whole number of seconds; minimum 1 when > 0.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// statusRecorder remembers the status code for access logs.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController (used by ReverseProxy to flush)
// reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
