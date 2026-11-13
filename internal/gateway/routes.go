package gateway

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xungirl/rate-gate/internal/config"
	"github.com/xungirl/rate-gate/internal/limiter"
	"github.com/xungirl/rate-gate/internal/metrics"
)

// routeTable is an immutable snapshot of all routes built from one config.
// Hot reload builds a new table and swaps a pointer; requests already running
// keep using the old one, so no locks are needed on the request path.
type routeTable struct {
	routes  []*route // sorted longest prefix first
	closers []func() error
}

type route struct {
	name   string
	prefix string
	proxy  *httputil.ReverseProxy
	limit  *routeLimit // nil => route is not rate limited

	// Metric handles resolved once at build time, not per request.
	allowed, limited, unavailable prometheus.Counter
}

type routeLimit struct {
	keyFunc   keyFunc
	backend   string
	def       limiter.Limiter            // default rule
	overrides map[string]limiter.Limiter // per-key rules, by raw id
	latency   prometheus.Observer
}

// limiterFor returns the override limiter for id if one exists.
func (rl *routeLimit) limiterFor(id string) limiter.Limiter {
	if l, ok := rl.overrides[id]; ok {
		return l
	}
	return rl.def
}

// match finds the route with the longest matching path prefix.
func (t *routeTable) match(path string) *route {
	for _, r := range t.routes {
		if strings.HasPrefix(path, r.prefix) {
			return r
		}
	}
	return nil
}

func (t *routeTable) close() {
	for _, c := range t.closers {
		_ = c()
	}
}

// buildTable turns a validated config into routes with proxies and limiters.
func buildTable(cfg *config.Config, deps Deps, transport http.RoundTripper) (*routeTable, error) {
	t := &routeTable{}
	for _, rc := range cfg.Routes {
		target, err := url.Parse(rc.Upstream)
		if err != nil {
			return nil, err
		}
		r := &route{
			name:        rc.Name,
			prefix:      rc.PathPrefix,
			proxy:       newProxy(rc, target, transport, deps.Metrics),
			allowed:     deps.Metrics.RequestsTotal.WithLabelValues(rc.Name, metrics.DecisionAllowed),
			limited:     deps.Metrics.RequestsTotal.WithLabelValues(rc.Name, metrics.DecisionLimited),
			unavailable: deps.Metrics.RequestsTotal.WithLabelValues(rc.Name, metrics.DecisionUnavailable),
		}
		if rl := rc.RateLimit; rl != nil {
			if r.limit, err = buildRouteLimit(cfg, rc.Name, rl, deps, t); err != nil {
				t.close()
				return nil, fmt.Errorf("route %s: %w", rc.Name, err)
			}
		}
		t.routes = append(t.routes, r)
	}
	sort.SliceStable(t.routes, func(i, j int) bool {
		return len(t.routes[i].prefix) > len(t.routes[j].prefix)
	})
	return t, nil
}

func buildRouteLimit(cfg *config.Config, routeName string, rl *config.RateLimit, deps Deps, t *routeTable) (*routeLimit, error) {
	alg, _ := limiter.ParseAlgorithm(rl.Algorithm) // validated already
	out := &routeLimit{
		keyFunc:   newKeyFunc(rl.KeyBy, cfg.TrustForwardedFor),
		backend:   cfg.Backend.Type,
		overrides: map[string]limiter.Limiter{},
		latency:   deps.Metrics.LimiterLatency.WithLabelValues(routeName, cfg.Backend.Type),
	}
	// Redis key prefix includes the algorithm so changing algorithms on
	// reload never reinterprets another algorithm's stored state.
	prefix := fmt.Sprintf("rg:%s:%s:", routeName, alg)

	var err error
	if out.def, err = newLimiter(cfg, deps, alg, rl.Rule(), prefix, t); err != nil {
		return nil, err
	}
	for _, o := range rl.Overrides {
		l, err := newLimiter(cfg, deps, alg, o.Rule(*rl), prefix+"ovr:", t)
		if err != nil {
			return nil, err
		}
		out.overrides[o.Key] = l
	}
	return out, nil
}

// newLimiter builds one limiter for the configured backend. For Redis it
// wraps the shared limiter with the breaker + failure policy.
func newLimiter(cfg *config.Config, deps Deps, alg limiter.Algorithm, rule limiter.Rule, prefix string, t *routeTable) (limiter.Limiter, error) {
	newMem := func() (limiter.Limiter, error) {
		l, err := limiter.NewMemory(alg, rule, limiter.MemoryOptions{
			Clock:           deps.Clock,
			CleanupInterval: cfg.Backend.CleanupInterval,
		})
		if err != nil {
			return nil, err
		}
		t.closers = append(t.closers, l.Close)
		return l, nil
	}

	if cfg.Backend.Type == "memory" {
		return newMem()
	}

	primary, err := limiter.NewRedis(deps.Redis, alg, rule, limiter.RedisOptions{
		Prefix:  prefix,
		Timeout: cfg.Backend.Redis.Timeout,
	})
	if err != nil {
		return nil, err
	}
	mode, _ := limiter.ParseFailureMode(cfg.Backend.FailureMode)
	var fallback limiter.Limiter
	if mode == limiter.LocalFallback {
		if fallback, err = newMem(); err != nil {
			return nil, err
		}
	}
	return limiter.NewResilient(primary, fallback, deps.Breaker, mode, limiter.ResilientHooks{
		PrimaryError: func(err error) {
			deps.Metrics.RedisErrors.Inc()
			deps.Logger.Debug("redis limiter error", "err", err)
		},
		Degraded: func(m limiter.FailureMode) {
			deps.Metrics.Degraded.WithLabelValues(string(m)).Inc()
		},
	})
}

// bufferPool reuses the 32 KiB buffers ReverseProxy copies response bodies
// through. Without it, every proxied request allocates a fresh buffer.
var bufferPool = &syncBufferPool{pool: sync.Pool{New: func() any {
	b := make([]byte, 32*1024)
	return &b
}}}

type syncBufferPool struct{ pool sync.Pool }

func (p *syncBufferPool) Get() []byte  { return *p.pool.Get().(*[]byte) }
func (p *syncBufferPool) Put(b []byte) { p.pool.Put(&b) }

// newProxy builds the reverse proxy for one route.
func newProxy(rc config.Route, target *url.URL, transport http.RoundTripper, m *metrics.Metrics) *httputil.ReverseProxy {
	upstreamErrors := m.UpstreamErrors.WithLabelValues(rc.Name)
	return &httputil.ReverseProxy{
		// Rewrite (Go 1.20+) receives the inbound request and an outbound
		// copy to modify. Hop-by-hop and X-Forwarded-* headers from the
		// client have already been removed from pr.Out.
		Rewrite: func(pr *httputil.ProxyRequest) {
			if rc.StripPrefix {
				p := strings.TrimPrefix(pr.In.URL.Path, strings.TrimSuffix(rc.PathPrefix, "/"))
				if !strings.HasPrefix(p, "/") {
					p = "/" + p
				}
				pr.Out.URL.Path, pr.Out.URL.RawPath = p, ""
			}
			pr.SetURL(target) // scheme/host of upstream, path joined
			pr.SetXForwarded()
		},
		Transport:  transport,
		BufferPool: bufferPool,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			upstreamErrors.Inc()
			writeJSONError(w, http.StatusBadGateway, "upstream unavailable")
		},
	}
}
