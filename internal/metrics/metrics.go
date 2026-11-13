// Package metrics defines the gateway's Prometheus metrics.
//
// A private Registry (instead of the global default) keeps tests isolated:
// every test can create its own Metrics without "duplicate metric" panics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Decision label values for RequestsTotal.
const (
	DecisionAllowed     = "allowed"     // passed the limiter (or route is unlimited)
	DecisionLimited     = "limited"     // 429
	DecisionUnavailable = "unavailable" // 503: limiter down and fail_closed
	DecisionNoRoute     = "no_route"    // 404
)

// Metrics holds every metric the gateway exports.
type Metrics struct {
	Registry *prometheus.Registry

	RequestsTotal  *prometheus.CounterVec   // labels: route, decision
	LimiterLatency *prometheus.HistogramVec // labels: route, backend
	RedisErrors    prometheus.Counter
	Degraded       *prometheus.CounterVec // labels: mode
	BreakerState   prometheus.Gauge       // 0 closed, 1 open, 2 half-open
	UpstreamErrors *prometheus.CounterVec // labels: route
	ConfigReloads  *prometheus.CounterVec // labels: result
}

// New creates and registers all metrics.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_requests_total",
			Help: "Requests handled by the gateway, by route and rate-limit decision.",
		}, []string{"route", "decision"}),
		LimiterLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "rategate_limiter_duration_seconds",
			Help: "Time spent in the rate limiter per request.",
			// 10us .. ~160ms: in-memory decisions land in the first buckets,
			// Redis round trips in the middle ones.
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 15),
		}, []string{"route", "backend"}),
		RedisErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rategate_redis_errors_total",
			Help: "Redis limiter calls that failed (timeouts, connection errors).",
		}),
		Degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_degraded_decisions_total",
			Help: "Decisions made by the failure policy because Redis was unavailable.",
		}, []string{"mode"}),
		BreakerState: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "rategate_breaker_state",
			Help: "Redis circuit breaker state: 0 closed, 1 open, 2 half-open.",
		}),
		UpstreamErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_upstream_errors_total",
			Help: "Requests that failed to reach the upstream (502).",
		}, []string{"route"}),
		ConfigReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rategate_config_reloads_total",
			Help: "Hot config reload attempts by result.",
		}, []string{"result"}),
	}
	m.Registry.MustRegister(
		m.RequestsTotal, m.LimiterLatency, m.RedisErrors, m.Degraded,
		m.BreakerState, m.UpstreamErrors, m.ConfigReloads,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
