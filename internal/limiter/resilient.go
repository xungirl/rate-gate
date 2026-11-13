package limiter

import (
	"context"
	"errors"
	"fmt"

	"github.com/xungirl/rate-gate/internal/breaker"
)

// FailureMode decides what happens when the shared (Redis) limiter is
// unavailable - Redis down, timing out, or the circuit breaker open.
type FailureMode string

const (
	// FailOpen lets every request through. Availability over protection:
	// right for limits that protect fairness rather than survival.
	FailOpen FailureMode = "fail_open"
	// FailClosed rejects every request (the gateway answers 503). Protection
	// over availability: right when the upstream would fall over unprotected
	// or the limit is contractual (e.g. a paid quota).
	FailClosed FailureMode = "fail_closed"
	// LocalFallback switches to an in-process limiter with the same rule.
	// Each instance then enforces the limit on its own, so N instances allow
	// up to N x the limit - degraded, but still bounded.
	LocalFallback FailureMode = "local_fallback"
)

// ParseFailureMode converts a config string into a FailureMode.
func ParseFailureMode(s string) (FailureMode, error) {
	switch m := FailureMode(s); m {
	case FailOpen, FailClosed, LocalFallback:
		return m, nil
	}
	return "", fmt.Errorf("unknown failure mode %q (want fail_open, fail_closed or local_fallback)", s)
}

// ErrLimiterUnavailable is returned in FailClosed mode when the primary
// limiter cannot answer.
var ErrLimiterUnavailable = errors.New("rate limiter unavailable")

// ResilientHooks lets the caller observe failures (used for metrics/logging).
// Any field may be nil.
type ResilientHooks struct {
	PrimaryError func(err error)
	Degraded     func(mode FailureMode)
}

// Resilient wraps a primary (shared, remote) limiter with a circuit breaker
// and a failure policy. It is itself a Limiter, so the gateway does not know
// or care that Redis might be down - the decorator pattern.
type Resilient struct {
	primary  Limiter
	fallback Limiter // used only in LocalFallback mode
	breaker  *breaker.Breaker
	mode     FailureMode
	hooks    ResilientHooks
}

// NewResilient builds the wrapper. fallback is required for LocalFallback
// and ignored otherwise. One breaker may be shared by many Resilient
// limiters, since they all depend on the same Redis.
func NewResilient(primary, fallback Limiter, br *breaker.Breaker, mode FailureMode, hooks ResilientHooks) (*Resilient, error) {
	if mode == LocalFallback && fallback == nil {
		return nil, errors.New("local_fallback mode needs a fallback limiter")
	}
	return &Resilient{primary: primary, fallback: fallback, breaker: br, mode: mode, hooks: hooks}, nil
}

// Allow asks the primary limiter; on failure, applies the failure mode.
func (r *Resilient) Allow(ctx context.Context, key string) (Decision, error) {
	if r.breaker.Allow() {
		d, err := r.primary.Allow(ctx, key)
		switch {
		case err == nil:
			r.breaker.Success()
			return d, nil
		case ctx.Err() != nil:
			// The *client* went away (request cancelled). That says nothing
			// about Redis health, so don't count it against the breaker.
			r.breaker.Discard()
			return Decision{}, ctx.Err()
		default:
			r.breaker.Failure()
			if r.hooks.PrimaryError != nil {
				r.hooks.PrimaryError(err)
			}
		}
	}
	return r.degraded(ctx, key)
}

func (r *Resilient) degraded(ctx context.Context, key string) (Decision, error) {
	if r.hooks.Degraded != nil {
		r.hooks.Degraded(r.mode)
	}
	switch r.mode {
	case FailOpen:
		return Decision{Allowed: true, Degraded: true}, nil
	case LocalFallback:
		d, err := r.fallback.Allow(ctx, key)
		d.Degraded = true
		return d, err
	default: // FailClosed
		return Decision{Degraded: true}, ErrLimiterUnavailable
	}
}
