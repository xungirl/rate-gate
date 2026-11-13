// Package limiter contains the rate-limiting algorithms.
//
// Every algorithm, whether it keeps state in process memory or in Redis,
// implements the same small Limiter interface. The HTTP gateway only ever
// talks to that interface, so algorithms and backends can be swapped from
// config without touching the request path.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Algorithm names a rate-limiting algorithm. The string values are what users
// write in the YAML config.
type Algorithm string

const (
	TokenBucket          Algorithm = "token_bucket"
	FixedWindow          Algorithm = "fixed_window"
	SlidingWindowLog     Algorithm = "sliding_window_log"
	SlidingWindowCounter Algorithm = "sliding_window_counter"
)

// Algorithms lists every supported algorithm (handy for tests and validation).
var Algorithms = []Algorithm{TokenBucket, FixedWindow, SlidingWindowLog, SlidingWindowCounter}

// Rule says how many requests a single key may make.
//
//	Limit  requests per Window, e.g. 100 per 1s.
//	Burst  token bucket only: bucket capacity. 0 means "same as Limit".
//
// For the token bucket, the refill rate is Limit/Window tokens per second.
type Rule struct {
	Limit  int64
	Window time.Duration
	Burst  int64
}

// Validate reports whether the rule is usable.
func (r Rule) Validate() error {
	if r.Limit <= 0 {
		return fmt.Errorf("limit must be > 0, got %d", r.Limit)
	}
	if r.Window <= 0 {
		return fmt.Errorf("window must be > 0, got %s", r.Window)
	}
	if r.Burst < 0 {
		return fmt.Errorf("burst must be >= 0, got %d", r.Burst)
	}
	return nil
}

// Capacity is the token bucket size: Burst, or Limit when Burst is 0.
// Window-based algorithms ignore Burst.
func (r Rule) Capacity() int64 {
	if r.Burst > 0 {
		return r.Burst
	}
	return r.Limit
}

// Decision is the result of asking a limiter about one request. It carries
// everything the gateway needs to fill in the X-RateLimit-* headers.
type Decision struct {
	Allowed bool
	// Limit is the advertised limit (X-RateLimit-Limit).
	Limit int64
	// Remaining is how many more requests would be allowed right now.
	Remaining int64
	// ResetAfter is how long until the limiter is back to its full allowance.
	ResetAfter time.Duration
	// RetryAfter is only meaningful when Allowed is false: the earliest time
	// a retry could succeed.
	RetryAfter time.Duration
	// Degraded is true when the shared limiter was unavailable and the
	// decision came from the failure policy (fail-open or local fallback).
	Degraded bool
}

// Limiter decides whether the request identified by key may proceed.
//
// Implementations must be safe for concurrent use: the gateway calls Allow
// from many goroutines at once, often with the same key.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}

// ErrUnknownAlgorithm is returned by constructors given a bad Algorithm.
var ErrUnknownAlgorithm = errors.New("unknown rate-limit algorithm")

// ParseAlgorithm converts a config string into an Algorithm.
func ParseAlgorithm(s string) (Algorithm, error) {
	for _, a := range Algorithms {
		if string(a) == s {
			return a, nil
		}
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownAlgorithm, s)
}
