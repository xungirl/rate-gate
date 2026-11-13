// Package breaker implements a minimal circuit breaker.
//
// When Redis is down, every request would otherwise wait for a network
// timeout before failing. The breaker notices repeated failures and "opens":
// for a cool-down period callers skip Redis entirely and go straight to their
// fallback, which keeps latency low and gives Redis room to recover. After the
// cool-down one probe request is let through ("half-open"); if it succeeds the
// breaker closes again, if it fails the cool-down restarts.
//
//	closed --(threshold consecutive failures)--> open
//	open   --(openTimeout elapsed)-------------> half-open (one probe)
//	half-open --success--> closed     half-open --failure--> open
package breaker

import (
	"sync"
	"time"

	"github.com/xungirl/rate-gate/internal/clock"
)

// State is the breaker's current mode.
type State int

const (
	Closed State = iota // normal: calls go through
	Open                // failing: calls are skipped
	HalfOpen            // probing: one call is allowed to test recovery
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	}
	return "unknown"
}

// Breaker is safe for concurrent use.
type Breaker struct {
	threshold   int
	openTimeout time.Duration
	clock       clock.Clock
	onChange    func(State)

	mu            sync.Mutex
	state         State
	failures      int
	openedAt      time.Time
	probeInFlight bool
}

// New returns a closed breaker that opens after threshold consecutive
// failures and stays open for openTimeout. onChange (may be nil) is called,
// under the breaker's lock, whenever the state changes - used for metrics.
func New(threshold int, openTimeout time.Duration, clk clock.Clock, onChange func(State)) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Breaker{threshold: threshold, openTimeout: openTimeout, clock: clk, onChange: onChange}
}

// Allow reports whether the caller may try the protected dependency now.
// Every Allow() that returns true must be followed by Success() or Failure().
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Closed:
		return true
	case Open:
		if b.clock.Now().Sub(b.openedAt) < b.openTimeout {
			return false
		}
		b.setState(HalfOpen)
		b.probeInFlight = true
		return true
	default: // HalfOpen: only one probe at a time
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	}
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.probeInFlight = false
	if b.state != Closed {
		b.setState(Closed)
	}
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeInFlight = false
	switch b.state {
	case HalfOpen:
		b.trip()
	case Closed:
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	}
}

// Discard ends an Allow()ed call without judging the dependency, e.g. when
// the caller's own request was cancelled. It only frees the half-open probe.
func (b *Breaker) Discard() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeInFlight = false
}

// State returns the current state (for tests and health checks).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.openedAt = b.clock.Now()
	b.failures = 0
	b.setState(Open)
}

func (b *Breaker) setState(s State) {
	b.state = s
	if b.onChange != nil {
		b.onChange(s)
	}
}
