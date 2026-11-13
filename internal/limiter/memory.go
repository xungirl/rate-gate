package limiter

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xungirl/rate-gate/internal/clock"
)

// LimiterCloser is a Limiter that owns background resources (for in-memory
// limiters: the expiry-cleanup goroutine) and must be closed when discarded.
type LimiterCloser interface {
	Limiter
	Close() error
}

// MemoryOptions configures in-memory limiters.
type MemoryOptions struct {
	// Clock defaults to the real wall clock. Tests pass a *clock.Fake.
	Clock clock.Clock
	// CleanupInterval is how often expired keys are swept. 0 disables the
	// background sweeper (tests call sweep directly).
	CleanupInterval time.Duration
}

// NewMemory builds a single-process limiter for the given algorithm.
//
// State lives in this process only, so two gateway instances each enforce the
// limit independently (N instances => up to N x the limit). Use NewRedis to
// share limits across instances.
func NewMemory(alg Algorithm, rule Rule, opts MemoryOptions) (LimiterCloser, error) {
	if err := rule.Validate(); err != nil {
		return nil, err
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real{}
	}
	switch alg {
	case TokenBucket:
		return &memTokenBucket{rule: rule, clock: opts.Clock,
			store: newShardedStore[tokenBucketState](opts.Clock, opts.CleanupInterval)}, nil
	case FixedWindow:
		return &memFixedWindow{rule: rule, clock: opts.Clock,
			store: newShardedStore[fixedWindowState](opts.Clock, opts.CleanupInterval)}, nil
	case SlidingWindowLog:
		return &memSlidingLog{rule: rule, clock: opts.Clock,
			store: newShardedStore[slidingLogState](opts.Clock, opts.CleanupInterval)}, nil
	case SlidingWindowCounter:
		return &memSlidingCounter{rule: rule, clock: opts.Clock,
			store: newShardedStore[slidingCounterState](opts.Clock, opts.CleanupInterval)}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownAlgorithm, alg)
	}
}

// ---------------------------------------------------------------------------
// Token bucket
//
// Each key has a bucket holding up to Capacity tokens. Tokens refill
// continuously at Limit/Window per second; each request spends one token.
// A full bucket lets a burst of Capacity requests through at once, after which
// requests pass at the steady refill rate.
// ---------------------------------------------------------------------------

type tokenBucketState struct {
	tokens  float64
	last    time.Time
	started bool
}

type memTokenBucket struct {
	rule  Rule
	clock clock.Clock
	store *shardedStore[tokenBucketState]
}

func (l *memTokenBucket) Allow(_ context.Context, key string) (Decision, error) {
	now := l.clock.Now()
	capacity := float64(l.rule.Capacity())
	ratePerNs := float64(l.rule.Limit) / float64(l.rule.Window.Nanoseconds())
	var d Decision

	l.store.update(key, now, func(s *tokenBucketState) time.Duration {
		if !s.started { // a new key starts with a full bucket
			s.tokens, s.last, s.started = capacity, now, true
		}
		// Refill for the time elapsed since the last request (never negative).
		if elapsed := now.Sub(s.last); elapsed > 0 {
			s.tokens = math.Min(capacity, s.tokens+float64(elapsed)*ratePerNs)
			s.last = now
		}

		if s.tokens >= 1 {
			s.tokens--
			d.Allowed = true
		} else {
			d.RetryAfter = time.Duration(math.Ceil((1 - s.tokens) / ratePerNs))
		}
		d.Limit = l.rule.Capacity()
		d.Remaining = int64(math.Floor(s.tokens))
		d.ResetAfter = time.Duration(math.Ceil((capacity - s.tokens) / ratePerNs))
		// Once the bucket is full again the state is equivalent to a brand-new
		// key, so it can be forgotten.
		return d.ResetAfter
	})
	return d, nil
}

func (l *memTokenBucket) Close() error { l.store.close(); return nil }

// ---------------------------------------------------------------------------
// Fixed window
//
// Time is cut into windows [0,W), [W,2W), ... and each key gets Limit requests
// per window. Cheap (one counter) but a client can send Limit requests at the
// very end of one window and Limit more at the start of the next: 2x Limit in
// a very short span. Included as a baseline to show that weakness.
// ---------------------------------------------------------------------------

type fixedWindowState struct {
	window int64 // index of the current window: unixNanos / W
	count  int64
}

type memFixedWindow struct {
	rule  Rule
	clock clock.Clock
	store *shardedStore[fixedWindowState]
}

func (l *memFixedWindow) Allow(_ context.Context, key string) (Decision, error) {
	now := l.clock.Now()
	w := l.rule.Window.Nanoseconds()
	idx := now.UnixNano() / w
	resetAfter := time.Duration((idx+1)*w - now.UnixNano())
	var d Decision

	l.store.update(key, now, func(s *fixedWindowState) time.Duration {
		if s.window != idx { // a new window started: forget the old count
			s.window, s.count = idx, 0
		}
		if s.count < l.rule.Limit {
			s.count++
			d.Allowed = true
		} else {
			d.RetryAfter = resetAfter
		}
		d.Limit = l.rule.Limit
		d.Remaining = l.rule.Limit - s.count
		d.ResetAfter = resetAfter
		return resetAfter
	})
	return d, nil
}

func (l *memFixedWindow) Close() error { l.store.close(); return nil }

// ---------------------------------------------------------------------------
// Sliding window log
//
// Remember the timestamp of every allowed request in the last W. A request is
// allowed if fewer than Limit timestamps are in (now-W, now]. Exact, but
// memory grows with Limit (one timestamp per allowed request per key).
// ---------------------------------------------------------------------------

type slidingLogState struct {
	times []int64 // unix nanos of allowed requests, oldest first
}

type memSlidingLog struct {
	rule  Rule
	clock clock.Clock
	store *shardedStore[slidingLogState]
}

func (l *memSlidingLog) Allow(_ context.Context, key string) (Decision, error) {
	now := l.clock.Now().UnixNano()
	w := l.rule.Window.Nanoseconds()
	var d Decision

	l.store.update(key, time.Unix(0, now), func(s *slidingLogState) time.Duration {
		// Drop timestamps that have slid out of the window.
		cutoff := now - w
		i := 0
		for i < len(s.times) && s.times[i] <= cutoff {
			i++
		}
		n := copy(s.times, s.times[i:]) // shift left in place, no new allocation
		s.times = s.times[:n]

		if int64(len(s.times)) < l.rule.Limit {
			s.times = append(s.times, now)
			d.Allowed = true
		} else {
			// The oldest entry leaving the window frees one slot.
			d.RetryAfter = time.Duration(s.times[0] + w - now)
		}
		d.Limit = l.rule.Limit
		d.Remaining = l.rule.Limit - int64(len(s.times))
		d.ResetAfter = time.Duration(s.times[len(s.times)-1] + w - now)
		return d.ResetAfter
	})
	return d, nil
}

func (l *memSlidingLog) Close() error { l.store.close(); return nil }

// ---------------------------------------------------------------------------
// Sliding window counter
//
// Keep only two counters per key: this fixed window and the previous one.
// Estimate the count over the last W by weighting the previous window by how
// much of it still overlaps the sliding window:
//
//	estimate = prev * (W - elapsed) / W + curr
//
// O(1) memory like fixed window, and it removes the boundary burst. It is an
// approximation that assumes the previous window's requests were evenly spread.
// ---------------------------------------------------------------------------

type slidingCounterState struct {
	window int64 // index of the current window
	curr   int64
	prev   int64
}

type memSlidingCounter struct {
	rule  Rule
	clock clock.Clock
	store *shardedStore[slidingCounterState]
}

func (l *memSlidingCounter) Allow(_ context.Context, key string) (Decision, error) {
	now := l.clock.Now()
	w := l.rule.Window.Nanoseconds()
	idx := now.UnixNano() / w
	elapsed := now.UnixNano() - idx*w
	var d Decision

	l.store.update(key, now, func(s *slidingCounterState) time.Duration {
		switch {
		case s.window == idx: // same window, nothing to roll
		case s.window == idx-1: // moved into the next window
			s.prev, s.curr = s.curr, 0
		default: // idle for 2+ windows: both counters are stale
			s.prev, s.curr = 0, 0
		}
		s.window = idx

		d = slidingCounterDecide(l.rule.Limit, w, elapsed, s.prev, s.curr)
		if d.Allowed {
			s.curr++
		}
		return time.Duration(2*w - elapsed) // after that both counters are stale
	})
	return d, nil
}

func (l *memSlidingCounter) Close() error { l.store.close(); return nil }

// slidingCounterDecide holds the sliding-window-counter math so the decision
// and its headers are computed identically everywhere. It does not mutate
// state; the caller increments curr when Allowed is true.
func slidingCounterDecide(limit, w, elapsed, prev, curr int64) Decision {
	weight := float64(w-elapsed) / float64(w)
	estimate := float64(prev)*weight + float64(curr)
	d := Decision{Limit: limit}

	if estimate+1 <= float64(limit) {
		d.Allowed = true
		estimate++
		curr++
	} else {
		d.RetryAfter = slidingCounterRetry(limit, w, elapsed, prev, curr)
	}
	d.Remaining = int64(math.Max(0, math.Floor(float64(limit)-estimate)))
	if curr > 0 {
		// curr becomes "prev" next window and then decays over one more window.
		d.ResetAfter = time.Duration(2*w - elapsed)
	} else {
		d.ResetAfter = time.Duration(w - elapsed)
	}
	return d
}

// slidingCounterRetry solves the estimate formula for the first moment one
// more request would fit.
func slidingCounterRetry(limit, w, elapsed, prev, curr int64) time.Duration {
	W, e, L := float64(w), float64(elapsed), float64(limit)
	if curr+1 > limit {
		// Can't fit in this window at all. Wait for the next one, where the
		// current count becomes "prev" and must decay enough:
		// curr*(W-e')/W + 1 <= L  =>  e' >= W*(1-(L-1)/curr)
		wait := (W - e) + math.Max(0, W*(1-(L-1)/float64(curr)))
		return time.Duration(math.Ceil(wait))
	}
	// Fits once the previous window's weight decays enough:
	// prev*(W-e-t)/W + curr + 1 <= L  =>  t >= (W-e) - (L-curr-1)*W/prev
	wait := (W - e) - (L-float64(curr)-1)*W/float64(prev)
	return time.Duration(math.Max(1, math.Ceil(wait)))
}
