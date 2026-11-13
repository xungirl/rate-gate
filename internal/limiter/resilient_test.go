package limiter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xungirl/rate-gate/internal/breaker"
	"github.com/xungirl/rate-gate/internal/clock"
)

// downRedis returns a limiter whose Redis has been shut down, so every call
// fails with a connection error.
func downRedis(t *testing.T) *RedisLimiter {
	m := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = c.Close() })
	m.Close()
	l, err := NewRedis(c, TokenBucket, Rule{Limit: 3, Window: time.Minute}, RedisOptions{Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestFailureModes(t *testing.T) {
	ctx := context.Background()

	t.Run("fail_open allows everything", func(t *testing.T) {
		r, _ := NewResilient(downRedis(t), nil, breaker.New(5, time.Second, nil, nil), FailOpen, ResilientHooks{})
		for i := 0; i < 10; i++ {
			d, err := r.Allow(ctx, "k")
			if err != nil || !d.Allowed || !d.Degraded {
				t.Fatalf("got %+v, %v; want allowed+degraded", d, err)
			}
		}
	})

	t.Run("fail_closed rejects everything", func(t *testing.T) {
		r, _ := NewResilient(downRedis(t), nil, breaker.New(5, time.Second, nil, nil), FailClosed, ResilientHooks{})
		d, err := r.Allow(ctx, "k")
		if !errors.Is(err, ErrLimiterUnavailable) || d.Allowed {
			t.Fatalf("got %+v, %v; want ErrLimiterUnavailable", d, err)
		}
	})

	t.Run("local_fallback enforces the rule locally", func(t *testing.T) {
		local, _ := NewMemory(TokenBucket, Rule{Limit: 3, Window: time.Minute}, MemoryOptions{})
		defer local.Close()
		r, _ := NewResilient(downRedis(t), local, breaker.New(5, time.Second, nil, nil), LocalFallback, ResilientHooks{})
		allowed := 0
		for i := 0; i < 10; i++ {
			d, err := r.Allow(ctx, "k")
			if err != nil || !d.Degraded {
				t.Fatalf("got %+v, %v", d, err)
			}
			if d.Allowed {
				allowed++
			}
		}
		if allowed != 3 {
			t.Errorf("fallback allowed %d, want 3", allowed)
		}
	})
}

// TestBreakerStopsCallingRedis: after the threshold, requests must skip
// Redis entirely (no more errors, no more timeouts) until the cool-down ends.
func TestBreakerStopsCallingRedis(t *testing.T) {
	fc := clock.NewFake(base)
	br := breaker.New(3, 5*time.Second, fc, nil)
	var redisErrors atomic.Int64
	r, _ := NewResilient(downRedis(t), nil, br, FailOpen, ResilientHooks{
		PrimaryError: func(error) { redisErrors.Add(1) },
	})
	for i := 0; i < 100; i++ {
		_, _ = r.Allow(context.Background(), "k")
	}
	if got := redisErrors.Load(); got != 3 {
		t.Errorf("Redis was tried %d times, want 3 (then the breaker opens)", got)
	}
	if br.State() != breaker.Open {
		t.Fatalf("breaker state %s, want open", br.State())
	}
	fc.Advance(5 * time.Second)
	_, _ = r.Allow(context.Background(), "k") // the single half-open probe
	if got := redisErrors.Load(); got != 4 {
		t.Errorf("after cool-down Redis tried %d times total, want 4", got)
	}
}

// TestRecoversWhenRedisReturns: breaker closes and shared limiting resumes.
func TestRecoversWhenRedisReturns(t *testing.T) {
	m := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
	defer c.Close()
	primary, _ := NewRedis(c, FixedWindow, Rule{Limit: 100, Window: time.Minute}, RedisOptions{Timeout: 100 * time.Millisecond})
	fc := clock.NewFake(base)
	br := breaker.New(1, time.Second, fc, nil)
	r, _ := NewResilient(primary, nil, br, FailClosed, ResilientHooks{})

	m.SetError("simulated outage")
	if _, err := r.Allow(context.Background(), "k"); !errors.Is(err, ErrLimiterUnavailable) {
		t.Fatalf("err=%v during outage, want ErrLimiterUnavailable", err)
	}
	m.SetError("")
	fc.Advance(time.Second)
	d, err := r.Allow(context.Background(), "k")
	if err != nil || !d.Allowed || d.Degraded {
		t.Fatalf("after recovery got %+v, %v", d, err)
	}
	if br.State() != breaker.Closed {
		t.Errorf("breaker %s, want closed", br.State())
	}
}

func TestCancelledRequestDoesNotTripBreaker(t *testing.T) {
	br := breaker.New(1, time.Minute, nil, nil)
	r, _ := NewResilient(downRedis(t), nil, br, FailOpen, ResilientHooks{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = r.Allow(ctx, "k")
	if br.State() != breaker.Closed {
		t.Errorf("client cancellation tripped the breaker")
	}
}
