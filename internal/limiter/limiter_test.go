package limiter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xungirl/rate-gate/internal/clock"
)

// base is aligned to a whole second so window boundaries are predictable.
var base = time.Unix(1_700_000_000, 0)

// backend lets the same test body run against every storage backend.
// newLimiter builds a limiter; advance moves that backend's notion of "now".
type backend struct {
	name       string
	newLimiter func(t *testing.T, alg Algorithm, rule Rule) Limiter
	advance    func(d time.Duration)
}

// memoryBackend uses a fake clock, so time only moves when the test says so.
func memoryBackend(t *testing.T) backend {
	fc := clock.NewFake(base)
	return backend{
		name: "memory",
		newLimiter: func(t *testing.T, alg Algorithm, rule Rule) Limiter {
			l, err := NewMemory(alg, rule, MemoryOptions{Clock: fc})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			return l
		},
		advance: fc.Advance,
	}
}

// miniredisBackend runs the real Lua scripts in an in-process fake Redis whose
// TIME command we control (SetTime) and whose TTLs we can fast-forward.
func miniredisBackend(t *testing.T) backend {
	m := miniredis.RunT(t)
	now := base
	m.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	n := 0
	return backend{
		name: "miniredis",
		newLimiter: func(t *testing.T, alg Algorithm, rule Rule) Limiter {
			n++ // fresh key namespace per limiter
			l, err := NewRedis(client, alg, rule, RedisOptions{Prefix: fmt.Sprintf("t%d:", n), Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			return l
		},
		advance: func(d time.Duration) {
			now = now.Add(d)
			m.SetTime(now)
			m.FastForward(d)
		},
	}
}

// realRedisClient connects to a real Redis when RATEGATE_REDIS_ADDR is set,
// e.g. RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test ./... ; otherwise skips.
func realRedisClient(t testing.TB) *redis.Client {
	addr := os.Getenv("RATEGATE_REDIS_ADDR")
	if addr == "" {
		t.Skip("RATEGATE_REDIS_ADDR not set; skipping real-Redis test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 256})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("cannot reach Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// uniquePrefix keeps real-Redis test runs from colliding with each other.
func uniquePrefix(t testing.TB) string {
	return fmt.Sprintf("test:%s:%d:", t.Name(), time.Now().UnixNano())
}

func backends(t *testing.T) []backend {
	return []backend{memoryBackend(t), miniredisBackend(t)}
}

func allow(t *testing.T, l Limiter, key string) Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return d
}

// countAllowed sends n requests and returns how many were allowed.
func countAllowed(t *testing.T, l Limiter, key string, n int) int {
	t.Helper()
	allowed := 0
	for i := 0; i < n; i++ {
		if allow(t, l, key).Allowed {
			allowed++
		}
	}
	return allowed
}

func TestAllAlgorithmsEnforceLimit(t *testing.T) {
	for _, alg := range Algorithms {
		for _, b := range backends(t) {
			t.Run(string(alg)+"/"+b.name, func(t *testing.T) {
				l := b.newLimiter(t, alg, Rule{Limit: 5, Window: time.Second})
				for i := 0; i < 5; i++ {
					d := allow(t, l, "k")
					if !d.Allowed {
						t.Fatalf("request %d denied, want allowed", i+1)
					}
					if want := int64(4 - i); d.Remaining != want {
						t.Errorf("request %d: remaining=%d want %d", i+1, d.Remaining, want)
					}
					if d.Limit != 5 {
						t.Errorf("limit=%d want 5", d.Limit)
					}
				}
				d := allow(t, l, "k")
				if d.Allowed {
					t.Fatal("6th request allowed, want denied")
				}
				// Sliding counter may need up to 2 windows: this window's count
				// becomes "prev" and must decay before a request fits.
				if d.RetryAfter <= 0 || d.RetryAfter > 2*time.Second {
					t.Errorf("RetryAfter=%s, want in (0, 2s]", d.RetryAfter)
				}
				// Keys are independent.
				if !allow(t, l, "other").Allowed {
					t.Error("different key was denied")
				}
			})
		}
	}
}

func TestTokenBucketBurstThenSteadyRate(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			// 10 tokens/s, bucket holds 20.
			l := b.newLimiter(t, TokenBucket, Rule{Limit: 10, Window: time.Second, Burst: 20})

			if got := countAllowed(t, l, "k", 25); got != 20 {
				t.Fatalf("initial burst allowed %d, want 20 (the bucket size)", got)
			}
			d := allow(t, l, "k")
			if d.RetryAfter != 100*time.Millisecond {
				t.Errorf("RetryAfter=%s, want 100ms (time for one token)", d.RetryAfter)
			}

			b.advance(100 * time.Millisecond) // exactly one token refills
			if got := countAllowed(t, l, "k", 5); got != 1 {
				t.Errorf("after 100ms allowed %d, want 1", got)
			}

			b.advance(time.Second) // 10 tokens refill
			if got := countAllowed(t, l, "k", 15); got != 10 {
				t.Errorf("after 1s allowed %d, want 10", got)
			}

			b.advance(time.Hour) // refill is capped at the bucket size
			if got := countAllowed(t, l, "k", 30); got != 20 {
				t.Errorf("after idle allowed %d, want 20 (capped)", got)
			}
		})
	}
}

func TestFixedWindowResetsAtBoundary(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			l := b.newLimiter(t, FixedWindow, Rule{Limit: 3, Window: time.Second})
			b.advance(400 * time.Millisecond)
			if got := countAllowed(t, l, "k", 5); got != 3 {
				t.Fatalf("allowed %d, want 3", got)
			}
			d := allow(t, l, "k")
			if d.RetryAfter != 600*time.Millisecond {
				t.Errorf("RetryAfter=%s, want 600ms (until the window ends)", d.RetryAfter)
			}
			b.advance(600 * time.Millisecond)
			if got := countAllowed(t, l, "k", 5); got != 3 {
				t.Errorf("new window allowed %d, want 3", got)
			}
		})
	}
}

// TestBoundaryBurst is the classic fixed-window flaw: limit 10/s, a client
// sends 10 requests at t=0.9s and 10 more at t=1.0s. Fixed window allows all
// 20 within 100ms (2x the limit). Sliding windows don't.
func TestBoundaryBurst(t *testing.T) {
	want := map[Algorithm]int{
		FixedWindow:          20, // the bug we are demonstrating
		SlidingWindowLog:     10, // exact: never more than 10 in any 1s span
		SlidingWindowCounter: 10, // estimate = 10*(1.0) + 0 = 10 => full
		TokenBucket:          11, // 10 burst + 1 token refilled in 100ms
	}
	for _, b := range backends(t) {
		for _, alg := range Algorithms {
			t.Run(string(alg)+"/"+b.name, func(t *testing.T) {
				// Re-create the backend per subtest so clocks start aligned.
				var bb backend
				if b.name == "memory" {
					bb = memoryBackend(t)
				} else {
					bb = miniredisBackend(t)
				}
				l := bb.newLimiter(t, alg, Rule{Limit: 10, Window: time.Second})
				bb.advance(900 * time.Millisecond)
				first := countAllowed(t, l, "k", 10)
				bb.advance(100 * time.Millisecond) // now exactly on the boundary
				second := countAllowed(t, l, "k", 10)
				if got := first + second; got != want[alg] {
					t.Errorf("allowed %d in 100ms, want %d (first=%d second=%d)", got, want[alg], first, second)
				}
			})
		}
	}
}

// TestSlidingLogNeverExceedsLimitInAnyWindow starts traffic at t=0.9s (late in
// a window), sends 10 requests every 10ms for 5s, and checks every 1s span:
// the sliding log must never allow more than Limit. Fixed window is run
// alongside to show that it does exceed it.
func TestSlidingLogNeverExceedsLimitInAnyWindow(t *testing.T) {
	maxInWindow := func(alg Algorithm) int {
		fc := clock.NewFake(base.Add(900 * time.Millisecond))
		l, _ := NewMemory(alg, Rule{Limit: 50, Window: time.Second}, MemoryOptions{Clock: fc})
		defer l.Close()
		var allowedAt []time.Time
		for i := 0; i < 500; i++ {
			// Demand is 1000/s, far above the limit of 50/s.
			for j := 0; j < 10; j++ {
				if d, _ := l.Allow(context.Background(), "k"); d.Allowed {
					allowedAt = append(allowedAt, fc.Now())
				}
			}
			fc.Advance(10 * time.Millisecond)
		}
		worst := 0
		for i := range allowedAt { // count allowed in [allowedAt[i], +1s)
			n := 0
			for j := i; j < len(allowedAt) && allowedAt[j].Sub(allowedAt[i]) < time.Second; j++ {
				n++
			}
			worst = max(worst, n)
		}
		return worst
	}
	if got := maxInWindow(SlidingWindowLog); got > 50 {
		t.Errorf("sliding log allowed %d in some 1s span, limit 50", got)
	}
	if got := maxInWindow(FixedWindow); got <= 50 {
		t.Errorf("fixed window max %d; expected it to exceed 50 across a boundary", got)
	} else {
		t.Logf("fixed window allowed up to %d in a 1s span (limit 50); sliding log <= 50", got)
	}
}

func TestSlidingCounterWeightsPreviousWindow(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			l := b.newLimiter(t, SlidingWindowCounter, Rule{Limit: 10, Window: time.Second})
			if got := countAllowed(t, l, "k", 10); got != 10 {
				t.Fatalf("allowed %d want 10", got)
			}
			// Half-way through the next window the previous window counts for
			// 50%: estimate = 10*0.5 = 5, so 5 more fit.
			b.advance(1500 * time.Millisecond)
			if got := countAllowed(t, l, "k", 10); got != 5 {
				t.Errorf("allowed %d at 50%% overlap, want 5", got)
			}
			// Two windows later everything has decayed.
			b.advance(2 * time.Second)
			if got := countAllowed(t, l, "k", 20); got != 10 {
				t.Errorf("allowed %d after decay, want 10", got)
			}
		})
	}
}

func TestSlidingCounterRetryAfterIsAccurate(t *testing.T) {
	fc := clock.NewFake(base)
	l, _ := NewMemory(SlidingWindowCounter, Rule{Limit: 10, Window: time.Second}, MemoryOptions{Clock: fc})
	defer l.Close()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, _ = l.Allow(ctx, "k")
	}
	fc.Advance(1200 * time.Millisecond) // estimate = 10*0.8 = 8 -> 2 fit
	for i := 0; i < 2; i++ {
		_, _ = l.Allow(ctx, "k")
	}
	d, _ := l.Allow(ctx, "k")
	if d.Allowed {
		t.Fatal("expected denial")
	}
	// The promised moment must work, and slightly before it must not.
	fc.Advance(d.RetryAfter - time.Millisecond)
	if d2, _ := l.Allow(ctx, "k"); d2.Allowed {
		t.Errorf("allowed 1ms before RetryAfter (%s)", d.RetryAfter)
	}
	fc.Advance(time.Millisecond)
	if d3, _ := l.Allow(ctx, "k"); !d3.Allowed {
		t.Errorf("denied at RetryAfter (%s)", d.RetryAfter)
	}
}

func TestSlidingLogRetryAfter(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			l := b.newLimiter(t, SlidingWindowLog, Rule{Limit: 2, Window: time.Second})
			allow(t, l, "k")                  // t=0
			b.advance(300 * time.Millisecond) // t=0.3
			allow(t, l, "k")
			b.advance(100 * time.Millisecond) // t=0.4
			d := allow(t, l, "k")
			if d.Allowed || d.RetryAfter != 600*time.Millisecond {
				t.Fatalf("got allowed=%v retry=%s, want denied with 600ms (oldest leaves at t=1.0)", d.Allowed, d.RetryAfter)
			}
			b.advance(600 * time.Millisecond)
			if !allow(t, l, "k").Allowed {
				t.Error("denied after RetryAfter")
			}
		})
	}
}

// TestConcurrentNeverExceedsLimit hammers one key from many goroutines.
// Without proper locking (memory) or atomic scripts (Redis), two goroutines
// could both see "99 used" and both be allowed. Exactly Limit must pass.
func TestConcurrentNeverExceedsLimit(t *testing.T) {
	const goroutines, perGoroutine, limit = 100, 20, 100
	rule := Rule{Limit: limit, Window: time.Hour} // effectively no refill during the test

	run := func(t *testing.T, l Limiter) {
		var allowed atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release all goroutines at once for maximum contention
				for i := 0; i < perGoroutine; i++ {
					d, err := l.Allow(context.Background(), "hot")
					if err != nil {
						t.Error(err)
						return
					}
					if d.Allowed {
						allowed.Add(1)
					}
				}
			}()
		}
		close(start)
		wg.Wait()
		if got := allowed.Load(); got != limit {
			t.Errorf("allowed %d of %d concurrent requests, want exactly %d", got, goroutines*perGoroutine, limit)
		}
	}

	for _, alg := range Algorithms {
		t.Run(string(alg)+"/memory", func(t *testing.T) {
			l, _ := NewMemory(alg, rule, MemoryOptions{})
			defer l.Close()
			run(t, l)
		})
		t.Run(string(alg)+"/miniredis", func(t *testing.T) {
			m := miniredis.RunT(t)
			c := redis.NewClient(&redis.Options{Addr: m.Addr(), PoolSize: 50})
			defer c.Close()
			l, _ := NewRedis(c, alg, rule, RedisOptions{Prefix: "c:", Timeout: 5 * time.Second})
			run(t, l)
		})
		t.Run(string(alg)+"/real-redis", func(t *testing.T) {
			c := realRedisClient(t)
			l, _ := NewRedis(c, alg, rule, RedisOptions{Prefix: uniquePrefix(t), Timeout: 5 * time.Second})
			run(t, l)
		})
	}
}

// TestRealRedisBasics checks the scripts against a real Redis server (real
// TIME, real Lua 5.1), not just miniredis's emulation.
func TestRealRedisBasics(t *testing.T) {
	c := realRedisClient(t)
	for _, alg := range Algorithms {
		t.Run(string(alg), func(t *testing.T) {
			l, err := NewRedis(c, alg, Rule{Limit: 5, Window: 10 * time.Second}, RedisOptions{Prefix: uniquePrefix(t), Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if got := countAllowed(t, l, "k", 8); got != 5 {
				t.Fatalf("allowed %d want 5", got)
			}
			d := allow(t, l, "k")
			// <= 2 windows: the sliding counter may wait for "prev" to decay.
			if d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 20*time.Second {
				t.Errorf("denied decision looks wrong: %+v", d)
			}
		})
	}
}

func TestRedisKeysExpire(t *testing.T) {
	m := miniredis.RunT(t)
	m.SetTime(base)
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer c.Close()
	for _, alg := range Algorithms {
		l, _ := NewRedis(c, alg, Rule{Limit: 5, Window: time.Second}, RedisOptions{Prefix: string(alg) + ":", Timeout: time.Second})
		if _, err := l.Allow(context.Background(), "k"); err != nil {
			t.Fatal(err)
		}
		if ttl := m.TTL(string(alg) + ":{k}"); ttl <= 0 || ttl > 3*time.Second {
			t.Errorf("%s: TTL=%s, want a positive TTL of at most a few windows", alg, ttl)
		}
	}
}

func TestMemoryCleanupRemovesIdleKeys(t *testing.T) {
	fc := clock.NewFake(base)
	for _, alg := range Algorithms {
		t.Run(string(alg), func(t *testing.T) {
			l, _ := NewMemory(alg, Rule{Limit: 5, Window: time.Second}, MemoryOptions{Clock: fc})
			defer l.Close()
			for i := 0; i < 100; i++ {
				_, _ = l.Allow(context.Background(), fmt.Sprintf("user-%d", i))
			}
			store := storeLen(l)
			if store() != 100 {
				t.Fatalf("stored %d keys, want 100", store())
			}
			fc.Advance(3 * time.Second) // longer than any algorithm's TTL here
			sweepNow(l, fc.Now())
			if n := store(); n != 0 {
				t.Errorf("after sweep %d keys remain, want 0", n)
			}
		})
	}
}

// storeLen and sweepNow reach into the concrete type to test cleanup.
func storeLen(l LimiterCloser) func() int {
	switch v := l.(type) {
	case *memTokenBucket:
		return v.store.len
	case *memFixedWindow:
		return v.store.len
	case *memSlidingLog:
		return v.store.len
	case *memSlidingCounter:
		return v.store.len
	}
	panic("unknown type")
}

func sweepNow(l LimiterCloser, now time.Time) {
	switch v := l.(type) {
	case *memTokenBucket:
		v.store.sweep(now)
	case *memFixedWindow:
		v.store.sweep(now)
	case *memSlidingLog:
		v.store.sweep(now)
	case *memSlidingCounter:
		v.store.sweep(now)
	}
}

func TestRuleValidation(t *testing.T) {
	bad := []Rule{{Limit: 0, Window: time.Second}, {Limit: 1, Window: 0}, {Limit: 1, Window: time.Second, Burst: -1}}
	for _, r := range bad {
		if _, err := NewMemory(TokenBucket, r, MemoryOptions{}); err == nil {
			t.Errorf("rule %+v accepted, want error", r)
		}
	}
	if _, err := ParseAlgorithm("leaky"); err == nil {
		t.Error("unknown algorithm accepted")
	}
}
