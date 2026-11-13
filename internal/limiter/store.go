package limiter

import (
	"hash/maphash"
	"sync"
	"time"

	"github.com/xungirl/rate-gate/internal/clock"
)

// shardCount is the number of independent lock+map pairs in a shardedStore.
// With one global mutex every request in the gateway would queue on the same
// lock; with 64 shards, requests for different keys usually take different
// locks and proceed in parallel.
const shardCount = 64

// entry wraps one key's algorithm state with an expiry time used by cleanup.
type entry[T any] struct {
	state     T
	expiresAt time.Time
}

type shard[T any] struct {
	mu   sync.Mutex
	data map[string]*entry[T]
}

// shardedStore is a concurrent map from key to per-key state T.
//
// T is a type parameter (Go generics): each algorithm stores its own state
// type (a bucket, a counter, a list of timestamps) but shares this code.
type shardedStore[T any] struct {
	shards [shardCount]shard[T]
	seed   maphash.Seed
	clock  clock.Clock
	stop   chan struct{}
	once   sync.Once
}

func newShardedStore[T any](clk clock.Clock, cleanupEvery time.Duration) *shardedStore[T] {
	s := &shardedStore[T]{
		seed:  maphash.MakeSeed(),
		clock: clk,
		stop:  make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*entry[T])
	}
	if cleanupEvery > 0 {
		go s.cleanupLoop(cleanupEvery)
	}
	return s
}

func (s *shardedStore[T]) shardFor(key string) *shard[T] {
	h := maphash.String(s.seed, key)
	return &s.shards[h%shardCount]
}

// update runs fn on key's state while holding that key's shard lock, so fn's
// read-modify-write is atomic with respect to other requests for the same key.
// A missing or expired entry is replaced by a zero-value T before fn runs.
// fn returns how long the state stays relevant; after that cleanup may drop it.
func (s *shardedStore[T]) update(key string, now time.Time, fn func(state *T) (ttl time.Duration)) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.data[key]
	if !ok || now.After(e.expiresAt) {
		e = &entry[T]{}
		sh.data[key] = e
	}
	ttl := fn(&e.state)
	e.expiresAt = now.Add(ttl)
}

// cleanupLoop periodically removes expired entries so memory stays bounded by
// the number of *active* keys, not every key ever seen.
func (s *shardedStore[T]) cleanupLoop(every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sweep(s.clock.Now())
		case <-s.stop:
			return
		}
	}
}

// sweep deletes every entry that expired before now. It locks one shard at a
// time so the request path is never blocked on all shards at once.
func (s *shardedStore[T]) sweep(now time.Time) (removed int) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, e := range sh.data {
			if now.After(e.expiresAt) {
				delete(sh.data, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// len returns the number of stored keys (used by tests).
func (s *shardedStore[T]) len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		n += len(sh.data)
		sh.mu.Unlock()
	}
	return n
}

// close stops the cleanup goroutine. Safe to call more than once.
func (s *shardedStore[T]) close() {
	s.once.Do(func() { close(s.stop) })
}
