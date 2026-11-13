package limiter

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Micro-benchmarks for every algorithm. Rule: 100 requests/s per key, a
// realistic per-client limit (a huge limit would make the sliding log store
// millions of timestamps and measure something no one deploys).
//
//	go test -run '^$' -bench . -benchmem ./internal/limiter/
//	RATEGATE_REDIS_ADDR=127.0.0.1:6390 go test -run '^$' -bench Redis ./internal/limiter/
var benchRule = Rule{Limit: 100, Window: time.Second}

const benchKeys = 10_000

// keyNames is pre-built so benchmarks don't measure strconv.
var keyNames = func() []string {
	k := make([]string, benchKeys)
	for i := range k {
		k[i] = "user-" + strconv.Itoa(i)
	}
	return k
}()

func BenchmarkMemory(b *testing.B) {
	for _, alg := range Algorithms {
		b.Run(string(alg)+"/serial-one-key", func(b *testing.B) {
			l, _ := NewMemory(alg, benchRule, MemoryOptions{})
			defer l.Close()
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = l.Allow(ctx, "k")
			}
		})
		b.Run(string(alg)+"/parallel-10k-keys", func(b *testing.B) {
			l, _ := NewMemory(alg, benchRule, MemoryOptions{})
			defer l.Close()
			benchParallel(b, l, benchKeys)
		})
		b.Run(string(alg)+"/parallel-one-key", func(b *testing.B) {
			l, _ := NewMemory(alg, benchRule, MemoryOptions{})
			defer l.Close()
			benchParallel(b, l, 1) // every goroutine fights for one shard lock
		})
	}
}

func BenchmarkRedis(b *testing.B) {
	client := realRedisClient(b)
	for _, alg := range Algorithms {
		b.Run(string(alg)+"/parallel-10k-keys", func(b *testing.B) {
			l, _ := NewRedis(client, alg, benchRule, RedisOptions{Prefix: uniquePrefix(b), Timeout: time.Second})
			// Each call waits a network round trip, so use more goroutines
			// than CPUs (16 x GOMAXPROCS = 128 here) to keep Redis busy.
			b.SetParallelism(16)
			benchParallel(b, l, benchKeys)
		})
	}
}

// benchParallel calls Allow from many goroutines over nKeys keys. Each
// goroutine walks the keys from its own starting offset with a local counter;
// a shared atomic counter would itself become the bottleneck.
func benchParallel(b *testing.B, l Limiter, nKeys int) {
	var worker atomic.Int64
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(worker.Add(1)) * 7919 // spread starting points
		for pb.Next() {
			i++
			if _, err := l.Allow(ctx, keyNames[i%nKeys]); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
