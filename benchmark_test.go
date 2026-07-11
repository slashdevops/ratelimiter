package ratelimiter

import (
	"strconv"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// BenchmarkGetOrAdd_Existing measures the hot path: fetching an existing key.
func BenchmarkGetOrAdd_Existing(b *testing.B) {
	storage := NewInMemoryStorage[string, Limiter]()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(1000), 100), time.Minute, storage)
	b.Cleanup(func() { _ = bl.Close() })
	bl.GetOrAdd("hot")

	b.ReportAllocs()
	for b.Loop() {
		bl.GetOrAdd("hot")
	}
}

// BenchmarkGetOrAdd_Parallel measures contended access across many keys.
func BenchmarkGetOrAdd_Parallel(b *testing.B) {
	storage := NewInMemoryStorage[string, Limiter]()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(1000), 100), time.Minute, storage)
	b.Cleanup(func() { _ = bl.Close() })

	keys := make([]string, 256)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
		bl.GetOrAdd(keys[i])
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bl.GetOrAdd(keys[i%len(keys)]).Allow()
			i++
		}
	})
}
