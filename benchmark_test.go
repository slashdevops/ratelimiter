package ratelimiter

import (
	"strconv"
	"testing"
	"time"

	"context"
	"errors"
	"io"
	"log/slog"

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

func BenchmarkMemoryBackendTake(b *testing.B) {
	backend := NewMemoryBackend(WithMemorySweepInterval(0))
	defer backend.Close()

	limit := Limit{Requests: 1 << 30, Period: time.Hour}
	ctx := context.Background()

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_, _ = backend.Take(ctx, "key", limit, 1)
	}
}

// BenchmarkMemoryBackendTakeParallel is the one that matters: a rate limiter's
// hot path is contended by construction, which is why the backend is sharded.
func BenchmarkMemoryBackendTakeParallel(b *testing.B) {
	backend := NewMemoryBackend(WithMemorySweepInterval(0))
	defer backend.Close()

	limit := Limit{Requests: 1 << 30, Period: time.Hour}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		i := 0

		for pb.Next() {
			_, _ = backend.Take(ctx, keys[i%len(keys)], limit, 1)
			i++
		}
	})
}

func BenchmarkBackendLimiterAllow(b *testing.B) {
	backend := NewMemoryBackend(WithMemorySweepInterval(0))
	defer backend.Close()

	l := NewBackendLimiter(backend, "key",
		Limit{Requests: 1 << 30, Period: time.Hour},
		WithoutBackendTimeout())

	b.ResetTimer()

	for b.Loop() {
		l.Allow()
	}
}

// BenchmarkBackendLimiterDegraded measures the path an outage puts every
// request on. It must not involve the backend at all — that is the whole point
// of the circuit breaker.
func BenchmarkBackendLimiterDegraded(b *testing.B) {
	l := NewBackendLimiter(failingBackend{}, "key",
		Limit{Requests: 1 << 30, Period: time.Hour},
		WithFallback(RateLimiter{rate.NewLimiter(rate.Inf, 1)}),
		WithCircuitBreaker(1, time.Hour),
		WithBackendLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)

	l.Allow() // trip the breaker

	b.ResetTimer()

	for b.Loop() {
		l.Allow()
	}
}

type failingBackend struct{}

func (failingBackend) Take(context.Context, string, Limit, int) (Decision, error) {
	return Decision{}, errBenchDown
}

var errBenchDown = errors.New("down")

var keys = func() []string {
	k := make([]string, 64)
	for i := range k {
		k[i] = "key-" + strconv.Itoa(i)
	}

	return k
}()
