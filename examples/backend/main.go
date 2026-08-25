// Command backend demonstrates the Backend seam: the same limiter, backed by
// in-process state or by a shared store, chosen with one branch.
//
// The "remote" backend here is a map with an artificial failure switch rather
// than Valkey, so the example runs with no dependencies — but it exercises the
// three things BackendLimiter adds: the local fallback, the circuit breaker,
// and the degraded signal.
//
//	go run ./examples/backend
package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

	"github.com/slashdevops/ratelimiter"
)

// flakyBackend wraps a real backend and can be told to start failing, standing
// in for a datastore that becomes unreachable.
type flakyBackend struct {
	inner ratelimiter.Backend
	down  bool
	calls int
}

func (b *flakyBackend) Take(ctx context.Context, key string, limit ratelimiter.Limit, cost int) (ratelimiter.Decision, error) {
	b.calls++

	if b.down {
		return ratelimiter.Decision{}, fmt.Errorf("backend unreachable")
	}

	return b.inner.Take(ctx, key, limit, cost)
}

func main() {
	limit := ratelimiter.Limit{Requests: 3, Period: time.Minute}

	shared := &flakyBackend{inner: ratelimiter.NewMemoryBackend()}

	// The local limiter is both the cache-disabled path and the fallback.
	newLocal := func() ratelimiter.Limiter {
		return ratelimiter.RateLimiter{Limiter: rate.NewLimiter(rate.Limit(limit.Rate()), limit.Capacity())}
	}

	degraded := false

	bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
		ratelimiter.WithLimiterFactoryForKey(func(key string) ratelimiter.Limiter {
			return ratelimiter.NewBackendLimiter(shared, key, limit,
				ratelimiter.WithFallback(newLocal()),
				ratelimiter.WithCircuitBreaker(2, time.Minute),
				ratelimiter.WithOnDegraded(func(_ string, d bool) { degraded = d }),
			)
		}),
	)
	defer bl.Close()

	fmt.Println("shared backend healthy — the budget is 3 and it is global")

	for i := range 5 {
		fmt.Printf("  request %d  allow=%-5v  backend_calls=%d\n", i+1, bl.GetOrAdd("alice").Allow(), shared.calls)
	}

	fmt.Println("\nbackend goes down — the limiter falls back to the local budget")
	shared.down = true

	for i := range 6 {
		allow := bl.GetOrAdd("alice").Allow()
		fmt.Printf("  request %d  allow=%-5v  backend_calls=%d  degraded=%v\n", i+1, allow, shared.calls, degraded)
	}

	fmt.Println("\nNote the backend call count stops climbing: after the threshold")
	fmt.Println("the limiter answers locally with no network call at all, which is")
	fmt.Println("what stops an outage adding a timeout to every request.")
}
