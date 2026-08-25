// Command keyfactory demonstrates WithLimiterFactoryForKey: building each key's
// Limiter from the key itself, which is what a limiter needs when its state
// lives outside the process.
//
// The "shared" limiter here is a map behind a mutex rather than Valkey, so the
// example runs with no dependencies — but it has the property that matters:
// several limiter instances built for the same key address ONE budget, and
// evicting an instance does not reset it.
//
//	go run ./examples/keyfactory
//	go run ./examples/keyfactory -shared=false
package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/slashdevops/ratelimiter"
)

// sharedStore stands in for Valkey: state addressed by key, outliving any
// particular limiter object.
type sharedStore struct {
	mu     sync.Mutex
	tokens map[string]int
}

func (s *sharedStore) take(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tokens[key] <= 0 {
		return false
	}

	s.tokens[key]--

	return true
}

// sharedLimiter is a ratelimiter.Limiter whose Allow consults the shared store
// for ITS key. Building one requires knowing the key — which is exactly what
// the argument-less factory could not provide.
type sharedLimiter struct {
	store *sharedStore
	key   string
	burst int
}

func (l sharedLimiter) Burst() int { return l.burst }
func (l sharedLimiter) Allow() bool {
	return l.store.take(l.key)
}

func (l sharedLimiter) Wait(_ context.Context) error { return nil }

func main() {
	shared := flag.Bool("shared", true, "use the shared (out-of-process) limiter")
	flag.Parse()

	store := &sharedStore{tokens: map[string]int{"alice": 2, "bob": 2}}

	// The one seam. Swap the branch and everything above it stays the same —
	// including the storage, which is the ordinary in-memory one either way:
	// it caches handles, it does not hold the budget.
	newLimiter := func(key string) ratelimiter.Limiter {
		if *shared {
			return sharedLimiter{store: store, key: key, burst: 2}
		}

		return ratelimiter.RateLimiter{Limiter: rate.NewLimiter(rate.Every(time.Hour), 2)}
	}

	bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
		ratelimiter.WithLimiterFactoryForKey(newLimiter),
	)
	defer bl.Close()

	for _, key := range []string{"alice", "alice", "alice", "bob"} {
		fmt.Printf("%-6s allow=%v\n", key, bl.GetOrAdd(key).Allow())
	}

	// Evict alice's handle. With a shared limiter the budget stays spent,
	// because it was never in the handle. With the in-process one it comes
	// back full — run with -shared=false to see the difference.
	bl.Remove("alice")
	fmt.Printf("\nafter evicting alice's handle:\n%-6s allow=%v\n", "alice", bl.GetOrAdd("alice").Allow())
}
