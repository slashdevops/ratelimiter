// Command key demonstrates the basics of per-key rate limiting: creating a
// manager, fetching a key's limiter, and observing the token bucket allow and
// then refill over time.
//
// Run it:
//
//	go run ./examples/key -limit 1 -burst 3
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/slashdevops/ratelimiter"
	"golang.org/x/time/rate"
)

func main() {
	limit := flag.Int("limit", 1, "sustained rate limit (requests per second)")
	burst := flag.Int("burst", 3, "burst size (bucket capacity)")
	deleteAfter := flag.Duration("deleteAfter", 5*time.Second, "idle duration after which an unused limiter is evicted")
	flag.Parse()

	// 1. A store that keeps one Limiter per key.
	storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()

	// 2. A factory that builds an independent token bucket for each new key.
	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(float64(*limit)), *burst)

	// 3. The manager. Close() stops its background eviction goroutine.
	manager := ratelimiter.NewBucketLimiter(newLimiter, *deleteAfter, storage)
	defer manager.Close()

	// Two distinct keys prove isolation: "alice" draining her bucket does not
	// affect "bob".
	fmt.Println("== independent buckets ==")
	for range *burst + 2 {
		fmt.Printf("alice: %-8v bob: %v\n",
			allow(manager, "alice"), allow(manager, "bob"))
	}

	// Watch a single key refill over time at the configured rate.
	fmt.Println("\n== refill over time (alice) ==")
	for i := range 6 {
		fmt.Printf("t=%ds  alice: %v\n", i, allow(manager, "alice"))
		time.Sleep(time.Second)
	}
}

func allow(m *ratelimiter.BucketLimiter[string], key string) string {
	if m.GetOrAdd(key).Allow() {
		return "allowed"
	}
	return "limited"
}
