// Command strategy shows what the strategy actually changes.
//
// The difference is NOT that one bursts and the other never does — give a leaky
// bucket a capacity and it bursts too. The difference is how the capacity comes
// BACK: a token bucket refills continuously, a leaky bucket releases one slot
// per interval. And a leaky bucket at capacity 1 paces strictly, which a token
// bucket cannot do at all.
//
//	go run ./examples/strategy
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/slashdevops/ratelimiter"
)

func drain(l ratelimiter.Limiter, n int) int {
	admitted := 0

	for range n {
		if l.Allow() {
			admitted++
		}
	}

	return admitted
}

func main() {
	// Identical Limit for both: 10 per second, burst defaulting to 10.
	limit := ratelimiter.Limit{Requests: 10, Period: time.Second}

	fmt.Println("Same Limit — 10 per second — under each strategy.")
	fmt.Println()

	limiters := map[string]ratelimiter.Limiter{}

	for _, name := range []string{"token_bucket", "leaky_bucket"} {
		strategy, err := ratelimiter.ParseStrategy(name)
		if err != nil {
			panic(err)
		}

		newLimiter, err := ratelimiter.NewLimiterFunc(strategy, limit)
		if err != nil {
			panic(err)
		}

		l := newLimiter()
		limiters[name] = l

		fmt.Printf("  %-14s first burst: %2d of 10 admitted\n", name, drain(l, 10))
	}

	// Both absorbed the burst. The difference is what happens next.
	fmt.Println("\nBoth burst. Now wait 300ms and try ten more —")
	fmt.Println("this is where they diverge:")
	fmt.Println()

	time.Sleep(300 * time.Millisecond)

	for _, name := range []string{"token_bucket", "leaky_bucket"} {
		fmt.Printf("  %-14s after 300ms:  %2d of 10 admitted\n", name, drain(limiters[name], 10))
	}

	fmt.Println("\n  token bucket refilled ~3 slots continuously (300ms at 10/s).")
	fmt.Println("  leaky bucket released 3 slots, one per 100ms interval.")

	// Capacity 1 is the configuration a token bucket cannot express: no burst
	// at all, ever.
	fmt.Println("\nA leaky bucket at capacity 1 paces strictly — no burst, ever:")

	strict, err := ratelimiter.NewLimiterFunc(
		ratelimiter.StrategyLeakyBucket,
		ratelimiter.Limit{Requests: 10, Period: time.Second, Burst: 1},
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("  leaky (burst 1) first burst: %2d of 10 admitted\n", drain(strict(), 10))

	// Wait shapes rather than drops: it sleeps until each request conforms.
	lb := ratelimiter.NewLeakyBucket(50*time.Millisecond, 1)
	start := time.Now()

	for range 5 {
		if err := lb.Wait(context.Background()); err != nil {
			panic(err)
		}
	}

	fmt.Printf("\nShaping: 5 requests through Wait at one per 50ms took %v\n",
		time.Since(start).Round(10*time.Millisecond))
	fmt.Println("Wait sleeps until each request conforms instead of dropping it,")
	fmt.Println("which is what you want draining a queue against a metered API.")
}
