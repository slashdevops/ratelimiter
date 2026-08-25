// Command leakybucket shows what a leaky bucket is for: pacing calls to
// something that meters you, and shaping rather than dropping.
//
// It also demonstrates the thing most comparisons get wrong — with the same
// parameters, a leaky bucket and a token bucket behave identically.
//
//	go run ./examples/leakybucket
package main

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/time/rate"

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
	fmt.Println("1. Same parameters, same behaviour")
	fmt.Println("   The comparison you usually see is rigged by changing the burst.")
	fmt.Println()

	tb := ratelimiter.RateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 1)}
	lb := ratelimiter.NewLeakyBucket(100*time.Millisecond, 1)

	fmt.Printf("   token bucket 10/s burst 1 : %d of 10 admitted now\n", drain(tb, 10))
	fmt.Printf("   leaky bucket 100ms cap 1  : %d of 10 admitted now\n", drain(lb, 10))

	time.Sleep(250 * time.Millisecond)

	fmt.Printf("   token bucket, 250ms later : %d of 10\n", drain(tb, 10))
	fmt.Printf("   leaky bucket, 250ms later : %d of 10\n", drain(lb, 10))
	fmt.Println("\n   Identical. They are duals.")

	// ---------------------------------------------------------------------
	fmt.Println("\n2. What it is actually for: shaping")
	fmt.Println("   Wait sleeps until the request conforms instead of dropping it,")
	fmt.Println("   which is what you want draining a queue against a metered API.")
	fmt.Println()

	paced := ratelimiter.NewLeakyBucket(80*time.Millisecond, 1)
	start := time.Now()

	for i := range 5 {
		if err := paced.Wait(context.Background()); err != nil {
			panic(err)
		}

		fmt.Printf("   call %d at %v\n", i+1, time.Since(start).Round(10*time.Millisecond))
	}

	// ---------------------------------------------------------------------
	fmt.Println("\n3. Retry-After is exact, and Cancel gives the slot back")
	fmt.Println()

	bucket := ratelimiter.NewLeakyBucket(time.Second, 1)

	// Reserve TAKES the slot when it conforms. That is what makes Cancel
	// meaningful — there is something to give back.
	res := bucket.Reserve()
	fmt.Printf("   reserved: ok=%v\n", res.OK())
	fmt.Printf("   bucket now exhausted: allow=%v\n", bucket.Allow())

	res.Cancel()
	fmt.Printf("   after Cancel, admitted again: %v\n", bucket.Allow())

	// A reservation that was REFUSED never took a slot, so cancelling it is a
	// no-op rather than free budget.
	refused := bucket.Reserve()
	fmt.Printf("\n   refused reservation: ok=%v, retry after %v\n",
		refused.OK(), refused.Delay().Round(10*time.Millisecond))

	refused.Cancel()
	fmt.Printf("   cancelling a refused reservation grants nothing: allow=%v\n", bucket.Allow())

	fmt.Println("\n   A window-counter backend cannot do any of this — it has no way")
	fmt.Println("   to hand a slot back that differs from spending one less.")

	// ---------------------------------------------------------------------
	fmt.Println("\n4. Per key, through the manager")
	fmt.Println()

	bl := ratelimiter.NewBucketLimiter(
		ratelimiter.NewLeakyBucketFunc(time.Second, 1),
		time.Minute,
		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
	)
	defer bl.Close()

	for _, key := range []string{"alice", "alice", "bob"} {
		fmt.Printf("   %-6s allow=%v\n", key, bl.GetOrAdd(key).Allow())
	}

	fmt.Println("\n   alice is paced; bob is unaffected. Independent buckets per key.")
}
