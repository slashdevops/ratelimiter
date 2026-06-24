package main // Changed from 'example' to 'main' to make it runnable

import (
	"flag"
	"fmt"
	"time"

	"github.com/slashdevops/ratelimiter"
	"golang.org/x/time/rate"
)

func main() {
	limit := flag.Int("limit", 1, "Rate limit (requests per second)")
	burst := flag.Int("burst", 1, "Burst size")
	deleteAfter := flag.Duration("deleteAfter", 5*time.Second, "Duration after which the limiter will be deleted if not used")
	flag.Parse()

	// Example usage of the BucketLimiter
	// Create a storage system
	storage := ratelimiter.NewInMemoryStorage()

	// Create a base rate limiter
	baseLimiter := rate.NewLimiter(rate.Limit(float64(*limit)), *burst)

	// Create a bucket limiter with a deleteAfter duration of 5 seconds
	// This means inactive limiters will be removed after 5 seconds
	bucketLimiter := ratelimiter.NewBucketLimiter(baseLimiter, *deleteAfter, storage)

	// Get or add a limiter for a specific key
	key := "user123"

	// Simulate multiple requests
	for i := range 15 {
		userLimiter := bucketLimiter.GetOrAdd(key)

		// Check if the user is allowed to make a request
		if userLimiter.Allow() {
			fmt.Printf("Request %d for key '%s': Allowed\n", i+1, key)
			// Process the request
		} else {
			fmt.Printf("Request %d for key '%s': Rejected (Rate Limit Exceeded)\n", i+1, key)
			// Reject the request
		}

		time.Sleep(500 * time.Millisecond) // Wait between requests
	}

	// Wait for cleanup to potentially happen (longer than deleteAfter)
	fmt.Println()
	fmt.Println("Waiting for potential cleanup...")
	time.Sleep(6 * time.Second)

	// Try again after cleanup, a new limiter should be created if deleted
	userLimiter := bucketLimiter.GetOrAdd(key)
	if userLimiter.Allow() {
		fmt.Printf("Request after cleanup for key '%s': Allowed\n", key)
	} else {
		fmt.Printf("Request after cleanup for key '%s': Rejected\n", key)
	}
}
