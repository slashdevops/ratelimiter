package main

import (
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/slashdevops/ratelimiter"
	"golang.org/x/time/rate"
)

// Middleware is a function that wraps an http.Handler to provide additional functionality
type Middleware func(http.Handler) http.Handler

// ThenFunc wraps an http.HandlerFunc with a middleware
func (m Middleware) ThenFunc(h http.HandlerFunc) http.Handler {
	return m(http.HandlerFunc(h))
}

// IPRateLimiter is a middleware that limits the number of requests from a single IP address
func IPRateLimiter(limiter *ratelimiter.BucketLimiter) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := strings.Split(r.RemoteAddr, ":")[0]

			lim := limiter.GetOrAdd(ip)
			if !lim.Allow() {
				http.Error(w, fmt.Sprintf("too many requests from ip address %s", ip), http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func main() {
	limit := flag.Int("limit", 2, "Rate limit (requests per second)")
	burst := flag.Int("burst", 1, "Burst size")
	deleteAfter := flag.Duration("deleteAfter", 5*time.Second, "Duration after which the limiter will be deleted if not used")
	numberOfRequests := flag.Int("numberOfRequests", 30, "Number of requests to send")
	waitBetweenRequests := flag.Duration("waitBetweenRequests", 100*time.Millisecond, "Wait time between requests")

	flag.Parse()

	// Create a storage system
	storage := ratelimiter.NewInMemoryStorage()

	// Create a base rate limiter
	baseLimiter := rate.NewLimiter(rate.Limit(float64(*limit)), *burst)

	// Create a bucket limiter with a deleteAfter duration of 5 seconds
	bucketLimiter := ratelimiter.NewBucketLimiter(baseLimiter, *deleteAfter, storage)

	// Create a new HTTP server
	mux := http.NewServeMux()

	// Register the rate limiting middleware
	mux.Handle("GET /", IPRateLimiter(bucketLimiter).ThenFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, world without limit!")
	}))

	// go routine doing request
	go func() {
		// wait until the server is up
		fmt.Println("Waiting for server to start...")
		time.Sleep(2 * time.Second)

		for range *numberOfRequests {

			resp, err := http.Get("http://localhost:8080/")
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				fmt.Println("Request allowed")
			} else {
				fmt.Println("Request rejected:", resp.Status)
			}
			time.Sleep(*waitBetweenRequests) // Wait between requests
		}

		fmt.Println()
		fmt.Println("Finished sending requests, press Ctrl+C to stop the server")
	}()

	// Start the server
	fmt.Println("Starting server on :8080...")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
