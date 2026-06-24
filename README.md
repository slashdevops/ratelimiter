# ratelimiter

[![Go Reference](https://pkg.go.dev/badge/github.com/slashdevops/ratelimiter.svg)](https://pkg.go.dev/github.com/slashdevops/ratelimiter)

## Introduction

`ratelimiter` is a flexible and extensible rate limiter library for Go, built as a wrapper around the standard `golang.org/x/time/rate` package. It utilizes the token bucket algorithm to control the rate of operations, making it suitable for scenarios like API request limiting, resource access control, and preventing abuse.

The library allows creating rate limiters specific to different keys (e.g., user IDs, IP addresses) that can be safely shared across multiple goroutines. It includes an in-memory storage backend by default and features automatic cleanup of limiters that haven't been used for a configurable duration.

For advanced use cases, you can provide your own storage backend by implementing the `Storage` interface or even define custom rate-limiting logic by implementing the `Limiter` interface.

## Features

- **Token Bucket Algorithm:** Based on the robust `golang.org/x/time/rate` implementation.
- **Key-Based Limiting:** Create and manage separate limiters for different identifiers (e.g., IP addresses, user IDs).
- **Goroutine-Safe:** Designed for concurrent use.
- **In-Memory Storage:** Comes with a built-in `InMemoryStorage` for easy setup.
- **Automatic Cleanup:** Unused limiters are automatically removed from storage after a specified duration to conserve resources.
- **Extensible:**
  - Implement the `Storage` interface for custom persistence (e.g., Redis, Memcached).
  - Implement the `Limiter` interface for custom rate-limiting strategies.
- **Middleware Example:** Includes a practical example for integrating with `net/http` handlers.

## Installation

```bash
go get github.com/slashdevops/ratelimiter
```

## Core Concepts

- **`Limiter` Interface:** Defines the basic contract for any rate limiter, primarily the `Allow()` method. The standard `golang.org/x/time/rate.Limiter` satisfies this interface.
- **`Storage` Interface:** Defines methods (`Store`, `Load`, `Delete`) for storing and retrieving limiter instances associated with keys.
- **`InMemoryStorage`:** A thread-safe, map-based implementation of the `Storage` interface.
- **`BucketLimiter`:** The main orchestrator. It uses a `Limiter` (like `rate.Limiter`) as a template, a `Storage` backend, and manages the creation, retrieval, and automatic cleanup of key-specific limiters.

## Basic Example

```go
package main

import (
  "fmt"
  "time"

  "github.com/slashdevops/ratelimiter"
  "golang.org/x/time/rate"
)

func main() {
  // 1. Create a storage system (in-memory is provided)
  storage := ratelimiter.NewInMemoryStorage()

  // 2. Define the base rate limit (e.g., 5 requests per second with a burst of 10)
  baseRate := rate.Limit(5)
  burstSize := 10
  baseLimiter := rate.NewLimiter(baseRate, burstSize)

  // 3. Configure the BucketLimiter
  //    Limiters unused for 1 minute will be automatically removed.
  cleanupInterval := 1 * time.Minute
  bucketLimiter := ratelimiter.NewBucketLimiter(baseLimiter, cleanupInterval, storage)

  // 4. Get or create a limiter for a specific key (e.g., user ID or IP)
  userID := "user-123"
  userLimiter := bucketLimiter.GetOrAdd(userID)

  // 5. Check if the operation is allowed
  if userLimiter.Allow() {
    fmt.Println("Operation allowed for", userID)
    // ... perform the rate-limited action ...
  } else {
    fmt.Println("Rate limit exceeded for", userID)
  }

  // Example: Simulate multiple requests
  for i := 0; i < 15; i++ {
    if bucketLimiter.GetOrAdd(userID).Allow() {
      fmt.Printf("Request %d allowed\n", i+1)
    } else {
      fmt.Printf("Request %d blocked\n", i+1)
    }
    time.Sleep(100 * time.Millisecond) // Simulate some delay
  }
}

```

## Middleware Example

See [examples](examples/) for more usage examples, including the HTTP middleware.

Middleware example:

```go
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
```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
