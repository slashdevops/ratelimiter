// Package ratelimiter provides a flexible, goroutine-safe rate limiter for Go,
// built as a thin manager around the token-bucket implementation in
// golang.org/x/time/rate.
//
// The central type is [BucketLimiter], a manager that hands out an independent
// [Limiter] per key (for example a user ID or IP address). Each key gets its
// own token bucket, so exhausting one key never affects another.
//
// Limiters are created lazily on first use and evicted after they have been
// idle (not accessed) for a configurable duration. Eviction is performed by a
// single background goroutine that is started when the manager is created and
// stopped by [BucketLimiter.Close].
//
// Basic usage:
//
//	storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()
//	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(5), 10) // 5 rps, burst 10
//	bl := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
//	defer bl.Close()
//
//	if bl.GetOrAdd("user-123").Allow() {
//		// allowed
//	}
//
// The [Storage] interface can be implemented to plug in a custom in-process
// store. Note that the token-bucket state lives in memory inside each
// *rate.Limiter, so this package targets single-process rate limiting.
// Distributed rate limiting across multiple instances requires a different
// algorithm and is out of scope.
package ratelimiter
