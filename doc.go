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
// Limiters are consumed through the [Limiter] interface (Allow, Wait, Burst).
// A limiter may optionally also implement [Reserver] to reserve a token and
// report the exact delay until it is valid; the default limiter from
// [NewRateLimiterFunc] does, which lets HTTP middleware emit accurate
// Retry-After headers for any backend. See the examples directory for a runnable
// HTTP middleware.
//
// The [Storage] interface can be implemented to plug in a custom in-process
// store; see [Storage] and the customstorage example. Note that the
// token-bucket state lives in memory inside each *rate.Limiter, so this package
// targets single-process rate limiting. Distributed rate limiting across
// multiple instances requires a different algorithm (a datastore-backed
// [Limiter]) and is out of scope for the bundled types.
package ratelimiter
