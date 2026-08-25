// Package ratelimiter provides a flexible, goroutine-safe rate limiter for Go,
// built as a thin manager around the token-bucket implementation in
// golang.org/x/time/rate.
//
// The central type is [BucketLimiter], a manager that hands out an independent
// [Limiter] per key (for example a user ID or IP address). Each key gets its
// own bucket, so exhausting one key never affects another.
//
// Limiters are created lazily on first use and evicted after they have been
// idle (not accessed) for a configurable duration. Eviction is performed by a
// single background goroutine that is started when the manager is created and
// stopped by [BucketLimiter.Close].
//
// # Basic usage
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
// # Two strategies
//
// [StrategyTokenBucket] admits a burst and refills continuously;
// [StrategyLeakyBucket] enforces a minimum spacing between admissions. Both can
// be configured for "60 a minute" and they behave completely differently — 60
// at once versus one per second — so the choice matters more than the numbers.
// [NewLimiterFunc] selects one from a [Strategy] value, which is meant to
// survive a round trip through configuration; [ParseStrategy] validates one
// coming back. See docs/TOKEN_BUCKET.md and docs/LEAKY_BUCKET.md.
//
// Limiters are consumed through the [Limiter] interface (Allow, Wait, Burst).
// A limiter may optionally also implement [Reserver] to reserve a token and
// report the exact delay until it is valid; the default limiter from
// [NewRateLimiterFunc] does, which lets HTTP middleware emit accurate
// Retry-After headers for any backend. See the examples directory for a
// runnable HTTP middleware.
//
// # Three extension points, and which one you want
//
// They are easy to confuse, and picking the wrong one produces a limiter that
// looks like it works. The rule of thumb:
//
//	a different in-process CONTAINER (LRU, metrics, sharding) → Storage
//	a different ALGORITHM (leaky bucket, GCRA, …)             → Limiter
//	a limit SHARED across processes                           → Backend
//
// [Storage] holds the per-key [Limiter] values in this process. It is a
// container, and it is in-process by definition: [BucketLimiter.GetOrAdd] hands
// the caller the limiter and the caller mutates it, so a Storage that
// serialised limiter state into a datastore would return a full bucket on every
// call and never limit anything. See docs/CUSTOM_STORAGE.md.
//
// [Limiter] is the decision. Implement it to change how tokens are accounted —
// including accounting them somewhere other than this process.
//
// [Backend] is where the count lives: an in-process map, Valkey, Redis, or
// anything else that can count atomically per key. It is deliberately
// Take-shaped rather than Get/Set-shaped, because the token update is a
// read-modify-write and splitting that across a network is a race in which the
// limit silently becomes twice what was configured. The decision has to run
// where the state lives.
//
// # Sharing one limit across processes
//
//	backend := ratelimiter.NewMemoryBackend() // or your own, over Valkey/Redis
//	limit   := ratelimiter.Limit{Requests: 100, Period: time.Minute}
//
//	bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
//		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
//		ratelimiter.WithLimiterFactoryForKey(
//			ratelimiter.NewBackendLimiterFunc(backend, limit,
//				ratelimiter.WithFallback(local)),
//		),
//	)
//
// [WithLimiterFactoryForKey] is what makes this possible: a [Limiter] is bound
// to exactly one key — Allow and Wait take no arguments, so the instance IS the
// bucket — and a limiter whose counter lives elsewhere has to know which remote
// key is its own.
//
// [BackendLimiter] adds a local fallback, a circuit breaker and a degraded
// signal, because a remote backend can be unreachable and neither refusing nor
// allowing is an acceptable answer to that on its own. See docs/BACKENDS.md.
package ratelimiter
