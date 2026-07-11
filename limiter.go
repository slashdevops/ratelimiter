package ratelimiter

import "context"

// Limiter is the minimal contract a per-key rate limiter must satisfy. It is a
// subset of the methods on *golang.org/x/time/rate.Limiter, so a standard
// *rate.Limiter satisfies Limiter directly.
//
// A Limiter may optionally also implement [Reserver] to expose token
// reservations (and the delay until a token is available). Callers such as HTTP
// middleware feature-detect that capability with a type assertion; the default
// limiter from [NewRateLimiterFunc] provides it.
//
// Implementations MUST be safe for concurrent use by multiple goroutines.
type Limiter interface {
	// Burst returns the maximum number of tokens that can be consumed in a
	// single instant (the bucket capacity). A burst of zero allows no events,
	// unless the rate is Inf.
	Burst() int

	// Allow reports whether one event may happen now, consuming a token if so.
	// It never blocks and is the right choice for drop-on-limit workloads such
	// as HTTP request throttling.
	Allow() bool

	// Wait blocks until a token is available or ctx is done, returning ctx's
	// error in the latter case. Use it to shape (delay) work rather than drop
	// it.
	Wait(ctx context.Context) (err error)
}
