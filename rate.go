package ratelimiter

import "golang.org/x/time/rate"

// NewRateLimiterFunc returns a factory suitable for [NewBucketLimiter] that
// builds an independent *golang.org/x/time/rate.Limiter for each new key.
//
//   - limit is the sustained refill rate in tokens per second. Use
//     rate.Every(d) to express "one token every d", or rate.Inf for no limit.
//   - burst is the bucket capacity: the maximum number of tokens available in
//     a single instant (and thus the largest momentary burst allowed).
//
// Example: 5 requests per second, absorbing bursts of up to 10:
//
//	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(5), 10)
func NewRateLimiterFunc(limit rate.Limit, burst int) func() Limiter {
	return func() Limiter {
		return rate.NewLimiter(limit, burst)
	}
}
