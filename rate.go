package ratelimiter

import "golang.org/x/time/rate"

// RateLimiter adapts a *golang.org/x/time/rate.Limiter to both the [Limiter]
// and [Reserver] interfaces. The embedded *rate.Limiter supplies Allow, Wait,
// and Burst directly (along with its other methods, such as TokensAt); the
// added Reserve method exposes reservations through the backend-agnostic
// [Reservation] interface, so callers like HTTP middleware can compute accurate
// Retry-After headers without depending on the concrete *rate.Limiter type.
//
// [NewRateLimiterFunc] hands these out, so the default limiter is a [Reserver].
type RateLimiter struct {
	*rate.Limiter
}

// Reserve implements [Reserver]. It reserves one token at the current instant;
// the returned [Reservation]'s Delay reports how long until it becomes valid.
func (r RateLimiter) Reserve() Reservation {
	return r.Limiter.Reserve()
}

// NewRateLimiterFunc returns a factory suitable for [NewBucketLimiter] that
// builds an independent [RateLimiter] (wrapping a *golang.org/x/time/rate.Limiter)
// for each new key.
//
//   - limit is the sustained refill rate in tokens per second. Use
//     rate.Every(d) to express "one token every d", or rate.Inf for no limit.
//   - burst is the bucket capacity: the maximum number of tokens available in
//     a single instant (and thus the largest momentary burst allowed).
//
// Example: 5 requests per second, absorbing bursts of up to 10:
//
//	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(5), 10)
//
// The returned limiters satisfy [Reserver] in addition to [Limiter].
func NewRateLimiterFunc(limit rate.Limit, burst int) func() Limiter {
	return func() Limiter {
		return RateLimiter{rate.NewLimiter(limit, burst)}
	}
}

// Compile-time guarantees for the adapter and the interface it mirrors.
var (
	_ Limiter     = RateLimiter{}
	_ Reserver    = RateLimiter{}
	_ Reservation = (*rate.Reservation)(nil)
)
