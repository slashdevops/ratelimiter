package ratelimiter

import "time"

// Reservation is a backend-agnostic view of a single token that has been
// reserved for the earliest instant it can be granted. It mirrors the useful
// subset of the methods on *golang.org/x/time/rate.Reservation, so a standard
// *rate.Reservation satisfies Reservation directly.
//
// A reservation holds a token until it is either used (by proceeding after
// Delay) or returned with Cancel. Whichever a caller does, it must do exactly
// one of the two to avoid leaking or double-counting tokens.
type Reservation interface {
	// OK reports whether a token can be granted within the limiter's limits.
	// If it returns false the caller must not proceed and must not act on
	// Delay; the request can never be satisfied (for example a burst larger
	// than the bucket capacity).
	OK() bool

	// Delay returns how long the caller must wait, from now, before the
	// reserved token becomes valid. Zero means it may be used immediately.
	Delay() time.Duration

	// Cancel returns the reserved token to the limiter if it has not yet been
	// consumed, so it does not count against the limit. Call it when you decide
	// not to proceed (for example when rejecting an over-limit request).
	Cancel()
}

// Reserver is an OPTIONAL capability a [Limiter] may implement in addition to
// the core Allow/Wait/Burst methods. When a limiter can pre-reserve a token and
// report the exact delay until it is valid, implementing Reserver lets callers
// such as HTTP middleware emit accurate Retry-After and RateLimit-Reset headers
// for any backend — not only *rate.Limiter.
//
// Feature-detect it with a type assertion and degrade gracefully when it is
// absent:
//
//	if r, ok := lim.(ratelimiter.Reserver); ok {
//		res := r.Reserve()
//		defer res.Cancel() // if you end up not proceeding
//		// ... use res.OK() / res.Delay() for headers and the decision
//	} else {
//		allowed := lim.Allow() // no timing information available
//	}
//
// The [RateLimiter] returned by [NewRateLimiterFunc] implements Reserver.
type Reserver interface {
	// Reserve reserves one token for the earliest instant it can be granted and
	// returns a [Reservation] describing it. The token is held until it is used
	// or Reservation.Cancel is called.
	Reserve() Reservation
}
