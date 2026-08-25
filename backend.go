package ratelimiter

import (
	"context"
	"time"
)

// Limit is how much a rule allows, expressed the way an operator states it.
//
// Requests over Period, not a float rate: "300 per minute" is what a person
// says, what a form collects, and what a database row should hold. The rate is
// derived. Storing the float instead makes every caller guess at the window it
// came from.
type Limit struct {
	// Requests is the budget over one Period.
	Requests int

	// Period is the window the budget applies to. One second, one minute, one
	// hour — whatever the rule says.
	Period time.Duration

	// Burst is the capacity available in a single instant. Zero means
	// Requests, which is the sensible default: a window-based backend has no
	// separate notion of burst.
	Burst int
}

// Rate returns the sustained refill rate in tokens per second.
func (l Limit) Rate() float64 {
	if l.Period <= 0 {
		return 0
	}

	return float64(l.Requests) / l.Period.Seconds()
}

// Capacity returns Burst, defaulting to Requests when Burst is unset.
func (l Limit) Capacity() int {
	if l.Burst > 0 {
		return l.Burst
	}

	return l.Requests
}

// Decision is what a [Backend] answers.
type Decision struct {
	// Allowed reports whether the tokens were granted.
	Allowed bool

	// Remaining is the budget left in the current window after this call. It
	// is best-effort: a backend that cannot compute it cheaply may report -1,
	// and callers must treat a negative value as "unknown" rather than zero.
	Remaining int

	// RetryAfter is how long until the request would be granted. It is only
	// meaningful when Allowed is false, and it may be an estimate — a
	// window-based backend derives it from the window edge rather than from a
	// continuous refill.
	RetryAfter time.Duration
}

// Backend is the pluggable state store for a rate limiter: an in-process map,
// Valkey, Redis, DynamoDB, or anything else that can count atomically per key.
//
// # Why this is Take-shaped and not Get/Set-shaped
//
// It is tempting to define a backend as a small key-value interface — Get,
// Set, Incr — and let this package do the token arithmetic. That does not work
// for anything but an in-process store, and the reason is the whole design:
//
//	read (tokens, timestamp) → refill by elapsed time → compare → write back
//
// is a read-modify-write. Split across a network it is a race: two replicas
// read the same state, both decide they may proceed, and both write. The limit
// silently becomes 2x. Making it safe needs either a transaction with a retry
// loop on a key that is contended by definition, or a server-side script — and
// neither is expressible through a Get/Set interface.
//
// So the decision has to run WHERE THE STATE LIVES, and the interface has to be
// the decision, not the storage. [Backend.Take] is that: one call, one answer,
// atomicity owned by the implementation because only the implementation can
// provide it.
//
// This is also exactly why [Storage] cannot be used for distributed limiting.
// Storage holds [Limiter] VALUES in this process; it is a container, and its
// Load/Store shape is the Get/Set shape described above. See
// docs/CUSTOM_STORAGE.md.
//
// # The contract
//
// Implementations MUST be safe for concurrent use, and Take MUST be atomic per
// key: two concurrent Takes for the same key must not both succeed on the
// strength of the same remaining token. Everything else — the algorithm, the
// key encoding, the expiry — is the implementation's business.
//
// An implementation SHOULD expire its own state. A rate limiter's key space is
// usually unbounded (one entry per client address), so a backend that never
// forgets is a leak.
type Backend interface {
	// Take attempts to consume cost tokens for key under limit, and reports
	// what happened. cost is normally 1.
	//
	// An error means the decision could not be made — not that it was denied.
	// Callers decide what an unavailable backend implies; [BackendLimiter]
	// falls back to a local limiter rather than guessing.
	Take(ctx context.Context, key string, limit Limit, cost int) (Decision, error)
}
