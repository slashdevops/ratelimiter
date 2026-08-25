package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// LeakyBucket is a [Limiter] that enforces a minimum spacing between
// admissions, implemented by virtual scheduling (GCRA — the Generic Cell Rate
// Algorithm).
//
// # How it relates to the token bucket
//
// Given the same parameters, they behave IDENTICALLY. A token bucket refilling
// at rate r with capacity b admits exactly what a leaky bucket draining at r
// with capacity b admits — they are duals, and measured side by side in this
// package they agree request for request. Comparisons claiming otherwise have
// usually changed the burst between the two examples.
//
// What differs is how the limit is EXPRESSED, and the arithmetic underneath:
//
//   - Configured by an interval rather than a rate and a burst, so strict
//     pacing — "one every 100ms" — is the obvious configuration rather than a
//     non-obvious burst of 1 that is easy to leave at a default.
//   - Exact: one time.Time and integer-duration arithmetic, so there are no
//     floating-point tokens accumulating rounding error over a long uptime.
//
// Reach for it when you are expressing a PACE. "No more than 1000 an hour" is a
// budget and reads better as a token bucket; "no more than one call every
// 100ms" is a pace and reads better as this.
//
// # Virtual scheduling, not a queue
//
// There is no queue and no timer. The bucket keeps one instant: the theoretical
// arrival time (TAT) at which the next conforming request may be admitted.
// Admitting a request pushes the TAT forward by one emission interval; a
// request that arrives more than the burst allowance ahead of the TAT is
// refused, and the amount it is early by IS the retry delay.
//
// That makes every operation O(1), allocation-free and exact — no accumulated
// floating-point drift, because nothing accumulates.
//
// # Capacity
//
// capacity is how many requests may be taken back-to-back from an idle bucket.
//
//   - capacity 1 is a strict leaky bucket: perfectly even spacing, no burst at
//     all. This is what you want in front of a rate-limited third party.
//   - capacity n allows n immediately and then paces at the interval, which is
//     a token bucket's shape with a leaky bucket's recovery.
//
// # Shaping versus dropping
//
// [LeakyBucket.Wait] is the shaping call: it sleeps until the request conforms,
// so traffic is smoothed rather than rejected. [LeakyBucket.Allow] drops
// instead, for the cases where waiting is worse than failing — an HTTP handler
// that would rather answer 429 than hold a connection.
//
// The zero value is not usable; construct one with [NewLeakyBucket].
type LeakyBucket struct {
	mu sync.Mutex

	// tat is the theoretical arrival time of the next conforming request.
	tat time.Time

	interval  time.Duration // one emission: 1/rate
	tolerance time.Duration // how far ahead of the TAT a request may arrive
	capacity  int
	now       func() time.Time
}

// LeakyBucketOption configures a [LeakyBucket].
type LeakyBucketOption func(*leakyBucketConfig)

type leakyBucketConfig struct {
	now func() time.Time
}

// WithLeakyClock overrides the time source, which makes pacing deterministic in
// tests.
func WithLeakyClock(now func() time.Time) LeakyBucketOption {
	return func(c *leakyBucketConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// NewLeakyBucket returns a [LeakyBucket] admitting one request every interval,
// allowing up to capacity back-to-back from idle.
//
// A capacity below one is raised to one: a bucket that admits nothing is a
// configuration error, and silently refusing everything is the least helpful
// way to report it.
//
//	// one request per second, strictly evenly spaced
//	lb := ratelimiter.NewLeakyBucket(time.Second, 1)
//
//	// 60 per minute, allowing 5 back-to-back after a quiet period
//	lb := ratelimiter.NewLeakyBucket(time.Minute/60, 5)
func NewLeakyBucket(interval time.Duration, capacity int, opts ...LeakyBucketOption) *LeakyBucket {
	cfg := leakyBucketConfig{now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}

	if capacity < 1 {
		capacity = 1
	}

	if interval < 0 {
		interval = 0
	}

	return &LeakyBucket{
		interval:  interval,
		tolerance: time.Duration(capacity) * interval,
		capacity:  capacity,
		now:       cfg.now,
	}
}

// NewLeakyBucketFunc returns a factory suitable for [NewBucketLimiter], giving
// every key its own independently paced bucket.
//
//	// each client IP gets one request per second, no bursting
//	newLimiter := ratelimiter.NewLeakyBucketFunc(time.Second, 1)
//	bl := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
func NewLeakyBucketFunc(interval time.Duration, capacity int, opts ...LeakyBucketOption) func() Limiter {
	return func() Limiter {
		return NewLeakyBucket(interval, capacity, opts...)
	}
}

// Burst implements [Limiter]. It reports the capacity: how many requests may be
// taken back-to-back from an idle bucket.
func (b *LeakyBucket) Burst() int { return b.capacity }

// Allow implements [Limiter]. It admits the request if it conforms, and never
// blocks.
func (b *LeakyBucket) Allow() bool {
	ok, _ := b.reserve(true)

	return ok
}

// Wait implements [Limiter]. It sleeps until the request conforms, which is how
// a leaky bucket SHAPES traffic rather than dropping it, and returns ctx's
// error if the context ends first.
//
// A request that cannot conform before ctx's deadline does not consume the
// slot: the reservation is rolled back, so a caller who gives up does not make
// the next caller wait for a request that never happened.
func (b *LeakyBucket) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ok, delay := b.reserve(true)
	if ok {
		return nil
	}

	// reserve reported the wait without taking the slot; take it by waiting.
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		// The delay has elapsed, so this request now conforms. Claim it.
		if ok, _ := b.reserve(true); ok {
			return nil
		}

		// Another goroutine took the slot while this one slept. Recurse rather
		// than spin: the next delay is bounded by one interval.
		return b.Wait(ctx)
	}
}

// Reserve implements [Reserver].
//
// Unlike a window counter, a leaky bucket CAN give a slot back: the TAT is a
// single instant, so cancelling is rewinding it by one interval. That makes
// Retry-After exact and makes Cancel meaningful — a rejected request does not
// have to count against the caller.
func (b *LeakyBucket) Reserve() Reservation {
	ok, delay := b.reserve(true)

	return &leakyReservation{bucket: b, ok: ok, delay: delay, taken: ok}
}

// reserve is the GCRA step. When commit is true and the request conforms, the
// TAT is advanced. It returns whether the request conforms and, when it does
// not, how long until it would.
func (b *LeakyBucket) reserve(commit bool) (ok bool, delay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()

	// An interval of zero means no pacing at all: admit everything.
	if b.interval == 0 {
		return true, 0
	}

	tat := b.tat
	if tat.Before(now) {
		tat = now
	}

	next := tat.Add(b.interval)

	// The earliest instant at which this request conforms. Arriving before it
	// means the caller is going faster than the bucket drains.
	allowAt := next.Add(-b.tolerance)

	if now.Before(allowAt) {
		return false, allowAt.Sub(now)
	}

	if commit {
		b.tat = next
	}

	return true, 0
}

// rewind returns one interval to the bucket. It never moves the TAT into the
// past relative to now, so cancelling a reservation cannot hand out extra
// budget that was never scheduled.
func (b *LeakyBucket) rewind() {
	b.mu.Lock()
	defer b.mu.Unlock()

	rewound := b.tat.Add(-b.interval)

	now := b.now()
	if rewound.Before(now) {
		rewound = now
	}

	b.tat = rewound
}

// leakyReservation is a [Reservation] over a leaky bucket slot.
type leakyReservation struct {
	bucket *LeakyBucket
	delay  time.Duration
	ok     bool
	taken  bool
	once   sync.Once
}

func (r *leakyReservation) OK() bool             { return r.ok }
func (r *leakyReservation) Delay() time.Duration { return r.delay }

// Cancel returns the slot if this reservation took one. It is safe to call more
// than once; only the first call has an effect, so a defer plus an explicit
// call cannot double-refund.
func (r *leakyReservation) Cancel() {
	if !r.taken {
		return
	}

	r.once.Do(r.bucket.rewind)
}

var (
	_ Limiter  = (*LeakyBucket)(nil)
	_ Reserver = (*LeakyBucket)(nil)
)
