package ratelimiter

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Default circuit-breaker settings for [BackendLimiter]. Small numbers on
// purpose: being wrong in the pessimistic direction costs a few seconds of
// local-only limiting, and being wrong in the optimistic direction costs a
// timeout on every request.
const (
	DefaultBackendFailureThreshold = 3
	DefaultBackendCooldown         = 5 * time.Second
	DefaultBackendTimeout          = 100 * time.Millisecond
)

// BackendLimiter is a [Limiter] whose state lives in a [Backend] — an
// in-process map, Valkey, Redis, anything that can count atomically per key.
//
// It is bound to one key, because a Limiter is: Allow and Wait take no
// arguments, so the instance IS the bucket. Build them with
// [NewBackendLimiterFunc] and [WithLimiterFactoryForKey], which is what gives
// each one its key.
//
// # What it adds over calling a Backend directly
//
// Three things, and they are the reason this is in the library rather than
// copied into every consumer:
//
//  1. A LOCAL FALLBACK. A remote backend can be unreachable, and neither
//     answer to that is acceptable on its own: refusing turns a cache blip into
//     a total outage, and allowing deletes the limiter exactly when it is
//     needed. With a fallback limiter there is nothing to choose between —
//     there is always a local answer, so an outage degrades to per-process
//     limiting instead of to no limiting or to no service.
//
//  2. A CIRCUIT BREAKER. Falling back is only half a fallback. Without this,
//     every request during an outage pays a failed round trip — connect, wait,
//     time out — before reaching the local answer it was always going to get.
//     The limiter would keep limiting correctly and make the whole service
//     slower by the timeout, for the duration of the outage. After
//     FailureThreshold consecutive failures the backend is skipped entirely
//     until Cooldown elapses, then probed once.
//
//  3. A TIMEOUT. This call sits in front of everything else the process does,
//     so it needs a budget far tighter than a normal query.
//
// # Being in the degraded state must be visible
//
// It is invisible from a request: the service keeps working, keeps limiting,
// and is silently enforcing N times the intended limit across N processes.
// [BackendLimiter.Degraded] reports it, and OnDegraded fires on each
// transition. Alert on the state, not on the error rate — a handful of errors
// is noise, a sustained fallback is the thing somebody has to know about.
type BackendLimiter struct {
	backend  Backend
	key      string
	limit    Limit
	fallback Limiter

	timeout          time.Duration
	failureThreshold int
	cooldown         time.Duration

	logger     *slog.Logger
	onDegraded func(key string, degraded bool)
	clock      func() time.Time
	failures   atomic.Int64
	skipUntil  atomic.Int64 // unix nanos; zero means "ask the backend"
	degraded   atomic.Bool
	// probing is held by the single caller re-testing a failed backend after
	// the cooldown. See shouldAsk for why this is a CAS and not a mutex.
	probing atomic.Bool
}

// BackendLimiterOption configures a [BackendLimiter].
type BackendLimiterOption func(*backendLimiterConfig)

type backendLimiterConfig struct {
	fallback         Limiter
	timeout          time.Duration
	failureThreshold int
	cooldown         time.Duration
	logger           *slog.Logger
	onDegraded       func(key string, degraded bool)
	clock            func() time.Time
}

// WithFallback supplies the limiter consulted when the backend cannot answer.
//
// Strongly recommended, and the reason is in the type: [Backend.Take] returns
// an error, [Limiter.Allow] returns a bool. Without a fallback there is nowhere
// for "I do not know" to go, and the limiter has to invent an answer. With one,
// it does not have to.
//
// When it is absent an unreachable backend ALLOWS the request, because refusing
// would make the backend a hard dependency of every request — the failure mode
// that is worse than the one being prevented. The choice is logged.
func WithFallback(l Limiter) BackendLimiterOption {
	return func(c *backendLimiterConfig) { c.fallback = l }
}

// WithBackendTimeout bounds a single Take. Defaults to
// [DefaultBackendTimeout]. A value <= 0 is ignored; use
// [WithoutBackendTimeout] to remove the bound.
func WithBackendTimeout(d time.Duration) BackendLimiterOption {
	return func(c *backendLimiterConfig) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithoutBackendTimeout removes the per-call deadline.
//
// Use it for an IN-PROCESS backend, where the timeout can only cost and never
// help: context.WithTimeout allocates and arms a timer on every request, which
// measurably dominates a backend that answers in ~100ns. Measured with
// BenchmarkBackendLimiterAllow at the time this was added:
//
//	with a timeout:     391 ns/op
//	without:            110 ns/op
//
// Never use it for a network backend. Without a deadline a hung datastore
// blocks the request that touched it for however long the client's own
// timeouts allow, which is the failure the circuit breaker exists to bound —
// and the breaker cannot trip on a call that has not returned.
func WithoutBackendTimeout() BackendLimiterOption {
	return func(c *backendLimiterConfig) { c.timeout = 0 }
}

// WithCircuitBreaker sets how many consecutive failures skip the backend, and
// for how long. A threshold of zero or less disables the breaker, which means
// accepting a failed round trip on every request during an outage.
func WithCircuitBreaker(threshold int, cooldown time.Duration) BackendLimiterOption {
	return func(c *backendLimiterConfig) {
		c.failureThreshold = threshold
		if cooldown > 0 {
			c.cooldown = cooldown
		}
	}
}

// WithBackendLogger sets the logger used for backend failures and for
// transitions in and out of the degraded state. Defaults to [slog.Default].
func WithBackendLogger(l *slog.Logger) BackendLimiterOption {
	return func(c *backendLimiterConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithOnDegraded registers a callback fired on each transition into or out of
// the degraded state. Use it to drive a gauge.
func WithOnDegraded(f func(key string, degraded bool)) BackendLimiterOption {
	return func(c *backendLimiterConfig) { c.onDegraded = f }
}

// WithBackendClock overrides the time source, for tests.
func WithBackendClock(now func() time.Time) BackendLimiterOption {
	return func(c *backendLimiterConfig) {
		if now != nil {
			c.clock = now
		}
	}
}

// NewBackendLimiter returns a [BackendLimiter] for one key.
//
// Prefer [NewBackendLimiterFunc] with [WithLimiterFactoryForKey]: a
// BucketLimiter needs a factory, and building limiters by hand means keeping
// the key and the limiter in step yourself.
func NewBackendLimiter(b Backend, key string, limit Limit, opts ...BackendLimiterOption) *BackendLimiter {
	cfg := backendLimiterConfig{
		timeout:          DefaultBackendTimeout,
		failureThreshold: DefaultBackendFailureThreshold,
		cooldown:         DefaultBackendCooldown,
		logger:           slog.Default(),
		clock:            time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &BackendLimiter{
		backend:          b,
		key:              key,
		limit:            limit,
		fallback:         cfg.fallback,
		timeout:          cfg.timeout,
		failureThreshold: cfg.failureThreshold,
		cooldown:         cfg.cooldown,
		logger:           cfg.logger,
		onDegraded:       cfg.onDegraded,
		clock:            cfg.clock,
	}
}

// NewBackendLimiterFunc returns a factory suitable for
// [WithLimiterFactoryForKey], building one [BackendLimiter] per key.
//
//	backend := ratelimiter.NewMemoryBackend()          // or a Valkey one
//	limit   := ratelimiter.Limit{Requests: 100, Period: time.Minute}
//
//	bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
//		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
//		ratelimiter.WithLimiterFactoryForKey(
//			ratelimiter.NewBackendLimiterFunc(backend, limit,
//				ratelimiter.WithFallback(localLimiter)),
//		),
//	)
func NewBackendLimiterFunc(b Backend, limit Limit, opts ...BackendLimiterOption) func(string) Limiter {
	return func(key string) Limiter {
		return NewBackendLimiter(b, key, limit, opts...)
	}
}

// Burst implements [Limiter].
func (l *BackendLimiter) Burst() int { return l.limit.Capacity() }

// Degraded reports whether the backend is currently being skipped. Alert on
// this rather than on an error count: it is the state that changes what the
// limiter enforces.
func (l *BackendLimiter) Degraded() bool { return l.degraded.Load() }

// Allow implements [Limiter].
func (l *BackendLimiter) Allow() bool {
	return l.take(context.Background(), 1).Allowed
}

// Wait implements [Limiter]. It sleeps until the backend would grant a token or
// ctx is done, whichever comes first.
func (l *BackendLimiter) Wait(ctx context.Context) error {
	for {
		d := l.take(ctx, 1)
		if d.Allowed {
			return nil
		}

		delay := d.RetryAfter
		if delay <= 0 {
			delay = l.limit.Period
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()

			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Reserve implements [Reserver], so callers such as HTTP middleware can emit
// Retry-After and RateLimit-Reset for a backend-managed limit.
//
// The reservation is NOT cancellable in the way a token bucket's is: a window
// counter has no way to hand a token back that is distinguishable from
// spending one less. Cancel is therefore a no-op, and that is stated rather
// than hidden — a caller that rejects a request has still consumed from the
// window. Over-counting refused requests is the conservative direction, and
// closing it would cost a second round trip on the rejection path.
func (l *BackendLimiter) Reserve() Reservation {
	d := l.take(context.Background(), 1)

	return backendReservation{ok: d.Allowed, delay: d.RetryAfter}
}

func (l *BackendLimiter) take(ctx context.Context, cost int) Decision {
	ask, isProbe := l.shouldAsk()
	if !ask {
		return l.fallbackDecision()
	}

	if isProbe {
		// Release the probe slot however this call ends, so a failed probe
		// does not wedge the breaker shut until the process restarts.
		defer l.probing.Store(false)
	}

	if l.timeout > 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}

	d, err := l.backend.Take(ctx, l.key, l.limit, cost)
	if err != nil {
		l.recordFailure(err)

		return l.fallbackDecision()
	}

	l.recordSuccess()

	return d
}

// shouldAsk reports whether the backend is worth calling, and whether this
// caller is the one probing a failed backend.
//
// Once the failure threshold is crossed the backend is skipped until the
// cooldown elapses. Exactly one caller then gets through to re-test it, and the
// rest keep using the fallback until it reports — a cooldown expiry must not
// release every in-flight request at a datastore that is, by hypothesis,
// already unwell.
//
// The single-prober guarantee is a CAS on probing, not a mutex held across the
// call. A mutex looks right and is not: with `defer Unlock()` the lock is
// released when this function returns, nanoseconds later and long before the
// backend answers, so every caller acquires it in turn and all of them probe.
// TestBackendLimiterProbesOnceUnderConcurrency measured exactly that — 20 of 20
// callers reached the backend.
func (l *BackendLimiter) shouldAsk() (ask, isProbe bool) {
	if l.failureThreshold <= 0 {
		return true, false
	}

	if l.failures.Load() < int64(l.failureThreshold) {
		return true, false
	}

	if l.clock().UnixNano() < l.skipUntil.Load() {
		return false, false
	}

	// Cooldown elapsed. The winner of this CAS holds the probe slot until its
	// call finishes; see take.
	if !l.probing.CompareAndSwap(false, true) {
		return false, false
	}

	return true, true
}

func (l *BackendLimiter) recordFailure(err error) {
	n := l.failures.Add(1)
	l.skipUntil.Store(l.clock().Add(l.cooldown).UnixNano())

	if l.failureThreshold > 0 && n == int64(l.failureThreshold) && l.degraded.CompareAndSwap(false, true) {
		l.logger.Warn("rate limiter backend unavailable; falling back to the local limiter",
			"key", l.key,
			"consecutive_failures", n,
			"cooldown", l.cooldown,
			"effect", "the limit is now enforced per process, not globally",
			"error", err,
		)

		if l.onDegraded != nil {
			l.onDegraded(l.key, true)
		}

		return
	}

	l.logger.Debug("rate limiter backend call failed", "key", l.key, "error", err)
}

func (l *BackendLimiter) recordSuccess() {
	l.failures.Store(0)
	l.skipUntil.Store(0)

	if l.degraded.CompareAndSwap(true, false) {
		l.logger.Info("rate limiter backend recovered; the limit is global again", "key", l.key)

		if l.onDegraded != nil {
			l.onDegraded(l.key, false)
		}
	}
}

// fallbackDecision answers from the local limiter, or allows when there is
// none. See [WithFallback] for why allowing is the default.
func (l *BackendLimiter) fallbackDecision() Decision {
	if l.fallback == nil {
		return Decision{Allowed: true, Remaining: -1}
	}

	if !l.fallback.Allow() {
		return Decision{Allowed: false, Remaining: 0, RetryAfter: l.limit.Period}
	}

	return Decision{Allowed: true, Remaining: -1}
}

// backendReservation is a Reservation over a decision already made. See
// [BackendLimiter.Reserve] for why Cancel does nothing.
type backendReservation struct {
	ok    bool
	delay time.Duration
}

func (r backendReservation) OK() bool             { return r.ok }
func (r backendReservation) Delay() time.Duration { return r.delay }
func (r backendReservation) Cancel()              {}

var (
	_ Limiter  = (*BackendLimiter)(nil)
	_ Reserver = (*BackendLimiter)(nil)
)
