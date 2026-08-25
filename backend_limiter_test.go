package ratelimiter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBackendDown = errors.New("backend unreachable")

// scriptedBackend answers however the test tells it to, and counts calls —
// which is what the circuit-breaker tests actually assert, because the OUTCOME
// is identical whether or not the breaker works. Only the call count differs.
type scriptedBackend struct {
	mu      sync.Mutex
	fail    bool
	allowed bool
	calls   int
}

func (b *scriptedBackend) Take(context.Context, string, Limit, int) (Decision, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.calls++

	if b.fail {
		return Decision{}, errBackendDown
	}

	return Decision{Allowed: b.allowed, Remaining: 1}, nil
}

func (b *scriptedBackend) setFail(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.fail = v
}

func (b *scriptedBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.calls
}

// alwaysLimiter is a fallback whose answer the test controls.
type alwaysLimiter struct {
	allow bool
	calls atomic.Int64
}

func (l *alwaysLimiter) Burst() int { return 1 }
func (l *alwaysLimiter) Allow() bool {
	l.calls.Add(1)

	return l.allow
}
func (l *alwaysLimiter) Wait(context.Context) error { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestBackendLimiterAnswersFromTheBackend(t *testing.T) {
	t.Parallel()

	for _, allowed := range []bool{true, false} {
		b := &scriptedBackend{allowed: allowed}
		l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
			WithBackendLogger(quietLogger()))

		if got := l.Allow(); got != allowed {
			t.Errorf("Allow() = %v, want %v", got, allowed)
		}
	}
}

// TestBackendLimiterFallsBackWhenTheBackendFails is the property that makes a
// remote backend safe to depend on: neither answer to "the backend is down" is
// acceptable on its own. Refusing turns a cache blip into a total outage;
// allowing deletes the limiter exactly when it is needed. With a fallback there
// is nothing to choose between.
func TestBackendLimiterFallsBackWhenTheBackendFails(t *testing.T) {
	t.Parallel()

	t.Run("the fallback refuses", func(t *testing.T) {
		t.Parallel()

		b := &scriptedBackend{fail: true}
		fb := &alwaysLimiter{allow: false}

		l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
			WithFallback(fb), WithBackendLogger(quietLogger()))

		if l.Allow() {
			t.Error("allowed while the backend was down and the fallback refused")
		}

		if fb.calls.Load() == 0 {
			t.Error("the fallback was never consulted")
		}
	})

	t.Run("the fallback allows", func(t *testing.T) {
		t.Parallel()

		b := &scriptedBackend{fail: true}
		fb := &alwaysLimiter{allow: true}

		l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
			WithFallback(fb), WithBackendLogger(quietLogger()))

		if !l.Allow() {
			t.Error("refused while the fallback allowed; a backend outage must not reject traffic")
		}
	})

	t.Run("no fallback allows, rather than making the backend a hard dependency", func(t *testing.T) {
		t.Parallel()

		b := &scriptedBackend{fail: true}
		l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
			WithBackendLogger(quietLogger()))

		if !l.Allow() {
			t.Error("refused with no fallback configured; that would make every request depend on the backend being up")
		}
	})
}

// TestBackendLimiterStopsCallingAFailedBackend asserts the CALL COUNT, not the
// outcome, and that is the whole point: with or without the breaker the request
// is decided the same way. What differs is whether every request first pays a
// failed round trip — connect, wait, time out — which during an outage makes
// the whole service slower by the timeout while the limiter keeps working.
func TestBackendLimiterStopsCallingAFailedBackend(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := &scriptedBackend{fail: true}
	fb := &alwaysLimiter{allow: true}

	l := NewBackendLimiter(b, "k", Limit{Requests: 100, Period: time.Second},
		WithFallback(fb),
		WithCircuitBreaker(3, 5*time.Second),
		WithBackendClock(clock),
		WithBackendLogger(quietLogger()),
	)

	for range 50 {
		l.Allow()
	}

	if got := b.callCount(); got != 3 {
		t.Errorf("backend called %d times across 50 requests, want 3; after the threshold it must be skipped entirely", got)
	}

	if !l.Degraded() {
		t.Error("Degraded() is false while the backend is being skipped; the state is invisible from a request and has to be reportable")
	}
}

func TestBackendLimiterProbesAfterTheCooldownAndRecovers(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := &scriptedBackend{fail: true}
	fb := &alwaysLimiter{allow: true}

	var transitions []bool

	var mu sync.Mutex

	l := NewBackendLimiter(b, "k", Limit{Requests: 100, Period: time.Second},
		WithFallback(fb),
		WithCircuitBreaker(3, 5*time.Second),
		WithBackendClock(clock),
		WithBackendLogger(quietLogger()),
		WithOnDegraded(func(_ string, d bool) {
			mu.Lock()
			transitions = append(transitions, d)
			mu.Unlock()
		}),
	)

	for range 10 {
		l.Allow()
	}

	if got := b.callCount(); got != 3 {
		t.Fatalf("backend called %d times, want 3", got)
	}

	// Inside the cooldown nothing gets through.
	advance(2 * time.Second)
	l.Allow()

	if got := b.callCount(); got != 3 {
		t.Errorf("backend called %d times inside the cooldown, want 3", got)
	}

	// Past it, exactly one probe.
	advance(4 * time.Second)
	b.setFail(false)
	b.allowed = true

	if !l.Allow() {
		t.Error("the probe should have been allowed once the backend recovered")
	}

	if got := b.callCount(); got != 4 {
		t.Errorf("backend called %d times after the cooldown, want 4", got)
	}

	if l.Degraded() {
		t.Error("still degraded after a successful probe")
	}

	// And it is asking again, every time.
	l.Allow()

	if got := b.callCount(); got != 5 {
		t.Errorf("backend called %d times after recovery, want 5; a success must reset the breaker", got)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(transitions) != 2 || !transitions[0] || transitions[1] {
		t.Errorf("degraded transitions = %v, want [true false]; a gauge needs both edges", transitions)
	}
}

// TestBackendLimiterReportsASecondOutage covers what a recovery has to leave
// behind. The degraded transition fires when the consecutive-failure count
// REACHES the threshold, so if a success does not reset that count the second
// outage counts 4, 5, 6... and never equals it again: the limiter would go
// degraded silently, for ever after, having reported it correctly exactly once.
//
// A monitoring signal that works the first time and never again is worse than
// none, because it is trusted.
func TestBackendLimiterReportsASecondOutage(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := &scriptedBackend{fail: true}

	var transitions []bool

	var mu sync.Mutex

	l := NewBackendLimiter(b, "k", Limit{Requests: 100, Period: time.Second},
		WithFallback(&alwaysLimiter{allow: true}),
		WithCircuitBreaker(3, 5*time.Second),
		WithBackendClock(clock),
		WithBackendLogger(quietLogger()),
		WithOnDegraded(func(_ string, d bool) {
			mu.Lock()
			transitions = append(transitions, d)
			mu.Unlock()
		}),
	)

	// Outage one.
	for range 5 {
		l.Allow()
	}

	// Recover.
	advance(6 * time.Second)
	b.setFail(false)
	l.Allow()

	// Outage two.
	b.setFail(true)

	for range 10 {
		advance(6 * time.Second) // past each cooldown, so every call reaches the backend
		l.Allow()
	}

	if !l.Degraded() {
		t.Error("Degraded() is false during a second outage")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(transitions) != 3 {
		t.Fatalf("degraded transitions = %v, want [true false true]; a recovery must reset the failure count or the second outage is never reported", transitions)
	}

	if !transitions[0] || transitions[1] || !transitions[2] {
		t.Errorf("degraded transitions = %v, want [true false true]", transitions)
	}
}

func TestBackendLimiterWithoutABreakerKeepsCalling(t *testing.T) {
	t.Parallel()

	b := &scriptedBackend{fail: true}
	l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
		WithFallback(&alwaysLimiter{allow: true}),
		WithCircuitBreaker(0, 0), // disabled
		WithBackendLogger(quietLogger()),
	)

	for range 10 {
		l.Allow()
	}

	if got := b.callCount(); got != 10 {
		t.Errorf("backend called %d times with the breaker disabled, want 10", got)
	}
}

func TestBackendLimiterReserveReportsTheDelay(t *testing.T) {
	t.Parallel()

	b := &scriptedBackend{allowed: false}
	l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
		WithBackendLogger(quietLogger()))

	var r Reserver = l

	res := r.Reserve()
	if res.OK() {
		t.Error("Reserve reported OK for a refused decision")
	}

	// Cancel is a no-op by design; calling it must not panic and must not
	// change the answer.
	res.Cancel()

	if res.OK() {
		t.Error("Cancel changed the reservation")
	}
}

func TestBackendLimiterBurstReportsCapacity(t *testing.T) {
	t.Parallel()

	l := NewBackendLimiter(&scriptedBackend{}, "k",
		Limit{Requests: 10, Period: time.Second}, WithBackendLogger(quietLogger()))

	if got := l.Burst(); got != 10 {
		t.Errorf("Burst() = %d, want 10 (Requests, since Burst is unset)", got)
	}

	l = NewBackendLimiter(&scriptedBackend{}, "k",
		Limit{Requests: 10, Period: time.Second, Burst: 25}, WithBackendLogger(quietLogger()))

	if got := l.Burst(); got != 25 {
		t.Errorf("Burst() = %d, want 25", got)
	}
}

// TestBackendLimiterThroughBucketLimiter is the whole assembly: a BucketLimiter
// handing out one key-bound BackendLimiter per key, which is how a consumer
// actually wires this.
func TestBackendLimiterThroughBucketLimiter(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	backend := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer backend.Close()

	limit := Limit{Requests: 2, Period: time.Second}

	bl := NewBucketLimiter(nil, time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(NewBackendLimiterFunc(backend, limit,
			WithBackendLogger(quietLogger()))),
	)
	defer bl.Close()

	for i := range 2 {
		if !bl.GetOrAdd("alice").Allow() {
			t.Fatalf("alice request %d refused inside her budget", i+1)
		}
	}

	if bl.GetOrAdd("alice").Allow() {
		t.Error("alice's third request was allowed against a budget of two")
	}

	if !bl.GetOrAdd("bob").Allow() {
		t.Error("bob was refused because alice spent her budget")
	}
}

func TestLimitHelpers(t *testing.T) {
	t.Parallel()

	l := Limit{Requests: 300, Period: time.Minute}
	if got := l.Rate(); got != 5 {
		t.Errorf("Rate() = %v, want 5 (300 per minute)", got)
	}

	if got := l.Capacity(); got != 300 {
		t.Errorf("Capacity() = %d, want 300 when Burst is unset", got)
	}

	if got := (Limit{Requests: 1, Period: 0}).Rate(); got != 0 {
		t.Errorf("Rate() with a zero period = %v, want 0 rather than a division by zero", got)
	}
}

func TestBackendLimiterWaitReturnsWhenAllowed(t *testing.T) {
	t.Parallel()

	l := NewBackendLimiter(&scriptedBackend{allowed: true}, "k",
		Limit{Requests: 1, Period: time.Second},
		WithBackendLogger(quietLogger()), WithBackendTimeout(time.Second))

	if err := l.Wait(context.Background()); err != nil {
		t.Errorf("Wait returned %v for an allowed request", err)
	}
}

// TestBackendLimiterWaitHonoursContext: Wait must not outlive its context. A
// limiter that blocks past cancellation holds a request handler open long after
// the client has gone.
func TestBackendLimiterWaitHonoursContext(t *testing.T) {
	t.Parallel()

	l := NewBackendLimiter(&scriptedBackend{allowed: false}, "k",
		Limit{Requests: 1, Period: time.Hour},
		WithBackendLogger(quietLogger()), WithoutBackendTimeout())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()

	err := l.Wait(ctx)
	if err == nil {
		t.Fatal("Wait returned nil for a permanently refused limiter")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait returned %v, want the context's error", err)
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Wait blocked for %v past its context; it must not wait out the limit period", elapsed)
	}
}

func TestBackendReservationReportsDelay(t *testing.T) {
	t.Parallel()

	b := &scriptedBackend{}
	l := NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
		WithBackendLogger(quietLogger()))

	// A refusal from the memory backend carries a real RetryAfter.
	mem := NewMemoryBackend(WithMemorySweepInterval(0))
	defer mem.Close()

	limit := Limit{Requests: 1, Period: time.Minute}
	ml := NewBackendLimiter(mem, "k", limit, WithBackendLogger(quietLogger()))

	if !ml.Allow() {
		t.Fatal("the first request should be allowed")
	}

	res := ml.Reserve()
	if res.OK() {
		t.Error("Reserve reported OK past the budget")
	}

	if res.Delay() <= 0 {
		t.Error("a refused reservation must report how long until it would be granted")
	}

	res.Cancel() // no-op by design; must not panic

	_ = l
}

func TestWithoutBackendTimeoutRemovesTheDeadline(t *testing.T) {
	t.Parallel()

	// A backend that reports whether it saw a deadline.
	var sawDeadline atomic.Bool

	b := backendFunc(func(ctx context.Context, _ string, _ Limit, _ int) (Decision, error) {
		_, ok := ctx.Deadline()
		sawDeadline.Store(ok)

		return Decision{Allowed: true}, nil
	})

	NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
		WithBackendLogger(quietLogger()), WithoutBackendTimeout()).Allow()

	if sawDeadline.Load() {
		t.Error("a deadline was applied despite WithoutBackendTimeout")
	}

	NewBackendLimiter(b, "k", Limit{Requests: 1, Period: time.Second},
		WithBackendLogger(quietLogger()), WithBackendTimeout(time.Second)).Allow()

	if !sawDeadline.Load() {
		t.Error("no deadline was applied despite WithBackendTimeout")
	}
}

// backendFunc adapts a function to Backend.
type backendFunc func(context.Context, string, Limit, int) (Decision, error)

func (f backendFunc) Take(ctx context.Context, key string, limit Limit, cost int) (Decision, error) {
	return f(ctx, key, limit, cost)
}

// TestBackendLimiterProbesOnceUnderConcurrency: when the cooldown elapses,
// exactly ONE caller reaches the backend to find out whether it is back.
//
// Letting every in-flight request through at that instant turns each cooldown
// expiry into a thundering herd against a datastore that is, by hypothesis,
// already unwell — and the requests that pile up are precisely the ones the
// fallback could have answered for free.
func TestBackendLimiterProbesOnceUnderConcurrency(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))

	var (
		blocking    atomic.Bool
		probes      atomic.Int64
		inFlight    atomic.Int64
		maxInFlight atomic.Int64
		release     = make(chan struct{})
	)

	b := backendFunc(func(context.Context, string, Limit, int) (Decision, error) {
		probes.Add(1)

		n := inFlight.Add(1)
		defer inFlight.Add(-1)

		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}

		if blocking.Load() {
			<-release
		}

		return Decision{}, errBackendDown
	})

	l := NewBackendLimiter(b, "k", Limit{Requests: 100, Period: time.Second},
		WithFallback(&alwaysLimiter{allow: true}),
		WithCircuitBreaker(1, 5*time.Second),
		WithBackendClock(clock),
		WithBackendLogger(quietLogger()),
		WithoutBackendTimeout(),
	)

	// Trip the breaker with one non-blocking failure.
	l.Allow()

	if got := probes.Load(); got != 1 {
		t.Fatalf("setup: probes = %d, want 1", got)
	}

	// Past the cooldown, race twenty callers at the probe window.
	blocking.Store(true)
	advance(6 * time.Second)

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()

	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() { l.Allow() })
	}

	wg.Wait()

	if got := probes.Load(); got != 2 {
		t.Errorf("probes = %d, want 2 (one to trip the breaker, one to re-test it); a cooldown expiry must not become a thundering herd", got)
	}

	if got := maxInFlight.Load(); got != 1 {
		t.Errorf("max concurrent backend calls = %d, want 1", got)
	}
}

// TestBackendLimiterRetriesAfterAFailedProbe: a probe that fails must not wedge
// the breaker shut.
//
// The single-prober guard is a slot one caller claims. If it is only released
// when the probe SUCCEEDS, a probe that fails keeps it for ever: no later
// caller can claim it, so the backend is never re-tested and the limiter stays
// degraded permanently — including long after the datastore has recovered.
//
// That is worse than the outage it is reacting to, because it does not end when
// the outage does, and nothing about a healthy-looking service says why.
func TestBackendLimiterRetriesAfterAFailedProbe(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := &scriptedBackend{fail: true}

	l := NewBackendLimiter(b, "k", Limit{Requests: 100, Period: time.Second},
		WithFallback(&alwaysLimiter{allow: true}),
		WithCircuitBreaker(1, 5*time.Second),
		WithBackendClock(clock),
		WithBackendLogger(quietLogger()),
	)

	l.Allow() // trips the breaker

	if got := b.callCount(); got != 1 {
		t.Fatalf("setup: backend called %d times, want 1", got)
	}

	// First probe — still down.
	advance(6 * time.Second)
	l.Allow()

	if got := b.callCount(); got != 2 {
		t.Fatalf("first probe: backend called %d times, want 2", got)
	}

	// Second probe, another cooldown later. This is the one that never happens
	// if a failed probe keeps the slot.
	advance(6 * time.Second)
	l.Allow()

	if got := b.callCount(); got != 3 {
		t.Errorf("backend called %d times, want 3; a failed probe must release the slot or the backend is never re-tested", got)
	}

	// And once it recovers, the limiter comes back.
	b.setFail(false)
	b.allowed = true

	advance(6 * time.Second)

	if !l.Allow() {
		t.Error("the limiter never recovered after the backend came back")
	}

	if l.Degraded() {
		t.Error("still degraded after a successful probe")
	}
}
