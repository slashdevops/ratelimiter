package ratelimiter

import (
	"context"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestLeakyBucketPacesStrictly is the property that separates a leaky bucket
// from a token bucket, and the reason to have one at all.
//
// A token bucket configured for 60/min admits all 60 in the first second and
// nothing for the rest of the minute. In front of a dependency with its own
// per-second quota, that burst is the thing that breaks you. A leaky bucket at
// capacity 1 admits one, then one per interval, for ever.
func TestLeakyBucketPacesStrictly(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 1, WithLeakyClock(clock))

	if !b.Allow() {
		t.Fatal("the first request from an idle bucket must be admitted")
	}

	if b.Allow() {
		t.Error("a second request in the same instant was admitted; capacity 1 means no burst at all")
	}

	// Not quite an interval later: still too early.
	advance(999 * time.Millisecond)

	if b.Allow() {
		t.Error("a request 1ms before the interval elapsed was admitted")
	}

	advance(time.Millisecond)

	if !b.Allow() {
		t.Error("a request exactly one interval later was refused")
	}
}

func TestLeakyBucketCapacityAllowsABurstThenPaces(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 3, WithLeakyClock(clock))

	for i := range 3 {
		if !b.Allow() {
			t.Fatalf("request %d refused; capacity 3 must admit three back-to-back from idle", i+1)
		}
	}

	if b.Allow() {
		t.Error("a fourth back-to-back request was admitted against a capacity of three")
	}

	// From here it paces at one per interval rather than refilling the burst.
	advance(time.Second)

	if !b.Allow() {
		t.Error("one interval later, one request should be admitted")
	}

	if b.Allow() {
		t.Error("two requests were admitted after a single interval; the burst must not refill in one step")
	}
}

// TestLeakyBucketRetryAfterIsExact: the amount a request is early by IS the
// delay, which is what makes Retry-After exact rather than a guess. A window
// counter can only report the distance to the window edge.
func TestLeakyBucketRetryAfterIsExact(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 1, WithLeakyClock(clock))

	if !b.Allow() {
		t.Fatal("the first request should be admitted")
	}

	advance(400 * time.Millisecond)

	res := b.Reserve()
	if res.OK() {
		t.Fatal("a request 400ms into a 1s interval should not conform")
	}

	if got, want := res.Delay(), 600*time.Millisecond; got != want {
		t.Errorf("Delay() = %v, want %v — exactly the remainder of the interval", got, want)
	}
}

// TestLeakyBucketCancelReturnsTheSlot: unlike a window counter, a leaky bucket
// CAN give a slot back, because the TAT is a single instant and cancelling is
// rewinding it. A rejected request therefore need not count against the caller.
func TestLeakyBucketCancelReturnsTheSlot(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 1, WithLeakyClock(clock))

	res := b.Reserve()
	if !res.OK() {
		t.Fatal("the first reservation should conform")
	}

	// Without the rollback this is refused: the slot is already spent.
	res.Cancel()

	if !b.Allow() {
		t.Error("the slot was not returned by Cancel")
	}
}

func TestLeakyBucketCancelIsIdempotent(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))

	// Capacity 3, not 1. At capacity 1 the clamp inside rewind — which refuses
	// to move the TAT into the past — masks a non-idempotent Cancel entirely,
	// so a test written that way passes whether or not the guard exists. It is
	// only when the TAT is several intervals ahead that repeated cancels can
	// refund slots that were never taken. Found by mutating the guard away and
	// watching the test still pass.
	b := NewLeakyBucket(time.Second, 3, WithLeakyClock(clock))

	// Spend the whole capacity, so the TAT is three intervals ahead.
	res := b.Reserve()
	if !res.OK() {
		t.Fatal("the first reservation should conform")
	}

	for range 2 {
		if !b.Allow() {
			t.Fatal("the capacity should be spendable")
		}
	}

	if b.Allow() {
		t.Fatal("the bucket should be exhausted")
	}

	// Cancel the ONE reservation three times.
	res.Cancel()
	res.Cancel()
	res.Cancel()

	if !b.Allow() {
		t.Fatal("the cancelled slot was not returned")
	}

	// Exactly one slot came back, not three.
	if b.Allow() {
		t.Error("cancelling one reservation three times refunded more than one slot; that hands out budget the bucket never scheduled")
	}
}

func TestLeakyBucketCancelOnARefusedReservationDoesNothing(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 1, WithLeakyClock(clock))

	if !b.Allow() {
		t.Fatal("setup")
	}

	res := b.Reserve()
	if res.OK() {
		t.Fatal("this reservation should have been refused")
	}

	// It never took a slot, so returning one would be inventing budget.
	res.Cancel()

	if b.Allow() {
		t.Error("cancelling a refused reservation handed out a slot")
	}
}

// TestLeakyBucketWaitShapes: Wait is the shaping call. It sleeps until the
// request conforms rather than dropping it, which is the whole point of using a
// leaky bucket in front of something with its own quota.
func TestLeakyBucketWaitShapes(t *testing.T) {
	t.Parallel()

	// A real clock here: the point is that Wait actually sleeps.
	b := NewLeakyBucket(30*time.Millisecond, 1)

	if !b.Allow() {
		t.Fatal("the first request should be admitted")
	}

	start := time.Now()

	if err := b.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("Wait returned after %v; it should have paced to the interval rather than admitting immediately", elapsed)
	}
}

func TestLeakyBucketWaitHonoursContext(t *testing.T) {
	t.Parallel()

	b := NewLeakyBucket(time.Hour, 1)

	if !b.Allow() {
		t.Fatal("setup")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()

	if err := b.Wait(ctx); err == nil {
		t.Fatal("Wait returned nil despite an expired context")
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Wait blocked %v past its context; it must not wait out the interval", elapsed)
	}
}

func TestLeakyBucketWaitReturnsImmediatelyOnADeadContext(t *testing.T) {
	t.Parallel()

	b := NewLeakyBucket(time.Hour, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := b.Wait(ctx); err == nil {
		t.Error("Wait admitted a request on an already-cancelled context")
	}
}

func TestLeakyBucketNormalisesConstruction(t *testing.T) {
	t.Parallel()

	// A capacity below one would admit nothing at all. Refusing every request
	// for ever is the least helpful way to report a configuration error.
	b := NewLeakyBucket(time.Second, 0)
	if got := b.Burst(); got != 1 {
		t.Errorf("Burst() = %d, want 1; a capacity below one is raised to one", got)
	}

	if !b.Allow() {
		t.Error("a bucket built with capacity 0 admits nothing")
	}

	// A zero interval means no pacing.
	unpaced := NewLeakyBucket(0, 1)
	for range 100 {
		if !unpaced.Allow() {
			t.Fatal("a zero interval should admit everything")
		}
	}
}

func TestLeakyBucketIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 50, WithLeakyClock(clock))

	var (
		mu      sync.Mutex
		allowed int
	)

	var wg sync.WaitGroup

	for range 200 {
		wg.Go(func() {
			if b.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	// The clock never moves, so exactly the capacity should get through — no
	// more (a lost update) and no fewer (a spurious refusal).
	if allowed != 50 {
		t.Errorf("allowed %d of 200 concurrent requests, want exactly 50 (the capacity)", allowed)
	}
}

func TestLeakyBucketFuncBuildsIndependentBuckets(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))

	bl := NewBucketLimiter(
		NewLeakyBucketFunc(time.Second, 1, WithLeakyClock(clock)),
		time.Minute,
		NewInMemoryStorage[string, Limiter](),
	)
	defer bl.Close()

	if !bl.GetOrAdd("alice").Allow() {
		t.Fatal("alice's first request should be admitted")
	}

	if bl.GetOrAdd("alice").Allow() {
		t.Error("alice got two in one instant against capacity 1")
	}

	if !bl.GetOrAdd("bob").Allow() {
		t.Error("bob was paced by alice's request; buckets must be independent")
	}
}

// TestLeakyBucketAndTokenBucketAgreeOnTheSameParameters pins the claim the
// documentation makes, because it is the claim most easily lost.
//
// A leaky bucket and a token bucket are DUALS: same rate, same capacity, same
// admissions. The comparison usually seen in the wild — "one bursts, the other
// paces" — is rigged by giving the two sides different capacities.
//
// This matters as a test rather than a footnote: an earlier draft of
// LEAKY_BUCKET.md and the README both made the stronger claim, and it was only
// measuring the two side by side that showed it was false. If someone later
// "fixes" the leaky bucket to behave differently at the same settings, this
// fails and the docs stay true.
func TestLeakyBucketAndTokenBucketAgreeOnTheSameParameters(t *testing.T) {
	t.Parallel()

	const (
		perSecond = 10
		capacity  = 1
	)

	tb := RateLimiter{Limiter: rate.NewLimiter(rate.Limit(perSecond), capacity)}
	lb := NewLeakyBucket(time.Second/perSecond, capacity)

	drain := func(l Limiter) int {
		admitted := 0

		for range 10 {
			if l.Allow() {
				admitted++
			}
		}

		return admitted
	}

	if got, want := drain(tb), drain(lb); got != want {
		t.Errorf("first burst: token bucket admitted %d, leaky bucket %d — with equal parameters they must agree", got, want)
	}

	time.Sleep(250 * time.Millisecond)

	if got, want := drain(tb), drain(lb); got != want {
		t.Errorf("after 250ms: token bucket admitted %d, leaky bucket %d", got, want)
	}
}

func TestLeakyBucketNegativeIntervalIsClamped(t *testing.T) {
	t.Parallel()

	// A negative interval would make the TAT run backwards, handing out
	// unlimited budget. Clamping to zero degrades to "no pacing", which is at
	// least a state the caller can observe.
	b := NewLeakyBucket(-time.Second, 1)
	for range 10 {
		if !b.Allow() {
			t.Fatal("a negative interval should degrade to no pacing, not to refusing everything")
		}
	}
}

// TestLeakyBucketWaitRetriesWhenAnotherCallerTakesTheSlot covers the race Wait
// has to survive: it sleeps for the reported delay, but by the time it wakes,
// another goroutine may have taken the slot it was waiting for. It must wait
// again rather than admit a request that no longer conforms.
func TestLeakyBucketWaitRetriesWhenAnotherCallerTakesTheSlot(t *testing.T) {
	t.Parallel()

	b := NewLeakyBucket(25*time.Millisecond, 1)

	// Exhaust it, then have several goroutines all wait. Each must be paced;
	// none may jump the queue.
	if !b.Allow() {
		t.Fatal("setup")
	}

	const waiters = 4

	start := time.Now()

	var wg sync.WaitGroup

	for range waiters {
		wg.Go(func() {
			if err := b.Wait(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}

	wg.Wait()

	// Four waiters at one per 25ms cannot finish faster than three intervals.
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Errorf("%d waiters completed in %v; they must be paced, not admitted together", waiters, elapsed)
	}
}

func TestLeakyBucketRewindNeverMovesTheTATIntoThePast(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewLeakyBucket(time.Second, 1, WithLeakyClock(clock))

	res := b.Reserve()
	if !res.OK() {
		t.Fatal("setup")
	}

	// Let the reservation age past its own interval, then cancel. Rewinding
	// blindly would put the TAT a second in the past and hand out a slot that
	// was never scheduled.
	advance(5 * time.Second)
	res.Cancel()

	if !b.Allow() {
		t.Fatal("one slot should be available")
	}

	if b.Allow() {
		t.Error("cancelling an aged reservation granted more than one slot")
	}
}
