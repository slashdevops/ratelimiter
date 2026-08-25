package ratelimiter

import (
	"context"
	"sync"
	"testing"
	"time"
)

func fixedClock(t time.Time) (func() time.Time, func(time.Duration)) {
	var mu sync.Mutex

	now := t

	return func() time.Time {
			mu.Lock()
			defer mu.Unlock()

			return now
		}, func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()

			now = now.Add(d)
		}
}

func TestMemoryBackendAllowsExactlyTheBudget(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 5, Period: time.Second}

	for i := range 5 {
		d, err := b.Take(context.Background(), "k", limit, 1)
		if err != nil {
			t.Fatalf("Take %d: %v", i, err)
		}

		if !d.Allowed {
			t.Fatalf("request %d refused inside the budget", i+1)
		}

		if want := 5 - (i + 1); d.Remaining != want {
			t.Errorf("request %d: Remaining = %d, want %d", i+1, d.Remaining, want)
		}
	}

	d, _ := b.Take(context.Background(), "k", limit, 1)
	if d.Allowed {
		t.Error("the sixth request was allowed against a budget of five")
	}

	if d.RetryAfter <= 0 {
		t.Error("a refusal must say how long until it would be granted")
	}
}

func TestMemoryBackendKeysAreIndependent(t *testing.T) {
	t.Parallel()

	clock, _ := fixedClock(time.Unix(0, 0))
	b := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 1, Period: time.Second}

	if d, _ := b.Take(context.Background(), "alice", limit, 1); !d.Allowed {
		t.Fatal("alice's first request should be allowed")
	}

	if d, _ := b.Take(context.Background(), "bob", limit, 1); !d.Allowed {
		t.Error("bob was refused because alice spent her budget; keys must not share one")
	}
}

// TestMemoryBackendSlidingWindowSmoothsTheBoundary is the reason this is not a
// plain fixed window.
//
// A fixed window admits the whole budget at the end of one window and the whole
// budget again at the start of the next — 2x the limit across the boundary, in
// an instant. The weighting carries the previous window's spend forward in
// proportion to how far into the new one the clock is, so the doubling cannot
// happen.
func TestMemoryBackendSlidingWindowSmoothsTheBoundary(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(100, 0))
	b := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 10, Period: time.Second}

	// Spend the whole budget at the very end of a window.
	advance(900 * time.Millisecond)

	for range 10 {
		if d, _ := b.Take(context.Background(), "k", limit, 1); !d.Allowed {
			t.Fatal("the budget should be spendable inside one window")
		}
	}

	// Step just over the boundary. A fixed window would hand back all ten.
	advance(200 * time.Millisecond)

	allowed := 0

	for range 10 {
		if d, _ := b.Take(context.Background(), "k", limit, 1); d.Allowed {
			allowed++
		}
	}

	if allowed >= 10 {
		t.Errorf("allowed %d immediately after the boundary; a fixed window would give 10, which is the 2x doubling this design exists to avoid", allowed)
	}

	if allowed == 0 {
		t.Error("allowed 0 after the boundary; the window should have released some budget")
	}
}

func TestMemoryBackendResetsAfterAFullGap(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 1, Period: time.Second}

	if d, _ := b.Take(context.Background(), "k", limit, 1); !d.Allowed {
		t.Fatal("the first request should be allowed")
	}

	// Two whole windows later nothing is carried forward: a counter shifted
	// into a slot it does not belong in would refuse this.
	advance(3 * time.Second)

	if d, _ := b.Take(context.Background(), "k", limit, 1); !d.Allowed {
		t.Error("a request three windows later was refused; stale counters must be dropped, not shifted")
	}
}

func TestMemoryBackendRefusesANonsenseLimit(t *testing.T) {
	t.Parallel()

	b := NewMemoryBackend(WithMemorySweepInterval(0))
	defer b.Close()

	for name, limit := range map[string]Limit{
		"zero period":   {Requests: 10, Period: 0},
		"zero requests": {Requests: 0, Period: time.Second},
		"negative":      {Requests: -1, Period: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			// A misconfigured rule must not become an open door. Refusing is
			// visible; allowing everything looks exactly like working.
			if d, _ := b.Take(context.Background(), "k", limit, 1); d.Allowed {
				t.Error("a limit that allows nothing allowed a request")
			}
		})
	}
}

func TestMemoryBackendEvictsIdleKeys(t *testing.T) {
	t.Parallel()

	clock, advance := fixedClock(time.Unix(0, 0))
	b := NewMemoryBackend(WithMemoryClock(clock), WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 10, Period: time.Second}

	for _, k := range []string{"a", "b", "c"} {
		if _, err := b.Take(context.Background(), k, limit, 1); err != nil {
			t.Fatal(err)
		}
	}

	if got := b.Len(); got != 3 {
		t.Fatalf("Len = %d, want 3", got)
	}

	advance(time.Hour)
	b.sweep(time.Minute)

	if got := b.Len(); got != 0 {
		t.Errorf("Len = %d after the sweep, want 0; an unbounded key space that is never evicted is a leak", got)
	}
}

func TestMemoryBackendIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	b := NewMemoryBackend(WithMemorySweepInterval(0))
	defer b.Close()

	// A budget far larger than the number of requests, so every one should be
	// allowed and any lost update shows up as a refusal.
	limit := Limit{Requests: 10_000, Period: time.Hour}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		refused int
	)

	for range 200 {
		wg.Go(func() {
			d, err := b.Take(context.Background(), "hot", limit, 1)
			if err != nil {
				t.Error(err)
			}

			if !d.Allowed {
				mu.Lock()
				refused++
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if refused != 0 {
		t.Errorf("%d of 200 concurrent requests were refused against a 10000 budget", refused)
	}
}

// TestMemoryBackendSweeperRuns covers the background goroutine rather than only
// the sweep it calls: a sweeper that is never started is a leak that no unit
// test of sweep() would notice.
func TestMemoryBackendSweeperRuns(t *testing.T) {
	t.Parallel()

	b := NewMemoryBackend(WithMemorySweepInterval(5 * time.Millisecond))
	defer b.Close()

	limit := Limit{Requests: 10, Period: time.Millisecond}

	if _, err := b.Take(context.Background(), "k", limit, 1); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for b.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("the key was never evicted; the background sweeper is not running")
		}

		time.Sleep(2 * time.Millisecond)
	}
}

func TestMemoryBackendCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	b := NewMemoryBackend(WithMemorySweepInterval(time.Hour))
	b.Close()
	b.Close() // must not panic or block

	// Take keeps working after Close; only eviction stops.
	if _, err := b.Take(context.Background(), "k",
		Limit{Requests: 1, Period: time.Second}, 1); err != nil {
		t.Errorf("Take after Close: %v", err)
	}
}

func TestMemoryBackendNormalisesCost(t *testing.T) {
	t.Parallel()

	b := NewMemoryBackend(WithMemorySweepInterval(0))
	defer b.Close()

	limit := Limit{Requests: 1, Period: time.Minute}

	// A zero or negative cost is treated as one rather than as free, so a
	// caller cannot spend nothing forever.
	if d, _ := b.Take(context.Background(), "k", limit, 0); !d.Allowed {
		t.Fatal("the first request should be allowed")
	}

	if d, _ := b.Take(context.Background(), "k", limit, -5); d.Allowed {
		t.Error("a negative cost was treated as free; it must count as one")
	}
}
