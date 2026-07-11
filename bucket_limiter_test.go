package ratelimiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// newStorage is a small helper for the common string-keyed store used by tests.
func newStorage() *InMemoryStorage[string, Limiter] {
	return NewInMemoryStorage[string, Limiter]()
}

// TestNewBucketLimiter verifies the constructor wires its fields.
func TestNewBucketLimiter(t *testing.T) {
	storage := newStorage()
	deleteAfter := 5 * time.Second

	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), deleteAfter, storage)
	t.Cleanup(func() { _ = bl.Close() })

	if bl == nil {
		t.Fatal("NewBucketLimiter returned nil")
	}
	if bl.deleteAfter != deleteAfter {
		t.Errorf("deleteAfter = %v, want %v", bl.deleteAfter, deleteAfter)
	}
	if bl.storage != storage {
		t.Error("storage field should be the exact instance passed in")
	}
}

// TestBucketLimiter_PerKeyIsolation is the key regression test: distinct keys
// MUST have independent token buckets.
func TestBucketLimiter_PerKeyIsolation(t *testing.T) {
	storage := newStorage()
	// burst of 3, effectively no refill during the test window.
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(1), 3), time.Minute, storage)
	t.Cleanup(func() { _ = bl.Close() })

	a := bl.GetOrAdd("user-A")
	for i := range 3 {
		if !a.Allow() {
			t.Fatalf("user-A request %d should be allowed", i+1)
		}
	}
	if a.Allow() {
		t.Error("user-A should be exhausted after its burst")
	}

	// A completely separate key must still have a full, independent bucket.
	b := bl.GetOrAdd("user-B")
	allowed := 0
	for range 3 {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("user-B must have its own bucket independent of user-A: allowed %d, want 3", allowed)
	}
}

// TestBucketLimiter_GetOrAdd_New verifies a new key is created and stored.
func TestBucketLimiter_GetOrAdd_New(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), 100*time.Millisecond, storage)
	t.Cleanup(func() { _ = bl.Close() })

	limiter := bl.GetOrAdd("k")
	if limiter == nil {
		t.Fatal("GetOrAdd returned nil")
	}

	stored, ok := storage.Load("k")
	if !ok {
		t.Fatal("new key should be stored")
	}
	if stored != limiter {
		t.Error("stored limiter should be the same instance returned by GetOrAdd")
	}
}

// TestBucketLimiter_GetOrAdd_Existing verifies the same instance is returned.
func TestBucketLimiter_GetOrAdd_Existing(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), 5*time.Second, storage)
	t.Cleanup(func() { _ = bl.Close() })

	first := bl.GetOrAdd("k")
	second := bl.GetOrAdd("k")
	if first != second {
		t.Error("existing key must return the same instance")
	}
}

// TestBucketLimiter_EvictsIdle verifies idle keys are evicted, using a fake
// clock so the assertion is deterministic instead of timing-dependent.
func TestBucketLimiter_EvictsIdle(t *testing.T) {
	storage := newStorage()

	var nowNanos atomic.Int64
	nowNanos.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNanos.Load()) }

	deleteAfter := time.Second
	bl := NewBucketLimiter(
		NewRateLimiterFunc(rate.Limit(10), 5),
		deleteAfter,
		storage,
		WithClock(clock),
		WithSweepInterval(5*time.Millisecond),
	)
	t.Cleanup(func() { _ = bl.Close() })

	bl.GetOrAdd("k")
	if _, ok := storage.Load("k"); !ok {
		t.Fatal("key should be present right after creation")
	}

	// Advance the clock well past the idle window; the sweeper must evict it.
	nowNanos.Add(int64(2 * deleteAfter))

	if !eventually(t, time.Second, 5*time.Millisecond, func() bool {
		_, ok := storage.Load("k")
		return !ok
	}) {
		t.Error("idle key should be evicted")
	}
}

// TestBucketLimiter_AccessKeepsAlive verifies that continued use prevents
// eviction (the "idle since last use" semantics).
func TestBucketLimiter_AccessKeepsAlive(t *testing.T) {
	storage := newStorage()

	var nowNanos atomic.Int64
	nowNanos.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, nowNanos.Load()) }

	deleteAfter := time.Second
	bl := NewBucketLimiter(
		NewRateLimiterFunc(rate.Limit(10), 5),
		deleteAfter,
		storage,
		WithClock(clock),
		WithSweepInterval(5*time.Millisecond),
	)
	t.Cleanup(func() { _ = bl.Close() })

	bl.GetOrAdd("k")

	// Advance time but keep touching the key: it must survive several sweeps.
	for i := range 5 {
		nowNanos.Add(int64(deleteAfter / 2))
		bl.GetOrAdd("k")
		time.Sleep(10 * time.Millisecond) // let the sweeper run at least once
		if _, ok := storage.Load("k"); !ok {
			t.Fatalf("actively-used key must not be evicted (iteration %d)", i)
		}
	}
}

// TestBucketLimiter_Remove verifies manual removal.
func TestBucketLimiter_Remove(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), 5*time.Second, storage)
	t.Cleanup(func() { _ = bl.Close() })

	bl.GetOrAdd("k")
	if _, ok := storage.Load("k"); !ok {
		t.Fatal("key should be present after GetOrAdd")
	}

	bl.Remove("k")
	if _, ok := storage.Load("k"); ok {
		t.Error("key should be gone after Remove")
	}
}

// TestBucketLimiter_Close verifies Close stops the sweeper and is idempotent.
func TestBucketLimiter_Close(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), time.Second, storage)

	if err := bl.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := bl.Close(); err != nil {
		t.Fatalf("Close must be idempotent, second call returned: %v", err)
	}

	// The manager still serves requests after Close.
	if bl.GetOrAdd("k") == nil {
		t.Error("manager should still serve requests after Close")
	}
}

// TestBucketLimiter_NoEvictionWhenDisabled verifies deleteAfter <= 0 keeps keys.
func TestBucketLimiter_NoEvictionWhenDisabled(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), 0, storage)
	t.Cleanup(func() { _ = bl.Close() })

	bl.GetOrAdd("k")
	time.Sleep(50 * time.Millisecond)
	if _, ok := storage.Load("k"); !ok {
		t.Error("eviction disabled: key must remain")
	}
}

// TestBucketLimiter_GetOrAdd_Concurrent verifies concurrent creation of the
// same key yields a single shared instance (no TOCTOU duplicate).
func TestBucketLimiter_GetOrAdd_Concurrent(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(10), 5), time.Second, storage)
	t.Cleanup(func() { _ = bl.Close() })

	const numGoroutines = 100
	results := make([]Limiter, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func() {
			defer wg.Done()
			results[i] = bl.GetOrAdd("concurrent-key")
		}()
	}
	wg.Wait()

	stored, ok := storage.Load("concurrent-key")
	if !ok {
		t.Fatal("concurrent key should be stored")
	}
	for i, got := range results {
		if got != stored {
			t.Errorf("goroutine %d got a different instance", i)
		}
	}
}

// TestBucketLimiter_DistinctKeysConcurrent stresses many independent keys.
func TestBucketLimiter_DistinctKeysConcurrent(t *testing.T) {
	storage := newStorage()
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(100), 10), time.Second, storage)
	t.Cleanup(func() { _ = bl.Close() })

	const numKeys = 200
	var wg sync.WaitGroup
	wg.Add(numKeys)
	for k := range numKeys {
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", k)
			// Each key's first burst-worth of Allow() calls should all pass on
			// its own fresh bucket.
			lim := bl.GetOrAdd(key)
			if !lim.Allow() {
				t.Errorf("first request for %s should pass", key)
			}
		}()
	}
	wg.Wait()
}

// TestBucketLimiter_Wait verifies Wait shapes traffic to the configured rate.
func TestBucketLimiter_Wait(t *testing.T) {
	storage := newStorage()
	// 2 tokens/sec, burst 1.
	bl := NewBucketLimiter(NewRateLimiterFunc(rate.Limit(2), 1), 3*time.Second, storage)
	t.Cleanup(func() { _ = bl.Close() })

	limiter := bl.GetOrAdd("k")
	ctx := context.Background()

	const numRequests = 4
	start := time.Now()
	for range numRequests {
		if err := limiter.Wait(ctx); err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Req 0 is immediate (burst), the next 3 wait ~0.5s each -> ~1.5s total.
	// Use a lower bound only to avoid flakiness under race/CI scheduling.
	if elapsed < 1200*time.Millisecond {
		t.Errorf("Wait must shape traffic to roughly the configured rate: elapsed %v, want >= 1.2s", elapsed)
	}
}

// eventually polls condition until it returns true or the timeout elapses. It
// mirrors the small slice of testify's assert.Eventually the tests rely on,
// without the dependency.
func eventually(t *testing.T, timeout, interval time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// ExampleBucketLimiter demonstrates basic per-key usage.
func ExampleBucketLimiter() {
	storage := NewInMemoryStorage[string, Limiter]()
	newLimiter := NewRateLimiterFunc(rate.Limit(5), 3) // 5 rps, burst 3
	bl := NewBucketLimiter(newLimiter, time.Minute, storage)
	defer bl.Close()

	// user-A and user-B have independent buckets.
	fmt.Println(bl.GetOrAdd("user-A").Allow())
	fmt.Println(bl.GetOrAdd("user-B").Allow())
	// Output:
	// true
	// true
}
