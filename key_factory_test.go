package ratelimiter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// sharedLimiter stands in for a limiter whose state lives somewhere other than
// this object — Redis, Valkey, a database. What matters for these tests is the
// property that makes such a limiter different from *rate.Limiter: several
// instances built for the SAME key share one budget, and instances built for
// different keys do not.
type sharedLimiter struct {
	key   string
	state *sharedState
}

type sharedState struct {
	mu        sync.Mutex
	remaining map[string]int
}

func newSharedState(perKey int, keys ...string) *sharedState {
	s := &sharedState{remaining: make(map[string]int, len(keys))}
	for _, k := range keys {
		s.remaining[k] = perKey
	}

	return s
}

func (l *sharedLimiter) Burst() int { return 0 }

func (l *sharedLimiter) Allow() bool {
	l.state.mu.Lock()
	defer l.state.mu.Unlock()

	if l.state.remaining[l.key] <= 0 {
		return false
	}

	l.state.remaining[l.key]--

	return true
}

func (l *sharedLimiter) Wait(context.Context) error { return nil }

// TestWithLimiterFactoryForKeyBindsTheKey is the reason the option exists.
//
// A Limiter is bound to exactly one key — Allow and Wait take no arguments, so
// the instance IS the bucket. An argument-less factory can therefore only build
// limiters that keep their state in themselves. A limiter whose counter lives
// in a shared datastore has to know WHICH remote key is its own, and before
// this option nothing in the API ever told it.
func TestWithLimiterFactoryForKeyBindsTheKey(t *testing.T) {
	t.Parallel()

	var built []string

	var mu sync.Mutex

	state := newSharedState(2, "alice", "bob")

	bl := NewBucketLimiter(nil, time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(func(key string) Limiter {
			mu.Lock()
			built = append(built, key)
			mu.Unlock()

			return &sharedLimiter{key: key, state: state}
		}),
	)
	defer bl.Close()

	// Two tokens each, independently.
	for range 2 {
		if !bl.GetOrAdd("alice").Allow() {
			t.Fatal("alice should have budget")
		}

		if !bl.GetOrAdd("bob").Allow() {
			t.Fatal("bob should have budget")
		}
	}

	if bl.GetOrAdd("alice").Allow() {
		t.Error("alice's budget should be spent")
	}

	if bl.GetOrAdd("bob").Allow() {
		t.Error("bob's budget should be spent; one key's spending must not affect another's")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(built) != 2 {
		t.Errorf("factory called %d times for 2 keys: %v — limiters must be built once per key and cached", len(built), built)
	}
}

// TestKeyFactoryStateSurvivesEviction pins the property that makes a shared
// limiter useful: because the state is not in the returned object, evicting the
// object does not reset the budget. An in-process *rate.Limiter would come back
// with a full bucket here; a datastore-backed one must not.
func TestKeyFactoryStateSurvivesEviction(t *testing.T) {
	t.Parallel()

	state := newSharedState(1, "k")

	bl := NewBucketLimiter(nil, time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(func(key string) Limiter {
			return &sharedLimiter{key: key, state: state}
		}),
	)
	defer bl.Close()

	if !bl.GetOrAdd("k").Allow() {
		t.Fatal("the first call should be allowed")
	}

	// Drop the cached handle, exactly as idle eviction would.
	bl.Remove("k")

	// A fresh handle is built, but it addresses the same shared state.
	if bl.GetOrAdd("k").Allow() {
		t.Error("budget came back after the handle was evicted; the limiter is keeping state in the object, not in the shared store")
	}
}

// TestNewBucketLimiterPrefersTheKeyFactory: when both are supplied the
// key-aware one wins, because it is strictly more informed.
func TestNewBucketLimiterPrefersTheKeyFactory(t *testing.T) {
	t.Parallel()

	bl := NewBucketLimiter(
		func() Limiter { return RateLimiter{rate.NewLimiter(rate.Inf, 1)} },
		time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(func(key string) Limiter {
			return &sharedLimiter{key: key, state: newSharedState(0, key)}
		}),
	)
	defer bl.Close()

	if _, ok := bl.GetOrAdd("k").(*sharedLimiter); !ok {
		t.Error("the argument-less factory was used even though a key-aware one was supplied")
	}
}

// TestNewBucketLimiterWithNoFactoryPanics: nothing could ever be built, and
// saying so at construction beats a nil-limiter panic on the first request for
// a new key, which is a different place and a much worse moment.
func TestNewBucketLimiterWithNoFactoryPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic when neither factory is supplied")
		}

		if msg := fmt.Sprint(r); msg == "" {
			t.Error("panic carried no message")
		}
	}()

	_ = NewBucketLimiter[string](nil, time.Minute, NewInMemoryStorage[string, Limiter]())
}

// TestWithLimiterFactoryForKeyRejectsAMismatchedKeyType: Option is not generic
// (making it so would break every existing WithClock call), so the factory's
// key type is checked at construction instead. A mismatch is a programming
// error and must not surface as a nil limiter later.
func TestWithLimiterFactoryForKeyRejectsAMismatchedKeyType(t *testing.T) {
	t.Parallel()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected a panic when the factory's key type does not match the limiter's")
		}

		// Assert WHICH panic. Checking only that "a panic happened" is too
		// weak here and was: with the type check removed the assertion simply
		// yields nil, the factory ends up unset, and the both-factories-nil
		// guard panics instead — so the test passed while the behaviour it
		// names was gone. Found by mutating the check away.
		if msg := fmt.Sprint(r); !strings.Contains(msg, "key type") {
			t.Errorf("panic was %q, which does not report a key-type mismatch", msg)
		}
	}()

	// int factory, string limiter. A non-nil newLimiter is supplied so that
	// the both-factories-nil guard cannot be what fires.
	_ = NewBucketLimiter[string](
		func() Limiter { return RateLimiter{rate.NewLimiter(rate.Inf, 1)} },
		time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(func(int) Limiter { return nil }),
	)
}

// TestKeyFactoryIsSafeUnderConcurrentFirstUse: GetOrAdd's contract is that
// concurrent callers racing on a brand-new key all receive the SAME instance.
// That has to keep holding when the instance is built from the key.
//
// The assertion is instance IDENTITY, deliberately, and an earlier version of
// this test got it wrong in a way worth recording: it counted tokens spent
// against the shared state. That passes whether or not creation is atomic —
// every instance addresses the same shared budget, so building ten of them
// still spends exactly ten tokens. It asserted something that could not fail.
//
// Note what is NOT asserted: that the factory runs once. GetOrAdd evaluates
// build(key) as the argument to LoadOrStore, so under a race each goroutine
// legitimately builds a candidate and all but one are discarded. Requiring a
// single call would pin an implementation detail the library does not promise.
func TestKeyFactoryIsSafeUnderConcurrentFirstUse(t *testing.T) {
	t.Parallel()

	bl := NewBucketLimiter(nil, time.Minute,
		NewInMemoryStorage[string, Limiter](),
		WithLimiterFactoryForKey(func(key string) Limiter {
			// A pointer, so identity is observable.
			return &sharedLimiter{key: key, state: newSharedState(1<<30, key)}
		}),
	)
	defer bl.Close()

	const racers = 100

	got := make([]Limiter, racers)

	var wg sync.WaitGroup

	for i := range racers {
		wg.Go(func() {
			got[i] = bl.GetOrAdd("hot")
		})
	}

	wg.Wait()

	first := got[0]
	for i, l := range got {
		if l != first {
			t.Fatalf("racer %d received a different limiter instance; concurrent callers on one key must share a bucket, and two instances means two budgets", i)
		}
	}
}
