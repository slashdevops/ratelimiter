package ratelimiter

import (
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestNewRateLimiterFuncImplementsReserver(t *testing.T) {
	lim := NewRateLimiterFunc(rate.Limit(1), 1)()

	if _, ok := lim.(Reserver); !ok {
		t.Fatalf("limiter from NewRateLimiterFunc does not implement Reserver")
	}
}

func TestReserveConsumesAndCancelReturnsToken(t *testing.T) {
	// 1 token/sec, burst 1.
	lim := NewRateLimiterFunc(rate.Limit(1), 1)()
	r := lim.(Reserver)

	r.Reserve() // consume the initial token (delay 0)

	// The next token is a bit under a second out.
	pending := r.Reserve()
	if !pending.OK() {
		t.Fatal("pending reservation should be OK")
	}
	withoutCancel := pending.Delay()
	if withoutCancel <= 0 {
		t.Fatalf("pending reservation should have a positive delay, got %v", withoutCancel)
	}

	// Returning that reserved token should make the following reservation land
	// at roughly the same delay rather than stacking a second token's wait.
	pending.Cancel()
	next := r.Reserve()
	defer next.Cancel()
	if got := next.Delay(); got > withoutCancel+500*time.Millisecond {
		t.Fatalf("Cancel did not return the token: delay grew from %v to %v", withoutCancel, got)
	}
}

func TestReserveDelayDrivesRetryAfter(t *testing.T) {
	// 2 tokens/sec, burst 1: after consuming the token the next is ~500ms out.
	lim := NewRateLimiterFunc(rate.Limit(2), 1)()
	r := lim.(Reserver)

	first := r.Reserve()
	if first.Delay() != 0 {
		t.Fatalf("first reservation should be immediate, got %v", first.Delay())
	}

	second := r.Reserve()
	defer second.Cancel()
	if got := second.Delay(); got <= 0 || got > time.Second {
		t.Fatalf("expected a sub-second positive delay, got %v", got)
	}
}
