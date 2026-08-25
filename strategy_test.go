package ratelimiter

import (
	"testing"
	"time"
)

func TestParseStrategy(t *testing.T) {
	t.Parallel()

	for _, want := range Strategies() {
		got, err := ParseStrategy(string(want))
		if err != nil {
			t.Errorf("ParseStrategy(%q): %v", want, err)
		}

		if got != want {
			t.Errorf("ParseStrategy(%q) = %q", want, got)
		}
	}

	// Rejected rather than defaulted. A typo that silently became a token
	// bucket would admit bursts the operator specifically asked it not to, and
	// nothing about the running service would say so.
	for _, bad := range []string{"", "tokenbucket", "Token_Bucket", "leaky", "gcra"} {
		if _, err := ParseStrategy(bad); err == nil {
			t.Errorf("ParseStrategy(%q) returned no error; an unrecognised strategy must not default", bad)
		}
	}
}

func TestStrategyValidAndString(t *testing.T) {
	t.Parallel()

	if !StrategyTokenBucket.Valid() || !StrategyLeakyBucket.Valid() {
		t.Error("a bundled strategy reported itself invalid")
	}

	if Strategy("nonsense").Valid() {
		t.Error("an unknown strategy reported itself valid")
	}

	if got := StrategyLeakyBucket.String(); got != "leaky_bucket" {
		t.Errorf("String() = %q", got)
	}
}

// TestNewLimiterFuncSelectsTheAlgorithm is the point of the whole type: the
// same Limit, enforced two different ways, and the difference is visible in one
// instant.
func TestNewLimiterFuncSelectsTheAlgorithm(t *testing.T) {
	t.Parallel()

	// 60 per minute. A token bucket makes all 60 available at once; a leaky
	// bucket paces them one per second.
	limit := Limit{Requests: 60, Period: time.Minute}

	t.Run("token bucket bursts", func(t *testing.T) {
		t.Parallel()

		f, err := NewLimiterFunc(StrategyTokenBucket, limit)
		if err != nil {
			t.Fatal(err)
		}

		l := f()

		allowed := 0

		for range 60 {
			if l.Allow() {
				allowed++
			}
		}

		if allowed != 60 {
			t.Errorf("token bucket admitted %d of 60 immediately, want 60 — absorbing a burst is what it is for", allowed)
		}
	})

	t.Run("leaky bucket paces", func(t *testing.T) {
		t.Parallel()

		clock, advance := fixedClock(time.Unix(0, 0))

		f, err := NewLimiterFunc(StrategyLeakyBucket, Limit{Requests: 60, Period: time.Minute, Burst: 1},
			WithStrategyClock(clock))
		if err != nil {
			t.Fatal(err)
		}

		l := f()

		allowed := 0

		for range 60 {
			if l.Allow() {
				allowed++
			}
		}

		if allowed != 1 {
			t.Errorf("leaky bucket admitted %d of 60 in one instant, want 1 — the burst is exactly what it exists to prevent", allowed)
		}

		// One second later — 60 per minute — the next one conforms.
		advance(time.Second)

		if !l.Allow() {
			t.Error("the leaky bucket refused a request one interval later")
		}
	})
}

func TestNewLimiterFuncRejectsBadInput(t *testing.T) {
	t.Parallel()

	good := Limit{Requests: 1, Period: time.Second}

	if _, err := NewLimiterFunc(Strategy("nope"), good); err == nil {
		t.Error("an unknown strategy was accepted")
	}

	for name, bad := range map[string]Limit{
		"zero requests": {Requests: 0, Period: time.Second},
		"zero period":   {Requests: 1, Period: 0},
		"negative":      {Requests: -1, Period: time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			// These usually arrive from configuration, so failing at startup
			// beats failing at request time.
			if _, err := NewLimiterFunc(StrategyTokenBucket, bad); err == nil {
				t.Error("an impossible limit was accepted")
			}
		})
	}
}

func TestMustNewLimiterFuncPanicsOnBadInput(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("MustNewLimiterFunc did not panic on an unknown strategy")
		}
	}()

	_ = MustNewLimiterFunc(Strategy("nope"), Limit{Requests: 1, Period: time.Second})
}

func TestStrategyRoundTripsThroughConfiguration(t *testing.T) {
	t.Parallel()

	// What a database column or a YAML field actually does with it.
	for _, s := range Strategies() {
		parsed, err := ParseStrategy(s.String())
		if err != nil || parsed != s {
			t.Errorf("%q did not survive a round trip: %v, %v", s, parsed, err)
		}
	}
}

func TestNewLimiterFuncBuildsIndependentLimiters(t *testing.T) {
	t.Parallel()

	f := MustNewLimiterFunc(StrategyTokenBucket, Limit{Requests: 1, Period: time.Hour})

	a, b := f(), f()

	if !a.Allow() {
		t.Fatal("the first limiter should admit one")
	}

	if !b.Allow() {
		t.Error("the second limiter shared the first one's budget; the factory must build independent limiters")
	}
}
