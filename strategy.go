package ratelimiter

import (
	"fmt"
	"slices"
	"time"
)

// Strategy names how a limit is enforced. Two limits with identical numbers can
// behave completely differently depending on which one is chosen.
//
// The values are lowercase snake_case because they are meant to survive a round
// trip through configuration: a database column, a YAML field, an environment
// variable. [ParseStrategy] validates one coming back.
type Strategy string

const (
	// StrategyTokenBucket admits a burst up to the capacity and refills
	// continuously. It answers "have you gone over budget?".
	//
	// The right default for protecting your own service, where a short burst
	// is harmless and the thing you care about is sustained volume.
	StrategyTokenBucket Strategy = "token_bucket"

	// StrategyLeakyBucket enforces a minimum spacing between admissions. It
	// answers "are you going too fast right now?".
	//
	// The right choice in front of a dependency with its own limit, where a
	// burst is precisely what breaks you. See [LeakyBucket].
	StrategyLeakyBucket Strategy = "leaky_bucket"
)

// Strategies returns every valid strategy, in a stable order. Useful for
// populating a form or validating a configuration schema.
func Strategies() []Strategy {
	return []Strategy{StrategyTokenBucket, StrategyLeakyBucket}
}

// String implements [fmt.Stringer].
func (s Strategy) String() string { return string(s) }

// Valid reports whether s names a strategy this package implements.
func (s Strategy) Valid() bool { return slices.Contains(Strategies(), s) }

// ParseStrategy converts a configuration value into a [Strategy], rejecting
// anything unrecognised.
//
// It rejects rather than defaulting on purpose. A typo in a config file that
// silently becomes "token bucket" gives you a limiter that admits bursts you
// specifically asked it not to — and nothing about the running service says so.
func ParseStrategy(s string) (Strategy, error) {
	if candidate := Strategy(s); candidate.Valid() {
		return candidate, nil
	}

	return "", fmt.Errorf("ratelimiter: unknown strategy %q, want one of %v", s, Strategies())
}

// StrategyOption configures the limiters built by [NewLimiterFunc].
type StrategyOption func(*strategyConfig)

type strategyConfig struct {
	now func() time.Time
}

// WithStrategyClock overrides the time source.
//
// It affects [StrategyLeakyBucket] only. The token bucket is
// golang.org/x/time/rate, which has no clock seam — so a test that needs a
// controllable clock across both strategies cannot have one, and this is said
// here rather than discovered.
func WithStrategyClock(now func() time.Time) StrategyOption {
	return func(c *strategyConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// NewLimiterFunc returns a [Limiter] factory for the named strategy, suitable
// for [NewBucketLimiter].
//
// One [Limit] describes both strategies — the same "N requests per period" an
// operator writes down — and the strategy decides how it is enforced:
//
//	limit := ratelimiter.Limit{Requests: 60, Period: time.Minute}
//
//	token_bucket → 60 available at once, refilling continuously
//	leaky_bucket → one every second, evenly spaced
//
// The burst differs too: the token bucket uses [Limit.Capacity] as its bucket
// size, and the leaky bucket uses it as how many may be taken back-to-back from
// idle. Leave Burst unset for a strict leaky bucket by setting it to 1.
//
// It returns an error for an unknown strategy or a limit that describes
// nothing, because both usually arrive from configuration and both are worth
// failing loudly at startup rather than quietly at request time.
func NewLimiterFunc(strategy Strategy, limit Limit, opts ...StrategyOption) (func() Limiter, error) {
	if !strategy.Valid() {
		return nil, fmt.Errorf("ratelimiter: unknown strategy %q, want one of %v", strategy, Strategies())
	}

	if limit.Requests <= 0 || limit.Period <= 0 {
		return nil, fmt.Errorf(
			"ratelimiter: invalid limit {Requests:%d Period:%v}; both must be positive",
			limit.Requests, limit.Period,
		)
	}

	cfg := strategyConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	switch strategy {
	case StrategyLeakyBucket:
		// One emission per request across the period: 60/minute is one per
		// second. Derived rather than configured, so the two strategies are
		// described by the same numbers.
		interval := limit.Period / time.Duration(limit.Requests)

		var leakyOpts []LeakyBucketOption
		if cfg.now != nil {
			leakyOpts = append(leakyOpts, WithLeakyClock(cfg.now))
		}

		return NewLeakyBucketFunc(interval, limit.Capacity(), leakyOpts...), nil

	case StrategyTokenBucket:
		fallthrough

	default:
		return NewRateLimiterFunc(rateLimitOf(limit), limit.Capacity()), nil
	}
}

// MustNewLimiterFunc is [NewLimiterFunc] for a strategy and limit that are
// known good at compile time. It panics on an error, so keep it out of any path
// that handles configuration supplied at run time.
func MustNewLimiterFunc(strategy Strategy, limit Limit, opts ...StrategyOption) func() Limiter {
	f, err := NewLimiterFunc(strategy, limit, opts...)
	if err != nil {
		panic(err)
	}

	return f
}
