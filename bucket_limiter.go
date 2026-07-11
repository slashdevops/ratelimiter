package ratelimiter

import (
	"sync"
	"sync/atomic"
	"time"
)

// defaultSweepDivisor controls how often the background eviction goroutine runs
// relative to deleteAfter when no explicit interval is configured. An idle
// entry is therefore removed somewhere between deleteAfter and
// deleteAfter*(1 + 1/divisor) after its last use.
const defaultSweepDivisor = 2

// BucketLimiter is a goroutine-safe manager that hands out an independent
// token-bucket [Limiter] per key (for example a user ID or IP address).
//
// Each distinct key receives its own [Limiter], produced by the newLimiter
// factory, so consuming one key's budget never affects another. Limiters are
// created lazily on first access and evicted after they have been idle (not
// accessed through GetOrAdd) for deleteAfter. Eviction runs in a single
// background goroutine started by [NewBucketLimiter] and stopped by
// [BucketLimiter.Close].
//
// The zero value is not usable; construct one with [NewBucketLimiter].
type BucketLimiter[K comparable] struct {
	newLimiter  func() Limiter
	deleteAfter time.Duration
	interval    time.Duration
	now         func() time.Time
	storage     Storage[K, Limiter]

	// access tracks the last-use time (unix nanoseconds) per key so the
	// sweeper can evict genuinely idle entries. It is kept separate from
	// storage so that custom Storage backends only ever hold Limiter values.
	access sync.Map // K -> *atomic.Int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// Option configures a [BucketLimiter] at construction time.
type Option func(*config)

type config struct {
	now      func() time.Time
	interval time.Duration
}

// WithClock overrides the time source used for idle tracking and eviction.
// It is primarily useful in tests to make eviction deterministic.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

// WithSweepInterval overrides how often the background eviction goroutine runs.
// When unset, it defaults to deleteAfter/2. Ignored when deleteAfter <= 0.
func WithSweepInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.interval = d
		}
	}
}

// NewBucketLimiter creates a [BucketLimiter].
//
//   - newLimiter is called once per new key to build that key's independent
//     [Limiter]. Use [NewRateLimiterFunc] for the common *rate.Limiter case.
//   - deleteAfter is the idle duration after which an unused key is evicted.
//     A value <= 0 disables eviction (limiters live until [BucketLimiter.Remove]
//     or [BucketLimiter.Close]); prefer this only for bounded key spaces.
//   - storage is the backing store, commonly
//     ratelimiter.NewInMemoryStorage[K, ratelimiter.Limiter]().
//
// When eviction is enabled a background goroutine is started; call
// [BucketLimiter.Close] to stop it and release resources.
func NewBucketLimiter[K comparable](
	newLimiter func() Limiter,
	deleteAfter time.Duration,
	storage Storage[K, Limiter],
	opts ...Option,
) *BucketLimiter[K] {
	cfg := config{now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}

	interval := cfg.interval
	if interval <= 0 {
		interval = deleteAfter / defaultSweepDivisor
		if interval <= 0 {
			interval = deleteAfter
		}
	}

	b := &BucketLimiter[K]{
		newLimiter:  newLimiter,
		deleteAfter: deleteAfter,
		interval:    interval,
		now:         cfg.now,
		storage:     storage,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}

	if deleteAfter > 0 {
		go b.sweepLoop()
	} else {
		close(b.done)
	}

	return b
}

// GetOrAdd returns the [Limiter] for key, creating and storing a new one via
// the newLimiter factory if none exists yet. Concurrent callers racing on the
// same new key all receive the same instance. Every call refreshes the key's
// idle timer.
func (b *BucketLimiter[K]) GetOrAdd(key K) Limiter {
	limiter, ok := b.storage.Load(key)
	if !ok {
		// LoadOrStore makes creation atomic: if another goroutine wins the
		// race, we discard our fresh limiter and use the stored one.
		limiter, _ = b.storage.LoadOrStore(key, b.newLimiter())
	}

	b.touch(key)
	return limiter
}

// touch records the current time as key's last-use time.
func (b *BucketLimiter[K]) touch(key K) {
	if b.deleteAfter <= 0 {
		return
	}
	v, _ := b.access.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Store(b.now().UnixNano())
}

// Remove immediately deletes the limiter for key. A subsequent GetOrAdd
// creates a fresh one.
func (b *BucketLimiter[K]) Remove(key K) {
	b.storage.Delete(key)
	b.access.Delete(key)
}

// Close stops the background eviction goroutine and waits for it to exit. It is
// safe to call multiple times and from multiple goroutines. After Close the
// manager can still serve GetOrAdd, but idle entries will no longer be evicted
// automatically.
func (b *BucketLimiter[K]) Close() error {
	b.closeOnce.Do(func() {
		close(b.stop)
	})
	<-b.done
	return nil
}

// sweepLoop periodically evicts entries idle for longer than deleteAfter.
func (b *BucketLimiter[K]) sweepLoop() {
	defer close(b.done)

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.evictIdle()
		}
	}
}

// evictIdle removes every key whose last use is older than deleteAfter.
func (b *BucketLimiter[K]) evictIdle() {
	cutoff := b.now().Add(-b.deleteAfter).UnixNano()
	b.access.Range(func(k, v any) bool {
		if v.(*atomic.Int64).Load() <= cutoff {
			key := k.(K)
			b.storage.Delete(key)
			b.access.Delete(key)
		}
		return true
	})
}
