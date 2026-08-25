package ratelimiter

import (
	"fmt"
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
	newLimiter func() Limiter

	// newLimiterForKey, when set by [WithLimiterFactoryForKey], is used in
	// place of newLimiter and receives the key the limiter will govern. It is
	// what makes a limiter backed by a shared datastore possible: such a
	// limiter has to know which remote key holds its counter, and Allow/Wait
	// take no arguments.
	newLimiterForKey func(K) Limiter

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

	// keyFactory holds a func(K) Limiter. It is stored as any because Option
	// is deliberately not generic: making it Option[K] would force every
	// existing call such as WithClock(now) to be explicitly instantiated,
	// which would break source compatibility for every current user. The type
	// is recovered with a checked assertion in NewBucketLimiter, so a mismatch
	// is caught at construction rather than at first use.
	keyFactory any
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

// WithLimiterFactoryForKey builds each key's [Limiter] from the key itself,
// instead of from the argument-less factory passed to [NewBucketLimiter].
//
// # Why this exists
//
// A [Limiter] is bound to exactly one key: Allow and Wait take no arguments, so
// the instance IS the bucket. For an in-process limiter that is invisible —
// every bucket is equivalent, so an argument-less factory suffices. For a
// limiter whose state lives somewhere else — Redis, Valkey, any shared store —
// it is the whole problem: the limiter must know WHICH remote key holds its
// counter, and nothing in the old API ever told it.
//
// Without this option the only injection point that sees both the key and a
// place to hold a shared client is [Storage.LoadOrStore], which meant using the
// store as a factory rather than as a value container. That works, but it
// reinterprets an interface whose documented job is to hold values, and it puts
// construction logic in a place nobody looks for it. This option is the
// first-class version: the store goes back to storing, and the factory does the
// building.
//
//	newLimiter := func(key string) ratelimiter.Limiter {
//		if sharedStoreAvailable {
//			return valkeylimiter.New(client, "rl:"+key, limit, burst)
//		}
//		return ratelimiter.RateLimiter{rate.NewLimiter(limit, burst)}
//	}
//
//	bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
//		ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
//		ratelimiter.WithLimiterFactoryForKey(newLimiter),
//	)
//
// Note what the storage is in that example: the ordinary in-memory one, in
// BOTH branches. The shared state lives in the datastore, inside the limiter;
// the map only caches one lightweight handle per key. A [Storage] is always an
// in-process container — see docs/CUSTOM_STORAGE.md.
//
// When this option is supplied the newLimiter argument to [NewBucketLimiter] is
// ignored and may be nil. Supplying neither panics at construction.
func WithLimiterFactoryForKey[K comparable](newLimiter func(K) Limiter) Option {
	return func(c *config) {
		if newLimiter != nil {
			c.keyFactory = newLimiter
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
//     It may be nil when [WithLimiterFactoryForKey] supplies a key-aware
//     factory instead; supplying neither panics.
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

	// Recover the key-aware factory's real type. A mismatch means the caller
	// wrote WithLimiterFactoryForKey with a different key type than the
	// BucketLimiter's, which is a programming error worth reporting here
	// rather than as a nil limiter on the first request for a new key.
	var newLimiterForKey func(K) Limiter

	if cfg.keyFactory != nil {
		typed, ok := cfg.keyFactory.(func(K) Limiter)
		if !ok {
			panic(fmt.Sprintf(
				"ratelimiter: WithLimiterFactoryForKey was given a %T, but this BucketLimiter's key type is %T",
				cfg.keyFactory, *new(K),
			))
		}

		newLimiterForKey = typed
	}

	if newLimiter == nil && newLimiterForKey == nil {
		panic("ratelimiter: NewBucketLimiter needs either a newLimiter factory or WithLimiterFactoryForKey; both are nil, so no limiter could ever be built")
	}

	b := &BucketLimiter[K]{
		newLimiter:       newLimiter,
		newLimiterForKey: newLimiterForKey,
		deleteAfter:      deleteAfter,
		interval:         interval,
		now:              cfg.now,
		storage:          storage,
		stop:             make(chan struct{}),
		done:             make(chan struct{}),
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
		limiter, _ = b.storage.LoadOrStore(key, b.build(key))
	}

	b.touch(key)
	return limiter
}

// build constructs the Limiter for key, preferring the key-aware factory.
func (b *BucketLimiter[K]) build(key K) Limiter {
	if b.newLimiterForKey != nil {
		return b.newLimiterForKey(key)
	}

	return b.newLimiter()
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
