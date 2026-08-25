package ratelimiter

import (
	"context"
	"hash/maphash"
	"sync"
	"time"
)

// memoryShards is how many independently-locked maps [MemoryBackend] spreads
// keys across. A rate limiter's hot path is one map operation per request, so a
// single mutex would serialise the whole server on it. 64 is enough that
// contention is negligible at any realistic core count while the memory
// overhead stays trivial.
const memoryShards = 64

// MemoryBackend is an in-process [Backend]: a sliding-window counter, sharded
// for concurrency, with its own expiry.
//
// # Why a sliding window and not a token bucket
//
// This package already has a token bucket — [RateLimiter], wrapping
// golang.org/x/time/rate — and it is the better in-process limiter: continuous
// refill, exact reservations, no window edges. MemoryBackend deliberately does
// NOT reproduce it.
//
// It exists so that the local and distributed paths agree. A Valkey or Redis
// backend will be a window counter, because that is what can be done in one
// atomic command; if the in-process backend were a token bucket, turning a
// cache on or off would silently change how traffic is admitted at a window
// boundary. Matching the algorithm makes [Backend] a seam you can move through
// without the behaviour moving under you.
//
// Use [RateLimiter] when you want the best in-process limiter. Use
// MemoryBackend when you want the same limiter you would get from a datastore,
// without the datastore.
//
// # The algorithm
//
// Two adjacent fixed windows, weighted by how far into the current one the
// clock is:
//
//	weighted = previous*(1 - elapsed/period) + current
//
// A plain fixed window admits up to 2x the budget across a boundary — spend it
// all at the end of one window and again at the start of the next. The
// weighting reduces that to a small approximation error while still costing one
// counter per window.
//
// The zero value is not usable; construct one with [NewMemoryBackend].
type MemoryBackend struct {
	shards [memoryShards]memoryShard
	seed   maphash.Seed
	now    func() time.Time

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type memoryShard struct {
	mu      sync.Mutex
	entries map[string]*memoryEntry
}

type memoryEntry struct {
	window   int64 // the current window index
	current  int
	previous int
	seen     time.Time
}

// MemoryBackendOption configures a [MemoryBackend].
type MemoryBackendOption func(*memoryBackendConfig)

type memoryBackendConfig struct {
	now           func() time.Time
	sweepInterval time.Duration
}

// WithMemoryClock overrides the time source, which makes window boundaries
// deterministic in tests.
func WithMemoryClock(now func() time.Time) MemoryBackendOption {
	return func(c *memoryBackendConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// WithMemorySweepInterval overrides how often idle keys are evicted. Zero
// disables the background sweeper, leaving only the lazy expiry that happens
// when a key is next touched — which is enough for a bounded key space and a
// leak for an unbounded one.
func WithMemorySweepInterval(d time.Duration) MemoryBackendOption {
	return func(c *memoryBackendConfig) {
		c.sweepInterval = d
	}
}

// NewMemoryBackend returns a ready-to-use MemoryBackend and starts its sweeper.
// Call [MemoryBackend.Close] to stop it.
func NewMemoryBackend(opts ...MemoryBackendOption) *MemoryBackend {
	cfg := memoryBackendConfig{now: time.Now, sweepInterval: time.Minute}
	for _, opt := range opts {
		opt(&cfg)
	}

	b := &MemoryBackend{
		seed: maphash.MakeSeed(),
		now:  cfg.now,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	for i := range b.shards {
		b.shards[i].entries = make(map[string]*memoryEntry)
	}

	if cfg.sweepInterval > 0 {
		go b.sweepLoop(cfg.sweepInterval)
	} else {
		close(b.done)
	}

	return b
}

func (b *MemoryBackend) shard(key string) *memoryShard {
	return &b.shards[maphash.String(b.seed, key)%memoryShards]
}

// Take implements [Backend].
func (b *MemoryBackend) Take(_ context.Context, key string, limit Limit, cost int) (Decision, error) {
	if limit.Period <= 0 || limit.Requests <= 0 {
		// A limit that allows nothing, or a nonsense window. Refusing is the
		// safe reading: a misconfigured rule must not become an open door.
		return Decision{Allowed: false, Remaining: 0}, nil
	}

	if cost <= 0 {
		cost = 1
	}

	now := b.now()
	window := now.UnixNano() / int64(limit.Period)
	elapsed := float64(now.UnixNano()%int64(limit.Period)) / float64(limit.Period)

	sh := b.shard(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[key]
	if !ok {
		e = &memoryEntry{window: window}
		sh.entries[key] = e
	}

	// Roll the windows forward. A gap of two or more windows means everything
	// known is stale, so both counters reset rather than one shifting into a
	// slot it does not belong in.
	switch delta := window - e.window; {
	case delta == 1:
		e.previous, e.current = e.current, 0
	case delta > 1:
		e.previous, e.current = 0, 0
	}

	e.window = window
	e.seen = now

	weighted := float64(e.previous)*(1-elapsed) + float64(e.current)

	if int(weighted)+cost > limit.Requests {
		return Decision{
			Allowed:    false,
			Remaining:  0,
			RetryAfter: time.Duration((1 - elapsed) * float64(limit.Period)),
		}, nil
	}

	e.current += cost

	remaining := limit.Requests - (int(weighted) + cost)
	if remaining < 0 {
		remaining = 0
	}

	return Decision{Allowed: true, Remaining: remaining}, nil
}

// sweepLoop evicts keys nobody has touched for long enough that both their
// windows are stale.
func (b *MemoryBackend) sweepLoop(interval time.Duration) {
	defer close(b.done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.sweep(interval)
		}
	}
}

func (b *MemoryBackend) sweep(idleFor time.Duration) {
	cutoff := b.now().Add(-2 * idleFor)

	for i := range b.shards {
		sh := &b.shards[i]

		sh.mu.Lock()

		for key, e := range sh.entries {
			if e.seen.Before(cutoff) {
				delete(sh.entries, key)
			}
		}

		sh.mu.Unlock()
	}
}

// Len reports how many keys are currently held. It is intended for tests and
// metrics, and is a best-effort snapshot rather than a consistent one.
func (b *MemoryBackend) Len() int {
	n := 0

	for i := range b.shards {
		sh := &b.shards[i]

		sh.mu.Lock()
		n += len(sh.entries)
		sh.mu.Unlock()
	}

	return n
}

// Close stops the background sweeper and waits for it to exit. It is safe to
// call more than once and from several goroutines. Take keeps working after
// Close; only eviction stops.
func (b *MemoryBackend) Close() {
	b.closeOnce.Do(func() { close(b.stop) })
	<-b.done
}

// compile-time check that the backend satisfies the port.
var _ Backend = (*MemoryBackend)(nil)
