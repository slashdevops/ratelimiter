# Migration guide

This release contains breaking API changes alongside a critical correctness
fix. This guide maps every change to a concrete before/after so you can upgrade
mechanically.

> **Why the churn?** The previous version shared a single token bucket across
> every key, so per-key limiting never actually worked. Fixing that required
> constructing an independent limiter per key, which is the root of most of the
> API changes below.

## At a glance

| Area                     | Before                                         | After                                                              |
|--------------------------|------------------------------------------------|-------------------------------------------------------------------|
| Store construction       | `NewInMemoryStorage()`                          | `NewInMemoryStorage[string, ratelimiter.Limiter]()`               |
| Manager construction     | `NewBucketLimiter(baseLimiter, d, storage)`     | `NewBucketLimiter(newLimiterFunc, d, storage)`                    |
| Building the limiter     | a single `*rate.Limiter` instance               | a `func() Limiter` factory (`NewRateLimiterFunc(limit, burst)`)   |
| Lifecycle                | none                                            | `defer manager.Close()`                                           |
| Getting a limiter        | `manager.GetOrAdd(key)`                         | `manager.GetOrAdd(key)` *(unchanged)*                            |
| Calling on the manager   | `manager.Allow()` / `.Wait()` / `.Burst()`      | removed — use `manager.GetOrAdd(key).Allow()` etc.                |
| Manual removal           | `err := manager.Remove(key)`                    | `manager.Remove(key)` *(no return value)*                        |
| `Storage` interface      | non-generic, `Store/Load/Delete`                | generic `Storage[K, V]`, adds `LoadOrStore` and `Range`          |

## 1. `NewInMemoryStorage` now requires type parameters

```go
// Before
storage := ratelimiter.NewInMemoryStorage()

// After
storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()
```

`K` is your key type (commonly `string`); `V` is `ratelimiter.Limiter`.

## 2. `NewBucketLimiter` takes a factory, not a limiter instance

A single `*rate.Limiter` cannot be shared across keys (that was the bug). Pass a
factory that builds a fresh limiter per key instead. Use the `NewRateLimiterFunc`
helper for the common case.

```go
// Before
baseLimiter := rate.NewLimiter(rate.Limit(5), 10)
manager := ratelimiter.NewBucketLimiter(baseLimiter, time.Minute, storage)

// After
newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(5), 10) // 5 rps, burst 10
manager := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
```

If you need custom construction per key, pass your own factory:

```go
newLimiter := func() ratelimiter.Limiter {
    return rate.NewLimiter(rate.Limit(5), 10)
}
```

`K` is inferred from `storage`, so no explicit type arguments are needed on
`NewBucketLimiter`.

## 3. Call `Close()` to stop the eviction goroutine

The manager now runs a single background sweeper for idle eviction. Stop it when
you are done to avoid leaking a goroutine.

```go
manager := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
defer manager.Close()
```

(If you set `deleteAfter <= 0`, eviction is disabled and no goroutine is started;
`Close()` is then a safe no-op.)

## 4. The manager is no longer a `Limiter`

`BucketLimiter` used to expose `Allow()`, `Wait()` and `Burst()` directly — but
those operated on the shared bucket, which was the bug. They are removed. Always
go through a key:

```go
// Before (operated on the shared bucket — incorrect)
if manager.Allow() { ... }

// After (per-key)
if manager.GetOrAdd(key).Allow() { ... }

err := manager.GetOrAdd(key).Wait(ctx)
burst := manager.GetOrAdd(key).Burst()
```

## 5. `Remove` no longer returns an error

```go
// Before
if err := manager.Remove(key); err != nil { ... }

// After
manager.Remove(key)
```

## 6. Custom `Storage` implementations

If you implemented `Storage` yourself, it is now generic and has two additional
methods:

```go
type Storage[K comparable, V any] interface {
    Store(key K, value V)
    Load(key K) (value V, ok bool)
    LoadOrStore(key K, value V) (actual V, loaded bool) // new — must be atomic
    Delete(key K)
    Range(f func(key K, value V) bool)                  // new
}
```

- `LoadOrStore` must be **atomic** so concurrent callers racing on the same key
  all receive the same stored value; the manager relies on this to avoid
  creating duplicate limiters.
- `Range` is used by the manager only indirectly; implement it to iterate the
  current entries (return `false` from `f` to stop early).

## Behavioral changes (no code change required)

- **Per-key isolation now works.** Distinct keys have independent buckets;
  exhausting one key no longer throttles others. If you had worked around the old
  shared-bucket behavior, you can remove those workarounds.
- **Eviction is now idle-based.** A key is evicted after `deleteAfter` of *no
  use*, and every `GetOrAdd` refreshes its timer — previously a key was deleted a
  fixed duration after *creation* regardless of activity. Ensure your
  `deleteAfter` reflects "idle time", not "total lifetime".

## Full before/after example

```go
// Before
storage := ratelimiter.NewInMemoryStorage()
baseLimiter := rate.NewLimiter(rate.Limit(5), 10)
manager := ratelimiter.NewBucketLimiter(baseLimiter, time.Minute, storage)

if manager.GetOrAdd("user-123").Allow() {
    // ...
}
_ = manager.Remove("user-123")
```

```go
// After
storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()
newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(5), 10)
manager := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
defer manager.Close()

if manager.GetOrAdd("user-123").Allow() {
    // ...
}
manager.Remove("user-123")
```
