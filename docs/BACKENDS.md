# Backends — one limit shared across processes

`Storage` holds limiters **in this process**. `Backend` holds the **count**,
wherever you want it: an in-process map, Valkey, Redis, DynamoDB, Postgres.

```go
type Backend interface {
	Take(ctx context.Context, key string, limit Limit, cost int) (Decision, error)
}
```

That is the whole interface. Implement it and every process shares one budget.

## Why it is `Take`-shaped and not `Get`/`Set`-shaped

This is the part worth reading before writing an implementation, because the
obvious design does not work.

It is tempting to define a backend as a small key-value interface — `Get`,
`Set`, `Incr` — and let this package do the token arithmetic. For an in-process
store that is fine. Across a network it is a race:

```text
read (tokens, timestamp) → refill by elapsed time → compare → write back
```

is a read-modify-write. Two processes read the same state, both decide they may
proceed, and both write. **The limit silently becomes 2×.** Making it safe needs
either a transaction with a retry loop — on a key that is contended by
definition, because one caller hammering one endpoint is the case a rate limiter
exists for — or a server-side script. Neither is expressible through `Get`/`Set`.

So the decision has to run **where the state lives**, and the interface has to be
the *decision*, not the storage. `Take` is that: one call, one answer, atomicity
owned by the implementation because only the implementation can provide it.

This is also exactly why [`Storage`](./CUSTOM_STORAGE.md) cannot be used for
distributed limiting. `Storage` holds `Limiter` **values** in this process; its
`Load`/`Store` shape *is* the `Get`/`Set` shape above.

| You want… | Extension point |
| --- | --- |
| a different **in-process container** (LRU, metrics) | `Storage` |
| a different **algorithm** | `Limiter` |
| a **shared** limit across processes | **`Backend`** |

## Wiring it up

```go
backend := ratelimiter.NewMemoryBackend()   // or your own
limit   := ratelimiter.Limit{Requests: 100, Period: time.Minute}

bl := ratelimiter.NewBucketLimiter(nil, time.Minute,
	ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
	ratelimiter.WithLimiterFactoryForKey(
		ratelimiter.NewBackendLimiterFunc(backend, limit,
			ratelimiter.WithFallback(local),
		),
	),
)
```

The `Storage` is the ordinary in-memory one, and stays that way whichever backend
you use: it caches one lightweight handle per key. The count lives in the
backend.

## Switching on configuration

The whole "shared when the datastore is there, local when it is not" decision is
one branch:

```go
newLimiter := func(key string) ratelimiter.Limiter {
	local := ratelimiter.RateLimiter{Limiter: rate.NewLimiter(limit.Rate(), limit.Capacity())}

	if backend == nil {           // cache disabled
		return local
	}

	return ratelimiter.NewBackendLimiter(backend, key, limit,
		ratelimiter.WithFallback(local))
}
```

## What `BackendLimiter` adds

Three things, and they are in the library rather than copied into every consumer
because getting any of them wrong is expensive.

### 1. A local fallback

A remote backend can be unreachable, and **neither answer is acceptable on its
own**:

- *Refusing* turns a cache blip into a total outage — self-inflicted, larger than
  any attack the limiter prevents, arriving at the worst possible moment.
- *Allowing* deletes the limiter exactly when it is needed.

With `WithFallback` there is nothing to choose between: there is always a local
answer, so an outage degrades to **per-process limiting** — which is what you
had before you added a backend at all.

Without a fallback an unreachable backend allows the request, because the
alternative is making the backend a hard dependency of every request. The choice
is logged.

### 2. A circuit breaker

Falling back is only **half** a fallback. Without a breaker, every request
during an outage pays a failed round trip — connect, wait, time out — before
reaching the local answer it was always going to get. The limiter keeps limiting
correctly *and* makes the whole service slower by the timeout, for the duration
of the outage.

After `FailureThreshold` consecutive failures the backend is skipped entirely
until `Cooldown` elapses, then probed once by a single caller.

Recovery needs no repair: a window-keyed backend either finds or creates the
current window on the next call.

### 3. A timeout

This call sits in front of everything else the process does, so its budget
should be far tighter than a normal query. Default 100 ms.

## Observability

Being in the degraded state is **invisible from a request**: the service keeps
working, keeps limiting, and is silently enforcing N × the intended limit across
N processes.

```go
ratelimiter.WithOnDegraded(func(key string, degraded bool) {
	gauge.Set(key, degraded)
})
```

**Alert on the state, not on the error rate.** A handful of errors is noise; a
sustained fallback is the thing somebody has to know about.

## Writing a Valkey backend

This package has no Valkey dependency and will not grow one, so the client code
is yours. It is short:

```go
type ValkeyBackend struct{ client valkey.Client }

func (b ValkeyBackend) Take(ctx context.Context, key string, limit ratelimiter.Limit, cost int) (ratelimiter.Decision, error) {
	now    := time.Now()
	window := now.UnixNano() / int64(limit.Period)
	k      := fmt.Sprintf("rl:%s:%d", key, window)

	// INCR is atomic on its own — no script, no WATCH, no dedicated connection.
	n, err := b.client.Do(ctx, b.client.B().Incr().Key(k).Build()).AsInt64()
	if err != nil {
		return ratelimiter.Decision{}, err
	}

	// Only the first hit of a window needs an expiry.
	if n == 1 {
		_ = b.client.Do(ctx, b.client.B().Pexpire().Key(k).
			Milliseconds(int64(2*limit.Period/time.Millisecond)).Build()).Error()
	}

	if n > int64(limit.Requests) {
		elapsed := time.Duration(now.UnixNano() % int64(limit.Period))

		return ratelimiter.Decision{Allowed: false, RetryAfter: limit.Period - elapsed}, nil
	}

	return ratelimiter.Decision{Allowed: true, Remaining: limit.Requests - int(n)}, nil
}
```

That is a **fixed** window — simple, one round trip, and it admits up to 2× the
budget across a boundary. `MemoryBackend` shows the two-window weighting that
reduces that to a small approximation error while still costing one counter per
window; the same technique works over `INCR` with two keys read together.

If you want an exact token bucket with continuous refill, that needs a
server-side script, because the refill is a read-modify-write. `Take` does not
care which you choose — that is the point of putting the decision behind an
interface.

## See also

- [CUSTOM_STORAGE.md](./CUSTOM_STORAGE.md) — the in-process container interface,
  and why it is not this one
- [TOKEN_BUCKET.md](./TOKEN_BUCKET.md) — the default in-process algorithm, and
  how a window counter differs from it
- [MIGRATION.md](./MIGRATION.md) — upgrading; `Backend` is purely additive

## Contract for implementers

- **Safe for concurrent use.**
- **`Take` must be atomic per key.** Two concurrent calls for the same key must
  not both succeed on the strength of the same remaining token.
- **Expire your own state.** A rate limiter's key space is usually unbounded —
  one entry per client address — so a backend that never forgets is a leak.
- **Return an error rather than a guess.** An error means "I could not decide",
  and `BackendLimiter` knows what to do with that. A backend that invents an
  answer takes that choice away from the caller.
- `Remaining` may be `-1` for "unknown"; callers must not read a negative value
  as zero.
