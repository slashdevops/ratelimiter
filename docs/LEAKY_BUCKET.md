# Leaky bucket

A leaky bucket configured as a *meter* and a token bucket are **the same
policy**. That is not a caveat buried at the bottom of this page — it is the
first thing to understand, because the internet is full of comparisons that
imply otherwise by quietly changing the burst setting between the two examples.

Measured, with this package:

```text
token bucket, 10/s burst 1   → 1 admitted immediately, 1 more after 250ms
leaky bucket, 100ms cap 1    → 1 admitted immediately, 1 more after 250ms
```

Identical. They are duals: a token bucket that refills at rate `r` with capacity
`b` admits exactly what a leaky bucket draining at `r` with capacity `b` admits.
If you see a comparison claiming one bursts and the other does not, check
whether the two sides were given the same capacity.

## So why is it here?

Two reasons, both real and neither of them "different behaviour".

**1. It is parameterised by spacing, not by rate-and-burst.** You configure it
with an *interval* — "one every 100ms" — which makes strict pacing the obvious
configuration. With a token bucket, the same thing is spelled `burst: 1`, which
is easy to leave at a default and get a burst you did not intend. The
parameterisation is the feature: it makes the safe configuration the natural one
to write.

**2. The implementation is exact.** GCRA keeps a single `time.Time` — the
theoretical arrival time — and does integer-duration arithmetic. There are no
floating-point tokens to accumulate rounding error over a long uptime, no
allocation, and every operation is O(1).

If you already have a token bucket with the burst you want, **you do not need to
switch.** Reach for the leaky bucket when you are expressing a *pacing*
requirement — "no more than one call every N" — and want the code to say that.

## Table of contents

- [When to choose which](#when-to-choose-which)
- [How it works: virtual scheduling](#how-it-works-virtual-scheduling)
- [Capacity](#capacity)
- [Shaping versus dropping](#shaping-versus-dropping)
- [Cancel actually works here](#cancel-actually-works-here)
- [Usage](#usage)
- [Choosing between them at run time](#choosing-between-them-at-run-time)
- [Further reading](#further-reading)

## When to choose which

| | Token bucket | Leaky bucket |
| --- | --- | --- |
| Configured by | rate + burst | interval + capacity |
| Natural default | absorbs a burst | paces |
| Same parameters | **identical behaviour** | **identical behaviour** |
| Arithmetic | float tokens | exact integer durations |
| Shaping (`Wait`) | yes | yes |
| `Cancel()` returns the slot | yes | yes |

The rule of thumb is about **what you are expressing**, not what you get:

- *"No more than 1000 an hour"* is a budget → token bucket.
- *"No more than one call every 100ms"* is a pace → leaky bucket.

Both can be made to do the other's job. Saying it the way you mean it is the
point.

## How it works: virtual scheduling

There is no queue and no timer — the "bucket" is one instant.

The limiter stores a **theoretical arrival time** (TAT): when the next
conforming request may be admitted. Admitting a request pushes the TAT forward
by one emission interval. A request arriving more than the burst allowance ahead
of the TAT is refused, and **the amount it is early by *is* the retry delay**.

```text
interval  = period / requests          e.g. 60/min → 1s
tolerance = capacity × interval

on arrival at t:
    tat     = max(stored_tat, t)
    next    = tat + interval
    allowAt = next − tolerance

    if t < allowAt:  refuse, retry after (allowAt − t)
    else:            stored_tat = next, admit
```

This is GCRA — the Generic Cell Rate Algorithm, from ATM traffic policing.
Every operation is O(1), allocation-free and exact: nothing accumulates, so
there is no floating-point drift to worry about over long uptimes.

## Capacity

`capacity` is how many requests may be taken back-to-back from an **idle**
bucket.

- **`1` is a strict leaky bucket**: perfectly even spacing, no burst at all.
  This is what you want in front of a rate-limited third party.
- **`n`** admits `n` immediately and then paces at the interval — a token
  bucket's shape with a leaky bucket's recovery. Note the difference from a
  token bucket: after the burst, capacity comes back **one interval at a time**,
  not as a refilling pool.

A capacity below one is raised to one. A bucket that admits nothing is a
configuration error, and refusing every request for ever is the least helpful
way to report it.

## Shaping versus dropping

This is the choice that matters at the call site:

```go
lb.Wait(ctx)   // SHAPES: sleeps until the request conforms
lb.Allow()     // DROPS:  returns false immediately
```

`Wait` is what makes a leaky bucket a *shaper* — traffic is smoothed rather than
rejected, which is usually what you want for a background worker draining a
queue against a rate-limited API.

`Allow` is for the cases where waiting is worse than failing: an HTTP handler
would generally rather answer `429` than hold a connection open.

`Wait` rolls its reservation back if the context ends first, so a caller who
gives up does not make the next caller wait for a request that never happened.

## Cancel actually works here

Worth knowing if you are choosing a backend:

- A **window counter** cannot hand a slot back in a way that is distinguishable
  from spending one less, so its `Cancel()` is a documented no-op and a rejected
  request still counts.
- A **leaky bucket** *can*: the TAT is a single instant, so cancelling is
  rewinding it by one interval. A rejected request need not count against the
  caller.

```go
res := lb.Reserve()
if !res.OK() {
	w.Header().Set("Retry-After", strconv.Itoa(int(res.Delay().Seconds())))
	http.Error(w, "slow down", http.StatusTooManyRequests)
	res.Cancel()   // the slot goes back
	return
}
```

`Cancel` is idempotent — a `defer` plus an explicit call cannot double-refund.

## Usage

```go
// One request per second, strictly evenly spaced.
lb := ratelimiter.NewLeakyBucket(time.Second, 1)

if lb.Allow() {
	// conforms
}
```

Per key, through the manager:

```go
// Every client IP gets one request per second, no bursting.
bl := ratelimiter.NewBucketLimiter(
	ratelimiter.NewLeakyBucketFunc(time.Second, 1),
	time.Minute,
	ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter](),
)
defer bl.Close()

if bl.GetOrAdd(clientIP).Allow() {
	// ...
}
```

## Choosing between them at run time

The strategy is a value, so it can come from a database column, a YAML field or
an environment variable:

```go
strategy, err := ratelimiter.ParseStrategy(row.Strategy) // "token_bucket" | "leaky_bucket"
if err != nil {
	return err
}

limit := ratelimiter.Limit{Requests: row.Requests, Period: row.Period, Burst: row.Burst}

newLimiter, err := ratelimiter.NewLimiterFunc(strategy, limit)
if err != nil {
	return err
}

bl := ratelimiter.NewBucketLimiter(newLimiter, time.Minute, storage)
```

**One `Limit` describes both.** The same "N requests per period" an operator
writes down; the strategy decides how it is enforced. The burst means slightly
different things — bucket size for one, back-to-back allowance for the other —
which is stated here rather than left to be discovered.

`ParseStrategy` **rejects** an unrecognised value rather than defaulting. A typo
that silently became `token_bucket` would admit bursts the operator specifically
asked it not to, and nothing about the running service would say so.

`ratelimiter.Strategies()` returns every valid value, for populating a form or
validating a schema.

## Further reading

- [TOKEN_BUCKET.md](./TOKEN_BUCKET.md) — the default algorithm, and the
  comparison table this page is the other half of
- [BACKENDS.md](./BACKENDS.md) — sharing one limit across processes
- [CUSTOM_STORAGE.md](./CUSTOM_STORAGE.md) — in-process containers
- GCRA / leaky bucket — <https://en.wikipedia.org/wiki/Leaky_bucket>
- Generic Cell Rate Algorithm — <https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm>
