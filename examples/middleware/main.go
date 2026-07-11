// Command middleware demonstrates using ratelimiter as per-client HTTP
// middleware, including standard rate-limit response headers.
//
// It limits requests per client IP address. Each IP gets its own token bucket,
// so one noisy client cannot exhaust the budget of others. When a client is
// throttled the handler responds 429 Too Many Requests with an accurate
// Retry-After header, and every response carries RateLimit-* headers so
// well-behaved clients can self-throttle.
//
// Run it:
//
//	go run ./examples/middleware -limit 2 -burst 1
package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/slashdevops/ratelimiter"
	"golang.org/x/time/rate"
)

// Middleware wraps an http.Handler with additional behavior.
type Middleware func(http.Handler) http.Handler

// ThenFunc adapts an http.HandlerFunc through the middleware.
func (m Middleware) ThenFunc(h http.HandlerFunc) http.Handler {
	return m(http.HandlerFunc(h))
}

// clientIP extracts the client IP from the request. It uses net.SplitHostPort
// so it works for both IPv4 (1.2.3.4:5678) and IPv6 ([::1]:5678) remote
// addresses. In production behind a proxy or load balancer you would instead
// parse a trusted X-Forwarded-For / Forwarded header.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// IPRateLimiter returns middleware that rate limits requests per client IP.
//
// On every request it consults the client's own token bucket via Reserve,
// which lets us both decide whether to allow the request and compute an
// accurate Retry-After when we don't. The following response headers are set:
//
//   - RateLimit-Limit:     bucket capacity (burst) for this client.
//   - RateLimit-Remaining: whole tokens currently available.
//   - RateLimit-Reset:     seconds until the bucket is expected to have a token.
//   - Retry-After:         (429 only) seconds the client should wait before retrying.
//
// The RateLimit-* names follow the IETF "RateLimit header fields for HTTP"
// draft; the widely-deployed X-RateLimit-* variants are set too for
// compatibility with older clients.
func IPRateLimiter(manager *ratelimiter.BucketLimiter[string]) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := clientIP(r)
			limiter := manager.GetOrAdd(ip)

			setHeader(w, "RateLimit-Limit", strconv.Itoa(limiter.Burst()))

			// Prefer the backend-agnostic Reserver capability: it yields the exact
			// delay until a token is available, which drives accurate Retry-After
			// and RateLimit-Reset headers for ANY Limiter implementation — a
			// *rate.Limiter, a Redis/Valkey-backed one, etc. Limiters that cannot
			// reserve degrade to a plain Allow() decision without timing headers.
			reserver, ok := limiter.(ratelimiter.Reserver)
			if !ok {
				if !limiter.Allow() {
					w.Header().Set("Retry-After", "1")
					http.Error(w, fmt.Sprintf("too many requests from %s", ip), http.StatusTooManyRequests)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			res := reserver.Reserve()
			delay := res.Delay()

			setHeader(w, "RateLimit-Reset", strconv.Itoa(secondsCeil(delay)))
			// RateLimit-Remaining is backend-specific; enrich it opportunistically
			// when the limiter can report its current token count.
			if rl, ok := limiter.(ratelimiter.RateLimiter); ok {
				setHeader(w, "RateLimit-Remaining", strconv.Itoa(int(rl.TokensAt(time.Now()))))
			}

			if !res.OK() || delay > 0 {
				// Not allowed right now: give the token back and reject.
				res.Cancel()
				retryAfter := max(secondsCeil(delay), 1)
				w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
				http.Error(w, fmt.Sprintf("too many requests from %s", ip), http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// setHeader writes both the IETF draft name and the legacy X- prefixed name.
func setHeader(w http.ResponseWriter, name, value string) {
	w.Header().Set(name, value)
	w.Header().Set("X-"+name, value)
}

// secondsCeil rounds a duration up to whole seconds (never negative).
func secondsCeil(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}

func main() {
	limit := flag.Int("limit", 2, "sustained rate limit (requests per second)")
	burst := flag.Int("burst", 1, "burst size (bucket capacity)")
	deleteAfter := flag.Duration("deleteAfter", 5*time.Second, "idle duration after which a client's limiter is evicted")
	numberOfRequests := flag.Int("numberOfRequests", 30, "number of demo requests to send")
	waitBetweenRequests := flag.Duration("waitBetweenRequests", 100*time.Millisecond, "wait time between demo requests")
	flag.Parse()

	storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()
	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Limit(float64(*limit)), *burst)
	manager := ratelimiter.NewBucketLimiter(newLimiter, *deleteAfter, storage)
	defer manager.Close()

	mux := http.NewServeMux()
	mux.Handle("GET /", IPRateLimiter(manager).ThenFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello, world!")
	}))

	// Demo client that fires requests at the local server.
	go func() {
		fmt.Println("Waiting for server to start...")
		time.Sleep(500 * time.Millisecond)

		for range *numberOfRequests {
			resp, err := http.Get("http://localhost:8080/")
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusOK:
				fmt.Printf("allowed  (remaining=%s)\n", resp.Header.Get("RateLimit-Remaining"))
			default:
				fmt.Printf("rejected %s (retry-after=%s)\n", resp.Status, resp.Header.Get("Retry-After"))
			}
			time.Sleep(*waitBetweenRequests)
		}
		fmt.Println("\nFinished sending requests, press Ctrl+C to stop the server")
	}()

	fmt.Println("Starting server on :8080...")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
