// Command customstorage shows how to plug your own [ratelimiter.Storage]
// implementation into a [ratelimiter.BucketLimiter] instead of the bundled
// [ratelimiter.InMemoryStorage].
//
// The store here is a size-bounded LRU: it keeps at most -cap distinct keys and
// evicts the least-recently-used key when it would overflow. This is the classic
// reason to bring your own store — capping memory for an unbounded key space
// (for example per-IP limiting exposed to the internet) instead of, or in
// addition to, the manager's time-based idle eviction.
//
// Run it:
//
//	go run ./examples/customstorage -cap 2
//
// Watch the log lines: asking for a third distinct key evicts the
// least-recently-used one, and a limiter rebuilt after eviction starts from a
// fresh, full bucket.
package main

import (
	"container/list"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/slashdevops/ratelimiter"
	"golang.org/x/time/rate"
)

// LRUStorage is a goroutine-safe, size-bounded implementation of
// [ratelimiter.Storage]. It holds at most capacity entries; inserting beyond
// that evicts the least-recently-used key. Load and LoadOrStore count as uses
// and move a key to the most-recently-used position.
//
// It stores the Limiter instances in process memory, exactly like the bundled
// InMemoryStorage — the only added behavior is the LRU size cap.
type LRUStorage[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List // front = most-recently-used
	items    map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUStorage returns an empty store that keeps at most capacity keys.
func NewLRUStorage[K comparable, V any](capacity int) *LRUStorage[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &LRUStorage[K, V]{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[K]*list.Element, capacity),
	}
}

// Store implements [ratelimiter.Storage].
func (s *LRUStorage[K, V]) Store(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(key, value)
}

// Load implements [ratelimiter.Storage]. A hit is promoted to most-recently-used.
func (s *LRUStorage[K, V]) Load(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		s.ll.MoveToFront(el)
		return el.Value.(*lruEntry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// LoadOrStore implements [ratelimiter.Storage]. It is atomic under the mutex, so
// concurrent callers racing on the same new key all receive the same value.
func (s *LRUStorage[K, V]) LoadOrStore(key K, value V) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		s.ll.MoveToFront(el)
		return el.Value.(*lruEntry[K, V]).value, true
	}
	s.set(key, value)
	return value, false
}

// Delete implements [ratelimiter.Storage].
func (s *LRUStorage[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		s.remove(el)
	}
}

// Range implements [ratelimiter.Storage]. Iteration order is most- to
// least-recently-used and does not count as a use.
func (s *LRUStorage[K, V]) Range(f func(K, V) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for el := s.ll.Front(); el != nil; el = el.Next() {
		e := el.Value.(*lruEntry[K, V])
		if !f(e.key, e.value) {
			return
		}
	}
}

// set inserts or updates key and evicts the LRU entry if over capacity.
// Callers must hold s.mu.
func (s *LRUStorage[K, V]) set(key K, value V) {
	if el, ok := s.items[key]; ok {
		el.Value.(*lruEntry[K, V]).value = value
		s.ll.MoveToFront(el)
		return
	}
	el := s.ll.PushFront(&lruEntry[K, V]{key: key, value: value})
	s.items[key] = el
	if s.ll.Len() > s.capacity {
		if oldest := s.ll.Back(); oldest != nil {
			evicted := oldest.Value.(*lruEntry[K, V]).key
			s.remove(oldest)
			fmt.Printf("  [LRUStorage] capacity %d exceeded, evicted %v\n", s.capacity, evicted)
		}
	}
}

// remove deletes an element from both the list and the index. Callers must hold s.mu.
func (s *LRUStorage[K, V]) remove(el *list.Element) {
	s.ll.Remove(el)
	delete(s.items, el.Value.(*lruEntry[K, V]).key)
}

func main() {
	capacity := flag.Int("cap", 2, "maximum number of distinct keys kept in the store")
	flag.Parse()

	// Compile-time check that our type satisfies the interface.
	var _ ratelimiter.Storage[string, ratelimiter.Limiter] = NewLRUStorage[string, ratelimiter.Limiter](*capacity)

	// Bring our own store instead of ratelimiter.NewInMemoryStorage(...).
	store := NewLRUStorage[string, ratelimiter.Limiter](*capacity)

	// One token per key; a used key needs ~1s to refill, so re-use is observable.
	newLimiter := ratelimiter.NewRateLimiterFunc(rate.Every(time.Second), 1)

	// deleteAfter <= 0 disables the manager's time-based eviction so the only
	// thing bounding memory here is our LRU cap — keeping the demo focused.
	manager := ratelimiter.NewBucketLimiter(newLimiter, 0, store)
	defer manager.Close()

	fmt.Printf("store capacity = %d\n\n", *capacity)

	// Touch three distinct keys. With cap=2 the first key is evicted when the
	// third arrives; its bucket is then rebuilt fresh on the next request.
	for _, key := range []string{"alice", "bob", "carol", "alice"} {
		allowed := manager.GetOrAdd(key).Allow()
		fmt.Printf("request %-6s -> allowed=%v\n", key, allowed)
	}

	fmt.Println("\ncurrent keys (most- to least-recently-used):")
	store.Range(func(k string, _ ratelimiter.Limiter) bool {
		fmt.Printf("  %s\n", k)
		return true
	})
}
