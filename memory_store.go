package ratelimiter

import "sync"

// InMemoryStorage is a goroutine-safe, map-backed implementation of
// [Storage] built on top of [sync.Map]. It keeps every value in process
// memory and never evicts on its own; eviction of idle entries is driven by
// the owning [BucketLimiter].
type InMemoryStorage[K comparable, V any] struct {
	data sync.Map
}

// NewInMemoryStorage returns a ready-to-use, empty [InMemoryStorage].
//
//	storage := ratelimiter.NewInMemoryStorage[string, ratelimiter.Limiter]()
func NewInMemoryStorage[K comparable, V any]() *InMemoryStorage[K, V] {
	return &InMemoryStorage[K, V]{}
}

// Store implements [Storage].
func (s *InMemoryStorage[K, V]) Store(key K, value V) {
	s.data.Store(key, value)
}

// Load implements [Storage].
func (s *InMemoryStorage[K, V]) Load(key K) (value V, ok bool) {
	v, ok := s.data.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return v.(V), true
}

// LoadOrStore implements [Storage]. It is atomic: concurrent callers racing on
// the same key all receive the same stored value.
func (s *InMemoryStorage[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded := s.data.LoadOrStore(key, value)
	return v.(V), loaded
}

// Delete implements [Storage].
func (s *InMemoryStorage[K, V]) Delete(key K) {
	s.data.Delete(key)
}

// Range implements [Storage].
func (s *InMemoryStorage[K, V]) Range(f func(key K, value V) bool) {
	s.data.Range(func(k, v any) bool {
		return f(k.(K), v.(V))
	})
}
