package ratelimiter

// Storage is the pluggable backing store used by [BucketLimiter] to keep the
// per-key [Limiter] instances. The default implementation is
// [InMemoryStorage], but any in-process store (for example a size-bounded LRU)
// can be supplied by implementing this interface.
//
// Implementations MUST be safe for concurrent use by multiple goroutines.
//
// K is the key type (commonly string) and V is the stored value type
// (commonly [Limiter]).
type Storage[K comparable, V any] interface {
	// Store unconditionally sets value for key.
	Store(key K, value V)

	// Load returns the value stored for key, and whether it was present.
	Load(key K) (value V, ok bool)

	// LoadOrStore returns the existing value for key if present; otherwise it
	// stores and returns the given value. loaded reports whether the value was
	// already present. It MUST be atomic so that concurrent callers racing on
	// the same key all observe the same stored value.
	LoadOrStore(key K, value V) (actual V, loaded bool)

	// Delete removes key from the store. Deleting an absent key is a no-op.
	Delete(key K)

	// Range calls f sequentially for each key/value currently present. If f
	// returns false, Range stops iterating. The store may be modified during
	// iteration; the observed set of keys is a best-effort snapshot.
	Range(f func(key K, value V) bool)
}
