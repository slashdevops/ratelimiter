package ratelimiter

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryStorage_StoreLoadDelete(t *testing.T) {
	s := NewInMemoryStorage[string, int]()

	_, ok := s.Load("missing")
	assert.False(t, ok, "load of absent key returns zero, false")

	s.Store("a", 1)
	v, ok := s.Load("a")
	require.True(t, ok)
	assert.Equal(t, 1, v)

	s.Delete("a")
	_, ok = s.Load("a")
	assert.False(t, ok)
}

func TestInMemoryStorage_LoadOrStore(t *testing.T) {
	s := NewInMemoryStorage[string, int]()

	actual, loaded := s.LoadOrStore("k", 10)
	assert.False(t, loaded)
	assert.Equal(t, 10, actual)

	actual, loaded = s.LoadOrStore("k", 20)
	assert.True(t, loaded, "second call should load the existing value")
	assert.Equal(t, 10, actual, "existing value wins")
}

// TestInMemoryStorage_LoadOrStore_Atomic verifies concurrent racers converge on
// a single stored value.
func TestInMemoryStorage_LoadOrStore_Atomic(t *testing.T) {
	s := NewInMemoryStorage[string, int]()

	const n = 100
	results := make([]int, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			actual, _ := s.LoadOrStore("k", i)
			results[i] = actual
		}()
	}
	wg.Wait()

	winner := results[0]
	for i, got := range results {
		assert.Equal(t, winner, got, "goroutine %d disagreed on the stored value", i)
	}
}

func TestInMemoryStorage_Range(t *testing.T) {
	s := NewInMemoryStorage[string, int]()
	s.Store("a", 1)
	s.Store("b", 2)
	s.Store("c", 3)

	seen := map[string]int{}
	s.Range(func(k string, v int) bool {
		seen[k] = v
		return true
	})
	assert.Equal(t, map[string]int{"a": 1, "b": 2, "c": 3}, seen)

	// Early stop after the first element.
	count := 0
	s.Range(func(string, int) bool {
		count++
		return false
	})
	assert.Equal(t, 1, count, "returning false stops iteration")
}
