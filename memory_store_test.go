package ratelimiter

import (
	"maps"
	"sync"
	"testing"
)

func TestInMemoryStorage_StoreLoadDelete(t *testing.T) {
	s := NewInMemoryStorage[string, int]()

	if _, ok := s.Load("missing"); ok {
		t.Error("load of absent key should return zero, false")
	}

	s.Store("a", 1)
	v, ok := s.Load("a")
	if !ok {
		t.Fatal("stored key should be found")
	}
	if v != 1 {
		t.Errorf("Load(\"a\") = %d, want 1", v)
	}

	s.Delete("a")
	if _, ok := s.Load("a"); ok {
		t.Error("key should be gone after Delete")
	}
}

func TestInMemoryStorage_LoadOrStore(t *testing.T) {
	s := NewInMemoryStorage[string, int]()

	actual, loaded := s.LoadOrStore("k", 10)
	if loaded {
		t.Error("first LoadOrStore should report loaded=false")
	}
	if actual != 10 {
		t.Errorf("first LoadOrStore = %d, want 10", actual)
	}

	actual, loaded = s.LoadOrStore("k", 20)
	if !loaded {
		t.Error("second call should load the existing value")
	}
	if actual != 10 {
		t.Errorf("existing value should win: got %d, want 10", actual)
	}
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
		if got != winner {
			t.Errorf("goroutine %d disagreed on the stored value: got %d, want %d", i, got, winner)
		}
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
	if want := map[string]int{"a": 1, "b": 2, "c": 3}; !maps.Equal(seen, want) {
		t.Errorf("Range visited %v, want %v", seen, want)
	}

	// Early stop after the first element.
	count := 0
	s.Range(func(string, int) bool {
		count++
		return false
	})
	if count != 1 {
		t.Errorf("returning false should stop iteration after 1 element, got %d", count)
	}
}
