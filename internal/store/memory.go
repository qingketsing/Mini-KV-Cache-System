package store

import "sync"

// MemoryStore is an in-memory Store implementation for local development.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

// Get returns a copy of the value for key when it exists.
func (s *MemoryStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, false
	}

	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	return copyValue, true
}

// Put stores a copy of value under key.
func (s *MemoryStore) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	s.data[key] = copyValue
}

// Delete removes key and reports whether it existed.
func (s *MemoryStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}

	delete(s.data, key)
	return true
}
