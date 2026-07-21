package store

import "sync"

type entry struct {
	key        string
	value      ValueRef
	size       int64
	cost       int64
	expiresAt  int64
	generation uint64
}

type shard struct {
	mu sync.RWMutex

	entries map[string]*entry
	policy  slru
	bytes   int64
}

type touchEvent struct {
	shardID    uint32
	key        string
	generation uint64
}

func (e *entry) expired(now int64) bool {
	return e.expiresAt != 0 && e.expiresAt <= now
}

func (s *CoreStore) removeEntryLocked(target *shard, current *entry) (ValueRef, bool) {
	if target.entries[current.key] != current {
		return ValueRef{}, false
	}
	delete(target.entries, current.key)
	target.policy.remove(current.key, current.generation)
	target.bytes -= current.cost
	s.liveBytes.Add(-current.cost)
	s.payloadBytes.Add(-current.size)
	s.entryCount.Add(-1)
	return current.value, true
}
