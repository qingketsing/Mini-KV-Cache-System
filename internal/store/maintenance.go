package store

import (
	"context"
	"time"
)

func (s *CoreStore) maintenanceLoop() {
	ticker := time.NewTicker(s.cfg.TTLResolution)
	defer ticker.Stop()
	defer close(s.maintenanceDone)

	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			s.maintenanceStep(now)
		case <-s.pressure:
			s.maintenanceStep(s.clock.Now())
		}
	}
}

func (s *CoreStore) maintenanceStep(now time.Time) {
	for _, event := range s.wheel.advance(now) {
		s.expireIfMatch(event.shardID, event.key, event.generation, now.UnixNano())
	}

drainTouches:
	for processed := 0; processed < s.cfg.TouchBuffer; processed++ {
		select {
		case event := <-s.touches:
			s.applyTouch(event)
		default:
			break drainTouches
		}
	}

	if s.atHighWatermark() {
		s.evictToLowWatermark(context.Background())
	}
}

func (s *CoreStore) applyTouch(event touchEvent) {
	target := &s.shards[event.shardID]
	target.mu.Lock()
	current := target.entries[event.key]
	if current != nil && current.generation == event.generation {
		target.policy.touch(event.key, event.generation)
	}
	target.mu.Unlock()
}

func (s *CoreStore) expireIfMatch(shardID uint32, key string, generation uint64, now int64) bool {
	target := &s.shards[shardID]
	target.mu.Lock()
	current := target.entries[key]
	if current == nil || current.generation != generation || !current.expired(now) {
		target.mu.Unlock()
		return false
	}
	ref, removed := s.removeEntryLocked(target, current)
	target.mu.Unlock()
	if !removed {
		return false
	}
	s.arena.Release(ref)
	s.counters.expirations.Add(1)
	return true
}
