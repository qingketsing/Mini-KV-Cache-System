package store

import "context"

type evictionExclusion struct {
	shardID    uint32
	key        string
	generation uint64
	enabled    bool
}

func (e evictionExclusion) forShard(shardID uint32) policyExclusion {
	if !e.enabled || e.shardID != shardID {
		return policyExclusion{}
	}
	return policyExclusion{
		key:        e.key,
		generation: e.generation,
		enabled:    true,
	}
}

func (s *CoreStore) makeRoom(ctx context.Context, needed int64, exclusion evictionExclusion) int64 {
	if needed <= 0 {
		return 0
	}
	if err := ctx.Err(); err != nil {
		return 0
	}

	s.evictionMu.Lock()
	defer s.evictionMu.Unlock()

	victimLimit := s.entryCount.Load()
	var freed int64
	var removedEntries int64
	var emptyVisits uint32
	for freed < needed && removedEntries < victimLimit && emptyVisits < s.cfg.ShardCount {
		if err := ctx.Err(); err != nil {
			break
		}
		shardID := s.evictionCursor & (s.cfg.ShardCount - 1)
		s.evictionCursor++
		removed, bytes := s.evictFromShard(
			shardID,
			exclusion.forShard(shardID),
			s.clock.Now().UnixNano(),
			needed-freed,
		)
		if removed == 0 {
			emptyVisits++
			continue
		}
		emptyVisits = 0
		removedEntries += removed
		freed += bytes
	}
	return freed
}

func (s *CoreStore) evictFromShard(shardID uint32, exclusion policyExclusion, now, needed int64) (int64, int64) {
	target := &s.shards[shardID]
	var refs []ValueRef
	var freed int64
	var removed int64
	var expirations uint64
	var evictions uint64

	target.mu.Lock()
	for _, current := range target.entries {
		if !current.expired(now) {
			continue
		}
		ref, ok := s.removeEntryLocked(target, current)
		if !ok {
			continue
		}
		refs = append(refs, ref)
		freed += current.cost
		removed++
		expirations++
		if freed >= needed {
			break
		}
	}

	for freed < needed {
		candidate := target.policy.victim(exclusion)
		if !candidate.ok {
			break
		}
		current := target.entries[candidate.key]
		if current == nil || current.generation != candidate.generation {
			target.policy.remove(candidate.key, candidate.generation)
			continue
		}
		ref, ok := s.removeEntryLocked(target, current)
		if !ok {
			continue
		}
		refs = append(refs, ref)
		freed += current.cost
		removed++
		evictions++
		break
	}
	target.mu.Unlock()

	for _, ref := range refs {
		s.arena.Release(ref)
	}
	if expirations != 0 {
		s.counters.expirations.Add(expirations)
	}
	if evictions != 0 {
		s.counters.evictions.Add(evictions)
	}
	return removed, freed
}

func (s *CoreStore) evictToLowWatermark(ctx context.Context) {
	low := s.cfg.CapacityBytes * lowWatermarkNumerator / watermarkDenominator
	used := s.liveBytes.Load()
	if used <= low {
		return
	}
	s.makeRoom(ctx, used-low, evictionExclusion{})
}

func (s *CoreStore) atHighWatermark() bool {
	return s.liveBytes.Load()*watermarkDenominator >= s.cfg.CapacityBytes*highWatermarkNumerator
}
