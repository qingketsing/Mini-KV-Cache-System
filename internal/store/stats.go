package store

import "sync/atomic"

type storeCounters struct {
	gets         atomic.Uint64
	hits         atomic.Uint64
	misses       atomic.Uint64
	puts         atomic.Uint64
	deletes      atomic.Uint64
	evictions    atomic.Uint64
	expirations  atomic.Uint64
	rejectedPuts atomic.Uint64
	touchDrops   atomic.Uint64
}

func (s *CoreStore) Stats() Stats {
	return Stats{
		CapacityBytes: s.cfg.CapacityBytes,
		LiveBytes:     s.liveBytes.Load(),
		PayloadBytes:  s.payloadBytes.Load(),
		StagingBytes:  s.staging.usedBytes(),
		Entries:       s.entryCount.Load(),
		Gets:          s.counters.gets.Load(),
		Hits:          s.counters.hits.Load(),
		Misses:        s.counters.misses.Load(),
		Puts:          s.counters.puts.Load(),
		Deletes:       s.counters.deletes.Load(),
		Evictions:     s.counters.evictions.Load(),
		Expirations:   s.counters.expirations.Load(),
		RejectedPuts:  s.counters.rejectedPuts.Load(),
		TouchDrops:    s.counters.touchDrops.Load(),
	}
}
