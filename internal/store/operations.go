package store

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
)

func (s *CoreStore) Put(ctx context.Context, key []byte, source io.Reader, options PutOptions) (ObjectInfo, error) {
	if err := s.gate.enter(); err != nil {
		s.rejectPut(err)
		return ObjectInfo{}, err
	}
	defer s.gate.leave()
	callerCtx := ctx
	ctx, cancel := s.operationContext(callerCtx)
	defer cancel()

	if err := s.cancellationError(callerCtx); err != nil {
		s.rejectPut(err)
		return ObjectInfo{}, err
	}
	if err := validateKey(key); err != nil {
		s.rejectPut(err)
		return ObjectInfo{}, err
	}
	if options.Size < 0 {
		s.rejectPut(ErrSizeMismatch)
		return ObjectInfo{}, ErrSizeMismatch
	}
	if options.Size > s.cfg.MaxObjectBytes {
		s.rejectPut(ErrObjectTooLarge)
		return ObjectInfo{}, ErrObjectTooLarge
	}
	if options.TTL < 0 {
		s.rejectPut(ErrInvalidTTL)
		return ObjectInfo{}, ErrInvalidTTL
	}

	if err := s.staging.reserve(ctx, options.Size); err != nil {
		publicErr := s.normalizeOperationError(callerCtx, err)
		s.rejectPut(publicErr)
		return ObjectInfo{}, publicErr
	}
	defer s.staging.release(options.Size)

	ref, err := s.arena.Write(ctx, source, options.Size)
	if err != nil {
		publicErr := s.normalizeOperationError(callerCtx, err)
		s.rejectPut(publicErr)
		return ObjectInfo{}, publicErr
	}
	committed := false
	defer func() {
		if !committed {
			s.arena.Release(ref)
		}
	}()
	if err := ctx.Err(); err != nil {
		publicErr := s.normalizeOperationError(callerCtx, err)
		s.rejectPut(publicErr)
		return ObjectInfo{}, publicErr
	}

	info, err := s.commitStaged(ctx, key, ref, options)
	if err != nil {
		publicErr := s.normalizeOperationError(callerCtx, err)
		s.rejectPut(publicErr)
		return ObjectInfo{}, publicErr
	}
	committed = true
	return info, nil
}

func (s *CoreStore) commitStaged(ctx context.Context, key []byte, ref ValueRef, options PutOptions) (ObjectInfo, error) {
	shardID := sharding.IDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	cost := options.Size + int64(len(immutableKey)) + entryOverheadBytes
	target := &s.shards[shardID]

	for attempt := 0; attempt < maxAdmissionAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return ObjectInfo{}, err
		}
		target.mu.Lock()
		if err := ctx.Err(); err != nil {
			target.mu.Unlock()
			return ObjectInfo{}, err
		}
		old := target.entries[immutableKey]
		delta := cost
		if old != nil {
			delta -= old.cost
		}
		if delta > 0 && !s.reserveLive(delta) {
			shortage := s.capacityShortage(delta)
			exclusion := evictionExclusion{}
			if old != nil {
				exclusion = evictionExclusion{
					shardID:    shardID,
					key:        immutableKey,
					generation: old.generation,
					enabled:    true,
				}
			}
			target.mu.Unlock()
			if attempt+1 < maxAdmissionAttempts {
				s.makeRoom(ctx, shortage, exclusion)
			}
			continue
		}

		now := s.clock.Now()
		expiresAt := int64(0)
		if options.TTL > 0 {
			expiresAt = now.Add(options.TTL).UnixNano()
		}
		generation := s.generation.Add(1)
		current := &entry{
			key:        immutableKey,
			value:      ref,
			size:       options.Size,
			cost:       cost,
			expiresAt:  expiresAt,
			generation: generation,
		}
		target.entries[immutableKey] = current
		target.policy.insert(immutableKey, generation, cost)
		target.bytes += delta
		if delta < 0 {
			s.liveBytes.Add(delta)
		}
		payloadDelta := options.Size
		if old != nil {
			payloadDelta -= old.size
		} else {
			s.entryCount.Add(1)
		}
		s.payloadBytes.Add(payloadDelta)
		target.mu.Unlock()

		if old != nil {
			s.arena.Release(old.value)
		}
		if expiresAt != 0 {
			s.wheel.schedule(expirationEvent{
				shardID:    shardID,
				key:        immutableKey,
				generation: generation,
				expiresAt:  expiresAt,
			})
		}
		s.counters.puts.Add(1)
		if s.atHighWatermark() {
			select {
			case s.pressure <- struct{}{}:
			default:
			}
		}
		return ObjectInfo{Size: options.Size, ExpiresAt: timeFromUnixNano(expiresAt)}, nil
	}
	return ObjectInfo{}, ErrNoCapacity
}

func (s *CoreStore) capacityShortage(delta int64) int64 {
	available := s.cfg.CapacityBytes - s.liveBytes.Load()
	shortage := delta - available
	if shortage < 1 {
		return 1
	}
	return shortage
}

func (s *CoreStore) Get(ctx context.Context, key []byte) (Object, error) {
	if err := s.gate.enter(); err != nil {
		return nil, err
	}
	defer s.gate.leave()
	if err := s.cancellationError(ctx); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}

	s.counters.gets.Add(1)
	shardID := sharding.IDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	target := &s.shards[shardID]
	target.mu.RLock()
	if err := s.cancellationError(ctx); err != nil {
		target.mu.RUnlock()
		return nil, err
	}
	now := s.clock.Now().UnixNano()
	current := target.entries[immutableKey]
	if current == nil {
		target.mu.RUnlock()
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	if current.expired(now) {
		generation := current.generation
		target.mu.RUnlock()
		s.expireIfMatch(shardID, immutableKey, generation, now)
		s.counters.misses.Add(1)
		return nil, ErrNotFound
	}
	reader, err := s.arena.Open(current.value)
	if err != nil {
		target.mu.RUnlock()
		return nil, err
	}
	info := ObjectInfo{Size: current.size, ExpiresAt: timeFromUnixNano(current.expiresAt)}
	generation := current.generation
	keySnapshot := current.key
	target.mu.RUnlock()

	s.counters.hits.Add(1)
	select {
	case s.touches <- touchEvent{shardID: shardID, key: keySnapshot, generation: generation}:
	default:
		s.counters.touchDrops.Add(1)
	}
	return &storeObject{reader: reader, info: info}, nil
}

func (s *CoreStore) Delete(ctx context.Context, key []byte) (bool, error) {
	if err := s.gate.enter(); err != nil {
		return false, err
	}
	defer s.gate.leave()
	if err := s.cancellationError(ctx); err != nil {
		return false, err
	}
	if err := validateKey(key); err != nil {
		return false, err
	}

	shardID := sharding.IDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	target := &s.shards[shardID]
	target.mu.Lock()
	if err := s.cancellationError(ctx); err != nil {
		target.mu.Unlock()
		return false, err
	}
	current := target.entries[immutableKey]
	if current == nil {
		target.mu.Unlock()
		return false, nil
	}
	if current.expired(s.clock.Now().UnixNano()) {
		ref, removed := s.removeEntryLocked(target, current)
		target.mu.Unlock()
		if removed {
			s.arena.Release(ref)
			s.counters.expirations.Add(1)
		}
		return false, nil
	}
	ref, removed := s.removeEntryLocked(target, current)
	target.mu.Unlock()
	if !removed {
		return false, nil
	}
	s.arena.Release(ref)
	s.counters.deletes.Add(1)
	return true, nil
}

func (s *CoreStore) reserveLive(delta int64) bool {
	for {
		current := s.liveBytes.Load()
		if current > s.cfg.CapacityBytes || delta > s.cfg.CapacityBytes-current {
			return false
		}
		if s.liveBytes.CompareAndSwap(current, current+delta) {
			return true
		}
	}
}

func (s *CoreStore) rejectPut(err error) {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		s.counters.rejectedPuts.Add(1)
	}
}

func timeFromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value)
}

type storeObject struct {
	reader io.ReadCloser
	info   ObjectInfo

	once     sync.Once
	closeErr error
}

func (o *storeObject) Read(buffer []byte) (int, error) {
	return o.reader.Read(buffer)
}

func (o *storeObject) Info() ObjectInfo {
	return o.info
}

func (o *storeObject) Close() error {
	o.once.Do(func() {
		o.closeErr = o.reader.Close()
	})
	return o.closeErr
}
