package store

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

func (s *CoreStore) Put(ctx context.Context, key []byte, source io.Reader, options PutOptions) (ObjectInfo, error) {
	if err := s.gate.enter(); err != nil {
		return ObjectInfo{}, err
	}
	defer s.gate.leave()

	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if err := validateKey(key); err != nil {
		s.rejectPut(ctx)
		return ObjectInfo{}, err
	}
	if options.Size < 0 {
		s.rejectPut(ctx)
		return ObjectInfo{}, ErrSizeMismatch
	}
	if options.Size > s.cfg.MaxObjectBytes {
		s.rejectPut(ctx)
		return ObjectInfo{}, ErrObjectTooLarge
	}
	if options.TTL < 0 {
		s.rejectPut(ctx)
		return ObjectInfo{}, ErrInvalidTTL
	}

	if err := s.staging.reserve(ctx, options.Size); err != nil {
		s.rejectPut(ctx)
		return ObjectInfo{}, err
	}
	defer s.staging.release(options.Size)

	ref, err := s.arena.Write(ctx, source, options.Size)
	if err != nil {
		s.rejectPut(ctx)
		return ObjectInfo{}, err
	}
	committed := false
	defer func() {
		if !committed {
			s.arena.Release(ref)
		}
	}()
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}

	shardID := shardIDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	target := &s.shards[shardID]
	target.mu.Lock()
	if err := ctx.Err(); err != nil {
		target.mu.Unlock()
		return ObjectInfo{}, err
	}
	old := target.entries[immutableKey]
	cost := options.Size + int64(len(immutableKey)) + entryOverheadBytes
	delta := cost
	if old != nil {
		delta -= old.cost
	}
	if delta > 0 && !s.reserveLive(delta) {
		target.mu.Unlock()
		s.rejectPut(ctx)
		return ObjectInfo{}, ErrNoCapacity
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

	committed = true
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
	return ObjectInfo{Size: options.Size, ExpiresAt: timeFromUnixNano(expiresAt)}, nil
}

func (s *CoreStore) Get(ctx context.Context, key []byte) (Object, error) {
	if err := s.gate.enter(); err != nil {
		return nil, err
	}
	defer s.gate.leave()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil {
		return nil, err
	}

	s.counters.gets.Add(1)
	shardID := shardIDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	target := &s.shards[shardID]
	now := s.clock.Now().UnixNano()
	target.mu.RLock()
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
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validateKey(key); err != nil {
		return false, err
	}

	shardID := shardIDWithHash(key, s.cfg.ShardCount, s.hash)
	immutableKey := string(key)
	target := &s.shards[shardID]
	target.mu.Lock()
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

func (s *CoreStore) rejectPut(ctx context.Context) {
	if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
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
