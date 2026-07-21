package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type coreDependencies struct {
	arena            Arena
	clock            clock
	hash             hashFunc
	startMaintenance bool
}

// CoreStore is the concurrent, in-process MiniKV storage kernel.
type CoreStore struct {
	cfg Config

	arena Arena
	clock clock
	hash  hashFunc

	shards   []shard
	staging  *byteBudget
	wheel    *timingWheel
	touches  chan touchEvent
	pressure chan struct{}

	generation   atomic.Uint64
	liveBytes    atomic.Int64
	payloadBytes atomic.Int64
	entryCount   atomic.Int64
	counters     storeCounters

	gate            operationGate
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	stop            chan struct{}
	maintenanceDone chan struct{}
	closeOnce       sync.Once
	closeDone       chan struct{}
	closeErr        error

	evictionMu     sync.Mutex
	evictionCursor uint32
}

// New creates an in-memory Store from a validated Config.
func New(cfg Config) (Store, error) {
	return newCoreStore(cfg, coreDependencies{
		clock:            realClock{},
		hash:             protocolHash,
		startMaintenance: true,
	})
}

func newCoreStore(cfg Config, dependencies coreDependencies) (*CoreStore, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("store config: %w", err)
	}
	if dependencies.arena == nil {
		dependencies.arena = NewHeapArena(cfg.ChunkBytes)
	}
	if dependencies.clock == nil {
		dependencies.clock = realClock{}
	}
	if dependencies.hash == nil {
		dependencies.hash = protocolHash
	}

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	store := &CoreStore{
		cfg:             cfg,
		arena:           dependencies.arena,
		clock:           dependencies.clock,
		hash:            dependencies.hash,
		shards:          make([]shard, cfg.ShardCount),
		staging:         newByteBudget(cfg.MaxStagingBytes),
		touches:         make(chan touchEvent, cfg.TouchBuffer),
		pressure:        make(chan struct{}, 1),
		stop:            make(chan struct{}),
		maintenanceDone: make(chan struct{}),
		closeDone:       make(chan struct{}),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
	store.wheel = newTimingWheel(cfg.TTLResolution, store.clock.Now(), defaultTimingWheelSlots)
	for index := range store.shards {
		store.shards[index].entries = make(map[string]*entry)
		store.shards[index].policy = newSLRU(store.protectedLimit())
	}

	if dependencies.startMaintenance {
		go store.maintenanceLoop()
	} else {
		close(store.maintenanceDone)
	}
	return store, nil
}

func (s *CoreStore) protectedLimit() int64 {
	nominal := s.cfg.CapacityBytes / int64(s.cfg.ShardCount)
	limit := percentageFloor(nominal, 80, 100)
	if limit < 1 {
		return 1
	}
	return limit
}

func (s *CoreStore) Close() error {
	s.closeOnce.Do(func() {
		s.gate.closeAdmission()
		s.lifecycleCancel()
		s.staging.close()
		close(s.stop)
		s.gate.wait()
		<-s.maintenanceDone

		refs := make([]ValueRef, 0, int(s.entryCount.Load()))
		for index := range s.shards {
			target := &s.shards[index]
			target.mu.Lock()
			for _, current := range target.entries {
				refs = append(refs, current.value)
			}
			target.entries = make(map[string]*entry)
			target.policy = newSLRU(s.protectedLimit())
			target.bytes = 0
			target.mu.Unlock()
		}
		s.liveBytes.Store(0)
		s.payloadBytes.Store(0)
		s.entryCount.Store(0)
		for _, ref := range refs {
			s.arena.Release(ref)
		}
		s.closeErr = s.arena.Close()
		close(s.closeDone)
	})
	<-s.closeDone
	return s.closeErr
}
