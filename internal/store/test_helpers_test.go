package store

import (
	"context"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var testTime = time.Unix(100, 0)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

type clockFunc func() time.Time

func (f clockFunc) Now() time.Time {
	return f()
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func compactConfig() Config {
	return Config{
		CapacityBytes:   4096,
		MaxObjectBytes:  1024,
		MaxStagingBytes: 2048,
		ChunkBytes:      64,
		ShardCount:      4,
		TTLResolution:   time.Second,
		TouchBuffer:     16,
	}
}

func newTestStore(t testing.TB) *CoreStore {
	t.Helper()
	return newTestStoreWithClock(t, newManualClock(testTime), false)
}

func newTestStoreWithClock(t testing.TB, testClock clock, maintenance bool) *CoreStore {
	t.Helper()
	return newConfiguredTestStore(t, compactConfig(), testClock, maintenance, protocolHash)
}

func newConfiguredTestStore(t testing.TB, cfg Config, testClock clock, maintenance bool, hash hashFunc) *CoreStore {
	t.Helper()
	store, err := newCoreStore(cfg, coreDependencies{
		arena:            NewHeapArena(cfg.ChunkBytes),
		clock:            testClock,
		hash:             hash,
		startMaintenance: maintenance,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return store
}

func putString(t testing.TB, store *CoreStore, key, value string) {
	t.Helper()
	if _, err := store.Put(context.Background(), []byte(key), strings.NewReader(value), PutOptions{Size: int64(len(value))}); err != nil {
		t.Fatal(err)
	}
}

func assertValue(t testing.TB, store *CoreStore, key, want string) {
	t.Helper()
	object, err := store.Get(context.Background(), []byte(key))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(object)
	if closeErr := object.Close(); closeErr != nil {
		t.Errorf("close object: %v", closeErr)
	}
	if err != nil || string(got) != want {
		t.Fatalf("got=%q want=%q err=%v", got, want, err)
	}
}

func costFor(key string, payloadBytes int) int64 {
	return int64(payloadBytes+len(key)) + entryOverheadBytes
}

func newCapacityTestStore(t testing.TB, capacity int64) *CoreStore {
	t.Helper()
	return newCapacityStoreWithClock(t, capacity, newManualClock(testTime))
}

func newCapacityStoreWithClock(t testing.TB, capacity int64, testClock clock) *CoreStore {
	t.Helper()
	cfg := compactConfig()
	cfg.CapacityBytes = capacity
	return newConfiguredTestStore(t, cfg, testClock, false, func([]byte) uint64 { return 0 })
}

func newTestStoreWithTouchBuffer(t testing.TB, size int, maintenance bool) *CoreStore {
	t.Helper()
	cfg := compactConfig()
	cfg.TouchBuffer = size
	return newConfiguredTestStore(t, cfg, newManualClock(testTime), maintenance, func([]byte) uint64 { return 0 })
}

func newFuzzStore(t testing.TB) *CoreStore {
	t.Helper()
	cfg := compactConfig()
	cfg.CapacityBytes = 16 << 20
	cfg.MaxObjectBytes = 4096
	cfg.MaxStagingBytes = 8192
	return newConfiguredTestStore(t, cfg, newManualClock(testTime), false, protocolHash)
}

func newBenchmarkStore(b *testing.B, capacity int64) *CoreStore {
	b.Helper()
	cfg := DefaultConfig()
	cfg.CapacityBytes = capacity
	cfg.MaxObjectBytes = capacity / 2
	cfg.MaxStagingBytes = cfg.MaxObjectBytes
	if int64(cfg.ChunkBytes) > cfg.MaxObjectBytes {
		cfg.ChunkBytes = int(cfg.MaxObjectBytes)
	}
	return newConfiguredTestStore(b, cfg, realClock{}, true, protocolHash)
}

func waitForActiveOperations(t testing.TB, gate *operationGate, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for gate.activeCount.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("active operations = %d, want %d", gate.activeCount.Load(), want)
		}
		runtime.Gosched()
	}
}
