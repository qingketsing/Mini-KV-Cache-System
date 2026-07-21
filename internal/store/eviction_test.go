package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPutEvictsProbationLRUToFit(t *testing.T) {
	const valueBytes = 960
	store := newCapacityTestStore(t, costFor("a", valueBytes)+costFor("b", valueBytes))
	a := strings.Repeat("a", valueBytes)
	b := strings.Repeat("b", valueBytes)
	c := strings.Repeat("c", valueBytes)
	putString(t, store, "a", a)
	putString(t, store, "b", b)
	putString(t, store, "c", c)

	if _, err := store.Get(context.Background(), []byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a error = %v", err)
	}
	assertValue(t, store, "b", b)
	assertValue(t, store, "c", c)
	stats := store.Stats()
	if stats.LiveBytes > stats.CapacityBytes || stats.Evictions != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestFailedReplacementPreservesOldValue(t *testing.T) {
	store := newCapacityTestStore(t, 4096)
	old := strings.Repeat("o", 960)
	putString(t, store, "k", old)

	// Force the otherwise valid replacement through the no-victim path.
	store.cfg.CapacityBytes = store.liveBytes.Load()
	larger := strings.Repeat("n", 1024)
	_, err := store.Put(context.Background(), []byte("k"), strings.NewReader(larger), PutOptions{Size: int64(len(larger))})
	if !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("error = %v", err)
	}
	assertValue(t, store, "k", old)
}

func TestFailedStageDoesNotEvictLiveData(t *testing.T) {
	const valueBytes = 960
	store := newCapacityTestStore(t, 2*costFor("a", valueBytes))
	value := strings.Repeat("a", valueBytes)
	putString(t, store, "a", value)

	_, err := store.Put(context.Background(), []byte("b"), strings.NewReader("x"), PutOptions{Size: valueBytes})
	if !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("error = %v", err)
	}
	assertValue(t, store, "a", value)
	if got := store.Stats().Evictions; got != 0 {
		t.Fatalf("evictions = %d", got)
	}
}

func TestConcurrentCapacityNeverOverAdmits(t *testing.T) {
	const valueBytes = 256
	store := newCapacityTestStore(t, 8*costFor("k00", valueBytes))
	var wait sync.WaitGroup
	errorsFound := make(chan error, 32)

	for worker := 0; worker < 32; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			key := fmt.Sprintf("k%02d", worker)
			_, err := store.Put(context.Background(), []byte(key), bytes.NewReader(make([]byte, valueBytes)), PutOptions{Size: valueBytes})
			if err != nil && !errors.Is(err, ErrNoCapacity) {
				errorsFound <- err
				return
			}
			stats := store.Stats()
			if stats.LiveBytes > stats.CapacityBytes {
				errorsFound <- fmt.Errorf("live=%d capacity=%d", stats.LiveBytes, stats.CapacityBytes)
			}
		}(worker)
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func TestPressureRemovesExpiredBeforeLiveVictim(t *testing.T) {
	const valueBytes = 960
	testClock := newManualClock(testTime)
	store := newCapacityStoreWithClock(t, 2*costFor("a", valueBytes), testClock)
	a := strings.Repeat("a", valueBytes)
	b := strings.Repeat("b", valueBytes)
	c := strings.Repeat("c", valueBytes)
	if _, err := store.Put(context.Background(), []byte("a"), strings.NewReader(a), PutOptions{Size: valueBytes, TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	putString(t, store, "b", b)
	testClock.Advance(time.Second)
	putString(t, store, "c", c)

	assertValue(t, store, "b", b)
	assertValue(t, store, "c", c)
	stats := store.Stats()
	if stats.Expirations != 1 || stats.Evictions != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestReaderSurvivesEviction(t *testing.T) {
	const valueBytes = 960
	store := newCapacityTestStore(t, 2*costFor("a", valueBytes))
	a := strings.Repeat("a", valueBytes)
	putString(t, store, "a", a)
	putString(t, store, "b", strings.Repeat("b", valueBytes))
	object, err := store.Get(context.Background(), []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	putString(t, store, "c", strings.Repeat("c", valueBytes))

	got, err := io.ReadAll(object)
	if err != nil || string(got) != a {
		t.Fatalf("got bytes=%d err=%v", len(got), err)
	}
	object.Close()
}

func TestHitQueueDropDoesNotFailGet(t *testing.T) {
	store := newTestStoreWithTouchBuffer(t, 1, false)
	putString(t, store, "k", "value")
	first, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	second, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
	if got := store.Stats().TouchDrops; got != 1 {
		t.Fatalf("touch drops = %d", got)
	}
}

func TestBackgroundPressureEvictsToLowWatermark(t *testing.T) {
	const valueBytes = 850
	store := newCapacityTestStore(t, 4096)
	for _, key := range []string{"a", "b", "c", "d"} {
		putString(t, store, key, strings.Repeat(key, valueBytes))
	}
	before := store.Stats().LiveBytes
	if before*watermarkDenominator < store.cfg.CapacityBytes*highWatermarkNumerator {
		t.Fatalf("test did not reach high watermark: %d", before)
	}

	store.maintenanceStep(store.clock.Now())
	after := store.Stats().LiveBytes
	low := store.cfg.CapacityBytes * lowWatermarkNumerator / watermarkDenominator
	if after > low {
		t.Fatalf("live after maintenance = %d, low watermark = %d", after, low)
	}
}
