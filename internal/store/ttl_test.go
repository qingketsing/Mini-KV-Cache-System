package store

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestTTLStartsAtCommitAndLazyGetNeverReturnsExpiredValue(t *testing.T) {
	testClock := newManualClock(testTime)
	store := newTestStoreWithClock(t, testClock, false)
	reader, writer := io.Pipe()
	type putResult struct {
		info ObjectInfo
		err  error
	}
	result := make(chan putResult, 1)
	go func() {
		info, err := store.Put(context.Background(), []byte("k"), reader, PutOptions{Size: 2, TTL: 2 * time.Second})
		result <- putResult{info: info, err: err}
	}()

	if _, err := writer.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	testClock.Advance(10 * time.Second)
	if _, err := writer.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	put := <-result
	if put.err != nil {
		t.Fatal(put.err)
	}
	wantExpiration := testTime.Add(12 * time.Second)
	if !put.info.ExpiresAt.Equal(wantExpiration) {
		t.Fatalf("expires = %s, want %s", put.info.ExpiresAt, wantExpiration)
	}

	testClock.Advance(2 * time.Second)
	if _, err := store.Get(context.Background(), []byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
	stats := store.Stats()
	if stats.Expirations != 1 || stats.Entries != 0 || stats.LiveBytes != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestStaleExpirationCannotDeleteReplacement(t *testing.T) {
	testClock := newManualClock(testTime)
	store := newTestStoreWithClock(t, testClock, false)
	if _, err := store.Put(context.Background(), []byte("k"), strings.NewReader("a"), PutOptions{Size: 1, TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	putString(t, store, "k", "b")

	testClock.Advance(time.Second)
	store.maintenanceStep(testClock.Now())
	assertValue(t, store, "k", "b")
	if got := store.Stats().Expirations; got != 0 {
		t.Fatalf("expirations = %d", got)
	}
}

func TestDeleteExpiredEntryReturnsFalse(t *testing.T) {
	testClock := newManualClock(testTime)
	store := newTestStoreWithClock(t, testClock, false)
	if _, err := store.Put(context.Background(), []byte("k"), strings.NewReader("v"), PutOptions{Size: 1, TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	testClock.Advance(time.Second)

	deleted, err := store.Delete(context.Background(), []byte("k"))
	if err != nil || deleted {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
	stats := store.Stats()
	if stats.Entries != 0 || stats.Expirations != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestExpirationKeepsOpenReaderValid(t *testing.T) {
	testClock := newManualClock(testTime)
	store := newTestStoreWithClock(t, testClock, false)
	if _, err := store.Put(context.Background(), []byte("k"), strings.NewReader("value"), PutOptions{Size: 5, TTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}

	testClock.Advance(time.Second)
	store.maintenanceStep(testClock.Now())
	got, err := io.ReadAll(object)
	if err != nil || string(got) != "value" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	object.Close()
}

func TestMaintenanceAppliesCurrentTouch(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	object.Close()
	store.maintenanceStep(store.clock.Now())

	shardID := shardIDWithHash([]byte("k"), store.cfg.ShardCount, store.hash)
	target := &store.shards[shardID]
	target.mu.RLock()
	element := target.policy.nodes["k"]
	segment := element.Value.(*policyItem).segment
	target.mu.RUnlock()
	if segment != protected {
		t.Fatalf("segment = %d", segment)
	}
}

func TestMaintenanceIgnoresStaleTouch(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "old")
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	object.Close()
	putString(t, store, "k", "new")
	store.maintenanceStep(store.clock.Now())

	shardID := shardIDWithHash([]byte("k"), store.cfg.ShardCount, store.hash)
	target := &store.shards[shardID]
	target.mu.RLock()
	element := target.policy.nodes["k"]
	item := element.Value.(*policyItem)
	segment := item.segment
	generation := item.generation
	currentGeneration := target.entries["k"].generation
	target.mu.RUnlock()
	if segment != probation || generation != currentGeneration {
		t.Fatalf("segment=%d generation=%d current=%d", segment, generation, currentGeneration)
	}
}
