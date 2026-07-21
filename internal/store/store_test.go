package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestNewReturnsConfigErrorWithoutPanicking(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected config error")
	}
}

func TestCoreStorePutGetDelete(t *testing.T) {
	store := newTestStore(t)
	info, err := store.Put(context.Background(), []byte("alpha"), strings.NewReader("value"), PutOptions{Size: 5})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 5 || !info.ExpiresAt.IsZero() {
		t.Fatalf("info = %+v", info)
	}

	object, err := store.Get(context.Background(), []byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(object)
	if err != nil {
		t.Fatal(err)
	}
	if err := object.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != "value" {
		t.Fatalf("value = %q", got)
	}

	deleted, err := store.Delete(context.Background(), []byte("alpha"))
	if err != nil || !deleted {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
	if _, err := store.Get(context.Background(), []byte("alpha")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestCoreStorePutIsInvisibleUntilCommit(t *testing.T) {
	store := newTestStore(t)
	reader, writer := io.Pipe()
	result := make(chan error, 1)
	go func() {
		_, err := store.Put(context.Background(), []byte("key"), reader, PutOptions{Size: 4})
		result <- err
	}()

	if _, err := writer.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), []byte("key")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partially staged value error = %v", err)
	}
	if _, err := writer.Write([]byte("cd")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	assertValue(t, store, "key", "abcd")
}

func TestConcurrentSameKeyPutsUseLastCommitWins(t *testing.T) {
	store := newTestStore(t)
	reader, writer := io.Pipe()
	first := make(chan error, 1)
	go func() {
		_, err := store.Put(context.Background(), []byte("k"), reader, PutOptions{Size: 3})
		first <- err
	}()

	if _, err := writer.Write([]byte("o")); err != nil {
		t.Fatal(err)
	}
	putString(t, store, "k", "new")
	if _, err := writer.Write([]byte("ld")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	assertValue(t, store, "k", "old")
}

func TestCoreStoreReaderSurvivesOverwriteAndDelete(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "old")
	old, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}

	putString(t, store, "k", "new")
	if _, err := store.Delete(context.Background(), []byte("k")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(old)
	if err != nil || string(got) != "old" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoreStoreValidatesBeforeReading(t *testing.T) {
	store := newTestStore(t)
	panicReader := readerFunc(func([]byte) (int, error) {
		panic("reader must not be called")
	})
	cases := []struct {
		key  []byte
		opts PutOptions
		want error
	}{
		{key: nil, opts: PutOptions{Size: 1}, want: ErrInvalidKey},
		{key: bytes.Repeat([]byte("k"), 1025), opts: PutOptions{Size: 1}, want: ErrInvalidKey},
		{key: []byte("k"), opts: PutOptions{Size: -1}, want: ErrSizeMismatch},
		{key: []byte("k"), opts: PutOptions{Size: 1025}, want: ErrObjectTooLarge},
		{key: []byte("k"), opts: PutOptions{Size: 1, TTL: -1}, want: ErrInvalidTTL},
	}

	for _, testCase := range cases {
		if _, err := store.Put(context.Background(), testCase.key, panicReader, testCase.opts); !errors.Is(err, testCase.want) {
			t.Fatalf("error=%v want=%v", err, testCase.want)
		}
	}
}

func TestCoreStoreBinaryKeyEmptyValueAndKeyCopy(t *testing.T) {
	store := newTestStore(t)
	key := []byte{'a', 0, 'b'}
	if _, err := store.Put(context.Background(), key, bytes.NewReader(nil), PutOptions{Size: 0}); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'

	object, err := store.Get(context.Background(), []byte{'a', 0, 'b'})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(object)
	object.Close()
	if err != nil || len(got) != 0 {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestCoreStoreFailedPutPreservesOldValue(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "old")
	for _, source := range []io.Reader{strings.NewReader("x"), strings.NewReader("toolong")} {
		if _, err := store.Put(context.Background(), []byte("k"), source, PutOptions{Size: 3}); !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("error = %v", err)
		}
		assertValue(t, store, "k", "old")
	}
}

func TestCoreStoreKeepsCollidingKeysDistinct(t *testing.T) {
	cfg := compactConfig()
	store := newConfiguredTestStore(t, cfg, newManualClock(testTime), false, func([]byte) uint64 { return 0 })
	putString(t, store, "alpha", "one")
	putString(t, store, "beta", "two")
	assertValue(t, store, "alpha", "one")
	assertValue(t, store, "beta", "two")
}

func TestCoreStoreStatistics(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	object.Close()
	if _, err := store.Get(context.Background(), []byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := store.Delete(context.Background(), []byte("k")); err != nil {
		t.Fatal(err)
	}

	stats := store.Stats()
	if stats.Puts != 1 || stats.Gets != 2 || stats.Hits != 1 || stats.Misses != 1 || stats.Deletes != 1 {
		t.Fatalf("counters = %+v", stats)
	}
	if stats.Entries != 0 || stats.LiveBytes != 0 || stats.PayloadBytes != 0 || stats.StagingBytes != 0 {
		t.Fatalf("gauges = %+v", stats)
	}
}

func TestCoreStoreCanceledPutDoesNotRead(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	panicReader := readerFunc(func([]byte) (int, error) {
		panic("reader must not be called")
	})

	if _, err := store.Put(ctx, []byte("k"), panicReader, PutOptions{Size: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if got := store.Stats().RejectedPuts; got != 0 {
		t.Fatalf("rejected puts = %d", got)
	}
}

func TestGetObservesCancellationWhileWaitingForShardLock(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	shardID := shardIDWithHash([]byte("k"), store.cfg.ShardCount, store.hash)
	target := &store.shards[shardID]
	target.mu.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	type getResult struct {
		object Object
		err    error
	}
	result := make(chan getResult, 1)
	go func() {
		object, err := store.Get(ctx, []byte("k"))
		result <- getResult{object: object, err: err}
	}()
	waitForActiveOperations(t, &store.gate, 1)
	cancel()
	target.mu.Unlock()

	got := <-result
	if got.object != nil {
		got.object.Close()
	}
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("error = %v", got.err)
	}
}

func TestDeleteObservesCancellationWhileWaitingForShardLock(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	shardID := shardIDWithHash([]byte("k"), store.cfg.ShardCount, store.hash)
	target := &store.shards[shardID]
	target.mu.Lock()

	ctx, cancel := context.WithCancel(context.Background())
	type deleteResult struct {
		deleted bool
		err     error
	}
	result := make(chan deleteResult, 1)
	go func() {
		deleted, err := store.Delete(ctx, []byte("k"))
		result <- deleteResult{deleted: deleted, err: err}
	}()
	waitForActiveOperations(t, &store.gate, 1)
	cancel()
	target.mu.Unlock()

	got := <-result
	if got.deleted || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("deleted=%v error=%v", got.deleted, got.err)
	}
	assertValue(t, store, "k", "value")
}
