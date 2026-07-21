package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCloseRejectsOperationsAndKeepsOpenReaderValid(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), []byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("get = %v", err)
	}
	if _, err := store.Put(context.Background(), []byte("k"), strings.NewReader("x"), PutOptions{Size: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("put = %v", err)
	}
	if _, err := store.Delete(context.Background(), []byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatalf("delete = %v", err)
	}

	got, err := io.ReadAll(object)
	if err != nil || string(got) != "value" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if err := object.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWakesStagingWaiter(t *testing.T) {
	store := newTestStore(t)
	reserved := store.cfg.MaxStagingBytes
	if err := store.staging.reserve(context.Background(), reserved); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := store.Put(context.Background(), []byte("k"), strings.NewReader("x"), PutOptions{Size: 1})
		result <- err
	}()
	waitForActiveOperations(t, &store.gate, 1)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- store.Close()
	}()

	var putErr error
	select {
	case putErr = <-result:
		store.staging.release(reserved)
	case <-time.After(time.Second):
		t.Error("Close did not wake staging waiter")
		store.staging.release(reserved)
		putErr = <-result
	}
	if !errors.Is(putErr, ErrClosed) {
		t.Errorf("put error = %v", putErr)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestCloseReleasesIndexOwnershipButNotOpenReader(t *testing.T) {
	store := newTestStore(t)
	putString(t, store, "k", "value")
	object, err := store.Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}

	shardID := shardIDWithHash([]byte("k"), store.cfg.ShardCount, store.hash)
	target := &store.shards[shardID]
	target.mu.RLock()
	heapValue := target.entries["k"].value.handle.(*heapValue)
	target.mu.RUnlock()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	stats := store.Stats()
	if stats.Entries != 0 || stats.LiveBytes != 0 || stats.PayloadBytes != 0 {
		t.Fatalf("stats after close = %+v", stats)
	}
	if len(heapValue.chunks) == 0 {
		t.Fatal("open reader lost its payload during Close")
	}
	if err := object.Close(); err != nil {
		t.Fatal(err)
	}
	if len(heapValue.chunks) != 0 {
		t.Fatal("payload retained after final reader close")
	}
}

func TestConcurrentOperationsFinishDuringClose(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			key := []byte(fmt.Sprintf("k-%d", worker))
			for ctx.Err() == nil {
				_, err := store.Put(ctx, key, strings.NewReader("v"), PutOptions{Size: 1})
				if errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
					return
				}
				object, err := store.Get(ctx, key)
				if err == nil {
					object.Close()
				}
				_, _ = store.Delete(ctx, key)
			}
		}(worker)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("operations did not finish after Close")
	}
}
