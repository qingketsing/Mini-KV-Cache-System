package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func FuzzStoreRoundTrip(f *testing.F) {
	f.Add([]byte("key"), []byte("value"))
	f.Fuzz(func(t *testing.T, key, value []byte) {
		if len(key) == 0 || len(key) > maxKeyBytes || len(value) > 4096 {
			return
		}
		store := newFuzzStore(t)
		if _, err := store.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: int64(len(value))}); err != nil {
			t.Fatal(err)
		}
		object, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(object)
		object.Close()
		if readErr != nil || !bytes.Equal(got, value) {
			t.Fatalf("round trip mismatch: got=%x want=%x err=%v", got, value, readErr)
		}
	})
}

func FuzzHeapArenaDeclaredSize(f *testing.F) {
	f.Add([]byte("value"), int8(0))
	f.Fuzz(func(t *testing.T, value []byte, offset int8) {
		if len(value) > 4096 {
			return
		}
		normalized := int64(0)
		switch int(offset) % 3 {
		case 1, 2:
			normalized = 1
		case -1, -2:
			normalized = -1
		}
		declared := int64(len(value)) + normalized
		arena := NewHeapArena(64)
		ref, err := arena.Write(context.Background(), bytes.NewReader(value), declared)
		if normalized != 0 {
			if !errors.Is(err, ErrSizeMismatch) {
				t.Fatalf("error=%v declared=%d actual=%d", err, declared, len(value))
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		reader, err := arena.Open(ref)
		if err != nil {
			t.Fatal(err)
		}
		got, readErr := io.ReadAll(reader)
		reader.Close()
		arena.Release(ref)
		if readErr != nil || !bytes.Equal(got, value) {
			t.Fatalf("round trip mismatch")
		}
	})
}
