package store

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	clear(buffer)
	return len(buffer), nil
}

func TestProductionMaxObject(t *testing.T) {
	if os.Getenv("MINIKV_LARGE_TEST") != "1" {
		t.Skip("set MINIKV_LARGE_TEST=1")
	}
	cfg := DefaultConfig()
	store, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	source := io.LimitReader(zeroReader{}, cfg.MaxObjectBytes)
	if _, err := store.Put(context.Background(), []byte("max"), source, PutOptions{Size: cfg.MaxObjectBytes}); err != nil {
		t.Fatal(err)
	}
	object, err := store.Get(context.Background(), []byte("max"))
	if err != nil {
		t.Fatal(err)
	}
	bytesRead, readErr := io.Copy(io.Discard, object)
	object.Close()
	if readErr != nil || bytesRead != cfg.MaxObjectBytes {
		t.Fatalf("bytes=%d err=%v", bytesRead, readErr)
	}

	panicReader := readerFunc(func([]byte) (int, error) {
		panic("oversized source was read")
	})
	if _, err := store.Put(context.Background(), []byte("too-large"), panicReader, PutOptions{Size: cfg.MaxObjectBytes + 1}); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatalf("error = %v", err)
	}
}
