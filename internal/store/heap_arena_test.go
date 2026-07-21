package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestHeapArenaChunkingAndReadback(t *testing.T) {
	arena := NewHeapArena(4)
	values := [][]byte{nil, []byte("a"), []byte("abcd"), []byte("abcde")}

	for _, value := range values {
		ref, err := arena.Write(context.Background(), bytes.NewReader(value), int64(len(value)))
		if err != nil {
			t.Fatalf("write %q: %v", value, err)
		}
		heapValue := ref.handle.(*heapValue)
		for _, chunk := range heapValue.chunks {
			if len(chunk) > 4 {
				t.Fatalf("chunk length = %d", len(chunk))
			}
		}

		reader, err := arena.Open(ref)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("got %q, want %q", got, value)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		arena.Release(ref)
		if len(heapValue.chunks) != 0 {
			t.Fatal("chunks retained after final release")
		}
	}
}

func TestHeapArenaRejectsShortAndLongSources(t *testing.T) {
	arena := NewHeapArena(4)
	cases := []struct {
		size  int64
		value string
	}{
		{size: 4, value: "abc"},
		{size: 3, value: "abcd"},
	}

	for _, testCase := range cases {
		if _, err := arena.Write(context.Background(), strings.NewReader(testCase.value), testCase.size); !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("size=%d value=%q error=%v", testCase.size, testCase.value, err)
		}
	}
}

func TestHeapArenaReaderSurvivesOwnerReleaseAndArenaClose(t *testing.T) {
	arena := NewHeapArena(4)
	ref, err := arena.Write(context.Background(), strings.NewReader("payload"), 7)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := arena.Open(ref)
	if err != nil {
		t.Fatal(err)
	}

	arena.Release(ref)
	if err := arena.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != "payload" {
		t.Fatalf("got %q, err %v", got, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestHeapArenaCancellation(t *testing.T) {
	arena := NewHeapArena(4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := arena.Write(ctx, strings.NewReader("data"), 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestHeapArenaPreservesSourceError(t *testing.T) {
	arena := NewHeapArena(4)
	sourceErr := errors.New("source failed")
	src := readerFunc(func(p []byte) (int, error) {
		copy(p, "ab")
		return 2, sourceErr
	})

	if _, err := arena.Write(context.Background(), src, 4); !errors.Is(err, sourceErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestHeapArenaOpenAfterCloseFails(t *testing.T) {
	arena := NewHeapArena(4)
	ref, err := arena.Write(context.Background(), strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := arena.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := arena.Open(ref); !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v", err)
	}
	arena.Release(ref)
}
