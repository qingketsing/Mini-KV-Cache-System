package grpccommon

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
)

func TestDefaultLimits(t *testing.T) {
	t.Parallel()

	want := Limits{
		ChunkBytes:      256 << 10,
		MaxMessageBytes: 1 << 20,
		MaxObjectBytes:  128 << 20,
	}
	if got := DefaultLimits(); got != want {
		t.Fatalf("DefaultLimits() = %+v, want %+v", got, want)
	}
}

func TestLimitsValidate(t *testing.T) {
	t.Parallel()

	valid := DefaultLimits()
	tests := []struct {
		name   string
		limits Limits
		wantOK bool
	}{
		{name: "valid", limits: valid, wantOK: true},
		{name: "zero chunk", limits: Limits{ChunkBytes: 0, MaxMessageBytes: 1 << 20, MaxObjectBytes: 1 << 20}},
		{name: "negative chunk", limits: Limits{ChunkBytes: -1, MaxMessageBytes: 1 << 20, MaxObjectBytes: 1 << 20}},
		{name: "zero message", limits: Limits{ChunkBytes: 1, MaxMessageBytes: 0, MaxObjectBytes: 1}},
		{name: "negative message", limits: Limits{ChunkBytes: 1, MaxMessageBytes: -1, MaxObjectBytes: 1}},
		{name: "zero object", limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: 0}},
		{name: "negative object", limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: -1}},
		{name: "message equals reserve", limits: Limits{ChunkBytes: 1, MaxMessageBytes: 64 << 10, MaxObjectBytes: 1}},
		{name: "message below reserve", limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) - 1, MaxObjectBytes: 1}},
		{name: "chunk exceeds framed message", limits: Limits{ChunkBytes: 2, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: 2}},
		{name: "chunk exceeds object", limits: Limits{ChunkBytes: 2, MaxMessageBytes: (64 << 10) + 2, MaxObjectBytes: 1}},
		{name: "boundary", limits: Limits{ChunkBytes: 7, MaxMessageBytes: (64 << 10) + 7, MaxObjectBytes: 7}, wantOK: true},
		{name: "maximum sequence space", limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: int64(math.MaxUint32) + 1}, wantOK: true},
		{name: "object exceeds sequence space", limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: int64(math.MaxUint32) + 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.limits.Validate()
			if test.wantOK && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
		})
	}
}

func TestParsePutHeader(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	valid := &minikvv1.PutHeader{
		Key:             []byte{0x00, 0xff, 'k'},
		ValueSize:       17,
		TtlMilliseconds: 2500,
		RequestId:       bytes.Repeat([]byte{1}, 16),
	}

	t.Run("valid", func(t *testing.T) {
		key, size, ttl, err := ParsePutHeader(valid, limits)
		if err != nil {
			t.Fatalf("ParsePutHeader() error = %v", err)
		}
		if !bytes.Equal(key, valid.Key) || size != 17 || ttl != 2500*time.Millisecond {
			t.Fatalf("ParsePutHeader() = (%v, %d, %v), want (%v, 17, 2.5s)", key, size, ttl, valid.Key)
		}
	})

	tests := []struct {
		name   string
		header *minikvv1.PutHeader
		limits Limits
		want   error
	}{
		{name: "nil header", header: nil, limits: limits, want: errMalformedHeader},
		{name: "zero value", header: &minikvv1.PutHeader{Key: []byte("k")}, limits: limits},
		{name: "size over object max", header: &minikvv1.PutHeader{Key: []byte("k"), ValueSize: uint64(limits.MaxObjectBytes) + 1}, limits: limits, want: store.ErrObjectTooLarge},
		{name: "size above max int64", header: &minikvv1.PutHeader{Key: []byte("k"), ValueSize: uint64(math.MaxInt64) + 1}, limits: Limits{ChunkBytes: 1, MaxMessageBytes: (64 << 10) + 1, MaxObjectBytes: int64(math.MaxUint32) + 1}, want: store.ErrObjectTooLarge},
		{name: "ttl at boundary", header: &minikvv1.PutHeader{Key: []byte("k"), TtlMilliseconds: uint64(math.MaxInt64 / int64(time.Millisecond))}, limits: limits},
		{name: "ttl overflow", header: &minikvv1.PutHeader{Key: []byte("k"), TtlMilliseconds: uint64(math.MaxInt64/int64(time.Millisecond)) + 1}, limits: limits, want: store.ErrInvalidTTL},
		{name: "request id omitted", header: &minikvv1.PutHeader{Key: []byte("k")}, limits: limits},
		{name: "request id 15", header: &minikvv1.PutHeader{Key: []byte("k"), RequestId: make([]byte, 15)}, limits: limits, want: errInvalidRequestID},
		{name: "request id 16", header: &minikvv1.PutHeader{Key: []byte("k"), RequestId: make([]byte, 16)}, limits: limits},
		{name: "request id 17", header: &minikvv1.PutHeader{Key: []byte("k"), RequestId: make([]byte, 17)}, limits: limits, want: errInvalidRequestID},
		{name: "empty key", header: &minikvv1.PutHeader{}, limits: limits, want: store.ErrInvalidKey},
		{name: "key too long", header: &minikvv1.PutHeader{Key: make([]byte, 1025)}, limits: limits, want: store.ErrInvalidKey},
		{name: "binary key", header: &minikvv1.PutHeader{Key: []byte{0x00, 0xff}}, limits: limits},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := ParsePutHeader(test.header, test.limits)
			if test.want == nil && err != nil {
				t.Fatalf("ParsePutHeader() error = %v", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("ParsePutHeader() error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}

func TestValidateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  []byte
		want error
	}{
		{name: "empty", key: nil, want: store.ErrInvalidKey},
		{name: "one byte", key: []byte{0x00}},
		{name: "binary", key: []byte{0x00, 0xff, 0x80}},
		{name: "maximum", key: make([]byte, 1024)},
		{name: "too long", key: make([]byte, 1025), want: store.ErrInvalidKey},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateKey(test.key)
			if !errors.Is(err, test.want) {
				t.Fatalf("ValidateKey() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidateChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		chunk    *minikvv1.DataChunk
		expected uint32
		max      int
		want     error
	}{
		{name: "valid", chunk: &minikvv1.DataChunk{Sequence: 3, Data: []byte("abc")}, expected: 3, max: 3},
		{name: "nil", chunk: nil, max: 3, want: errMalformedFrame},
		{name: "empty", chunk: &minikvv1.DataChunk{}, max: 3, want: errEmptyChunk},
		{name: "oversized", chunk: &minikvv1.DataChunk{Data: []byte("abcd")}, max: 3, want: errChunkTooLarge},
		{name: "wrong sequence", chunk: &minikvv1.DataChunk{Sequence: 2, Data: []byte("a")}, expected: 1, max: 3, want: errInvalidSequence},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateChunk(test.chunk, test.expected, test.max)
			if !errors.Is(err, test.want) {
				t.Fatalf("ValidateChunk() error = %v, want errors.Is(_, %v)", err, test.want)
			}
		})
	}
}
