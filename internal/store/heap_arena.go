package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// HeapArena stores values as independently allocated immutable chunks.
type HeapArena struct {
	chunkBytes int

	mu     sync.RWMutex
	closed bool
}

type heapValue struct {
	refs   atomic.Int64
	chunks [][]byte
	size   int64
}

// NewHeapArena creates a heap-backed Arena with a fixed maximum chunk size.
func NewHeapArena(chunkBytes int) *HeapArena {
	if chunkBytes <= 0 {
		panic("store: heap arena chunk size must be positive")
	}
	return &HeapArena{chunkBytes: chunkBytes}
}

// Write reads exactly size bytes and publishes one owner reference on success.
func (a *HeapArena) Write(ctx context.Context, src io.Reader, size int64) (ValueRef, error) {
	if size < 0 || src == nil {
		return ValueRef{}, ErrSizeMismatch
	}
	if err := ctx.Err(); err != nil {
		return ValueRef{}, err
	}
	if a.isClosed() {
		return ValueRef{}, ErrClosed
	}

	chunkCount := (size + int64(a.chunkBytes) - 1) / int64(a.chunkBytes)
	chunks := make([][]byte, 0, int(chunkCount))
	remaining := size
	sourceEOF := false
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return ValueRef{}, err
		}

		chunkSize := int64(a.chunkBytes)
		if remaining < chunkSize {
			chunkSize = remaining
		}
		chunk := make([]byte, int(chunkSize))
		var err error
		sourceEOF, err = readArenaChunk(ctx, src, chunk)
		if err != nil {
			return ValueRef{}, err
		}
		chunks = append(chunks, chunk)
		remaining -= chunkSize
		if sourceEOF && remaining != 0 {
			return ValueRef{}, ErrSizeMismatch
		}
	}

	var probe [1]byte
	for !sourceEOF {
		if err := ctx.Err(); err != nil {
			return ValueRef{}, err
		}
		n, err := src.Read(probe[:])
		if n != 0 {
			return ValueRef{}, ErrSizeMismatch
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ValueRef{}, fmt.Errorf("probe value: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return ValueRef{}, err
	}

	value := &heapValue{chunks: chunks, size: size}
	value.refs.Store(1)
	return ValueRef{handle: value}, nil
}

func readArenaChunk(ctx context.Context, src io.Reader, chunk []byte) (bool, error) {
	read := 0
	for read < len(chunk) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n, err := src.Read(chunk[read:])
		if n < 0 || n > len(chunk)-read {
			return false, fmt.Errorf("read value: invalid byte count %d", n)
		}
		read += n
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			if read != len(chunk) {
				return false, ErrSizeMismatch
			}
			return true, nil
		}
		return false, fmt.Errorf("read value: %w", err)
	}
	return false, nil
}

// Open pins a value until the returned reader is closed.
func (a *HeapArena) Open(ref ValueRef) (io.ReadCloser, error) {
	if a.isClosed() {
		return nil, ErrClosed
	}
	value, ok := ref.handle.(*heapValue)
	if !ok || value == nil || !value.retain() {
		return nil, ErrNotFound
	}
	return &heapReader{value: value}, nil
}

// Release drops one owner reference.
func (a *HeapArena) Release(ref ValueRef) {
	value, ok := ref.handle.(*heapValue)
	if !ok || value == nil {
		return
	}
	value.release()
}

// Close prevents new writes and opens without invalidating existing readers.
func (a *HeapArena) Close() error {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	return nil
}

func (a *HeapArena) isClosed() bool {
	a.mu.RLock()
	closed := a.closed
	a.mu.RUnlock()
	return closed
}

func (v *heapValue) retain() bool {
	for {
		refs := v.refs.Load()
		if refs <= 0 {
			return false
		}
		if v.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (v *heapValue) release() {
	refs := v.refs.Add(-1)
	if refs < 0 {
		panic("store: heap value released too many times")
	}
	if refs == 0 {
		v.chunks = nil
		v.size = 0
	}
}

type heapReader struct {
	mu sync.Mutex

	value      *heapValue
	chunkIndex int
	chunkOff   int
	closed     bool
}

func (r *heapReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.chunkIndex >= len(r.value.chunks) {
		return 0, io.EOF
	}

	written := 0
	for len(p) > 0 && r.chunkIndex < len(r.value.chunks) {
		chunk := r.value.chunks[r.chunkIndex]
		n := copy(p, chunk[r.chunkOff:])
		written += n
		p = p[n:]
		r.chunkOff += n
		if r.chunkOff == len(chunk) {
			r.chunkIndex++
			r.chunkOff = 0
		}
	}
	return written, nil
}

func (r *heapReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	value := r.value
	r.value = nil
	r.mu.Unlock()

	value.release()
	return nil
}
