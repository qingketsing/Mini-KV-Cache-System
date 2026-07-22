package store

import (
	"context"
	"sync"
)

type byteBudget struct {
	mu sync.Mutex

	limit  int64
	used   int64
	closed bool
	wake   chan struct{}
}

func newByteBudget(limit int64) *byteBudget {
	return &byteBudget{
		limit: limit,
		wake:  make(chan struct{}),
	}
}

func (b *byteBudget) reserve(ctx context.Context, size int64) error {
	if size < 0 {
		return ErrSizeMismatch
	}
	if size > b.limit {
		return ErrNoCapacity
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if b.used <= b.limit && size <= b.limit-b.used {
			b.used += size
			b.mu.Unlock()
			return nil
		}
		wake := b.wake
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

func (b *byteBudget) release(size int64) {
	if size == 0 {
		return
	}

	b.mu.Lock()
	if size < 0 || size > b.used {
		b.mu.Unlock()
		panic("store: invalid byte budget release")
	}
	b.used -= size
	b.signalLocked()
	b.mu.Unlock()
}

func (b *byteBudget) usedBytes() int64 {
	b.mu.Lock()
	used := b.used
	b.mu.Unlock()
	return used
}

func (b *byteBudget) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.signalLocked()
	}
	b.mu.Unlock()
}

func (b *byteBudget) signalLocked() {
	wake := b.wake
	b.wake = make(chan struct{})
	close(wake)
}
