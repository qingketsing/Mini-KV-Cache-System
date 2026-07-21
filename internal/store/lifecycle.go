package store

import (
	"context"
	"sync"
	"sync/atomic"
)

type operationGate struct {
	mu sync.Mutex

	closed      bool
	active      sync.WaitGroup
	activeCount atomic.Int64
}

func (g *operationGate) enter() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrClosed
	}
	g.active.Add(1)
	g.activeCount.Add(1)
	return nil
}

func (g *operationGate) leave() {
	g.activeCount.Add(-1)
	g.active.Done()
}

func (g *operationGate) closeAdmission() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (g *operationGate) wait() {
	g.active.Wait()
}

func (s *CoreStore) operationContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stopLifecycleLink := context.AfterFunc(s.lifecycleCtx, cancel)
	return ctx, func() {
		stopLifecycleLink()
		cancel()
	}
}

func (s *CoreStore) cancellationError(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if s.lifecycleCtx.Err() != nil {
		return ErrClosed
	}
	return nil
}

func (s *CoreStore) normalizeOperationError(parent context.Context, err error) error {
	if err == nil {
		return nil
	}
	if cancellationErr := s.cancellationError(parent); cancellationErr != nil {
		return cancellationErr
	}
	return err
}
