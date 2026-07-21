package store

import (
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
