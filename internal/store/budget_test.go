package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestByteBudgetBlocksUntilRelease(t *testing.T) {
	budget := newByteBudget(10)
	if err := budget.reserve(context.Background(), 8); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	acquired := make(chan error, 1)
	go func() {
		close(started)
		acquired <- budget.reserve(context.Background(), 4)
	}()
	<-started
	select {
	case err := <-acquired:
		t.Fatalf("reservation returned before release: %v", err)
	default:
	}

	budget.release(8)
	if err := <-acquired; err != nil {
		t.Fatal(err)
	}
	budget.release(4)
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("used = %d", got)
	}
}

func TestByteBudgetCancellationAndClose(t *testing.T) {
	budget := newByteBudget(4)
	if err := budget.reserve(context.Background(), 4); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := budget.reserve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reservation error = %v", err)
	}

	budget.close()
	if err := budget.reserve(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed reservation error = %v", err)
	}
	budget.release(4)
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("used after close = %d", got)
	}
}

func TestByteBudgetCloseWakesWaiter(t *testing.T) {
	budget := newByteBudget(1)
	if err := budget.reserve(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		result <- budget.reserve(context.Background(), 1)
	}()
	budget.close()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("error = %v", err)
	}
	budget.release(1)
}

func TestByteBudgetRejectsImpossibleReservation(t *testing.T) {
	budget := newByteBudget(4)
	if err := budget.reserve(context.Background(), 5); !errors.Is(err, ErrNoCapacity) {
		t.Fatalf("error = %v", err)
	}
	if err := budget.reserve(context.Background(), -1); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("negative reservation error = %v", err)
	}
}

func TestByteBudgetConcurrentReservationsStayWithinLimit(t *testing.T) {
	const limit = int64(16)
	budget := newByteBudget(limit)
	var maximum atomic.Int64
	var wg sync.WaitGroup

	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(size int64) {
			defer wg.Done()
			for iteration := 0; iteration < 100; iteration++ {
				if err := budget.reserve(context.Background(), size); err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				used := budget.usedBytes()
				for current := maximum.Load(); used > current && !maximum.CompareAndSwap(current, used); current = maximum.Load() {
				}
				budget.release(size)
			}
		}(int64(worker%4 + 1))
	}
	wg.Wait()

	if got := maximum.Load(); got > limit {
		t.Fatalf("maximum used = %d", got)
	}
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("final used = %d", got)
	}
}
