package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestNewStaticRouterRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		nodeID     string
		address    string
		shardCount uint32
	}{
		{name: "empty node ID", address: "node:9091", shardCount: 1024},
		{name: "empty address", nodeID: "node-1", shardCount: 1024},
		{name: "zero shards", nodeID: "node-1", address: "node:9091"},
		{name: "non-power-of-two shards", nodeID: "node-1", address: "node:9091", shardCount: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewStaticRouter(test.nodeID, test.address, test.shardCount); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("NewStaticRouter() error = %v, want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestStaticRouterResolve(t *testing.T) {
	router, err := NewStaticRouter("node-1", "127.0.0.1:9091", 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })

	for i := 0; i < 3; i++ {
		route, err := router.Resolve(context.Background(), []byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
		if route.NodeID != "node-1" {
			t.Fatalf("Resolve() NodeID = %q, want %q", route.NodeID, "node-1")
		}
		if route.Address != "127.0.0.1:9091" {
			t.Fatalf("Resolve() Address = %q, want %q", route.Address, "127.0.0.1:9091")
		}
		if route.ShardID != 253 {
			t.Fatalf("Resolve() ShardID = %d, want 253", route.ShardID)
		}
		if route.Epoch != 0 {
			t.Fatalf("Resolve() Epoch = %d, want 0", route.Epoch)
		}
	}
}

func TestStaticRouterResolveAcceptsBinaryAndEmptyKeys(t *testing.T) {
	router, err := NewStaticRouter("node-1", "node:9091", 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = router.Close() })

	keys := [][]byte{
		{0x00, 0xff, 0x00, 0x80},
		{},
	}
	for _, key := range keys {
		if _, err := router.Resolve(context.Background(), key); err != nil {
			t.Fatalf("Resolve(%x) failed: %v", key, err)
		}
	}
}

func TestStaticRouterResolveChecksContextFirst(t *testing.T) {
	router, err := NewStaticRouter("node-1", "node:9091", 1024)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := router.Resolve(ctx, []byte("key")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() error = %v, want context.Canceled", err)
	}

	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Resolve(ctx, []byte("key")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() after Close with canceled context error = %v, want context.Canceled", err)
	}
}

func TestStaticRouterCloseIsIdempotent(t *testing.T) {
	router, err := NewStaticRouter("node-1", "node:9091", 1024)
	if err != nil {
		t.Fatal(err)
	}

	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	if err := router.Close(); err != nil {
		t.Fatalf("second Close() failed: %v", err)
	}
	if _, err := router.Resolve(context.Background(), []byte("key")); !errors.Is(err, ErrRouterClosed) {
		t.Fatalf("Resolve() error = %v, want ErrRouterClosed", err)
	}
}

func TestStaticRouterConcurrentResolveAndClose(t *testing.T) {
	router, err := NewStaticRouter("node-1", "node:9091", 1024)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wait.Done()
			<-start
			for j := 0; j < 100; j++ {
				_, err := router.Resolve(context.Background(), []byte("key"))
				if err != nil && !errors.Is(err, ErrRouterClosed) {
					errs <- err
					return
				}
			}
		}()
	}

	close(start)
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Resolve() returned unexpected error: %v", err)
	}
}
