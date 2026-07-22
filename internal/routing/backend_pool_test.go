package routing

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testTargetA = "passthrough:///backend-a"
	testTargetB = "passthrough:///backend-b"
)

type clientResult struct {
	client minikvv1.NodeServiceClient
	err    error
}

type observedContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneObserved) })
	return c.Context.Done()
}

func newLazyConn(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
	options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return grpc.NewClient(target, options...)
}

func TestBackendPoolReusesOneConnectionPerAddress(t *testing.T) {
	var dialCount atomic.Int32
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	connections := make(chan *grpc.ClientConn, 2)
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		if dialCount.Add(1) == 1 {
			close(dialStarted)
			<-releaseDial
		}
		conn, err := newLazyConn(target, options...)
		if err == nil {
			connections <- conn
		}
		return conn, err
	}
	pool := NewBackendPool(dial)
	t.Cleanup(func() { _ = pool.Close() })

	const callers = 32
	start := make(chan struct{})
	results := make(chan clientResult, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			ready.Done()
			<-start
			client, err := pool.Client(context.Background(), testTargetA)
			results <- clientResult{client: client, err: err}
		}()
	}
	ready.Wait()
	close(start)
	<-dialStarted
	close(releaseDial)

	clients := make([]minikvv1.NodeServiceClient, 0, callers)
	for i := 0; i < callers; i++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		clients = append(clients, result.client)
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("DialFunc calls for one address = %d, want 1", got)
	}
	for i, client := range clients[1:] {
		if client != clients[0] {
			t.Fatalf("client %d was not the cached NodeService client", i+1)
		}
	}
	firstConn := <-connections

	secondClient, err := pool.Client(context.Background(), testTargetB)
	if err != nil {
		t.Fatal(err)
	}
	if secondClient == clients[0] {
		t.Fatal("different addresses returned the same NodeService client")
	}
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("DialFunc calls for two addresses = %d, want 2", got)
	}
	secondConn := <-connections
	if firstConn == secondConn {
		t.Fatal("different addresses reused the same connection")
	}
}

func TestBackendPoolRejectsEmptyAddress(t *testing.T) {
	var dialCount atomic.Int32
	pool := NewBackendPool(func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		dialCount.Add(1)
		return newLazyConn(target, options...)
	})
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := pool.Client(context.Background(), ""); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Client() error = %v, want ErrInvalidConfiguration", err)
	}
	if got := dialCount.Load(); got != 0 {
		t.Fatalf("DialFunc calls = %d, want 0", got)
	}
}

func TestBackendPoolPreCanceledCallerDoesNotDial(t *testing.T) {
	var dialCount atomic.Int32
	pool := NewBackendPool(func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		dialCount.Add(1)
		return newLazyConn(target, options...)
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.Client(ctx, testTargetA); !errors.Is(err, context.Canceled) {
		t.Fatalf("Client() error = %v, want context.Canceled", err)
	}
	if got := dialCount.Load(); got != 0 {
		t.Fatalf("DialFunc calls = %d, want 0", got)
	}
}

func TestBackendPoolWaiterCancellationDoesNotCorruptDial(t *testing.T) {
	var dialCount atomic.Int32
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		dialCount.Add(1)
		close(dialStarted)
		<-releaseDial
		return newLazyConn(target, options...)
	}
	pool := NewBackendPool(dial)
	t.Cleanup(func() { _ = pool.Close() })

	creatorResult := make(chan clientResult, 1)
	go func() {
		client, err := pool.Client(context.Background(), testTargetA)
		creatorResult <- clientResult{client: client, err: err}
	}()
	<-dialStarted

	ctx, cancel := context.WithCancel(context.Background())
	observed := &observedContext{Context: ctx, doneObserved: make(chan struct{})}
	waiterResult := make(chan error, 1)
	go func() {
		_, err := pool.Client(observed, testTargetA)
		waiterResult <- err
	}()
	<-observed.doneObserved
	cancel()
	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting Client() error = %v, want context.Canceled", err)
	}

	close(releaseDial)
	creator := <-creatorResult
	if creator.err != nil {
		t.Fatal(creator.err)
	}
	cached, err := pool.Client(context.Background(), testTargetA)
	if err != nil {
		t.Fatal(err)
	}
	if cached != creator.client {
		t.Fatal("waiter cancellation replaced the shared client")
	}
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("DialFunc calls = %d, want 1", got)
	}
}

func TestBackendPoolSharesDialFailureAndRetries(t *testing.T) {
	dialFailure := errors.New("dial refused")
	var attempts atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		if attempts.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
			return nil, dialFailure
		}
		return newLazyConn(target, options...)
	}
	pool := NewBackendPool(dial)
	t.Cleanup(func() { _ = pool.Close() })

	results := make(chan error, 8)
	go func() {
		_, err := pool.Client(context.Background(), testTargetA)
		results <- err
	}()
	<-firstStarted

	contexts := make([]*observedContext, 7)
	for i := range contexts {
		contexts[i] = &observedContext{Context: context.Background(), doneObserved: make(chan struct{})}
		go func(ctx context.Context) {
			_, err := pool.Client(ctx, testTargetA)
			results <- err
		}(contexts[i])
	}
	for _, ctx := range contexts {
		<-ctx.doneObserved
	}
	close(releaseFirst)

	for i := 0; i < 8; i++ {
		err := <-results
		if !errors.Is(err, ErrBackendDial) {
			t.Fatalf("Client() error = %v, want ErrBackendDial", err)
		}
		if !errors.Is(err, dialFailure) {
			t.Fatalf("Client() error = %v, want wrapped dial failure", err)
		}
		if strings.Contains(err.Error(), testTargetA) {
			t.Fatalf("Client() error leaked backend address: %v", err)
		}
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("concurrent failed DialFunc calls = %d, want 1", got)
	}

	if _, err := pool.Client(context.Background(), testTargetA); err != nil {
		t.Fatalf("Client() retry failed: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("DialFunc calls after retry = %d, want 2", got)
	}
}

func TestBackendPoolCloseClosesConnectionsAndIsIdempotent(t *testing.T) {
	var mu sync.Mutex
	var connections []*grpc.ClientConn
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		conn, err := newLazyConn(target, options...)
		if err == nil {
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
		}
		return conn, err
	}
	pool := NewBackendPool(dial)

	for _, target := range []string{testTargetA, testTargetB} {
		if _, err := pool.Client(context.Background(), target); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close() failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(connections) != 2 {
		t.Fatalf("created connections = %d, want 2", len(connections))
	}
	for i, conn := range connections {
		if state := conn.GetState(); state != connectivity.Shutdown {
			t.Errorf("connection %d state = %v, want %v", i, state, connectivity.Shutdown)
		}
	}
	if _, err := pool.Client(context.Background(), testTargetA); !errors.Is(err, ErrBackendPoolClosed) {
		t.Fatalf("Client() after Close error = %v, want ErrBackendPoolClosed", err)
	}
}

func TestBackendPoolCloseRacingDial(t *testing.T) {
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	connection := make(chan *grpc.ClientConn, 1)
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		close(dialStarted)
		<-releaseDial
		conn, err := newLazyConn(target, options...)
		if err == nil {
			connection <- conn
		}
		return conn, err
	}
	pool := NewBackendPool(dial)

	clientResult := make(chan error, 1)
	go func() {
		_, err := pool.Client(context.Background(), testTargetA)
		clientResult <- err
	}()
	<-dialStarted

	closeResult := make(chan error, 1)
	go func() { closeResult <- pool.Close() }()
	waitForPoolClosed(t, pool)
	close(releaseDial)

	if err := <-clientResult; !errors.Is(err, ErrBackendPoolClosed) {
		t.Fatalf("Client() error = %v, want ErrBackendPoolClosed", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	conn := <-connection
	if state := conn.GetState(); state != connectivity.Shutdown {
		t.Fatalf("connection state = %v, want %v", state, connectivity.Shutdown)
	}
}

func TestBackendPoolDoesNotHoldMutexWhileDialing(t *testing.T) {
	var pool *BackendPool
	var dialCount atomic.Int32
	dial := func(target string, options ...grpc.DialOption) (*grpc.ClientConn, error) {
		dialCount.Add(1)
		if target == testTargetA {
			if _, err := pool.Client(context.Background(), testTargetB); err != nil {
				return nil, err
			}
		}
		return newLazyConn(target, options...)
	}
	pool = NewBackendPool(dial)
	t.Cleanup(func() { _ = pool.Close() })

	result := make(chan error, 1)
	go func() {
		_, err := pool.Client(context.Background(), testTargetA)
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Client() deadlocked while DialFunc re-entered the pool")
	}
	if got := dialCount.Load(); got != 2 {
		t.Fatalf("DialFunc calls = %d, want 2", got)
	}
}

func TestBackendPoolUsesDefaultDialerAndCopiesOptions(t *testing.T) {
	pool := NewBackendPool(nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if _, err := pool.Client(context.Background(), testTargetA); err != nil {
		t.Fatalf("Client() with default dialer failed: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}

	original := grpc.WithUserAgent("original")
	options := []grpc.DialOption{original}
	received := make(chan []grpc.DialOption, 1)
	pool = NewBackendPool(func(target string, got ...grpc.DialOption) (*grpc.ClientConn, error) {
		received <- append([]grpc.DialOption(nil), got...)
		return newLazyConn(target, got...)
	}, options...)
	t.Cleanup(func() { _ = pool.Close() })
	options[0] = grpc.WithUserAgent("mutated")

	if _, err := pool.Client(context.Background(), testTargetA); err != nil {
		t.Fatal(err)
	}
	got := <-received
	if len(got) != 1 || got[0] != original {
		t.Fatal("NewBackendPool did not preserve a defensive copy of dial options")
	}
}

func waitForPoolClosed(t *testing.T, pool *BackendPool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pool.mu.Lock()
		closed := pool.closed
		pool.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("pool did not enter closed state")
}
