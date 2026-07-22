package routing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"google.golang.org/grpc"
)

var errNilBackendConnection = errors.New("routing: dial returned a nil connection")

// DialFunc creates a backend connection. Returning a nil connection with a nil
// error is invalid and is normalized to an internal dial failure before
// publication.
type DialFunc func(string, ...grpc.DialOption) (*grpc.ClientConn, error)

type poolEntry struct {
	ready  chan struct{}
	conn   *grpc.ClientConn
	client minikvv1.NodeServiceClient
	err    error
}

type BackendPool struct {
	mu        sync.Mutex
	dial      DialFunc
	options   []grpc.DialOption
	entries   map[string]*poolEntry
	closed    bool
	closeDone chan struct{}
	closeErr  error
}

func NewBackendPool(dial DialFunc, options ...grpc.DialOption) *BackendPool {
	if dial == nil {
		dial = grpc.NewClient
	}
	return &BackendPool{
		dial:      dial,
		options:   append([]grpc.DialOption(nil), options...),
		entries:   make(map[string]*poolEntry),
		closeDone: make(chan struct{}),
	}
}

// Client returns the reusable client for address, creating it on first use. If
// Client overlaps Close, it may return the client or ErrBackendPoolClosed based
// on whether connection publication or pool closure linearizes first.
func (p *BackendPool) Client(ctx context.Context, address string) (minikvv1.NodeServiceClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if address == "" {
		return nil, fmt.Errorf("%w: backend address is required", ErrInvalidConfiguration)
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrBackendPoolClosed
	}
	if entry, ok := p.entries[address]; ok {
		p.mu.Unlock()
		select {
		case <-entry.ready:
			return entry.client, entry.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	entry := &poolEntry{ready: make(chan struct{})}
	p.entries[address] = entry
	p.mu.Unlock()

	options := append([]grpc.DialOption(nil), p.options...)
	conn, err := p.dial(address, options...)
	if conn == nil && err == nil {
		err = errNilBackendConnection
	}
	return p.publishDial(address, entry, conn, err)
}

func (p *BackendPool) publishDial(address string, entry *poolEntry, conn *grpc.ClientConn, dialErr error) (minikvv1.NodeServiceClient, error) {
	if dialErr != nil {
		closeErr := closeConnection(conn)
		publishedErr := joinError(fmt.Errorf("%w: %w", ErrBackendDial, dialErr), closeErr)
		p.mu.Lock()
		if p.closed {
			publishedErr = joinError(ErrBackendPoolClosed, closeErr)
			entry.err = publishedErr
			close(entry.ready)
			p.mu.Unlock()
			return nil, publishedErr
		}
		entry.err = publishedErr
		if p.entries[address] == entry {
			delete(p.entries, address)
		}
		close(entry.ready)
		p.mu.Unlock()
		return nil, publishedErr
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		closeErr := closeConnection(conn)
		publishedErr := joinError(ErrBackendPoolClosed, closeErr)
		p.mu.Lock()
		entry.err = publishedErr
		close(entry.ready)
		p.mu.Unlock()
		return nil, publishedErr
	}
	client := minikvv1.NewNodeServiceClient(conn)
	entry.conn = conn
	entry.client = client
	close(entry.ready)
	p.mu.Unlock()
	return client, nil
}

// Close closes connections published before pool closure. It does not wait for
// custom DialFunc calls that have not returned; a late result is closed when
// that dial is published. Close is safe to call concurrently and repeatedly.
func (p *BackendPool) Close() error {
	p.mu.Lock()
	if p.closed {
		done := p.closeDone
		p.mu.Unlock()
		<-done
		p.mu.Lock()
		err := p.closeErr
		p.mu.Unlock()
		return err
	}
	p.closed = true
	connections := make([]*grpc.ClientConn, 0, len(p.entries))
	for _, entry := range p.entries {
		select {
		case <-entry.ready:
			if entry.conn != nil {
				connections = append(connections, entry.conn)
			}
		default:
		}
	}
	p.mu.Unlock()

	var closeErrors []error
	closedConnections := make(map[*grpc.ClientConn]struct{}, len(connections))
	for _, conn := range connections {
		if _, ok := closedConnections[conn]; ok {
			continue
		}
		closedConnections[conn] = struct{}{}
		if err := conn.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("%w: %w", ErrBackendClose, err))
		}
	}
	closeErr := errors.Join(closeErrors...)

	p.mu.Lock()
	p.closeErr = closeErr
	close(p.closeDone)
	p.mu.Unlock()
	return closeErr
}

func closeConnection(conn *grpc.ClientConn) error {
	if conn == nil {
		return nil
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrBackendClose, err)
	}
	return nil
}

func joinError(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	return errors.Join(primary, secondary)
}
