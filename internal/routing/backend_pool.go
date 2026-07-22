package routing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"google.golang.org/grpc"
)

type DialFunc func(string, ...grpc.DialOption) (*grpc.ClientConn, error)

type poolEntry struct {
	ready    chan struct{}
	conn     *grpc.ClientConn
	client   minikvv1.NodeServiceClient
	err      error
	closeErr error
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
	return p.publishDial(address, entry, conn, err)
}

func (p *BackendPool) publishDial(address string, entry *poolEntry, conn *grpc.ClientConn, dialErr error) (minikvv1.NodeServiceClient, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed {
		closeErr := closeConnection(conn)
		p.mu.Lock()
		entry.err = ErrBackendPoolClosed
		entry.closeErr = closeErr
		close(entry.ready)
		p.mu.Unlock()
		return nil, ErrBackendPoolClosed
	}

	if dialErr != nil {
		closeErr := closeConnection(conn)
		wrapped := fmt.Errorf("%w: %w", ErrBackendDial, dialErr)
		p.mu.Lock()
		if p.closed {
			entry.err = ErrBackendPoolClosed
			entry.closeErr = closeErr
			close(entry.ready)
			p.mu.Unlock()
			return nil, ErrBackendPoolClosed
		}
		entry.err = wrapped
		entry.closeErr = closeErr
		if p.entries[address] == entry {
			delete(p.entries, address)
		}
		close(entry.ready)
		p.mu.Unlock()
		return nil, wrapped
	}

	client := minikvv1.NewNodeServiceClient(conn)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		closeErr := closeConnection(conn)
		p.mu.Lock()
		entry.err = ErrBackendPoolClosed
		entry.closeErr = closeErr
		close(entry.ready)
		p.mu.Unlock()
		return nil, ErrBackendPoolClosed
	}
	entry.conn = conn
	entry.client = client
	close(entry.ready)
	p.mu.Unlock()
	return client, nil
}

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
	entries := make([]*poolEntry, 0, len(p.entries))
	for _, entry := range p.entries {
		entries = append(entries, entry)
	}
	p.mu.Unlock()

	var closeErrors []error
	closedConnections := make(map[*grpc.ClientConn]struct{}, len(entries))
	for _, entry := range entries {
		<-entry.ready
		if entry.closeErr != nil {
			closeErrors = append(closeErrors, entry.closeErr)
		}
		if entry.conn == nil {
			continue
		}
		if _, ok := closedConnections[entry.conn]; ok {
			continue
		}
		closedConnections[entry.conn] = struct{}{}
		if err := entry.conn.Close(); err != nil {
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
