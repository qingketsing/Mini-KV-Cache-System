// Package routing resolves keys to backend nodes and reuses backend gRPC
// connections.
package routing

import (
	"context"
	"errors"
)

var (
	ErrRouterClosed         = errors.New("routing: router closed")
	ErrBackendPoolClosed    = errors.New("routing: backend pool closed")
	ErrInvalidConfiguration = errors.New("routing: invalid configuration")
	ErrBackendDial          = errors.New("routing: backend dial failed")
	ErrBackendClose         = errors.New("routing: backend close failed")
)

type Route struct {
	NodeID  string
	Address string
	ShardID uint32
	Epoch   uint64
}

type Router interface {
	Resolve(context.Context, []byte) (Route, error)
	Close() error
}
