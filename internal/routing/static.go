package routing

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
)

type StaticRouter struct {
	nodeID     string
	address    string
	shardCount uint32
	closed     atomic.Bool
}

func NewStaticRouter(nodeID, address string, shardCount uint32) (*StaticRouter, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("%w: node ID is required", ErrInvalidConfiguration)
	}
	if address == "" {
		return nil, fmt.Errorf("%w: backend address is required", ErrInvalidConfiguration)
	}
	if _, err := sharding.ID(nil, shardCount); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfiguration, err)
	}
	return &StaticRouter{
		nodeID:     nodeID,
		address:    address,
		shardCount: shardCount,
	}, nil
}

func (r *StaticRouter) Resolve(ctx context.Context, key []byte) (Route, error) {
	if err := ctx.Err(); err != nil {
		return Route{}, err
	}
	if r.closed.Load() {
		return Route{}, ErrRouterClosed
	}

	shardID, err := sharding.ID(key, r.shardCount)
	if err != nil {
		return Route{}, err
	}
	return Route{
		NodeID:  r.nodeID,
		Address: r.address,
		ShardID: shardID,
		Epoch:   0,
	}, nil
}

func (r *StaticRouter) Close() error {
	r.closed.Store(true)
	return nil
}
