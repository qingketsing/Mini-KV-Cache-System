package grpcnode

import (
	"fmt"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/transport/grpccommon"
)

// Server implements the node-local gRPC transport.
type Server struct {
	minikvv1.UnimplementedNodeServiceServer
	store      store.Store
	limits     grpccommon.Limits
	shardCount uint32
}

// New validates local dependencies and constructs a node transport server.
func New(st store.Store, limits grpccommon.Limits, shardCount uint32) (*Server, error) {
	if st == nil {
		return nil, fmt.Errorf("grpcnode: store is required")
	}
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("grpcnode: limits: %w", err)
	}
	if _, err := sharding.ID(nil, shardCount); err != nil {
		return nil, fmt.Errorf("grpcnode: shard count: %w", err)
	}
	return &Server{store: st, limits: limits, shardCount: shardCount}, nil
}
