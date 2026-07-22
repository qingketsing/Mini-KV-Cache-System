package grpcnode

import (
	"errors"
	"io"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/transport/grpccommon"
)

// Put streams one routed object directly into the Store.
func (s *Server) Put(stream minikvv1.NodeService_PutServer) error {
	request, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return invalidRequest()
	}
	if err != nil {
		return grpccommon.StatusError(err)
	}
	key, size, ttl, err := s.parsePutHeader(request)
	if err != nil {
		return err
	}
	reader := grpccommon.NewNodePutReader(stream.Context(), size, s.limits.ChunkBytes, stream.Recv)
	info, err := s.store.Put(stream.Context(), key, reader, store.PutOptions{
		Size: size,
		TTL:  ttl,
	})
	if err != nil {
		return grpccommon.StatusError(err)
	}
	if err := stream.SendAndClose(grpccommon.ObjectInfoResponse(info)); err != nil {
		return grpccommon.StatusError(err)
	}
	return nil
}
