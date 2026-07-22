package grpcnode

import (
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/transport/grpccommon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) parsePutHeader(request *minikvv1.NodePutRequest) ([]byte, int64, time.Duration, error) {
	if request == nil {
		return nil, 0, 0, invalidRequest()
	}
	frame, ok := request.GetFrame().(*minikvv1.NodePutRequest_Header)
	if !ok || frame.Header == nil || frame.Header.Route == nil || frame.Header.Request == nil {
		return nil, 0, 0, invalidRequest()
	}
	key, size, ttl, err := grpccommon.ParsePutHeader(frame.Header.Request, s.limits)
	if err != nil {
		return nil, 0, 0, grpccommon.StatusError(err)
	}
	expectedShard, err := sharding.ID(key, s.shardCount)
	if err != nil || frame.Header.Route.GetShardId() != expectedShard {
		return nil, 0, 0, invalidRequest()
	}
	return key, size, ttl, nil
}

func invalidRequest() error {
	return status.Error(codes.InvalidArgument, "invalid request")
}
