package grpccommon

import (
	"fmt"
	"math"
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
)

func checkedMilliseconds(value uint64) (time.Duration, error) {
	if value > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return 0, fmt.Errorf("convert ttl: %w", store.ErrInvalidTTL)
	}
	return time.Duration(value) * time.Millisecond, nil
}

// ObjectInfoResponse converts Store metadata to a Put response.
func ObjectInfoResponse(info store.ObjectInfo) *minikvv1.PutResponse {
	return &minikvv1.PutResponse{
		ValueSize:                 objectSize(info.Size),
		ExpiresAtUnixMilliseconds: expiresAtMilliseconds(info.ExpiresAt),
	}
}

// ObjectInfoHeader converts Store metadata to a Get header.
func ObjectInfoHeader(info store.ObjectInfo) *minikvv1.GetHeader {
	return &minikvv1.GetHeader{
		ValueSize:                 objectSize(info.Size),
		ExpiresAtUnixMilliseconds: expiresAtMilliseconds(info.ExpiresAt),
	}
}

func objectSize(size int64) uint64 {
	if size < 0 {
		return 0
	}
	return uint64(size)
}

func expiresAtMilliseconds(expiresAt time.Time) int64 {
	if expiresAt.IsZero() {
		return 0
	}
	return expiresAt.UnixMilli()
}
