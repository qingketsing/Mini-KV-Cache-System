package grpccommon

import (
	"fmt"
	"math"
)

const framingReserveBytes = 64 << 10

// Limits bounds gRPC messages, payload chunks, and complete objects.
type Limits struct {
	ChunkBytes      int
	MaxMessageBytes int
	MaxObjectBytes  int64
}

// DefaultLimits returns the transport limits used by MiniKV services.
func DefaultLimits() Limits {
	return Limits{
		ChunkBytes:      256 << 10,
		MaxMessageBytes: 1 << 20,
		MaxObjectBytes:  128 << 20,
	}
}

// Validate checks that the limits can safely frame every payload chunk.
func (l Limits) Validate() error {
	if l.ChunkBytes <= 0 {
		return fmt.Errorf("grpccommon: chunk size must be positive")
	}
	if l.MaxMessageBytes <= 0 {
		return fmt.Errorf("grpccommon: message size must be positive")
	}
	if l.MaxObjectBytes <= 0 {
		return fmt.Errorf("grpccommon: object size must be positive")
	}
	if l.MaxObjectBytes > int64(math.MaxUint32)+1 {
		return fmt.Errorf("grpccommon: object size exceeds chunk sequence space")
	}
	if l.MaxMessageBytes <= framingReserveBytes {
		return fmt.Errorf("grpccommon: message size must exceed framing reserve")
	}
	if l.ChunkBytes > l.MaxMessageBytes-framingReserveBytes {
		return fmt.Errorf("grpccommon: chunk size exceeds framed message size")
	}
	if int64(l.ChunkBytes) > l.MaxObjectBytes {
		return fmt.Errorf("grpccommon: chunk size exceeds object size")
	}
	return nil
}
