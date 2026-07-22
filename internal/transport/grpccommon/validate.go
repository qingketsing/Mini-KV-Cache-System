package grpccommon

import (
	"errors"
	"fmt"
	"math"
	"time"

	minikvv1 "github.com/qingketsing/Mini-KV-Cache-System/gen/go/minikv/v1"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
)

var (
	errMalformedHeader  = errors.New("grpccommon: malformed put header")
	errMalformedFrame   = errors.New("grpccommon: malformed stream frame")
	errInvalidRequestID = errors.New("grpccommon: invalid request id")
	errInvalidSequence  = errors.New("grpccommon: invalid chunk sequence")
	errEmptyChunk       = errors.New("grpccommon: empty chunk")
	errChunkTooLarge    = errors.New("grpccommon: chunk too large")
	errShortStream      = errors.New("grpccommon: short stream")
	errLongStream       = errors.New("grpccommon: long stream")
)

// ParsePutHeader validates and converts a wire Put header. The returned key is
// borrowed from header and remains valid while the protobuf message is owned.
func ParsePutHeader(header *minikvv1.PutHeader, limits Limits) ([]byte, int64, time.Duration, error) {
	if header == nil {
		return nil, 0, 0, fmt.Errorf("parse put header: %w", errMalformedHeader)
	}
	if err := limits.Validate(); err != nil {
		return nil, 0, 0, fmt.Errorf("parse put header limits: %w", err)
	}
	if err := ValidateKey(header.GetKey()); err != nil {
		return nil, 0, 0, fmt.Errorf("parse put header key: %w", err)
	}
	if header.GetValueSize() > uint64(math.MaxInt64) || header.GetValueSize() > uint64(limits.MaxObjectBytes) {
		return nil, 0, 0, fmt.Errorf("parse put header size: %w", store.ErrObjectTooLarge)
	}
	ttl, err := checkedMilliseconds(header.GetTtlMilliseconds())
	if err != nil {
		return nil, 0, 0, fmt.Errorf("parse put header ttl: %w", err)
	}
	requestIDBytes := len(header.GetRequestId())
	if requestIDBytes != 0 && requestIDBytes != 16 {
		return nil, 0, 0, fmt.Errorf("parse put header request id: %w", errInvalidRequestID)
	}
	return header.GetKey(), int64(header.GetValueSize()), ttl, nil
}

// ValidateKey checks the transport key-length contract. Keys may contain any bytes.
func ValidateKey(key []byte) error {
	if len(key) < 1 || len(key) > 1024 {
		return store.ErrInvalidKey
	}
	return nil
}

// ValidateChunk checks the payload and sequence of one data frame.
func ValidateChunk(chunk *minikvv1.DataChunk, expectedSequence uint32, chunkBytes int) error {
	if chunk == nil {
		return fmt.Errorf("validate chunk: %w", errMalformedFrame)
	}
	if chunk.GetSequence() != expectedSequence {
		return fmt.Errorf("validate chunk: %w", errInvalidSequence)
	}
	if len(chunk.GetData()) == 0 {
		return fmt.Errorf("validate chunk: %w", errEmptyChunk)
	}
	if chunkBytes <= 0 || len(chunk.GetData()) > chunkBytes {
		return fmt.Errorf("validate chunk: %w", errChunkTooLarge)
	}
	return nil
}
