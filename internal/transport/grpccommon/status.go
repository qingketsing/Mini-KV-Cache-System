package grpccommon

import (
	"context"
	"errors"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusError converts local transport and Store errors to sanitized gRPC statuses.
func StatusError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	case isMalformed(err),
		errors.Is(err, store.ErrInvalidKey),
		errors.Is(err, store.ErrInvalidTTL),
		errors.Is(err, store.ErrObjectTooLarge),
		errors.Is(err, store.ErrSizeMismatch):
		return status.Error(codes.InvalidArgument, "invalid request")
	case errors.Is(err, store.ErrNoCapacity):
		return status.Error(codes.ResourceExhausted, "cache capacity unavailable")
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "key not found")
	case errors.Is(err, store.ErrClosed):
		return status.Error(codes.Unavailable, "cache node unavailable")
	}

	if grpcStatus, ok := status.FromError(err); ok && grpcStatus.Code() != codes.Unknown {
		return status.Error(grpcStatus.Code(), "request failed")
	}
	return status.Error(codes.Internal, "internal error")
}

// BackendStatus converts a backend failure while giving the inbound context precedence.
func BackendStatus(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return StatusError(contextErr)
		}
	}
	if grpcStatus, ok := status.FromError(err); ok && grpcStatus.Code() != codes.Unknown {
		return status.Error(grpcStatus.Code(), "backend request failed")
	}
	return status.Error(codes.Internal, "internal error")
}

func isMalformed(err error) bool {
	return errors.Is(err, errMalformedHeader) ||
		errors.Is(err, errMalformedFrame) ||
		errors.Is(err, errInvalidRequestID) ||
		errors.Is(err, errInvalidSequence) ||
		errors.Is(err, errEmptyChunk) ||
		errors.Is(err, errChunkTooLarge) ||
		errors.Is(err, errShortStream) ||
		errors.Is(err, errLongStream)
}
