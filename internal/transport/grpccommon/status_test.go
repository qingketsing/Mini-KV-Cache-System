package grpccommon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatusErrorMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		code    codes.Code
		message string
	}{
		{name: "canceled", err: context.Canceled, code: codes.Canceled, message: "request canceled"},
		{name: "wrapped canceled", err: fmt.Errorf("wrapped: %w", context.Canceled), code: codes.Canceled, message: "request canceled"},
		{name: "deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded, message: "request deadline exceeded"},
		{name: "invalid key", err: store.ErrInvalidKey, code: codes.InvalidArgument, message: "invalid request"},
		{name: "invalid ttl", err: store.ErrInvalidTTL, code: codes.InvalidArgument, message: "invalid request"},
		{name: "object too large", err: store.ErrObjectTooLarge, code: codes.InvalidArgument, message: "invalid request"},
		{name: "size mismatch", err: store.ErrSizeMismatch, code: codes.InvalidArgument, message: "invalid request"},
		{name: "capacity", err: store.ErrNoCapacity, code: codes.ResourceExhausted, message: "cache capacity unavailable"},
		{name: "not found", err: store.ErrNotFound, code: codes.NotFound, message: "key not found"},
		{name: "closed", err: store.ErrClosed, code: codes.Unavailable, message: "cache node unavailable"},
		{name: "unexpected", err: errors.New("key=secret payload=private address=10.0.0.1"), code: codes.Internal, message: "internal error"},
		{name: "unknown status", err: status.Error(codes.Unknown, "secret backend"), code: codes.Internal, message: "internal error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := status.Convert(StatusError(test.err))
			if got.Code() != test.code || got.Message() != test.message {
				t.Fatalf("StatusError() = (%s, %q), want (%s, %q)", got.Code(), got.Message(), test.code, test.message)
			}
		})
	}

	if err := StatusError(nil); err != nil {
		t.Fatalf("StatusError(nil) = %v, want nil", err)
	}
}

func TestStatusErrorMalformedSentinelsSurviveWrapping(t *testing.T) {
	t.Parallel()

	malformed := []error{
		errMalformedHeader,
		errMalformedFrame,
		errInvalidRequestID,
		errInvalidSequence,
		errEmptyChunk,
		errChunkTooLarge,
		errShortStream,
		errLongStream,
	}
	for _, sentinel := range malformed {
		err := fmt.Errorf("arena wrapping: %w", sentinel)
		got := status.Convert(StatusError(err))
		if got.Code() != codes.InvalidArgument || got.Message() != "invalid request" {
			t.Errorf("StatusError(wrapped %v) = (%s, %q), want (InvalidArgument, %q)", sentinel, got.Code(), got.Message(), "invalid request")
		}
	}
}

func TestStatusErrorPreservesCodeAndSanitizesMessage(t *testing.T) {
	t.Parallel()

	err := status.Error(codes.PermissionDenied, "key=secret payload=private address=10.0.0.1")
	got := status.Convert(StatusError(err))
	if got.Code() != codes.PermissionDenied {
		t.Fatalf("StatusError() code = %s, want %s", got.Code(), codes.PermissionDenied)
	}
	if got.Message() != "request failed" {
		t.Fatalf("StatusError() message = %q, want %q", got.Message(), "request failed")
	}
	assertSanitized(t, got.Message())
}

func TestBackendStatus(t *testing.T) {
	t.Parallel()

	t.Run("nil", func(t *testing.T) {
		if err := BackendStatus(context.Background(), nil); err != nil {
			t.Fatalf("BackendStatus(nil) = %v, want nil", err)
		}
	})

	t.Run("preserves status code", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "dial 10.0.0.1:9091: secret")
		got := status.Convert(BackendStatus(context.Background(), err))
		if got.Code() != codes.Unavailable || got.Message() != "backend request failed" {
			t.Fatalf("BackendStatus() = (%s, %q), want (Unavailable, %q)", got.Code(), got.Message(), "backend request failed")
		}
		assertSanitized(t, got.Message())
	})

	t.Run("cancellation wins", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got := status.Convert(BackendStatus(ctx, status.Error(codes.Unavailable, "secret")))
		if got.Code() != codes.Canceled || got.Message() != "request canceled" {
			t.Fatalf("BackendStatus() = (%s, %q), want (Canceled, %q)", got.Code(), got.Message(), "request canceled")
		}
	})

	t.Run("deadline wins", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		got := status.Convert(BackendStatus(ctx, status.Error(codes.NotFound, "secret")))
		if got.Code() != codes.DeadlineExceeded || got.Message() != "request deadline exceeded" {
			t.Fatalf("BackendStatus() = (%s, %q), want (DeadlineExceeded, %q)", got.Code(), got.Message(), "request deadline exceeded")
		}
	})

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "unknown status", err: status.Error(codes.Unknown, "secret")},
		{name: "non status", err: errors.New("key=secret payload=private address=10.0.0.1")},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := status.Convert(BackendStatus(context.Background(), test.err))
			if got.Code() != codes.Internal || got.Message() != "internal error" {
				t.Fatalf("BackendStatus() = (%s, %q), want (Internal, %q)", got.Code(), got.Message(), "internal error")
			}
			assertSanitized(t, got.Message())
		})
	}
}

func TestObjectInfoMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		info       store.ObjectInfo
		wantSize   uint64
		wantExpiry int64
	}{
		{name: "zero", info: store.ObjectInfo{}},
		{name: "nonzero", info: store.ObjectInfo{Size: 42, ExpiresAt: time.UnixMilli(1712345678123)}, wantSize: 42, wantExpiry: 1712345678123},
		{name: "negative size does not wrap", info: store.ObjectInfo{Size: -1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := ObjectInfoResponse(test.info)
			if response.GetValueSize() != test.wantSize || response.GetExpiresAtUnixMilliseconds() != test.wantExpiry {
				t.Errorf("ObjectInfoResponse() = (%d, %d), want (%d, %d)", response.GetValueSize(), response.GetExpiresAtUnixMilliseconds(), test.wantSize, test.wantExpiry)
			}
			header := ObjectInfoHeader(test.info)
			if header.GetValueSize() != test.wantSize || header.GetExpiresAtUnixMilliseconds() != test.wantExpiry {
				t.Errorf("ObjectInfoHeader() = (%d, %d), want (%d, %d)", header.GetValueSize(), header.GetExpiresAtUnixMilliseconds(), test.wantSize, test.wantExpiry)
			}
		})
	}
}

func assertSanitized(t *testing.T, message string) {
	t.Helper()

	for _, forbidden := range []string{"secret", "private", "10.0.0.1", "key=", "payload=", "address="} {
		if strings.Contains(message, forbidden) {
			t.Errorf("status message %q contains forbidden text %q", message, forbidden)
		}
	}
}
