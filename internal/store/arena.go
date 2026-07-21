package store

import (
	"context"
	"io"
)

// ValueRef is an opaque Arena-owned value handle.
type ValueRef struct {
	handle any
}

// Arena owns immutable payload bytes independently from the Store index.
type Arena interface {
	Write(context.Context, io.Reader, int64) (ValueRef, error)
	Open(ValueRef) (io.ReadCloser, error)
	Release(ValueRef)
	Close() error
}
