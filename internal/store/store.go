package store

import (
	"context"
	"io"
	"time"
)

// Store is the in-process contract implemented by a MiniKV storage kernel.
type Store interface {
	Put(context.Context, []byte, io.Reader, PutOptions) (ObjectInfo, error)
	Get(context.Context, []byte) (Object, error)
	Delete(context.Context, []byte) (bool, error)
	Stats() Stats
	Close() error
}

// PutOptions describes the exact payload size and optional lifetime of a value.
type PutOptions struct {
	Size int64
	TTL  time.Duration
}

// Object is an immutable snapshot reader returned by Store.Get.
type Object interface {
	io.ReadCloser
	Info() ObjectInfo
}

// ObjectInfo is immutable metadata captured when an Object is opened.
type ObjectInfo struct {
	Size      int64
	ExpiresAt time.Time
}

// Stats is a weakly consistent snapshot of Store gauges and counters.
type Stats struct {
	CapacityBytes int64
	LiveBytes     int64
	PayloadBytes  int64
	StagingBytes  int64
	Entries       int64
	Gets          uint64
	Hits          uint64
	Misses        uint64
	Puts          uint64
	Deletes       uint64
	Evictions     uint64
	Expirations   uint64
	RejectedPuts  uint64
	TouchDrops    uint64
}
