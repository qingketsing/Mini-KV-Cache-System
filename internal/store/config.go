package store

import (
	"fmt"
	"time"
)

const (
	minKeyBytes = 1
	maxKeyBytes = 1024

	entryOverheadBytes int64 = 128

	highWatermarkNumerator int64 = 95
	lowWatermarkNumerator  int64 = 90
	watermarkDenominator   int64 = 100
	maxAdmissionAttempts         = 3
)

// Config controls memory limits and internal concurrency partitioning.
type Config struct {
	CapacityBytes   int64
	MaxObjectBytes  int64
	MaxStagingBytes int64
	ChunkBytes      int
	ShardCount      uint32
	TTLResolution   time.Duration
	TouchBuffer     int
}

// DefaultConfig returns the initial production node sizing.
func DefaultConfig() Config {
	const capacity = int64(64 << 30)
	const maxObject = int64(128 << 20)

	staging := capacity / 8
	if staging > 512<<20 {
		staging = 512 << 20
	}
	if staging < maxObject {
		staging = maxObject
	}

	return Config{
		CapacityBytes:   capacity,
		MaxObjectBytes:  maxObject,
		MaxStagingBytes: staging,
		ChunkBytes:      1 << 20,
		ShardCount:      1024,
		TTLResolution:   time.Second,
		TouchBuffer:     64 << 10,
	}
}

func (c Config) validate() error {
	if c.CapacityBytes <= 0 {
		return fmt.Errorf("capacity bytes must be positive")
	}
	if c.MaxObjectBytes <= 0 {
		return fmt.Errorf("max object bytes must be positive")
	}
	if c.CapacityBytes < c.MaxObjectBytes+maxKeyBytes+entryOverheadBytes {
		return fmt.Errorf("capacity cannot fit maximum object")
	}
	if c.MaxStagingBytes < c.MaxObjectBytes {
		return fmt.Errorf("staging cannot fit maximum object")
	}
	if c.ChunkBytes <= 0 || int64(c.ChunkBytes) > c.MaxObjectBytes {
		return fmt.Errorf("chunk bytes must be within object limit")
	}
	if c.ShardCount == 0 || c.ShardCount&(c.ShardCount-1) != 0 {
		return fmt.Errorf("shard count must be a power of two")
	}
	if c.TTLResolution <= 0 {
		return fmt.Errorf("ttl resolution must be positive")
	}
	if c.TouchBuffer <= 0 {
		return fmt.Errorf("touch buffer must be positive")
	}
	return nil
}
