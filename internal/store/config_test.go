package store

import (
	"math"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.CapacityBytes != 64<<30 {
		t.Fatalf("capacity = %d", cfg.CapacityBytes)
	}
	if cfg.MaxObjectBytes != 128<<20 {
		t.Fatalf("max object = %d", cfg.MaxObjectBytes)
	}
	if cfg.MaxStagingBytes != 512<<20 {
		t.Fatalf("staging = %d", cfg.MaxStagingBytes)
	}
	if cfg.ChunkBytes != 1<<20 {
		t.Fatalf("chunk = %d", cfg.ChunkBytes)
	}
	if cfg.ShardCount != 1024 {
		t.Fatalf("shards = %d", cfg.ShardCount)
	}
	if cfg.TTLResolution != time.Second {
		t.Fatalf("ttl resolution = %s", cfg.TTLResolution)
	}
}

func TestConfigValidationRejectsOverflowingObjectFit(t *testing.T) {
	cfg := Config{
		CapacityBytes:   math.MaxInt64,
		MaxObjectBytes:  math.MaxInt64 - 100,
		MaxStagingBytes: math.MaxInt64,
		ChunkBytes:      1,
		ShardCount:      1,
		TTLResolution:   time.Second,
		TouchBuffer:     1,
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("expected object fit validation error")
	}
}

func TestCapacityPercentagesDoNotOverflow(t *testing.T) {
	capacity := int64(math.MaxInt64)
	store := &CoreStore{cfg: Config{CapacityBytes: capacity, ShardCount: 1}}
	wantProtected := capacity/5*4 + capacity%5*4/5
	if got := store.protectedLimit(); got != wantProtected {
		t.Fatalf("protected limit = %d, want %d", got, wantProtected)
	}

	highWatermark := capacity/100*95 + (capacity%100*95+99)/100
	store.liveBytes.Store(highWatermark - 1)
	if store.atHighWatermark() {
		t.Fatal("value below high watermark reported pressure")
	}
	store.liveBytes.Store(highWatermark)
	if !store.atHighWatermark() {
		t.Fatal("high watermark was not detected")
	}
}

func TestConfigValidation(t *testing.T) {
	valid := Config{
		CapacityBytes:   4096,
		MaxObjectBytes:  1024,
		MaxStagingBytes: 2048,
		ChunkBytes:      256,
		ShardCount:      4,
		TTLResolution:   time.Second,
		TouchBuffer:     8,
	}
	cases := map[string]func(*Config){
		"capacity":                 func(c *Config) { c.CapacityBytes = 0 },
		"object":                   func(c *Config) { c.MaxObjectBytes = 0 },
		"object does not fit":      func(c *Config) { c.CapacityBytes = c.MaxObjectBytes + maxKeyBytes + entryOverheadBytes - 1 },
		"staging":                  func(c *Config) { c.MaxStagingBytes = c.MaxObjectBytes - 1 },
		"chunk":                    func(c *Config) { c.ChunkBytes = 0 },
		"chunk larger than object": func(c *Config) { c.ChunkBytes = int(c.MaxObjectBytes + 1) },
		"shards":                   func(c *Config) { c.ShardCount = 3 },
		"ttl":                      func(c *Config) { c.TTLResolution = 0 },
		"touch buffer":             func(c *Config) { c.TouchBuffer = 0 },
	}

	if err := valid.validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
