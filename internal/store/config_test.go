package store

import (
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
