package store

import (
	"context"
	"strings"
	"testing"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/sharding"
)

type hashFunc = sharding.HashFunc

var (
	protocolHash    = sharding.Hash
	shardIDWithHash = sharding.IDWithHash
)

func TestStoreUsesSharedHash(t *testing.T) {
	injectedHash := sharding.HashFunc(func(key []byte) uint64 {
		if got, want := string(key), "shared-hash"; got != want {
			t.Fatalf("hash key = %q, want %q", got, want)
		}
		return 3
	})
	cfg := compactConfig()
	store, err := newCoreStore(cfg, coreDependencies{
		clock: newManualClock(testTime),
		hash:  injectedHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	if _, err := store.Put(context.Background(), []byte("shared-hash"), strings.NewReader("value"), PutOptions{Size: 5}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.shards[3].entries["shared-hash"]; !ok {
		t.Fatal("injected shared hash did not select shard 3")
	}
}
