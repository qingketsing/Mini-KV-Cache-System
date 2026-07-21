package store

import (
	"testing"

	"github.com/zeebo/xxh3"
)

func TestShardIDUsesSeedZeroXXH3(t *testing.T) {
	const knownHelloXXH3 = uint64(0x9555e8555c62dcfd)

	if got := xxh3.HashSeed([]byte("hello"), shardHashSeed); got != knownHelloXXH3 {
		t.Fatalf("protocol hash = %#x", got)
	}
	if got, want := shardID([]byte("hello"), 1024), uint32(knownHelloXXH3&1023); got != want {
		t.Fatalf("shard = %d, want %d", got, want)
	}
}
