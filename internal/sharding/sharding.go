package sharding

import (
	"fmt"

	"github.com/zeebo/xxh3"
)

const Seed uint64 = 0

type HashFunc func([]byte) uint64

func Hash(key []byte) uint64 {
	return xxh3.HashSeed(key, Seed)
}

func ID(key []byte, shardCount uint32) (uint32, error) {
	if shardCount == 0 || shardCount&(shardCount-1) != 0 {
		return 0, fmt.Errorf("sharding: shard count must be a power of two")
	}
	return IDWithHash(key, shardCount, Hash), nil
}

func IDWithHash(key []byte, shardCount uint32, hash HashFunc) uint32 {
	return uint32(hash(key) & uint64(shardCount-1))
}
