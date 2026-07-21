package store

import "github.com/zeebo/xxh3"

const shardHashSeed uint64 = 0

type hashFunc func([]byte) uint64

func protocolHash(key []byte) uint64 {
	return xxh3.HashSeed(key, shardHashSeed)
}

func shardIDWithHash(key []byte, shardCount uint32, hash hashFunc) uint32 {
	return uint32(hash(key) & uint64(shardCount-1))
}

func shardID(key []byte, shardCount uint32) uint32 {
	return shardIDWithHash(key, shardCount, protocolHash)
}
