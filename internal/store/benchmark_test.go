package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
)

func BenchmarkStoreStreaming(b *testing.B) {
	for _, size := range []int{1 << 10, 1 << 20, 32 << 20} {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			store := newBenchmarkStore(b, int64(size)*2+(1<<20))
			value := make([]byte, size)
			key := []byte("benchmark")
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := store.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: int64(size)}); err != nil {
					b.Fatal(err)
				}
				object, err := store.Get(context.Background(), key)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, object); err != nil {
					b.Fatal(err)
				}
				object.Close()
			}
		})
	}

	b.Run("parallel_1KiB", func(b *testing.B) {
		store := newBenchmarkStore(b, 64<<20)
		value := make([]byte, 1<<10)
		var sequence atomic.Uint64
		b.SetBytes(1 << 10)
		b.ReportAllocs()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				id := sequence.Add(1)
				key := []byte(strconv.FormatUint(id%1024, 10))
				if _, err := store.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: 1 << 10}); err != nil {
					b.Error(err)
					return
				}
				object, err := store.Get(context.Background(), key)
				if err == nil {
					_, _ = io.Copy(io.Discard, object)
					object.Close()
				}
			}
		})
	})
}
