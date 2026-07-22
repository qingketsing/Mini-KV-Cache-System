package sharding

import "testing"

func TestIDUsesProtocolXXH3(t *testing.T) {
	const knownHelloXXH3 = uint64(0x9555e8555c62dcfd)

	key := []byte("hello")
	if got := Hash(key); got != knownHelloXXH3 {
		t.Fatalf("Hash(%q) = %#x, want %#x", key, got, knownHelloXXH3)
	}

	got, err := ID(key, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint32(253); got != want {
		t.Fatalf("ID(%q, 1024) = %d, want %d", key, got, want)
	}
}

func TestIDHandlesBinaryKeys(t *testing.T) {
	key := []byte{0x00, 0xff, 0x00, 0x80}

	got, err := ID(key, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint32(Hash(key) & 1023); got != want {
		t.Fatalf("ID(%x, 1024) = %d, want %d", key, got, want)
	}
}

func TestIDRejectsZeroShardCount(t *testing.T) {
	if _, err := ID([]byte("key"), 0); err == nil {
		t.Fatal("ID with zero shard count succeeded")
	}
}

func TestIDRejectsNonPowerOfTwoShardCount(t *testing.T) {
	if _, err := ID([]byte("key"), 3); err == nil {
		t.Fatal("ID with non-power-of-two shard count succeeded")
	}
}
