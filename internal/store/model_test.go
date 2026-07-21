package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"
)

type modelValue struct {
	value     []byte
	expiresAt int64
}

func TestStoreMatchesReferenceModel(t *testing.T) {
	testClock := newManualClock(testTime)
	cfg := compactConfig()
	cfg.CapacityBytes = 1 << 20
	store := newConfiguredTestStore(t, cfg, testClock, false, protocolHash)
	random := rand.New(rand.NewSource(1))
	model := make(map[string]modelValue)
	keys := make([][]byte, 64)
	for index := range keys {
		keys[index] = []byte{byte(index), 0, byte(index * 7)}
	}

	for operation := 0; operation < 10_000; operation++ {
		if operation != 0 && operation%100 == 0 {
			testClock.Advance(time.Second)
			store.maintenanceStep(testClock.Now())
		}
		key := keys[random.Intn(len(keys))]
		modelKey := string(key)
		now := testClock.Now().UnixNano()
		switch random.Intn(3) {
		case 0:
			value := make([]byte, random.Intn(33))
			if _, err := random.Read(value); err != nil {
				t.Fatal(err)
			}
			ttl := time.Duration(0)
			if random.Intn(3) == 0 {
				ttl = time.Duration(random.Intn(5)+1) * time.Second
			}
			if _, err := store.Put(context.Background(), key, bytes.NewReader(value), PutOptions{Size: int64(len(value)), TTL: ttl}); err != nil {
				t.Fatalf("operation %d put: %v", operation, err)
			}
			expiresAt := int64(0)
			if ttl > 0 {
				expiresAt = testClock.Now().Add(ttl).UnixNano()
			}
			model[modelKey] = modelValue{value: bytes.Clone(value), expiresAt: expiresAt}
		case 1:
			expected, exists := model[modelKey]
			if exists && expected.expiresAt != 0 && expected.expiresAt <= now {
				delete(model, modelKey)
				exists = false
			}
			deleted, err := store.Delete(context.Background(), key)
			if err != nil || deleted != exists {
				t.Fatalf("operation %d delete=%v want=%v err=%v", operation, deleted, exists, err)
			}
			delete(model, modelKey)
		case 2:
			expected, exists := model[modelKey]
			if exists && expected.expiresAt != 0 && expected.expiresAt <= now {
				delete(model, modelKey)
				exists = false
			}
			object, err := store.Get(context.Background(), key)
			if !exists {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("operation %d get error=%v", operation, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("operation %d get: %v", operation, err)
			}
			got, readErr := io.ReadAll(object)
			object.Close()
			if readErr != nil || !bytes.Equal(got, expected.value) {
				t.Fatalf("operation %d got=%x want=%x err=%v", operation, got, expected.value, readErr)
			}
		}
	}
}
