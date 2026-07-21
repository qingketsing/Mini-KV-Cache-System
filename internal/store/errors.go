package store

import "errors"

var (
	ErrNotFound       = errors.New("store: not found")
	ErrInvalidKey     = errors.New("store: invalid key")
	ErrInvalidTTL     = errors.New("store: invalid ttl")
	ErrObjectTooLarge = errors.New("store: object too large")
	ErrSizeMismatch   = errors.New("store: size mismatch")
	ErrNoCapacity     = errors.New("store: no capacity")
	ErrClosed         = errors.New("store: closed")
)

func validateKey(key []byte) error {
	if len(key) < minKeyBytes || len(key) > maxKeyBytes {
		return ErrInvalidKey
	}
	return nil
}
