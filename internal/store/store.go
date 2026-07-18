package store

// Store defines the core key-value operations used by the service layer.
type Store interface {
	Get(key string) (value []byte, ok bool)
	Put(key string, value []byte)
	Delete(key string) bool
}
