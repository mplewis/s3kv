package backing

// Key is the key for a key-value pair in the store.
type Key = string

// Backing is an interface by which a Store accesses data in some backend datastore.
type Backing interface {
	// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
	List(prefix string) ([]Key, error)
	// Get returns the value for the given key.
	Get(key Key) ([]byte, error)
	// Set sets the value for the given key.
	Set(key Key, value []byte) error
	// Del deletes the key-value pair for the given key.
	Del(key Key) error
}
