package s3kv

import (
	"errors"
	"fmt"

	"github.com/mplewis/s3kv/backing"
	"github.com/mplewis/s3kv/sloto"
)

const GLOBAL_NAMESPACE = "s3kv"
const NS_DELIM = "/"

type Store struct {
	namespace string
	backing   backing.Backing
	sloto     *sloto.Sloto
}

// Args are the arguments for a new store.
type Args struct {
	Namespace string          // Required. The namespace for this store's session and lock keys.
	Backing   backing.Backing // Required. The backend for this store, where the data lives and is accessed.
	Timeouts  *sloto.Args     // Optional. The timeout configuration for this store.
}

// New builds a new Store.
func New(args Args) (*Store, error) {
	if args.Namespace == "" {
		return nil, errors.New("namespace must not be blank")
	}
	if args.Backing == nil {
		return nil, errors.New("backing must not be nil")
	}
	if args.Timeouts == nil {
		args.Timeouts = &defaultSlotoArgs
	}
	sloto := sloto.New(*args.Timeouts)
	return &Store{
		namespace: args.Namespace,
		backing:   args.Backing,
		sloto:     sloto,
	}, nil
}

// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
func (s *Store) List(prefix string) ([]Key, error) {
	return s.backing.List(s.ns1(prefix))
}

// Get returns the value for the given key.
func (s *Store) Get(key string) ([]byte, error) {
	return s.backing.Get(s.ns1(key))
}

// Set sets the value for the given key. You must have an open session for the key.
func (s *Store) Set(sid SessionID, key string, value []byte) error {
	in := s.sloto.Contains(sid, key)
	if !in {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Set(s.ns1(key), value)
}

// Del deletes the key-value pair for the given key.
func (s *Store) Del(sid SessionID, key string) error {
	in := s.sloto.Contains(sid, key)
	if !in {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Del(s.ns1(key))
}

// Lock acquires the given keys for exclusive writing and returns a new session ID.
func (s *Store) Lock(keys ...string) (SessionID, error) {
	return s.sloto.Lock(keys...)
}

// Unlock releases the exclusive write lock on the keys in the session.
func (s *Store) Unlock(sid SessionID) {
	s.sloto.Unlock(sid)
}

func (s *Store) ns1(key string) string {
	return s.namespace + NS_DELIM + key
}
