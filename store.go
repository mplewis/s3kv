package s3kv

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/mplewis/s3kv/backing"
	"github.com/mplewis/s3kv/sloto"
	"github.com/thoas/go-funk"
)

const GLOBAL_NAMESPACE = "s3kv"
const SESS_KEYS_DELIM = "|"

type Store struct {
	namespace string
	backing   backing.Backing
	sloto     sloto.Sloto
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
		sloto:     *sloto,
	}, nil
}

// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
func (s *Store) List(prefix string) ([]Key, error) {
	return s.backing.List(prefix)
}

// Get returns the value for the given key.
func (s *Store) Get(key string) ([]byte, error) {
	return s.backing.Get(key)
}

// Set sets the value for the given key. You must have an open session for the key.
func (s *Store) Set(sid SessionID, key string, value []byte) error {
	in, err := s.sessionHas(sid, key)
	if err != nil {
		return err
	}
	if !in {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Set(key, value)
}

// Del deletes the key-value pair for the given key.
func (s *Store) Del(sid SessionID, key string) error {
	in, err := s.sessionHas(sid, key)
	if err != nil {
		return err
	}
	if !in {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Del(key)
}

// Lock acquires the given keys for exclusive writing and returns a new session ID.
func (s *Store) Lock(keys ...string) (SessionID, error) {
	sid := s.sessKey()
	sess := []Key{}

	unravel := func() {
		for _, key := range sess {
			if s.unlockKey(key) {
				log.Printf("WARN: failed to unlock key during unravel: %s\n", key)
			}
		}
	}

	for _, key := range keys {
		if err := s.lockKey(key); err != nil {
			unravel()
			return "", err
		}
		sess = append(sess, key)
	}

	err := s.setSession(sid, sess)
	if err != nil {
		unravel()
		return "", err
	}

	return sid, nil
}

// Unlock releases the exclusive write lock on the keys in the session.
func (s *Store) Unlock(sid SessionID) error {
	sess, err := s.getSession(sid)
	if err != nil {
		return err
	}

	errs := []error{}
	for _, key := range sess {
		if !s.unlockKey(key) {
			errs = append(errs, err)
		}
	}

	err = s.delSession(sid)
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d errors while unlocking session: %v", len(errs), errs)
	}

	return nil
}

// lockKey locks a key.
func (s *Store) lockKey(key Key) error {
	return s.mutex(key).Lock()
}

// unlockKey unlocks a key, returning True on success and False on failure.
func (s *Store) unlockKey(key Key) bool {
	return s.mutex(key).Unlock()
}

// sessionHas returns True if the session includes the given key.
func (s *Store) sessionHas(sid SessionID, key Key) (bool, error) {
	sess, err := s.getSession(sid)
	if err != nil {
		return false, err
	}
	return funk.Contains(sess, key), nil
}

// getSession returns the keys for the requested session.
func (s *Store) getSession(sid SessionID) ([]Key, error) {
	raw, err := s.redis.Get(s.redis.Context(), string(sid)).Result()
	if err != nil {
		return nil, err
	}
	return strings.Split(raw, SESS_KEYS_DELIM), nil
}

// setSession sets the keys for the requested session.
func (s *Store) setSession(sid SessionID, keys []Key) error {
	return s.redis.Set(s.redis.Context(), string(sid), strings.Join(keys, SESS_KEYS_DELIM), s.sessionTimeout).Err()
}

// delSession deletes the session.
func (s *Store) delSession(sid SessionID) error {
	return s.redis.Del(s.redis.Context(), string(sid)).Err()
}
