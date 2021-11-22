package s3kv

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mplewis/s3kv/multilock"
)

// list store keys with prefix
// get value for key
// open session, locking keys
// with session, set value for key
// close session

type Store struct {
	backing        Backing
	sessions       map[SessionID][]Key
	sessionLocks   *multilock.MultiLock
	sessionTimeout time.Duration
	timeoutLocks   *multilock.MultiLock
}

// Args are the arguments for a new store.
type Args struct {
	Backing        Backing       // Required. The backend for this store, where the data lives and is accessed.
	LockTimeout    time.Duration // Optional. The timeout for acquisition of all locks.
	SessionTimeout time.Duration // Optional. The timeout for a session if it is not closed by a client.
}

// New builds a new Store.
func New(args Args) (*Store, error) {
	if args.LockTimeout == 0 {
		args.LockTimeout = defaultLockTimeout
	}
	if args.SessionTimeout == 0 {
		args.SessionTimeout = defaultSessionTimeout
	}

	store := &Store{
		backing:        args.Backing,
		sessions:       map[SessionID][]Key{},
		sessionLocks:   multilock.New(args.LockTimeout),
		sessionTimeout: args.SessionTimeout,
		timeoutLocks:   multilock.New(args.LockTimeout),
	}
	return store, nil
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
	if err := s.keyInSess(sid, key); err != nil {
		return err
	}
	return s.backing.Set(key, value)
}

// Del deletes the key-value pair for the given key.
func (s *Store) Del(sid SessionID, key string, value []byte) error {
	if err := s.keyInSess(sid, key); err != nil {
		return err
	}
	return s.backing.Del(key)
}

// OpenSession acquires the given keys for exclusive writing.
func (s *Store) OpenSession(keys ...string) (SessionID, error) {
	acquired := []Key{}
	for _, key := range keys {
		if !s.sessionLocks.Acquire(string(key)) {
			s.unravel(acquired)
			return "", fmt.Errorf("could not acquire lock for key: %s", key)
		}
		acquired = append(acquired, Key(key))
	}
	sid := SessionID(uuid.New().String())
	s.sessions[sid] = acquired

	go func() {
		<-time.After(s.sessionTimeout)
		s.CloseSession(sid)
	}()

	return sid, nil
}

// CloseSession releases the exclusive write lock on the keys in the session.
func (s *Store) CloseSession(sid SessionID) {
	s.timeoutLocks.MustAcquire(string(sid))
	keys, ok := s.sessions[sid]
	if ok {
		s.unravel(keys)
		delete(s.sessions, sid)
	}
	s.timeoutLocks.MustRelease(string(sid))
}

// unravel ensures all the given keys are unlocked.
func (s *Store) unravel(keys []Key) {
	for _, key := range keys {
		s.sessionLocks.MustRelease(string(key))
	}
}

// keyInSess returns true if the session exists and includes the given key.
func (s *Store) keyInSess(sid SessionID, key string) error {
	keys, ok := s.sessions[sid]
	if !ok {
		return fmt.Errorf("session not found: %s", sid)
	}
	in := false
	for _, k := range keys {
		if k == key {
			in = true
			break
		}
	}
	if !in {
		return fmt.Errorf("session %s does not have key %s", sid, key)
	}
	return nil
}
