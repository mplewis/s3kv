package s3kv

import (
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/thoas/go-funk"
	"gopkg.in/redsync.v1"
)

// list store keys with prefix
// get value for key
// open session, locking keys
// with session, set value for key
// close session

const GLOBAL_NAMESPACE = "s3kv"

type Store struct {
	namespace string
	backing   Backing
	redis     redis.Client
	locks     redsync.Redsync
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
		backing:  args.Backing,
		sessions: map[SessionID][]Key{},
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
	if !s.sessionHas(sid, key) {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Set(key, value)
}

// Del deletes the key-value pair for the given key.
func (s *Store) Del(sid SessionID, key string) error {
	if !s.sessionHas(sid, key) {
		return fmt.Errorf("session %s does not include key %s", sid, key)
	}
	return s.backing.Del(key)
}

// Lock acquires the given keys for exclusive writing and returns a new session ID, or an error if the keys could not be locked.
func (s *Store) Lock(keys ...string) (SessionID, error) {
	sid := s.sessKey()
	sess := []Key{}
	for _, key := range keys {
		if err := s.lockKey(key); err != nil {
			for _, key := range sess {
				_ = s.unlockKey(key) // ignore errors
			}
			return "", err
		}
		sess = append(sess, key)
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
		if err := s.unlockKey(key); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors while unlocking session: %v", errs)
	}
	return nil
}

func (s *Store) nsKey(key Key) string {
	return fmt.Sprintf("%s:%s:%s", GLOBAL_NAMESPACE, s.namespace, key)
}

func (s *Store) sessKey() SessionID {
	return SessionID(s.nsKey("sess_" + uuid.New().String()))
}

func (s *Store) lockKey(key Key) error {
	// TODO
	return nil
}

func (s *Store) unlockKey(key Key) error {
	// TODO
	return nil
}

func (s *Store) sessionHas(sid SessionID, key Key) bool {
	sess, err := s.getSession(sid)
	if err != nil {
		log.Printf("WARN: failed to get session %s: %v", sid, err.Error())
		return false
	}
	return funk.Contains(sess, key)
}

func (s *Store) getSession(sid SessionID) ([]Key, error) {
	// TODO
	return []Key{}, nil
}

func (s *Store) setSession(sid SessionID, keys []Key) error {
	// TODO
	return nil
}
