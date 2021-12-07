// sloto stands for Session LockOut-TagOut, like the thing you use to keep industrial equipment safe.

package sloto

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

// DEFAULT_LOCK_ATTEMPT_TIMEOUT is how long we try to lock a given set of keys for a new session before giving up.
const DEFAULT_LOCK_ATTEMPT_TIMEOUT = 5 * time.Second

// DEFAULT_SESSION_TIMEOUT is how long we allow a session to exist before unlocking its keys and closing it.
const DEFAULT_SESSION_TIMEOUT = 15 * time.Second

// DEFAULT_LOCK_ATTEMPT_INTERVAL is the amount of time to wait between lock attempts.
const DEFAULT_LOCK_ATTEMPT_INTERVAL = time.Millisecond * 100

// JITTER_FRAC is the percentage of jitter to add to a try-lock delay.
const JITTER_FRAC = 0.1 // 10%

// Key is a unique identifier for a bit of your own data. It is locked by creating a session which contains it.
type Key = string

// SessionID is a unique identifier for a session, created when a session is created for keys.
type SessionID string

// Status represents whether a key is locked or unlocked.
type Status bool

const (
	LOCKED   Status = true
	UNLOCKED Status = false
)

// Sloto facilitates safe locking of groups of keys in auto-expiring sessions.
type Sloto struct {
	lattIntv time.Duration
	lockTO   time.Duration
	sessTO   time.Duration
	access   sync.Mutex
	keyLocks map[Key]Status
	sessions map[SessionID][]Key
}

// Args is the set of arguments for creating a new Sloto.
type Args struct {
	LockAttemptInterval time.Duration
	LockAttemptTimeout  time.Duration
	SessionTimeout      time.Duration
}

func New(args Args) *Sloto {
	if args.LockAttemptInterval == 0 {
		args.LockAttemptInterval = DEFAULT_LOCK_ATTEMPT_INTERVAL
	}
	if args.LockAttemptTimeout == 0 {
		args.LockAttemptTimeout = DEFAULT_LOCK_ATTEMPT_TIMEOUT
	}
	if args.SessionTimeout == 0 {
		args.SessionTimeout = DEFAULT_SESSION_TIMEOUT
	}
	return &Sloto{
		lattIntv: args.LockAttemptInterval,
		lockTO:   args.LockAttemptTimeout,
		sessTO:   args.SessionTimeout,
		access:   sync.Mutex{},
		keyLocks: map[Key]Status{},
		sessions: map[SessionID][]Key{},
	}
}

// scheduleUnlock schedules a session to be unlocked after a timeout.
func (s *Sloto) scheduleUnlock(sid SessionID) {
	go func() {
		<-time.After(s.sessTO)
		s.Unlock(sid)
	}()
}

// tryLock attempts to create a new session and lock the given keys.
func (s *Sloto) tryLock(keys ...Key) (sid SessionID, failed *Key) {
	s.access.Lock()
	defer s.access.Unlock()

	for _, key := range keys {
		if s.keyLocks[key] == LOCKED {
			return "", &key
		}
	}

	sid = SessionID(uuid.New().String())
	s.sessions[sid] = keys
	for _, key := range keys {
		s.keyLocks[key] = LOCKED
	}
	s.scheduleUnlock(sid)
	return sid, nil
}

// Lock creates a new session and locks the given keys.
func (s *Sloto) Lock(keys ...Key) (SessionID, error) {
	start := time.Now()
	for {
		sid, failed := s.tryLock(keys...)
		if failed == nil {
			return sid, nil
		}

		if time.Since(start) > s.lockTO {
			return "", fmt.Errorf("timed out locking key: %s", *failed)
		}

		jitter := float64(s.lattIntv) * rand.Float64() * JITTER_FRAC
		<-time.After(s.lattIntv + time.Duration(jitter))
	}
}

// Unlock unlocks the given keys and closes the session.
func (s *Sloto) Unlock(sid SessionID) {
	s.access.Lock()
	defer s.access.Unlock()

	keys, ok := s.sessions[sid]
	if !ok {
		return // already unlocked
	}

	for _, key := range keys {
		s.keyLocks[key] = UNLOCKED
	}
	delete(s.sessions, sid)
}

// Contains returns true if the given key is locked within the given session.
func (s *Sloto) Contains(sid SessionID, key Key) bool {
	keys, ok := s.sessions[sid]
	if !ok {
		return false
	}

	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}
