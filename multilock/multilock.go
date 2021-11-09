package multilock

import (
	"time"

	golock "github.com/viney-shih/go-lock"
)

// MultiLock is a system for locking of individual string-keyed resources.
type MultiLock struct {
	l     *golock.CASMutex
	locks map[string]*golock.CASMutex
}

// New instantiates a new MultiLock.
func New() *MultiLock {
	return &MultiLock{l: golock.NewCASMutex(), locks: map[string]*golock.CASMutex{}}
}

// Acquire acquires the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Acquire(timeout time.Duration, key string) bool {
	ok := m.l.TryLockWithTimeout(timeout)
	if !ok {
		return false
	}

	keyLock, ok := m.locks[key]
	if !ok {
		keyLock = golock.NewCASMutex()
		m.locks[key] = keyLock
	}
	m.l.Unlock()

	return keyLock.TryLockWithTimeout(timeout)
}

// Release releases the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Release(timeout time.Duration, key string) bool {
	ok := m.l.TryLockWithTimeout(timeout)
	if !ok {
		return false
	}
	defer m.l.Unlock()

	keyLock, ok := m.locks[key]
	if !ok {
		return true
	}
	keyLock.Unlock()
	return true
}
