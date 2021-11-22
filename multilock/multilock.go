// Package multilock implements a system for locking of individual string-keyed resources.
package multilock

import (
	"time"

	golock "github.com/viney-shih/go-lock"
)

// MultiLock is a system for locking of individual string-keyed resources.
type MultiLock struct {
	l       *golock.CASMutex
	locks   map[string]*golock.CASMutex
	timeout time.Duration
}

// New instantiates a new MultiLock using the specified timeout for all operations.
func New(timeout time.Duration) *MultiLock {
	return &MultiLock{l: golock.NewCASMutex(), locks: map[string]*golock.CASMutex{}, timeout: timeout}
}

// Acquire acquires the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Acquire(key string) bool {
	ok := m.l.TryLockWithTimeout(m.timeout)
	if !ok {
		return false
	}

	keyLock, ok := m.locks[key]
	if !ok {
		keyLock = golock.NewCASMutex()
		m.locks[key] = keyLock
	}
	m.l.Unlock()

	ok = keyLock.TryLockWithTimeout(m.timeout)
	return ok
}

// MustAcquire acquires the lock for the given key, waiting until it is available.
func (m *MultiLock) MustAcquire(key string) {
	m.l.Lock()
	keyLock, ok := m.locks[key]
	if !ok {
		keyLock = golock.NewCASMutex()
		m.locks[key] = keyLock
	}
	m.l.Unlock()
	keyLock.Lock()
}

// Release releases the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Release(key string) bool {
	ok := m.l.TryLockWithTimeout(m.timeout)
	if !ok {
		return false
	}

	keyLock, ok := m.locks[key]
	if !ok {
		m.l.Unlock()
		return true
	}

	m.l.Unlock()
	keyLock.Unlock()
	return true
}

// MustRelease releases the lock for the given key, waiting until it is available.
func (m *MultiLock) MustRelease(key string) {
	m.l.Lock()

	keyLock, ok := m.locks[key]
	if !ok {
		m.l.Unlock()
		return
	}

	m.l.Unlock()
	keyLock.Unlock()
}
