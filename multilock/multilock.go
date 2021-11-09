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
	// fmt.Printf("Acquiring lock for key %s\n", key)
	// fmt.Println("Acquiring lockbox")
	ok := m.l.TryLockWithTimeout(timeout)
	if !ok {
		// fmt.Println("Failed to acquire lockbox")
		return false
	}
	// fmt.Println("Acquired lockbox")

	keyLock, ok := m.locks[key]
	if !ok {
		keyLock = golock.NewCASMutex()
		m.locks[key] = keyLock
	}
	m.l.Unlock()
	// fmt.Println("Unlocked lockbox")

	ok = keyLock.TryLockWithTimeout(timeout)
	// fmt.Printf("Lock acquisition for key %s: %t\n", key, ok)
	return ok
}

// Release releases the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Release(timeout time.Duration, key string) bool {
	// fmt.Printf("Releasing lock for key %s\n", key)
	// fmt.Println("Acquiring lockbox")
	ok := m.l.TryLockWithTimeout(timeout)
	if !ok {
		// fmt.Println("Failed to acquire lockbox")
		return false
	}
	// fmt.Println("Acquired lockbox")
	defer m.l.Unlock()

	keyLock, ok := m.locks[key]
	if !ok {
		// fmt.Printf("No lock found for key %s\n", key)
		return true
	}
	keyLock.Unlock()
	// fmt.Println("Unlocked lockbox")
	// fmt.Printf("Released lock for key %s\n", key)
	return true
}
