package multilock

import (
	"fmt"
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
		fmt.Println("Failed to lock the lockbox for acquiry of", key)
		return false
	}
	fmt.Println("Locked lockbox")

	keyLock, ok := m.locks[key]
	if !ok {
		fmt.Println("Lock did not exist, creating for", key)
		keyLock = golock.NewCASMutex()
		m.locks[key] = keyLock
	}
	m.l.Unlock()
	fmt.Println("Unlocked lockbox")

	ok = keyLock.TryLockWithTimeout(timeout)
	if !ok {
		fmt.Println("Failed to acquire lock for", key)
		return false
	}
	fmt.Println("Acquired", key)
	return true
}

// Release releases the lock for the given key, returning True on success and False on timeout.
func (m *MultiLock) Release(timeout time.Duration, key string) bool {
	ok := m.l.TryLockWithTimeout(timeout)
	if !ok {
		fmt.Println("Failed to lock the lockbox for release of", key)
		return false
	}
	fmt.Println("Locked lockbox")
	// defer m.l.Unlock()
	defer func() {
		m.l.Unlock()
		fmt.Println("Unlocked lockbox")
	}()

	keyLock, ok := m.locks[key]
	if !ok {
		fmt.Println("Releasing lock did not exist for", key)
		return true
	}
	keyLock.Unlock()
	fmt.Println("Released", key)
	return true
}
