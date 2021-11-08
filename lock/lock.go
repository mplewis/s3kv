package lock

import (
	"time"

	golock "github.com/viney-shih/go-lock"
)

// Lock is a mutex lock that supports acquisition timeout.
type Lock struct {
	l *golock.ChanMutex
}

// New creates a new Lock.
func New() *Lock {
	return &Lock{golock.NewChanMutex()}
}

// Acquire returns True if the lock was acquired successfully and False if the timeout expired first.
func (l *Lock) Acquire(timeout time.Duration) bool {
	r := l.l.TryLockWithTimeout(timeout)
	return r
}

// Release releases the lock.
func (l *Lock) Release() {
	l.l.Unlock()
}
