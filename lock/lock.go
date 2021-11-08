package lock

import "time"

// Lock is a mutex lock that supports acquisition timeout.
type Lock struct {
	ch chan struct{}
}

// New creates a new Lock.
func New() *Lock {
	return &Lock{ch: make(chan struct{}, 1)}
}

// Acquire returns True if the lock was acquired successfully and False if the timeout expired first.
func (l *Lock) Acquire(timeout time.Duration) bool {
	select {
	case l.ch <- struct{}{}:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Release releases the lock.
func (l *Lock) Release() {
	<-l.ch
}
