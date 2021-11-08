package lock

import "time"

type Lock struct {
	ch chan struct{}
}

func New() *Lock {
	return &Lock{ch: make(chan struct{}, 1)}
}

func (l *Lock) Acquire(timeout time.Duration) bool {
	select {
	case l.ch <- struct{}{}:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (l *Lock) Release() {
	<-l.ch
}
