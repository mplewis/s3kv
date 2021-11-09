package s3kv_test

import (
	"testing"
	"time"

	"github.com/mplewis/s3kv/multilock"
)

const (
	key   = "somekey"
	t50ms = 50 * time.Millisecond
)

func BenchmarkMultilock(b *testing.B) {
	l := multilock.New()
	for i := 0; i < b.N; i++ {
		l.Acquire(t50ms, key)
		l.Release(t50ms, key)
	}
}
