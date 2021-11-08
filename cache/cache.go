package cache

import (
	"time"

	"github.com/mplewis/s3kv/etag"
	"github.com/mplewis/s3kv/multilock"
)

// TIMEOUT is the timeout for a cache mutation.
const TIMEOUT = 100 * time.Millisecond

// Locked is a synchronized key-to-ETag map. Use Acquire to lock the cache for your exclusive use.
type Locked struct {
	// m is the protected key-to-ETag map.
	m *map[string]etag.ETag
	// l protects against concurrent access to a single key.
	l *multilock.MultiLock
}

// AcquireResult provides an unlocked cache instance if the lock was successfully acquired.
type AcquireResult struct {
	Success  bool
	Unlocked Unlocked
}

// Unlocked is an instance of the cache that is exclusively locked and supports mutation.
type Unlocked interface {
	Set(v etag.ETag)
	Get() (v etag.ETag, present bool)
	Release()
}

// unlockedCacheImpl exposes the mutators of Locked.
type unlockedCacheImpl struct {
	c *Locked
	k string
}

// New instantiates a new Locked cache.
func New() *Locked {
	return &Locked{m: &map[string]etag.ETag{}, l: multilock.New()}
}

// Acquire attempts to lock the cache for access to a specific key and returns an AcquireResult.
func (c *Locked) Acquire(k string) AcquireResult {
	if !c.l.Acquire(TIMEOUT, k) {
		return AcquireResult{Success: false}
	}
	return AcquireResult{Success: true, Unlocked: &unlockedCacheImpl{c, k}}
}

func (uc *unlockedCacheImpl) Get() (v etag.ETag, present bool) {
	v, present = (*uc.c.m)[uc.k]
	return
}

func (uc *unlockedCacheImpl) Set(v etag.ETag) {
	(*uc.c.m)[uc.k] = v
}

func (uc *unlockedCacheImpl) Release() {
	uc.c.l.Release(TIMEOUT, uc.k)
	uc.c = nil
}
