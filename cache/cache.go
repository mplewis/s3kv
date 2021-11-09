package cache

import (
	"time"

	"github.com/mplewis/s3kv/multilock"
)

// timeout is the timeout for a cache mutation.
const timeout = 500 * time.Millisecond

// Taggable provides a unique identifier for a cache entry that changes when the corresponding value changes.
type Taggable interface {
	// Value returns the unique identifier, or nil if this represents the absence of a corresponding value.
	Value() *string
	// Equal returns true if the tags are equivalent and False otherwise.
	Equal(Taggable) bool
}

// Locked is a synchronized key-to-ETag map. Use Acquire to lock the cache for your exclusive use.
type Locked struct {
	// m is the protected key-to-ETag map.
	m *map[string]Taggable
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
	Set(v Taggable)
	Get() (v Taggable, present bool)
	Release()
}

// unlockedCacheImpl exposes the mutators of Locked.
type unlockedCacheImpl struct {
	c *Locked
	k string
}

// New instantiates a new Locked cache.
func New() *Locked {
	return &Locked{m: &map[string]Taggable{}, l: multilock.New()}
}

// Acquire attempts to lock the cache for access to a specific key and returns an AcquireResult.
func (c *Locked) Acquire(k string) AcquireResult {
	if !c.l.Acquire(timeout, k) {
		return AcquireResult{Success: false}
	}
	return AcquireResult{Success: true, Unlocked: &unlockedCacheImpl{c, k}}
}

func (uc *unlockedCacheImpl) Get() (v Taggable, present bool) {
	v, present = (*uc.c.m)[uc.k]
	return
}

func (uc *unlockedCacheImpl) Set(v Taggable) {
	(*uc.c.m)[uc.k] = v
}

func (uc *unlockedCacheImpl) Release() {
	uc.c.l.Release(timeout, uc.k)
	uc.c = nil
}
