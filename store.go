package s3kv

import (
	"bytes"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv/cache"
)

// Store is a key-value store backed by an S3 bucket.
// Its operations are atomic and require you to provide the expected state of the blob as an ETag before you mutate it.
// If the state you provide is out of date, you must fetch the value and ETag again before retrying.
//
// Get returns the value and ETag for the given key.
//
// Set sets the value for the given key, ensuring the expected ETag matches the actual ETag.
// If you are setting a value for a new key, use the `S3kv.ObjectMissing` sigil for your expected ETag.
//
// Del deletes the value for the given key, ensuring the expected ETag matches the actual ETag.
type Store interface {
	Get(key string) ([]byte, cache.Taggable, error)
	Set(key string, value []byte, expectedETag cache.Taggable) (cache.Taggable, error)
	Del(key string, expectedETag cache.Taggable) error
}

// store is the implementation of Store.
type store struct {
	s3     *s3.S3
	bucket string
	cache  *cache.Locked
}

// New creates a new key-value store backed by an S3 bucket.
func New(bucket string) Store {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	return store{svc, bucket, cache.New()}
}

func (s store) Get(key string) ([]byte, cache.Taggable, error) {
	resp, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if notFound(err) {
		return nil, ObjectMissing, nil
	}
	if err != nil {
		return nil, nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	et := cache.Taggable(newTag(resp.ETag))

	r := s.cache.Acquire(key)
	if !r.Success {
		return nil, nil, cacheLockErr("get", key)
	}
	defer r.Unlocked.Release()
	r.Unlocked.Set(et)

	return data, et, nil
}

func (s store) Set(key string, value []byte, xETag cache.Taggable) (cache.Taggable, error) {
	r := s.cache.Acquire(key)
	if !r.Success {
		return nil, cacheLockErr("set", key)
	}
	defer r.Unlocked.Release()

	err := s.check(r.Unlocked, key, xETag)
	if err != nil {
		return nil, err
	}

	resp, err := s.s3.PutObject(&s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(value)})
	if err != nil {
		return nil, err
	}
	et := cache.Taggable(newTag(resp.ETag))
	r.Unlocked.Set(et)
	return et, nil
}

func (s store) Del(key string, xETag cache.Taggable) error {
	r := s.cache.Acquire(key)
	if !r.Success {
		return cacheLockErr("delete", key)
	}
	defer r.Unlocked.Release()

	err := s.check(r.Unlocked, key, xETag)
	if err != nil {
		return err
	}

	_, err = s.s3.DeleteObject(&s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return err
	}
	r.Unlocked.Set(nil)
	return nil
}

// check ensures the expected tag for this key matches the actual tag for a key.
// Returns a StaleTagError if the tags do not match.
func (s store) check(uc cache.Unlocked, k string, xETag cache.Taggable) error {
	aETag, ok := uc.Get()
	if !ok {
		_, et, err := s.Get(k)
		if err != nil {
			return err
		}
		uc.Set(et)
		aETag = et
	}
	if !xETag.Equal(aETag) {
		return StaleTagError{k, xETag, aETag}
	}
	return nil
}
