// Package s3kv implements a key-value store backed by an S3 bucket.
//
// Example usage:
//
//		store := s3kv.New(s3kv.Args{Bucket: "my-s3-bucket"})
//
//		// Lock keys so you can exclusively interact with their data
//		keys, done, err := store.Lock("key_one", "key_two")
//		if err != nil {
//			return err
//		}
//		// Don't forget to release the locks eventually or these keys will be locked forever!
//		defer done()
//
//		// Grab a key from the locked set
//		obj := keys["key_one"]
//
//		// Get a value
//		data, find, err := obj.Get()
//		if err != nil {
//			return err
//		}
//		if find == s3kv.NotFound {
//			return errors.New("couldn't read data from key_one")
//		}
//		fmt.Println(data)
//
//		// Set a value
//		err = obj.Set([]byte("your data goes here"))
//		if err != nil {
//			return err
//		}
//
//		// Delete a value
//		err = obj.Del()
//		if err != nil {
//			return err
//		}
//
//		return nil
package s3kv

import (
	"time"

	"github.com/mplewis/s3kv/sloto"
)

// defaultSlotoArgs is the configuration used for sloto if none is specified in New().
var defaultSlotoArgs = sloto.Args{
	LockAttemptInterval: 100 * time.Millisecond,
	LockTimeout:         5 * time.Second,
	SessionTimeout:      15 * time.Second,
}

// Key is the key for a key-value pair in the store.
type Key = string

// SessionID is the ID for an open session. Use this to close the session.
type SessionID string
