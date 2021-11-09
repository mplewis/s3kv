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

import "time"

// defaultTimeout is the lock timeout used if none is specified in New().
const defaultTimeout = 15 * time.Second

// Key is the key for a key-value pair in the store.
type Key = string

// Done is the finalizer for a store.Lock() operation.
// It must be called when the client is done working with the locked data to release it for other clients.
// The easiest way to do this is to call `defer done()` immediately after checking for errors.
type Done = func()
