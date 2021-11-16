package s3kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/mplewis/s3kv/multilock"
)

// list store keys with prefix
// get value for key
// open session, locking keys
// with session, set value for key
// close session

type Store struct {
	client    *s3.Client
	bucket    string
	namespace string
	sessions  map[SessionID][]Key
	context   context.Context
	locks     *multilock.MultiLock
}

// Args are the arguments for a new store.
type Args struct {
	Bucket    string          // Required. The name of the S3 bucket to use.
	Namespace string          // Required. A prefix to use for all keys in this store.
	Timeout   time.Duration   // Optional. The timeout for acquisition of all locks.
	Context   context.Context // Optional. The context to use for all operations. Defaults to context.Background().
	Client    *s3.Client      // Optional. The client to use for all operations. Defaults to s3.NewFromConfig(config.NewConfig()).
}

// New builds a new Store.
func New(args Args) (*Store, error) {
	if args.Bucket == "" {
		return nil, errors.New("bucket must be provided")
	}
	if args.Namespace == "" {
		return nil, errors.New("namespace must be provided")
	}
	if args.Timeout == 0 {
		args.Timeout = defaultTimeout
	}
	if args.Context == nil {
		args.Context = context.Background()
	}
	if args.Client == nil {
		cfg, err := config.LoadDefaultConfig(args.Context)
		if err != nil {
			return nil, err
		}
		args.Client = s3.NewFromConfig(cfg)
	}

	store := &Store{
		client:    args.Client,
		bucket:    args.Bucket,
		namespace: args.Namespace,
		sessions:  map[SessionID][]Key{},
		context:   args.Context,
		locks:     multilock.New(args.Timeout),
	}
	return store, nil
}

// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
func (s *Store) List(prefix string) ([]Key, error) {
	var keys []Key
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, err
		}
		for _, c := range output.Contents {
			keys = append(keys, Key(*c.Key))
		}
	}
	return keys, nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) ([]byte, error) {
	r, err := s.client.GetObject(s.context, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
	})
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r.Body)
}

// Set sets the value for the given key. You must have an open session for the key.
func (s *Store) Set(sid SessionID, key string, value []byte) error {
	if err := s.keyInSess(sid, key); err != nil {
		return err
	}
	_, err := s.client.PutObject(s.context, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
		Body:   bytes.NewReader(value),
	})
	return err
}

// Del deletes the key-value pair for the given key.
func (s *Store) Del(sid SessionID, key string, value []byte) error {
	if err := s.keyInSess(sid, key); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(s.context, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
	})
	return err
}

// OpenSession acquires the given keys for exclusive writing.
// TODO: auto-unlock at timeout
func (s *Store) OpenSession(keys ...string) (SessionID, error) {
	acquired := []Key{}
	for _, key := range keys {
		if !s.locks.Acquire(string(key)) {
			s.unravel(acquired)
			return "", fmt.Errorf("could not acquire lock for key: %s", key)
		}
		acquired = append(acquired, Key(key))
	}
	sid := SessionID(uuid.New().String())
	s.sessions[sid] = acquired
	return sid, nil
}

// CloseSession releases the exclusive write lock on the keys in the session.
func (s *Store) CloseSession(sid SessionID) {
	s.unravel(s.sessions[sid])
	delete(s.sessions, sid)
}

// ns appends the namespace prefix to the given key.
func (s *Store) ns(key string) string {
	return fmt.Sprintf("%s/%s", s.namespace, key)
}

// unravel ensures all the given keys are unlocked.
func (s *Store) unravel(keys []Key) {
	for _, key := range keys {
		for !s.locks.Release(string(key)) {
			// this must succeed
		}
	}
}

// keyInSess returns true if the session exists and includes the given key.
func (s *Store) keyInSess(sid SessionID, key string) error {
	keys, ok := s.sessions[sid]
	if !ok {
		return fmt.Errorf("session not found: %s", sid)
	}
	in := false
	for _, k := range keys {
		if k == key {
			in = true
			break
		}
	}
	if !in {
		return fmt.Errorf("session %s does not have key %s", sid, key)
	}
	return nil
}
