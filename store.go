package s3kv

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv/multilock"
)

// Store is a key-value store backed by an S3 bucket.
type Store interface {
	Lock(keys ...Key) (map[Key]Object, Done, error)
}

// store is the implementation of Store.
type store struct {
	s3     *s3.S3
	bucket string
	locks  *multilock.MultiLock
}

// Args is the set of arguments used to configure a new Store.
//
// Bucket (mandatory) names the S3 bucket to use as a key-value store.
//
// Session (optional) is an AWS session to use for the S3 client. If not specified, a default session will be created.
//
// Example usage of the custom session to specify an alternate S3 endpoint:
//
// 		client := s3.New(session.Must(session.NewSessionWithOptions(options)))
// 		options := session.Options{
// 			Profile: "localhost",
// 			Config: aws.Config{
// 				Region:                        aws.String("us-east-1"),
// 				Endpoint:                      aws.String("http://my-custom-s3-domain:9999"),
// 				Credentials:                   credentials.NewStaticCredentials("<access-key>", "<secret-key>", ""),
// 				CredentialsChainVerboseErrors: aws.Bool(true),
// 				S3ForcePathStyle:              aws.Bool(true),
// 			},
// 		}
// 		sess := session.Must(session.NewSessionWithOptions(options))}
//		store := s3kv.New(s3kv.Args{Bucket: bucket, Session: sess})
//
// Timeout (optional) is the lock timeout used when acquiring locks. Defaults to 15 seconds.
type Args struct {
	Bucket  string
	Session *session.Session
	Timeout time.Duration
}

// New creates a new key-value store backed by an S3 bucket.
func New(args Args) Store {
	if args.Session == nil {
		args.Session = session.Must(session.NewSession())
	}
	if args.Timeout == time.Duration(0) {
		args.Timeout = defaultTimeout
	}
	svc := s3.New(args.Session)
	return store{svc, args.Bucket, multilock.New(args.Timeout)}
}

// Lock locks the specified keys to guarantee exclusive access to each.
func (s store) Lock(keys ...Key) (map[Key]Object, Done, error) {
	m := map[Key]Object{}

	acquired := []string{}
	objs := []*object{}

	done := func() {
		for _, obj := range objs {
			obj.stale = true
		}
		for _, key := range acquired {
			// this action MUST succeed, or the locks will be left in an inconsistent state
			for {
				if s.locks.Release(key) {
					break
				}
			}
		}
	}

	for _, key := range keys {
		ok := s.locks.Acquire(key)
		if !ok {
			done() // unwind the wip locks
			return nil, nil, fmt.Errorf("could not acquire cache lock for key %s", key)
		}
		acquired = append(acquired, key)
		o := object{stale: false, client: s.s3, bucket: s.bucket, key: key}
		objs = append(objs, &o)
		m[key] = o
	}

	return m, done, nil
}
