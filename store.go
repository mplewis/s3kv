package s3kv

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mplewis/s3kv/multilock"
)

const timeout = 2000 * time.Millisecond

// Key is the key for a key-value pair in the store.
type Key = string
type Done = func()

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

type S3kvArgs struct {
	Bucket  string
	Session *session.Session
}

// New creates a new key-value store backed by an S3 bucket, with an optionally-specified custom AWS session.
//
// Using the custom session to specify an alternate S3 endpoint:
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
//    s := s3kv.New(s3kv.S3kvArgs{Bucket: bucket, Session: sess})
func New(args S3kvArgs) Store {
	if args.Session == nil {
		args.Session = session.Must(session.NewSession())
	}
	svc := s3.New(args.Session)
	return store{svc, args.Bucket, multilock.New()}
}

func (s store) Lock(keys ...Key) (map[Key]Object, Done, error) {
	m := map[Key]Object{}

	acquired := []string{}
	objs := []*object{}

	done := func() {
		// fmt.Printf("Done called, unwinding %d objs and %d acquireds\n", len(objs), len(acquired))
		for _, obj := range objs {
			// fmt.Printf("Marking %s as stale\n", obj.key)
			obj.stale = true
		}
		for _, key := range acquired {
			// fmt.Printf("Releasing lock on %s\n", key)
			if !s.locks.Release(timeout, key) {
				log.Panicf("timed out while cleaning up locks; could not release lock for key %s in %+v", key, timeout)
			}
		}
	}

	for _, key := range keys {
		ok := s.locks.Acquire(timeout, key)
		if !ok {
			done() // unwind the wip locks
			return nil, nil, fmt.Errorf("could not acquire cache lock for key %s", key)
		}
		acquired = append(acquired, key)
		o := object{stale: false, client: s.s3, bucket: s.bucket, key: key}
		objs = append(objs, &o)
		m[key] = o
	}

	// fmt.Printf("Done configuring %d objs and %d acquireds\n", len(objs), len(acquired))

	return m, done, nil
}
