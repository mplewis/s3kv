package s3kv

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	awsSession "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/mplewis/s3kv/multilock"
	golock "github.com/viney-shih/go-lock"
)

type SessionID string

type S3KV struct {
	client   *s3.S3
	bucket   string
	metalock *golock.CASMutex
	locks    *multilock.MultiLock
	sessions map[SessionID]*Session
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

func New(args Args) *S3KV {
	if args.Session == nil {
		args.Session = awsSession.Must(awsSession.NewSession())
	}
	if args.Timeout == time.Duration(0) {
		args.Timeout = defaultTimeout
	}
	return &S3KV{
		client:   s3.New(args.Session),
		bucket:   args.Bucket,
		metalock: golock.NewCASMutex(),
		locks:    multilock.New(args.Timeout),
		sessions: make(map[SessionID]*Session),
	}
}

func unravel(s *S3KV, sess *Session) {
	for key := range sess.objs {
		// every unlock must succeed
		success := false
		for !success {
			success = s.locks.Release(key)
		}
	}
}

func (s *S3KV) OpenSession(keys ...string) (SessionID, *Session, error) {
	sess := Session{objs: map[string]Object{}}
	for _, key := range keys {
		ok := s.locks.Acquire(key)
		if !ok {
			unravel(s, &sess)
			return "", nil, fmt.Errorf("failed to acquire lock for key %s", key)
		}
		sess.objs[key] = object{client: s.client, bucket: s.bucket, key: key}
	}

	id := SessionID(uuid.New().String())
	s.sessions[id] = &sess
	return id, &sess, nil
}

func (s *S3KV) CloseSession(id SessionID) error {
	sess, ok := s.sessions[id]
	if !ok {
		return fmt.Errorf("session not found: %s", id)
	}
	unravel(s, sess)
	delete(s.sessions, id)
	return nil
}

func (s *S3KV) List(prefix string) ([]string, error) {
	keys := []string{}
	err := s.client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
		return true
	})
	return keys, err
}

func (s *S3KV) Get(key string) (data []byte, found bool, err error) {
	return object{client: s.client, bucket: s.bucket, key: key}.Get()
}

type Session struct {
	objs map[string]Object
}

func (s *Session) Get(key string) (Object, bool) {
	a, b := s.objs[key]
	return a, b
}
