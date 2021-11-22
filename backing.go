package s3kv

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Backing is an interface by which a Store accesses data in some backend datastore.
type Backing interface {
	// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
	List(prefix string) ([]Key, error)
	// Get returns the value for the given key.
	Get(key Key) ([]byte, error)
	// Set sets the value for the given key.
	Set(key Key, value []byte) error
	// Del deletes the key-value pair for the given key.
	Del(key Key) error
}

// S3Backing stores data in AWS S3.
type S3Backing struct {
	bucket    string
	namespace string
	client    *s3.Client
	context   context.Context
}

// S3BackingArgs are the arguments for creating a new S3 backing.
type S3BackingArgs struct {
	Bucket    string          // Required. The name of the S3 bucket to use.
	Namespace string          // Required. The namespace prefixed to all keys when stored in S3.
	Client    *s3.Client      // Optional. The S3 client to use. If not provided, a client will be automatically configured from your environment.
	Context   context.Context // Optional. The context to use for S3 operations. If not provided, defaults to context.Background().
}

// NewS3Backing creates a new backing which stores data in AWS S3.
func NewS3Backing(args S3BackingArgs) (Backing, error) {
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
	return &S3Backing{
		client:    args.Client,
		context:   args.Context,
		bucket:    args.Bucket,
		namespace: args.Namespace,
	}, nil
}

// ns appends the namespace prefix to the given key.
func (s *S3Backing) ns(key Key) Key {
	return fmt.Sprintf("%s/%s", s.namespace, key)
}

// List lists all keys in the store with the given prefix. This is likely a very slow operation, so use with caution.
func (s *S3Backing) List(prefix string) ([]Key, error) {
	var keys []Key
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(s.context)
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
func (s *S3Backing) Get(key Key) ([]byte, error) {
	r, err := s.client.GetObject(s.context, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
	})
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r.Body)
}

// Set sets the value for the given key.
func (s *S3Backing) Set(key Key, value []byte) error {
	_, err := s.client.PutObject(s.context, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
		Body:   bytes.NewReader(value),
	})
	return err
}

// Del deletes the key-value pair for the given key.
func (s *S3Backing) Del(key Key) error {
	_, err := s.client.DeleteObject(s.context, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ns(key)),
	})
	return err
}
