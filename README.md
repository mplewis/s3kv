# s3kv

Use an S3-compatible store as an atomic key-value store.

# Testing

With s3proxy as a local S3 provider:

```
docker run -p 9999:80 -e S3PROXY_AUTHORIZATION=none -it andrewgaul/s3proxy
go test
```

Against live S3:

```
TEST_WITH_LIVE_S3=1 go test
```
