// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

type S3ClientConfig struct {
	BucketName string
	Endpoint   string
	AccessKey  string
	SecretKey  string
	Secure     bool
}

func (c *S3ClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.BucketName, prefix+"bucket-name", "", "The name of the bucket (not prefixed by a scheme).")
	f.StringVar(&c.Endpoint, prefix+"endpoint", "", "The endpoint to contact when accessing the bucket.")
	f.StringVar(&c.AccessKey, prefix+"access-key", "", "The access key used in AWS Signature Version 4 authentication.")
	f.StringVar(&c.SecretKey, prefix+"secret-key", "", "The secret key used in AWS Signature Version 4 authentication.")
	f.BoolVar(&c.Secure, prefix+"secure", true, "If true (default), use HTTPS when connecting to the Bucket. If false, insecure HTTP is used.")
}

func (c *S3ClientConfig) Validate(prefix string) error {
	if c.BucketName == "" {
		return errors.New(prefix + "bucket name is missing")
	}
	if c.Endpoint == "" {
		return errors.New(prefix + "endpoint is missing")
	}
	if c.AccessKey == "" {
		return errors.New(prefix + "access-key is missing")
	}
	if c.SecretKey == "" {
		return errors.New(prefix + "secret-key is missing")
	}
	return nil
}

func (c *S3ClientConfig) ToBucket() (Bucket, error) {
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: c.Secure,
	})
	if err != nil {
		return nil, err
	}
	return &s3Bucket{
		Client:     client,
		bucketName: c.BucketName,
	}, nil
}

type s3Bucket struct {
	*minio.Client
	bucketName string
}

func (bkt *s3Bucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	obj, err := bkt.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (bkt *s3Bucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	d, ok := dstBucket.(*s3Bucket)
	if !ok {
		return errors.New("destination Bucket wasn't an S3 Bucket")
	}
	_, err := d.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: d.bucketName,
			Object: objectName,
		},
		minio.CopySrcOptions{
			Bucket: bkt.bucketName,
			Object: objectName,
		},
	)
	return err
}

func (bkt *s3Bucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	obj, err := bkt.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get source object from S3")
	}
	objInfo, err := obj.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to get source object information from S3")
	}
	if err := dstBucket.Upload(ctx, objectName, obj, objInfo.Size); err != nil {
		_ = obj.Close()
		return errors.Wrap(err, "failed to upload source object from S3 to destination")
	}
	return errors.Wrap(obj.Close(), "failed to close source object reader from S3")
}

func (bkt *s3Bucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, Delim) {
		prefix = prefix + Delim
	}
	options := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
	}
	result := make([]string, 0, 10)
	objects := bkt.ListObjects(ctx, bkt.bucketName, options)
	for obj := range objects {
		if obj.Err != nil {
			return nil, obj.Err
		}
		key := obj.Key
		if strings.HasPrefix(key, prefix) {
			key = strings.TrimPrefix(key, prefix)
		} else {
			return nil, errors.Errorf("listPrefix: path has invalid prefix: %v, expected prefix: %v", key, prefix)
		}
		result = append(result, key)
	}
	return result, ctx.Err()
}

func (bkt *s3Bucket) Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error {
	_, err := bkt.PutObject(ctx, bkt.bucketName, objectName, reader, contentLength, minio.PutObjectOptions{})
	return err
}

func (bkt *s3Bucket) Name() string {
	return bkt.bucketName
}
