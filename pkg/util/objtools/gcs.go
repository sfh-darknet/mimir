// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type GCSClientConfig struct {
	BucketName string
}

func (c *GCSClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.BucketName, prefix+"bucket-name", "", "The name of the GCS bucket (not prefixed by a scheme).")
}

func (c *GCSClientConfig) Validate(prefix string) error {
	if c.BucketName == "" {
		return fmt.Errorf("the GCS bucket name provided in (%s) is required", prefix+"bucket-name")
	}
	return nil
}

func (c *GCSClientConfig) ToBucket(ctx context.Context) (Bucket, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create GCS storage client")
	}
	return &gcsBucket{
		BucketHandle: *client.Bucket(c.BucketName),
		name:         c.BucketName,
	}, nil
}

type gcsBucket struct {
	storage.BucketHandle
	name string
}

func (bkt *gcsBucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	obj := bkt.Object(objectName)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (bkt *gcsBucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	d, ok := dstBucket.(*gcsBucket)
	if !ok {
		return errors.New("destination Bucket wasn't a GCS Bucket")
	}
	srcObj := bkt.Object(objectName)
	dstObject := d.BucketHandle.Object(objectName)
	copier := dstObject.CopierFrom(srcObj)
	_, err := copier.Run(ctx)
	return err
}

func (bkt *gcsBucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error {
	srcObj := bkt.Object(objectName)
	reader, err := srcObj.NewReader(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get GCS source object reader")
	}
	if err := dstBucket.Upload(ctx, objectName, reader, reader.Attrs.Size); err != nil {
		_ = reader.Close()
		return errors.Wrap(err, "failed to upload GCS source object to destination")
	}
	return errors.Wrap(reader.Close(), "failed closing GCS source object reader")
}

func (bkt *gcsBucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if len(prefix) > 0 && prefix[len(prefix)-1:] != Delim {
		prefix = prefix + Delim
	}

	q := &storage.Query{
		Prefix: prefix,
	}
	if !recursive {
		q.Delimiter = Delim
	}

	var result []string

	it := bkt.Objects(ctx, q)
	for {
		obj, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrapf(err, "listPrefix: error listing %v", prefix)
		}

		path := ""
		if obj.Prefix != "" { // synthetic directory, only returned when recursive=false
			path = obj.Prefix
		} else {
			path = obj.Name
		}

		if strings.HasPrefix(path, prefix) {
			path = strings.TrimPrefix(path, prefix)
		} else {
			return nil, errors.Errorf("listPrefix: path has invalid prefix: %v, expected prefix: %v", path, prefix)
		}

		result = append(result, path)
	}

	return result, nil
}

func (bkt *gcsBucket) Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	obj := bkt.Object(objectName)
	w := obj.NewWriter(ctx)
	n, err := io.Copy(w, reader)
	if err != nil {
		return errors.Wrap(err, "failed during copy stage of GCS upload")
	}
	if n != contentLength {
		return errors.Wrapf(err, "unexpected content length from copy: expected=%d, actual=%d", contentLength, n)
	}
	return w.Close()
}

func (bkt *gcsBucket) Name() string {
	return bkt.name
}
