// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
)

const (
	serviceGCS = "gcs" // Google Cloud Storage
	serviceABS = "abs" // Azure Blob Storage
	serviceS3  = "s3"  // Amazon Simple Storage Service
	Delim      = "/"   // Used by Mimir to delimit tenants and blocks, and objects within blocks.
)

// Bucket is an object storage interface intended to be used by tools that require functionality that isn't in objstore
type Bucket interface {
	Get(ctx context.Context, objectName string) (io.ReadCloser, error)
	ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error
	ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket) error
	ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error)
	Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error
	Name() string
}

type BucketConfig struct {
	service string
	azure   AzureClientConfig
	gcs     GCSClientConfig
	s3      S3ClientConfig
}

func (c *BucketConfig) RegisterFlags(f *flag.FlagSet) {
	c.registerFlags("", f)
}

func ifNotEmptySuffix(s, suffix string) string {
	if s == "" {
		return ""
	}
	return s + suffix
}

func (c *BucketConfig) registerFlags(descriptor string, f *flag.FlagSet) {
	descriptorFlagPrefix := ifNotEmptySuffix(descriptor, "-")
	acceptedServices := fmt.Sprintf("%s, %s or %s.", serviceABS, serviceGCS, serviceS3)
	f.StringVar(&c.service, descriptorFlagPrefix+"service", "",
		fmt.Sprintf("The %sobject storage service. Accepted values are: %s", ifNotEmptySuffix(descriptor, " "), acceptedServices))
	c.azure.RegisterFlags("azure-"+descriptorFlagPrefix, f)
	c.gcs.RegisterFlags("gcs-"+descriptorFlagPrefix, f)
	c.s3.RegisterFlags("s3-"+descriptorFlagPrefix, f)
}

func (c *BucketConfig) Validate() error {
	return c.validate("")
}

func (c *BucketConfig) validate(descriptor string) error {
	descriptorFlagPrefix := ifNotEmptySuffix(descriptor, "-")
	if c.service == "" {
		return fmt.Errorf("--" + descriptorFlagPrefix + "service is missing")
	}
	switch c.service {
	case serviceABS:
		return c.azure.Validate("azure-" + descriptorFlagPrefix)
	case serviceGCS:
		return c.gcs.Validate("gcs-" + descriptorFlagPrefix)
	case serviceS3:
		return c.s3.Validate("s3-" + descriptorFlagPrefix)
	default:
		return fmt.Errorf("unknown service provided in --" + descriptorFlagPrefix + "service")
	}
}

func (c *BucketConfig) ToBucket(ctx context.Context) (Bucket, error) {
	switch c.service {
	case serviceABS:
		return c.azure.ToBucket()
	case serviceGCS:
		return c.gcs.ToBucket(ctx)
	case serviceS3:
		return c.s3.ToBucket()
	default:
		return nil, fmt.Errorf("unknown service: %v", c.service)
	}
}

type CopyBucketConfig struct {
	clientSideCopy bool
	source         BucketConfig
	destination    BucketConfig
}

func (c *CopyBucketConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.clientSideCopy, "client-side-copy", false, "Use client side copying. This option is only respected if copying between two buckets of the same service. Client side copying is always used when copying between different services.")
	c.source.registerFlags("source", f)
	c.destination.registerFlags("destination", f)
}

func (c *CopyBucketConfig) Validate() error {
	err := c.source.validate("source")
	if err != nil {
		return err
	}
	return c.destination.validate("destination")
}

func (c *CopyBucketConfig) ToBuckets(ctx context.Context) (source Bucket, destination Bucket, copyFunc CopyFunc, err error) {
	source, err = c.source.ToBucket(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	destination, err = c.destination.ToBucket(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return source, destination, c.toCopyFunc(source, destination), nil
}

// CopyFunc copies from the source to the destination either client-side or server-side depending on the configuration
type CopyFunc func(context.Context, string) error

func (c *CopyBucketConfig) toCopyFunc(source Bucket, destination Bucket) CopyFunc {
	if c.clientSideCopy || c.source.service != c.destination.service {
		return func(ctx context.Context, objectName string) error {
			return source.ClientSideCopy(ctx, objectName, destination)
		}
	} else {
		return func(ctx context.Context, objectName string) error {
			return source.ServerSideCopy(ctx, objectName, destination)
		}
	}
}
