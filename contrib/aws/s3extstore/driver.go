package s3extstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/extstore"
	"google.golang.org/protobuf/proto"
)

type s3Driver struct {
	bucketName string
	driverName string
	downloader *manager.Downloader
	namespace  string
	uploader   *manager.Uploader
}

// Options for constructing an S3 storage driver
type DriverOptions struct {
	// AWS configuration
	Config aws.Config
	// Required: The name of the bucket into which payloads are stored.
	BucketName string
	// Optional: The name of driver instance. Default is "temporal-aws-s3"
	DriverName string
	// Required: The name of the Temporal namespace.
	Namespace string
}

func NewDriver(options DriverOptions) (extstore.ExternalStorageDriver, error) {
	if len(options.Namespace) == 0 {
		return nil, errors.New("namespace cannot be empty")
	}
	if len(options.BucketName) == 0 {
		return nil, errors.New("bucketName cannot be empty")
	}

	client := awss3.NewFromConfig(options.Config)

	driverName := "temporal-aws-s3"
	if options.DriverName != "" {
		driverName = options.DriverName
	}

	return &s3Driver{
		bucketName: options.BucketName,
		downloader: manager.NewDownloader(client),
		driverName: driverName,
		namespace:  options.Namespace,
		uploader:   manager.NewUploader(client),
	}, nil
}

func (d *s3Driver) Name() string {
	return d.driverName
}

func (d *s3Driver) Type() string {
	return "temporal-aws-s3"
}

func (d *s3Driver) Store(ctx extstore.ExternalStorageDriverContext, payloads []*commonpb.Payload) ([]extstore.ExternalStorageClaim, error) {
	claims := make([]extstore.ExternalStorageClaim, len(payloads))
	for index, payload := range payloads {
		data, err := proto.Marshal(payload)
		if err != nil {
			return nil, err
		}

		sha := sha256.New()
		sha.Write(data)
		checksumHex := hex.EncodeToString(sha.Sum(nil))
		checksumKey := "sha256:" + checksumHex
		key := d.createKey("default", checksumKey)

		_, err = d.uploader.Upload(ctx, &awss3.PutObjectInput{
			Bucket: &d.bucketName,
			Key:    &key,
			Body:   bytes.NewReader(data),
		})

		claims[index] = extstore.ExternalStorageClaim{
			Data: map[string][]byte{
				"bucket": []byte(d.bucketName),
				"key":    []byte(key),
			},
		}
	}
	return claims, nil
}

func (d *s3Driver) Retrieve(ctx extstore.ExternalStorageDriverContext, claims []extstore.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		bucket, ok := claim.Data["bucket"]
		if !ok || len(bucket) == 0 {
			return nil, fmt.Errorf("Bucket does not exist")
		}
		bucketString := string(bucket)
		key, ok := claim.Data["key"]
		if !ok || len(key) == 0 {
			return nil, fmt.Errorf("Key does not exist")
		}
		keyString := string(key)

		buffer := manager.NewWriteAtBuffer([]byte{})
		_, err := d.downloader.Download(ctx, buffer, &awss3.GetObjectInput{
			Bucket: &bucketString,
			Key:    &keyString,
		})
		if err != nil {
			return nil, err
		}

		err = proto.Unmarshal(buffer.Bytes(), payloads[index])
	}
	return payloads, nil
}

func (d *s3Driver) createKey(namespace string, digest string) string {
	return fmt.Sprintf("/ns/%s/d/%s", namespace, digest)
}
