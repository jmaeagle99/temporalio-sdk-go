package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/externalstorage"
	"google.golang.org/protobuf/proto"
)

type s3Driver struct {
	bucketName       string
	driverNameSuffix string
	downloader       *manager.Downloader
	uploader         *manager.Uploader
}

type DriverOptions struct {
	Config           aws.Config
	BucketName       string
	DriverNameSuffix string
}

func NewDriver(options DriverOptions) externalstorage.ExternalStorageDriver {
	nameSuffix := options.DriverNameSuffix
	if nameSuffix == "" {
		nameSuffix = "default"
	}

	client := awss3.NewFromConfig(options.Config)

	return &s3Driver{
		bucketName:       options.BucketName,
		driverNameSuffix: options.DriverNameSuffix,
		downloader:       manager.NewDownloader(client),
		uploader:         manager.NewUploader(client),
	}
}

func (d *s3Driver) GetName() string {
	return "temporal-aws-s3-" + d.driverNameSuffix
}

func (d *s3Driver) Store(ctx context.Context, payloads []*commonpb.Payload) ([]externalstorage.ExternalStorageClaim, error) {
	claims := make([]externalstorage.ExternalStorageClaim, len(payloads))
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

		claims[index] = externalstorage.ExternalStorageClaim{
			Data: map[string][]byte{
				"bucket": []byte(d.bucketName),
				"key":    []byte(key),
			},
		}
	}
	return claims, nil
}

func (d *s3Driver) Retrieve(ctx context.Context, claims []externalstorage.ExternalStorageClaim) ([]*commonpb.Payload, error) {
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
