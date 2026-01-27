package samples

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/s3extstore"
	"go.temporal.io/sdk/extstore"
)

func TestS3(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	driver, err := s3extstore.NewDriver(cfg, "my-temporal-namespace", "my-s3-bucket")
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: extstore.NewWithThreshold(driver, extstore.DefaultThreshold),
	})

	return nil
}

func TestS3Threshold(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	driver, err := s3extstore.NewDriver(cfg, "my-temporal-namespace", "my-s3-bucket")
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: extstore.NewWithThreshold(driver, 10*1024),
	})

	return nil
}

func TestMultiThreshold(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	mediumDriver, err := s3extstore.NewDriver(cfg, "my-temporal-namespace", "my-s3-large-bucket", s3extstore.WithDriverName("s3-medium"))
	assert.NoError(t, err)

	largeDriver, err := s3extstore.NewDriver(cfg, "my-temporal-namespace", "my-s3-medium-bucket", s3extstore.WithDriverName("s3-large"))
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: extstore.ExternalStorageOptions{
			Drivers: []extstore.ExternalStorageDriver{mediumDriver, largeDriver},
			Selector: func(ctx extstore.DriverSelectorContext, payload *commonpb.Payload) (string, error) {
				payloadSize := payload.Size()
				if payloadSize > 512*1024 {
					return largeDriver.Name(), nil
				} else if payloadSize > 10*1024 {
					return mediumDriver.Name(), nil
				} else {
					return "", nil
				}
			},
		},
	})

	return nil
}
