package samples

import (
	"context"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/s3extstore"
	"go.temporal.io/sdk/extstore"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/internal/extstore/extstoreredis"
)

func TestS3(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	driver, err := s3extstore.NewDriver(s3extstore.DriverOptions{
		Config:     cfg,
		BucketName: "my-bucket",
		Namespace:  "my-temporal-namespace",
	})
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: extstore.SingleDriverWithThreshold(driver, extstore.DefaultThreshold),
	})

	return nil
}

func TestS3Threshold(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	driver, err := s3extstore.NewDriver(s3extstore.DriverOptions{
		Config:     cfg,
		BucketName: "my-bucket",
		Namespace:  "my-temporal-namespace",
	})
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: extstore.SingleDriverWithThreshold(driver, 10*1024),
	})

	return nil
}

func TestMultiThreshold(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	mediumDriver, err := s3extstore.NewDriver(s3extstore.DriverOptions{
		Config:     cfg,
		BucketName: "my-medium-bucket",
		Namespace:  "my-temporal-namespace",
		DriverName: "s3-medium",
	})
	assert.NoError(t, err)

	largeDriver, err := s3extstore.NewDriver(s3extstore.DriverOptions{
		Config:     cfg,
		BucketName: "my-large-bucket",
		Namespace:  "my-temporal-namespace",
		DriverName: "s3-large",
	})
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: &extstore.ExternalStorageOptions{
			Drivers: []extstore.ExternalStorageDriver{mediumDriver, largeDriver},
			Selector: func(ctx extstore.ExternalStorageDriverContext, payload *commonpb.Payload) (string, error) {
				payloadSize := payload.Size()
				if payloadSize > 512*1024 {
					return largeDriver.Name(), nil
				} else if payloadSize > extstore.DefaultThreshold {
					return mediumDriver.Name(), nil
				} else {
					return "", nil
				}
			},
		},
	})

	return nil
}

func TestMetadataChoice(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	s3Driver, err := s3extstore.NewDriver(s3extstore.DriverOptions{
		Config:     cfg,
		BucketName: "my-bucket",
		Namespace:  "my-temporal-namespace",
	})
	assert.NoError(t, err)

	redisDriver, err := extstoreredis.NewDriver(extstoreredis.DriverOptions{})
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: &extstore.ExternalStorageOptions{
			Drivers: []extstore.ExternalStorageDriver{s3Driver, redisDriver},
			Selector: func(ctx extstore.ExternalStorageDriverContext, payload *commonpb.Payload) (string, error) {
				payloadSize := payload.Size()
				if payloadSize < extstore.DefaultThreshold {
					return "", nil
				}
				var workflowName string
				if ctx.ActivityInfo != nil {
					workflowName = ctx.ActivityInfo.(*internal.ActivityInfo).WorkflowType.Name
				} else if ctx.WorkflowInfo != nil {
					workflowName = ctx.WorkflowInfo.(*internal.WorkflowInfo).WorkflowType.Name
				}
				if strings.HasSuffix(workflowName, "-fast") {
					return redisDriver.Name(), nil
				}
				return s3Driver.Name(), nil
			},
		},
	})

	return nil
}
