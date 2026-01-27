package samples

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/s3"
	"go.temporal.io/sdk/externalstorage"
)

func TestS3(t *testing.T) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	assert.NoError(t, err)

	client.Dial(client.Options{
		ExternalStorage: client.ExternalStorageOptions{
			Provider: externalstorage.NewSingleDriverProvider(
				s3.NewDriver(s3.DriverOptions{
					Config:     cfg,
					Namespace:  "my-temporal-namespace",
					BucketName: "my-s3-bucket",
				}),
			),
		},
	})

	return nil
}
