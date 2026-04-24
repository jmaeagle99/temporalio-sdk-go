package demos

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/contrib/aws/s3driver"
	"go.temporal.io/sdk/contrib/aws/s3driver/awssdkv2"
	"go.temporal.io/sdk/converter"
)

func CreateDriver() converter.StorageDriver {
	bucket := GetEnv("S3_BUCKET", "justin-s3-bug-bash")
	awsRegion := GetEnv("AWS_REGION", "us-east-1")
	awsProfile := os.Getenv("AWS_PROFILE")

	cfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(awsRegion),
	}
	if awsProfile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(awsProfile))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), cfgOpts...)
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	driver, err := s3driver.NewDriver(s3driver.Options{
		Client: awssdkv2.NewClient(s3.NewFromConfig(cfg)),
		Bucket: s3driver.StaticBucket(bucket),
	})
	if err != nil {
		log.Fatalf("create S3 driver: %v", err)
	}
	return driver
}
