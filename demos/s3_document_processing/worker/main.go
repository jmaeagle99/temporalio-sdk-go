// Command worker runs the document processing workflow and activities.
//
// It connects to Temporal with S3 external storage enabled. Large payloads
// produced by activities (10 MB each) are automatically offloaded to S3 by the
// storage driver instead of being stored inline in workflow history.
//
// Prerequisites:
//   - A running Temporal server (default: localhost:7233)
//   - AWS credentials configured (env vars, ~/.aws/credentials, or IAM role)
//   - An existing S3 bucket
//
// Usage:
//
//	S3_BUCKET=my-bucket go run ./worker
package main

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	docprocessing "go.temporal.io/sdk/demos/s3_document_processing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/s3driver"
	"go.temporal.io/sdk/contrib/aws/s3driver/awssdkv2"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
)

const taskQueue = "s3-document-processing"

func main() {
	temporalAddr := getenv("TEMPORAL_ADDRESS", "localhost:7233")
	bucket := getenv("S3_BUCKET", "justin-s3driver-demo")
	awsRegion := getenv("AWS_REGION", "us-east-2")
	awsProfile := os.Getenv("AWS_PROFILE")

	// Load AWS credentials and region from the environment or ~/.aws config.
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

	// Create the S3 storage driver. All payloads above the threshold (default
	// 256 KiB) will be transparently stored in and retrieved from this bucket.
	driver, err := s3driver.NewDriver(s3driver.Options{
		Client: awssdkv2.NewClient(awss3.NewFromConfig(cfg)),
		Bucket: s3driver.StaticBucket(bucket),
	})
	if err != nil {
		log.Fatalf("create S3 driver: %v", err)
	}

	// Connect to Temporal with external storage configured. The client will
	// intercept oversized payloads on the way to/from the server.
	c, err := client.Dial(client.Options{
		HostPort: temporalAddr,
		ExternalStorage: converter.ExternalStorage{
			Drivers: []converter.StorageDriver{driver},
		},
	})
	if err != nil {
		log.Fatalf("connect to Temporal: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})

	wf := &docprocessing.DocumentProcessingWorkflow{}
	w.RegisterWorkflow(wf.Run)
	w.RegisterActivity(docprocessing.GenerateDocument)
	w.RegisterActivity(docprocessing.AnalyzeDocument)
	w.RegisterActivity(docprocessing.EnrichDocument)
	w.RegisterActivity(docprocessing.GenerateSummary)

	log.Printf("worker started  task-queue=%s  bucket=%s", taskQueue, bucket)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker stopped: %v", err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
