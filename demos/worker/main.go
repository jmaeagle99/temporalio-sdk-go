package main

import (
	"log"
	"strings"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/demos"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const taskQueue = "echo-task-queue"

func GenerateDocument(ctx workflow.Context, documentSize int) (string, error) {
	paragraph := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
		"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris " +
		"nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in " +
		"reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla " +
		"pariatur. Excepteur sint occaecat cupidatat non proident, sunt in " +
		"culpa qui officia deserunt mollit anim id est laborum.\n\n"
	repetitions := documentSize/len(paragraph) + 1
	content := strings.Repeat(paragraph, repetitions)
	if len(content) > documentSize {
		content = content[:documentSize]
	}
	return content, nil
}

func main() {
	temporalAddr := demos.GetEnv("TEMPORAL_ADDRESS", "localhost:7233")

	dc := converter.NewCodecDataConverter(
		converter.GetDefaultDataConverter(),
		demos.NewPayloadCodec(),
	)

	c, err := client.Dial(client.Options{
		HostPort:      temporalAddr,
		DataConverter: dc,
		ExternalStorage: converter.ExternalStorage{
			Drivers:              []converter.StorageDriver{demos.CreateDriver()},
			PayloadSizeThreshold: 1024,
		},
	})
	if err != nil {
		log.Fatalf("unable to create Temporal client: %v", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(GenerateDocument)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("worker exited with error: %v", err)
	}
}
