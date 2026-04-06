// Command starter submits one document processing workflow run to Temporal and
// waits for the result.
//
// The starter does NOT need the S3 driver — the small WorkflowInput never
// exceeds the payload size threshold, so only the worker needs it.
//
// Usage:
//
//	go run ./starter
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"

	docprocessing "go.temporal.io/sdk/demos/s3_document_processing"

	"go.temporal.io/sdk/client"
)

const taskQueue = "s3-document-processing"

func main() {
	temporalAddr := getenv("TEMPORAL_ADDRESS", "localhost:7233")

	// No external storage needed on the client side — the tiny WorkflowInput
	// stays well below the 256 KiB threshold.
	c, err := client.Dial(client.Options{HostPort: temporalAddr})
	if err != nil {
		log.Fatalf("connect to Temporal: %v", err)
	}
	defer c.Close()

	inp := docprocessing.WorkflowInput{
		Title:        "Quarterly Business Report Q4 2025",
		DocumentSize: 10 * 1024 * 1024, // 10 MB
	}

	workflowID := fmt.Sprintf("doc-processing-%08x", rand.Int31())
	log.Printf("starting workflow  id=%s", workflowID)
	log.Println("  pipeline: generate → analyze → enrich → summarize")

	wf := &docprocessing.DocumentProcessingWorkflow{}
	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: taskQueue,
		},
		wf.Run,
		inp,
	)
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}

	var result docprocessing.DocumentSummary
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalf("workflow failed: %v", err)
	}

	phrases := result.KeyPhrases
	if len(phrases) > 3 {
		phrases = phrases[:3]
	}
	fmt.Printf("  Title       : %s\n", result.Title)
	fmt.Printf("  Word count  : %d\n", result.WordCount)
	fmt.Printf("  Key phrases : %s\n", strings.Join(phrases, ", "))
	fmt.Printf("  Summary     : %s\n", result.Summary)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
