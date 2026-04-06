// Package docprocessing demonstrates Temporal's external storage feature using
// the S3 storage driver. It runs a four-step document processing pipeline where
// each activity deliberately produces multi-megabyte payloads. These payloads
// are transparently offloaded to S3 instead of being stored inline in workflow
// history, keeping history compact regardless of payload size.
//
// Pipeline: generate → analyze → enrich → summarize
//
//   - generate: small input → 10 MB document
//   - analyze:  10 MB document → 10 MB analysis
//   - enrich:   20 MB combined input → 10 MB enriched document
//   - summarize: 10 MB input → small final summary
package docprocessing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

// WorkflowInput is the small value sent from the client. The large document is
// generated inside the worker so that no large payloads cross the client
// boundary.
type WorkflowInput struct {
	Title        string
	DocumentSize int // approximate document size in bytes to generate
}

// AnalysisResult holds the output of the AnalyzeDocument activity. RawAnalysis
// is intentionally large (~10 MB) to exercise external storage.
type AnalysisResult struct {
	WordCount   int
	KeyPhrases  []string
	RawAnalysis string
}

// EnrichmentInput bundles the large inputs for the EnrichDocument activity.
type EnrichmentInput struct {
	Title           string
	OriginalContent string
	Analysis        string
}

// SummaryInput bundles the large inputs for the GenerateSummary activity.
type SummaryInput struct {
	Title           string
	WordCount       int
	KeyPhrases      []string
	EnrichedContent string
}

// DocumentSummary is the small final result returned to the caller.
type DocumentSummary struct {
	Title      string
	WordCount  int
	KeyPhrases []string
	Summary    string
}

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

// GenerateDocument creates a large synthetic document inside the worker.
// Keeping generation on the worker side means the client only sends a small
// WorkflowInput; the large payload is created and stored externally without
// ever crossing the client/server boundary.
func GenerateDocument(_ context.Context, inp WorkflowInput) (string, error) {
	paragraph := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. " +
		"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris " +
		"nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in " +
		"reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla " +
		"pariatur. Excepteur sint occaecat cupidatat non proident, sunt in " +
		"culpa qui officia deserunt mollit anim id est laborum.\n\n"

	repetitions := inp.DocumentSize/len(paragraph) + 1
	content := strings.Repeat(paragraph, repetitions)
	if len(content) > inp.DocumentSize {
		content = content[:inp.DocumentSize]
	}
	return content, nil
}

// AnalyzeDocument analyzes the document and produces a large analysis payload
// (~10 MB) to exercise external storage on the activity result.
func AnalyzeDocument(_ context.Context, content string) (AnalysisResult, error) {
	words := strings.Fields(content)
	wordCount := len(words)

	// Extract representative key phrases (5-word chunks at intervals).
	var keyPhrases []string
	for i := 0; i < len(words) && i < 50; i += 10 {
		end := i + 5
		if end > len(words) {
			end = len(words)
		}
		keyPhrases = append(keyPhrases, strings.Join(words[i:end], " "))
	}

	// Build a large analysis blob to exercise external storage.
	header := fmt.Sprintf("=== Analysis ===\nWord count: %d\nKey phrases: %s\n\n",
		wordCount, strings.Join(keyPhrases, ", "))
	note := fmt.Sprintf("[analysis-detail] Document section verified — %d words processed.\n", wordCount)
	targetSize := 10 * 1024 * 1024
	rawAnalysis := header + strings.Repeat(note, targetSize/len(note))

	return AnalysisResult{
		WordCount:   wordCount,
		KeyPhrases:  keyPhrases,
		RawAnalysis: rawAnalysis,
	}, nil
}

// EnrichDocument combines the original document with its analysis to produce an
// enriched version (~10 MB).
func EnrichDocument(_ context.Context, inp EnrichmentInput) (string, error) {
	header := fmt.Sprintf(
		"=== Enriched Document: %s ===\n\n--- Original Content ---\n%s\n\n--- Analysis ---\n%s\n\n--- Enrichment Notes ---\n",
		inp.Title, inp.OriginalContent, inp.Analysis,
	)
	note := fmt.Sprintf("[enrichment] Cross-referenced section of %q with analysis data.\n", inp.Title)
	targetSize := 10 * 1024 * 1024
	repetitions := (targetSize - len(header)) / len(note)
	if repetitions < 0 {
		repetitions = 0
	}
	return header + strings.Repeat(note, repetitions), nil
}

// GenerateSummary produces a small final summary from the large enriched
// document. Its output is well under the payload size threshold and will be
// stored inline in workflow history.
func GenerateSummary(ctx context.Context, inp SummaryInput) (DocumentSummary, error) {
	phrases := inp.KeyPhrases
	if len(phrases) > 3 {
		phrases = phrases[:3]
	}
	summary := fmt.Sprintf(
		"Document %q contains %d words. Key themes include: %s. Enriched content is %d bytes.",
		inp.Title, inp.WordCount, strings.Join(phrases, ", "), len(inp.EnrichedContent),
	)
	activity.GetLogger(ctx).Info("generated summary", "title", inp.Title)
	return DocumentSummary{
		Title:      inp.Title,
		WordCount:  inp.WordCount,
		KeyPhrases: inp.KeyPhrases,
		Summary:    summary,
	}, nil
}

// ---------------------------------------------------------------------------
// Workflow
// ---------------------------------------------------------------------------

// DocumentProcessingWorkflow runs the four-step pipeline. Large intermediate
// payloads passed between activities are transparently offloaded to S3 by the
// external storage driver configured on the worker.
type DocumentProcessingWorkflow struct{}

// Run executes the document processing pipeline.
func (w *DocumentProcessingWorkflow) Run(ctx workflow.Context, inp WorkflowInput) (DocumentSummary, error) {
	ao := workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Generate a large document (small input → large output).
	var content string
	if err := workflow.ExecuteActivity(ctx, GenerateDocument, inp).Get(ctx, &content); err != nil {
		return DocumentSummary{}, fmt.Errorf("generate: %w", err)
	}

	// Step 2: Analyze (large input → large output).
	var analysis AnalysisResult
	if err := workflow.ExecuteActivity(ctx, AnalyzeDocument, content).Get(ctx, &analysis); err != nil {
		return DocumentSummary{}, fmt.Errorf("analyze: %w", err)
	}

	// Step 3: Enrich (large input → large output).
	var enriched string
	if err := workflow.ExecuteActivity(ctx, EnrichDocument, EnrichmentInput{
		Title:           inp.Title,
		OriginalContent: content,
		Analysis:        analysis.RawAnalysis,
	}).Get(ctx, &enriched); err != nil {
		return DocumentSummary{}, fmt.Errorf("enrich: %w", err)
	}

	// Step 4: Summarize (large input → small output).
	var summary DocumentSummary
	if err := workflow.ExecuteActivity(ctx, GenerateSummary, SummaryInput{
		Title:           inp.Title,
		WordCount:       analysis.WordCount,
		KeyPhrases:      analysis.KeyPhrases,
		EnrichedContent: enriched,
	}).Get(ctx, &summary); err != nil {
		return DocumentSummary{}, fmt.Errorf("summarize: %w", err)
	}

	return summary, nil
}
