# S3 Document Processing Demo

This demo shows how Temporal's **external storage** feature works using the
[S3 storage driver](../../contrib/aws/s3driver). Large payloads produced by
activities are transparently offloaded to Amazon S3 instead of being stored
inline in workflow history — keeping history compact no matter how large the
intermediate data is.

## What it does

The workflow runs a four-step document processing pipeline:

| Step | Activity | Input size | Output size |
|------|----------|------------|-------------|
| 1 | `GenerateDocument` | small (`WorkflowInput`) | ~10 MB |
| 2 | `AnalyzeDocument`  | ~10 MB | ~10 MB |
| 3 | `EnrichDocument`   | ~20 MB | ~10 MB |
| 4 | `GenerateSummary`  | ~10 MB | small (`DocumentSummary`) |

Every multi-megabyte payload crosses the 256 KiB default threshold, so the
driver stores it in S3 and writes only a small claim token to workflow history.
The final `DocumentSummary` is tiny and stays inline.

## Prerequisites

- A running Temporal server (e.g. `temporal server start-dev`)
- AWS credentials configured (env vars, `~/.aws/credentials`, or IAM role)
- An existing S3 bucket

## Running the demo

**1. Start the worker** (in one terminal):

```sh
S3_BUCKET=my-bucket go run ./worker
```

**2. Start a workflow run** (in another terminal):

```sh
go run ./starter
```

The starter prints the final summary once the workflow completes:

```
  Title       : Quarterly Business Report Q4 2025
  Word count  : 130,000
  Key phrases : Lorem ipsum dolor sit, amet consectetur adipiscing, ...
  Summary     : Document "Quarterly Business Report Q4 2025" contains 130000 words. ...
```

## Configuration

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal frontend address |
| `S3_BUCKET` | `my-temporal-payloads` | S3 bucket for payload storage |
| `AWS_REGION` | `us-east-1` | AWS region |

Standard AWS credential resolution applies — environment variables
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, or an
IAM role all work out of the box.

## Key points

- **Client needs no driver** — `WorkflowInput` is tiny and never hits the
  threshold. Only workers need the S3 driver configured.
- **Transparent to application code** — workflows and activities use normal
  `string` / struct types. The driver intercepts payloads at the SDK boundary.
- **Content-addressed storage** — S3 keys are derived from a SHA-256 hash of
  the serialized payload, so identical payloads are deduplicated automatically.
- **Integrity verified on read** — the driver re-checks the hash on every
  retrieval, catching any silent corruption in S3.
