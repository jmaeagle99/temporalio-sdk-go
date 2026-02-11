package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"go.temporal.io/sdk/extstore"

	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
)

type fileSystemStorageDriver struct {
	baseDir     string
	driverName  string
	nextClaimId int
	mu          sync.Mutex
}

type DriverOptions struct {
	BaseDir    string
	DriverName string
}

func NewDriver(options DriverOptions) (extstore.ExternalStorageDriver, error) {
	baseDir := options.BaseDir
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "temporal-payloads")
	}

	driverName := options.DriverName
	if driverName == "" {
		driverName = "filesystem"
	}

	// Create the base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", baseDir, err)
	}

	return &fileSystemStorageDriver{
		baseDir:     baseDir,
		driverName:  driverName,
		nextClaimId: 1,
	}, nil
}

func (d *fileSystemStorageDriver) Name() string {
	return d.driverName
}

func (d *fileSystemStorageDriver) Type() string {
	return "temporal-filesystem"
}

func (d *fileSystemStorageDriver) Store(ctx extstore.ExternalStorageDriverContext, payloads []*commonpb.Payload) ([]extstore.ExternalStorageClaim, error) {
	claims := make([]extstore.ExternalStorageClaim, len(payloads))

	for index, payload := range payloads {
		// Generate a unique claim ID
		d.mu.Lock()
		claimId := fmt.Sprintf("%d", d.nextClaimId)
		d.nextClaimId++
		d.mu.Unlock()

		// Construct the file path
		filename := fmt.Sprintf("payload-%s.bin", claimId)
		filepath := filepath.Join(d.baseDir, filename)

		// Serialize the payload to bytes
		data, err := proto.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload %s: %w", claimId, err)
		}

		// Write the payload to disk
		if err := os.WriteFile(filepath, data, 0644); err != nil {
			return nil, fmt.Errorf("failed to write payload %s to %s: %w", claimId, filepath, err)
		}

		// Create the claim with the claim ID
		claims[index] = extstore.ExternalStorageClaim{
			Data: map[string][]byte{
				"claim-id": []byte(claimId),
			},
		}
	}

	return claims, nil
}

func (d *fileSystemStorageDriver) Retrieve(ctx extstore.ExternalStorageDriverContext, claims []extstore.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))

	for index, claim := range claims {
		// Extract the claim ID from the claim data
		claimId := string(claim.Data["claim-id"])

		// Construct the file path
		filename := fmt.Sprintf("payload-%s.bin", claimId)
		filepath := filepath.Join(d.baseDir, filename)

		// Read the payload from disk
		data, err := os.ReadFile(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload %s from %s: %w", claimId, filepath, err)
		}

		// Deserialize the payload
		payload := &commonpb.Payload{}
		if err := proto.Unmarshal(data, payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload %s: %w", claimId, err)
		}

		payloads[index] = payload
	}

	return payloads, nil
}
