package memory

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/extstore"
)

type memoryStorageDriver struct {
	name        string
	nextClaimId int
	mapping     map[string]*commonpb.Payload
	mu          sync.Mutex
}

type DriverOptions struct {
	DriverName string
}

func NewDriver(options DriverOptions) extstore.ExternalStorageDriver {
	driverName := options.DriverName
	if driverName == "" {
		driverName = "memory"
	}

	return &memoryStorageDriver{
		name:        driverName,
		nextClaimId: 1,
		mapping:     map[string]*commonpb.Payload{},
	}
}

func (driver *memoryStorageDriver) Name() string {
	return driver.name
}

func (driver *memoryStorageDriver) Type() string {
	return "temporal-memory"
}

func (driver *memoryStorageDriver) Store(ctx context.Context, payloads []*commonpb.Payload) ([]extstore.ExternalStorageClaim, error) {
	claims := make([]extstore.ExternalStorageClaim, len(payloads))
	for index, payload := range payloads {
		driver.mu.Lock()
		claimId := fmt.Sprintf("%d", driver.nextClaimId)
		driver.nextClaimId++
		driver.mu.Unlock()

		driver.mapping[claimId] = payload

		claims[index] = extstore.ExternalStorageClaim{
			Data: map[string][]byte{"claim-id": []byte(claimId)},
		}
	}
	return claims, nil
}

func (driver *memoryStorageDriver) Retrieve(ctx context.Context, claims []extstore.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}
