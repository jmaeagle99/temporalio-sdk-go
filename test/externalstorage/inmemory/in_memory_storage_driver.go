package inmemory

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/externalstorage"
)

type inMemoryStorageDriver struct {
	suffix      string
	nextClaimId int
	mapping     map[string]*commonpb.Payload
	mu          sync.Mutex
}

type Options struct {
	Suffix string
}

func New(options Options) externalstorage.ExternalStorageDriver {
	suffix := options.Suffix
	if suffix == "" {
		suffix = "default"
	}

	return &inMemoryStorageDriver{
		suffix:      suffix,
		nextClaimId: 1,
		mapping:     map[string]*commonpb.Payload{},
	}
}

func (driver *inMemoryStorageDriver) GetName() string {
	return "in-memory-" + driver.suffix
}

func (driver *inMemoryStorageDriver) Store(context context.Context, payloads []*commonpb.Payload) ([]externalstorage.ExternalStorageClaim, error) {
	claims := make([]externalstorage.ExternalStorageClaim, len(payloads))
	for index, payload := range payloads {
		driver.mu.Lock()
		claimId := fmt.Sprintf("%d", driver.nextClaimId)
		driver.nextClaimId++
		driver.mu.Unlock()

		driver.mapping[claimId] = payload

		claims[index] = externalstorage.ExternalStorageClaim{
			Data: map[string][]byte{"claim-id": []byte(claimId)},
		}
	}
	return claims, nil
}

func (driver *inMemoryStorageDriver) Retrieve(context context.Context, claims []externalstorage.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}
