package memory

import (
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/extstore"
	"go.temporal.io/sdk/internal"
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

func (driver *memoryStorageDriver) Store(ctx extstore.ExternalStorageDriverContext, payloads []*commonpb.Payload) ([]extstore.ExternalStorageClaim, error) {

	var workflowName string
	if ai, ok := ctx.ActivityInfo.(*internal.ActivityInfo); ok && ai != nil {
		workflowName = ai.WorkflowExecution.ID
	} else if wi, ok := ctx.WorkflowInfo.(*internal.WorkflowInfo); ok && wi != nil {
		workflowName = wi.WorkflowExecution.ID
	}
	if len(workflowName) > 0 {
		fmt.Printf("MemoryDriver:Store:WorkflowID: %s\n", workflowName)
	}

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

func (driver *memoryStorageDriver) Retrieve(ctx extstore.ExternalStorageDriverContext, claims []extstore.ExternalStorageClaim) ([]*commonpb.Payload, error) {

	var workflowName string
	if ai, ok := ctx.ActivityInfo.(*internal.ActivityInfo); ok && ai != nil {
		workflowName = ai.WorkflowExecution.ID
	} else if wi, ok := ctx.WorkflowInfo.(*internal.WorkflowInfo); ok && wi != nil {
		workflowName = wi.WorkflowExecution.ID
	}
	if len(workflowName) > 0 {
		fmt.Printf("MemoryDriver:Retrieve:WorkflowID: %s\n", workflowName)
	}

	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}
