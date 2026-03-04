package internal

import (
	"encoding/json"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/converter"
)

type StorageDriverContext struct {
	// Intentionally left blank.
}

type StorageClaim struct {
	Data map[string]string
}

type StorageDriver interface {
	Name() string
	Type() string
	Store(ctx StorageDriverContext, payload []*commonpb.Payload) ([]StorageClaim, error)
	Retrieve(ctx StorageDriverContext, claims []StorageClaim) ([]*commonpb.Payload, error)
}

type StorageDriverSelector interface {
	SelectDriver(StorageDriverContext, *commonpb.Payload) (StorageDriver, error)
}

type StorageOptions struct {
	Drivers              []StorageDriver
	DriverSelector       StorageDriverSelector
	PayloadSizeThreshold int
}

type storageParameters struct {
	driverMap            map[string]StorageDriver
	driverSelector       StorageDriverSelector
	firstDriver          StorageDriver
	payloadSizeThreshold int
}

func StorageOptionsToParams(options StorageOptions) (storageParameters, error) {
	driverMap := make(map[string]StorageDriver, len(options.Drivers))
	for _, d := range options.Drivers {
		if _, exists := driverMap[d.Name()]; exists {
			return storageParameters{}, fmt.Errorf("duplicate storage driver name: %q", d.Name())
		}
		driverMap[d.Name()] = d
	}

	var firstDriver StorageDriver
	if len(options.Drivers) > 0 {
		firstDriver = options.Drivers[0]
	}

	return storageParameters{
		driverMap:            driverMap,
		driverSelector:       options.DriverSelector,
		firstDriver:          firstDriver,
		payloadSizeThreshold: options.PayloadSizeThreshold,
	}, nil
}

type storageOperationCallback interface {
	OnComplete(count int, size int64, duration time.Duration)
}

var storageOperationCallbackContextKey = "storageOperationCallback"

// metadataEncodingStorageRef is the metadata encoding value used to identify
// payloads that are storage references rather than actual data.
const metadataEncodingStorageRef = "json/external-storage-reference"

type storageReference struct {
	DriverName  string
	DriverClaim StorageClaim
}

func storageReferenceToPayload(ref storageReference, storedSizeBytes int64) (*commonpb.Payload, error) {
	data, err := json.Marshal(ref)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal storage reference: %w", err)
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			converter.MetadataEncoding: []byte(metadataEncodingStorageRef),
		},
		Data: data,
		ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
			{SizeBytes: storedSizeBytes},
		},
	}, nil
}

// payloadToStorageReference decodes a storage reference from a payload.
func payloadToStorageReference(p *commonpb.Payload) (storageReference, error) {
	if string(p.GetMetadata()[converter.MetadataEncoding]) != metadataEncodingStorageRef {
		return storageReference{}, fmt.Errorf("payload is not a storage reference: unexpected encoding %q", string(p.GetMetadata()[converter.MetadataEncoding]))
	}
	var ref storageReference
	if err := json.Unmarshal(p.Data, &ref); err != nil {
		return storageReference{}, fmt.Errorf("failed to unmarshal storage reference: %w", err)
	}
	return ref, nil
}

type storageRetrievalVisitor struct {
	params storageParameters
}

func (v *storageRetrievalVisitor) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	startTime := time.Now()

	// Identify which payloads are storage references and group them by driver.
	type driverBatch struct {
		driver  StorageDriver
		indices []int
		claims  []StorageClaim
	}
	var driverOrder []string
	driverBatches := map[string]*driverBatch{}

	result := make([]*commonpb.Payload, len(payloads))
	totalSize := int64(0)

	for i, p := range payloads {
		if len(p.GetExternalPayloads()) == 0 {
			result[i] = p
			continue
		}
		ref, err := payloadToStorageReference(p)
		if err != nil {
			return nil, err
		}

		driver, ok := v.params.driverMap[ref.DriverName]
		if !ok {
			return nil, fmt.Errorf("no storage driver registered with name %q", ref.DriverName)
		}

		batch, exists := driverBatches[ref.DriverName]
		if !exists {
			batch = &driverBatch{driver: driver}
			driverBatches[ref.DriverName] = batch
			driverOrder = append(driverOrder, ref.DriverName)
		}
		batch.indices = append(batch.indices, i)
		batch.claims = append(batch.claims, ref.DriverClaim)
	}

	driverCtx := StorageDriverContext{}
	for _, name := range driverOrder {
		batch := driverBatches[name]
		retrieved, err := batch.driver.Retrieve(driverCtx, batch.claims)
		if err != nil {
			return nil, fmt.Errorf("storage driver %q retrieve failed: %w", name, err)
		}
		if len(retrieved) != len(batch.claims) {
			return nil, fmt.Errorf("storage driver %q returned %d payloads for %d claims", name, len(retrieved), len(batch.claims))
		}
		for j, p := range retrieved {
			totalSize += int64(len(p.GetData()))
			result[batch.indices[j]] = p
		}
	}

	if callbackValue := ctx.Value(storageOperationCallbackContextKey); callbackValue != nil {
		if callback, isCallback := callbackValue.(storageOperationCallback); isCallback {
			callback.OnComplete(len(payloads), totalSize, time.Since(startTime))
		}
	}
	return result, nil
}

func NewStorageRetrievalVisitor(params storageParameters) PayloadVisitor {
	return &storageRetrievalVisitor{params: params}
}

type storageStoreVisitor struct {
	params storageParameters
}

func (v *storageStoreVisitor) Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	startTime := time.Now()

	if v.params.driverSelector == nil && v.params.firstDriver == nil {
		return payloads, nil
	}

	// Determine which driver (if any) should store each payload.
	type driverBatch struct {
		driver   StorageDriver
		indices  []int
		payloads []*commonpb.Payload
	}
	var driverOrder []string
	driverBatches := map[string]*driverBatch{}

	result := make([]*commonpb.Payload, len(payloads))
	totalSize := int64(0)
	driverCtx := StorageDriverContext{}

	for i, p := range payloads {
		var driver StorageDriver
		if v.params.driverSelector != nil {
			selected, err := v.params.driverSelector.SelectDriver(driverCtx, p)
			if err != nil {
				return nil, fmt.Errorf("storage driver selector failed: %w", err)
			}
			if selected != nil {
				registered, ok := v.params.driverMap[selected.Name()]
				if !ok {
					return nil, fmt.Errorf("storage driver selector returned unregistered driver %q", selected.Name())
				}
				driver = registered
			}
		} else {
			driver = v.params.firstDriver
		}

		if driver == nil {
			result[i] = p
			continue
		}

		name := driver.Name()
		batch, exists := driverBatches[name]
		if !exists {
			batch = &driverBatch{driver: driver}
			driverBatches[name] = batch
			driverOrder = append(driverOrder, name)
		}
		batch.indices = append(batch.indices, i)
		batch.payloads = append(batch.payloads, p)
	}

	for _, name := range driverOrder {
		batch := driverBatches[name]
		claims, err := batch.driver.Store(driverCtx, batch.payloads)
		if err != nil {
			return nil, fmt.Errorf("storage driver %q store failed: %w", name, err)
		}
		if len(claims) != len(batch.payloads) {
			return nil, fmt.Errorf("storage driver %q returned %d claims for %d payloads", name, len(claims), len(batch.payloads))
		}
		for j, claim := range claims {
			ref := storageReference{
				DriverName:  name,
				DriverClaim: claim,
			}
			storedSize := int64(batch.payloads[j].Size())
			totalSize += storedSize
			refPayload, err := storageReferenceToPayload(ref, storedSize)
			if err != nil {
				return nil, err
			}
			result[batch.indices[j]] = refPayload
		}
	}

	if callbackValue := ctx.Value(storageOperationCallbackContextKey); callbackValue != nil {
		if callback, isCallback := callbackValue.(storageOperationCallback); isCallback {
			callback.OnComplete(len(payloads), totalSize, time.Since(startTime))
		}
	}
	return result, nil
}

func NewStorageStoreVisitor(params storageParameters) PayloadVisitor {
	return &storageStoreVisitor{params: params}
}
