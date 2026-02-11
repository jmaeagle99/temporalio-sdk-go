package internal

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/extstore"
	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"
)

// externalStorageDataConverter is a DataConverter for storing and retrieving payloads to and from external storage.
type externalStorageDataConverter struct {
	innerConverter converter.DataConverter
	driverMap      map[string]extstore.ExternalStorageDriver
	selector       extstore.ExternalStorageDriverSelector
	activityInfo   ActivityInfo
	workflowInfo   *WorkflowInfo
}

// Create a data converter that uses the storage provider to store payloads externally. The inner converter is used for converting
// both input values, generated storage claims, and payloads that are passed through.
func newExternalStorageDataConverter(converter converter.DataConverter, logger log.Logger, options extstore.ExternalStorageOptions) converter.DataConverter {
	driverMap := make(map[string]extstore.ExternalStorageDriver)

	for _, driver := range options.Drivers {
		name := driver.Name()
		if _, ok := driverMap[name]; ok {
			logger.Warn(fmt.Sprintf("Driver with name %s already provided. Overwriting with latest driver.", name))
		}
		driverMap[name] = driver
	}

	return &externalStorageDataConverter{
		innerConverter: converter,
		driverMap:      driverMap,
		selector:       options.Selector,
	}
}

// externalStorageReference represents an externally stored payload.
// It contains information for indicating which driver was used to store the
// payload and the claim that can be used with that driver to retrieve the payload
// at a later time.
type externalStorageReference struct {
	// The name of the storage driver used to store a payload.
	DriverName string

	// The claim associated with the externally stored payload.
	StorageClaim extstore.ExternalStorageClaim
}

func (c *externalStorageDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	valuePayload, err := c.innerConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}

	if c.shouldPassthrough() {
		return valuePayload, nil
	}

	ctx := context.Background()
	driverCtx := extstore.ExternalStorageDriverContext{
		Context:      ctx,
		ActivityInfo: &c.activityInfo,
		WorkflowInfo: c.workflowInfo,
	}

	driverName, err := c.selector(driverCtx, valuePayload)
	if err != nil {
		return nil, err
	}
	if driverName == "" {
		// Selector indicated that the payload should be passed through
		// instead of storing it externally via a driver.
		return valuePayload, nil
	}
	driver, ok := c.driverMap[driverName]
	if !ok {
		return nil, NewDriverNotFoundError(driverName)
	}

	claims, err := driver.Store(driverCtx, []*commonpb.Payload{valuePayload})
	if err != nil {
		return nil, err
	}

	if len(claims) != 1 {
		return nil, fmt.Errorf("reference count didn't match payload count")
	}

	referencePayload, err := c.innerConverter.ToPayload(externalStorageReference{
		DriverName:   driverName,
		StorageClaim: claims[0],
	})
	referencePayload.ExternalPayloads = append(referencePayload.ExternalPayloads, &commonpb.Payload_ExternalPayloadDetails{
		SizeBytes: int64(valuePayload.Size()),
	})
	return referencePayload, nil
}

func (c *externalStorageDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	if c.shouldPassthrough() {
		return c.innerConverter.FromPayload(payload, valuePtr)
	}

	if len(payload.ExternalPayloads) == 0 {
		// This is not a external payload claim
		return c.innerConverter.FromPayload(payload, valuePtr)
	}

	var reference externalStorageReference
	err := c.innerConverter.FromPayload(payload, &reference)
	if err != nil {
		return err
	}

	driver, ok := c.driverMap[reference.DriverName]
	if !ok {
		return NewDriverNotFoundError(reference.DriverName)
	}

	ctx := context.Background()
	driverCtx := extstore.ExternalStorageDriverContext{
		Context:      ctx,
		ActivityInfo: &c.activityInfo,
		WorkflowInfo: c.workflowInfo,
	}

	valuePayload, err := driver.Retrieve(driverCtx, []extstore.ExternalStorageClaim{reference.StorageClaim})
	if err != nil {
		return err
	}

	return c.innerConverter.FromPayload(valuePayload[0], valuePtr)
}

func (c *externalStorageDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	payloads, err := c.innerConverter.ToPayloads(value)
	if err != nil || c.shouldPassthrough() {
		return payloads, err
	}

	result := &commonpb.Payloads{
		Payloads: make([]*commonpb.Payload, len(payloads.Payloads)),
	}

	ctx := context.Background()
	driverCtx := extstore.ExternalStorageDriverContext{
		Context:      ctx,
		ActivityInfo: &c.activityInfo,
		WorkflowInfo: c.workflowInfo,
	}

	indexesByDriverName := make(map[string][]int)
	for index, payload := range payloads.Payloads {
		driverName, err := c.selector(driverCtx, payload)
		if err != nil {
			return nil, err
		}
		if driverName == "" {
			// Selector indicated that the payload should be passed through
			// instead of storing it externally via a driver.
			result.Payloads[index] = payload
			continue
		}
		_, ok := c.driverMap[driverName]
		if !ok {
			return nil, NewDriverNotFoundError(driverName)
		}
		indexesByDriverName[driverName] = append(indexesByDriverName[driverName], index)
	}

	// Store the payloads partitioned by driver
	g, ctx := errgroup.WithContext(ctx)
	driverCtx = extstore.ExternalStorageDriverContext{
		Context:      ctx,
		ActivityInfo: &c.activityInfo,
		WorkflowInfo: c.workflowInfo,
	}
	for driverName, indexes := range indexesByDriverName {
		driver := c.driverMap[driverName]
		driverName := driverName
		indexes := indexes
		driverCtx := driverCtx

		g.Go(func() error {
			// Collect payloads and store them
			driverPayloads := make([]*commonpb.Payload, len(indexes))
			for indexIndex, payloadIndex := range indexes {
				driverPayloads[indexIndex] = payloads.Payloads[payloadIndex]
			}
			claims, err := driver.Store(driverCtx, driverPayloads)
			if err != nil {
				return err
			}
			if len(claims) != len(driverPayloads) {
				return fmt.Errorf("driver %s returned %d claims for %d payloads", driverName, len(claims), len(driverPayloads))
			}

			// Create storage references and convert to payloads
			references := make([]externalStorageReference, len(claims))
			for index, claim := range claims {
				references[index] = externalStorageReference{
					DriverName:   driverName,
					StorageClaim: claim,
				}
			}
			referenceInterfaces := make([]interface{}, len(references))
			for i, ref := range references {
				referenceInterfaces[i] = ref
			}
			referencePayloads, err := c.innerConverter.ToPayloads(referenceInterfaces...)
			if err != nil {
				return err
			}
			if len(referencePayloads.Payloads) != len(references) {
				return fmt.Errorf("converter returned %d payloads for %d references", len(referencePayloads.Payloads), len(references))
			}

			// Set metadata and put reference payloads at their corresponding indexes
			for index, referencePayload := range referencePayloads.Payloads {
				payloadIndex := indexes[index]
				referencePayload.ExternalPayloads = append(
					referencePayload.ExternalPayloads,
					&commonpb.Payload_ExternalPayloadDetails{
						SizeBytes: int64(payloads.Payloads[payloadIndex].Size()),
					})
				result.Payloads[payloadIndex] = referencePayload
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *externalStorageDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if c.shouldPassthrough() {
		return c.innerConverter.FromPayloads(payloads, valuePtrs...)
	}

	if payloads == nil {
		return nil
	}

	count := min(len(payloads.Payloads), len(valuePtrs))
	ctx := context.Background()

	// Create slice for retrieved/passthrough payloads
	retrievedPayloads := make([]*commonpb.Payload, count)

	// Partition payloads by driver
	type claimIndexInfo struct {
		index int
		claim extstore.ExternalStorageClaim
	}

	claimsByDriverName := make(map[string][]claimIndexInfo)

	for i, payload := range payloads.Payloads[:count] {
		if len(payload.ExternalPayloads) == 0 {
			// Not an external payload, use directly
			retrievedPayloads[i] = payload
			continue
		}

		var reference externalStorageReference
		err := c.innerConverter.FromPayload(payload, &reference)
		if err != nil {
			return err
		}

		_, ok := c.driverMap[reference.DriverName]
		if !ok {
			return NewDriverNotFoundError(reference.DriverName)
		}

		claimsByDriverName[reference.DriverName] = append(claimsByDriverName[reference.DriverName], claimIndexInfo{
			index: i,
			claim: reference.StorageClaim,
		})
	}

	// Retrieve payloads partitioned by driver in parallel
	g, ctx := errgroup.WithContext(ctx)
	driverCtx := extstore.ExternalStorageDriverContext{
		Context:      ctx,
		ActivityInfo: &c.activityInfo,
		WorkflowInfo: c.workflowInfo,
	}
	for driverName, infos := range claimsByDriverName {
		driverName := driverName
		infos := infos
		driver := c.driverMap[driverName]
		driverCtx := driverCtx

		g.Go(func() error {
			// Collect claims for this driver
			claims := make([]extstore.ExternalStorageClaim, len(infos))
			for i, info := range infos {
				claims[i] = info.claim
			}

			// Retrieve all payloads for this driver
			driverPayloads, err := driver.Retrieve(driverCtx, claims)
			if err != nil {
				return err
			}

			if len(driverPayloads) != len(claims) {
				return fmt.Errorf("driver %s returned %d payloads for %d claims", driverName, len(driverPayloads), len(claims))
			}

			// Place retrieved payloads at their corresponding indexes
			for i, payload := range driverPayloads {
				retrievedPayloads[infos[i].index] = payload
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return c.innerConverter.FromPayloads(
		&commonpb.Payloads{Payloads: retrievedPayloads},
		valuePtrs...)
}

func (c *externalStorageDataConverter) ToString(input *commonpb.Payload) string {
	if input == nil || len(input.ExternalPayloads) == 0 {
		// This is nil or not a external payload claim
		return c.innerConverter.ToString(input)
	}

	var reference externalStorageReference
	err := c.innerConverter.FromPayload(input, &reference)
	if err == nil {
		return fmt.Sprintf("External payload claim with driver %s", reference.DriverName)
	} else {
		return "External payload claim"
	}
}

func (c *externalStorageDataConverter) ToStrings(input *commonpb.Payloads) []string {
	if input == nil {
		return c.innerConverter.ToStrings(input)
	}
	strs := make([]string, len(input.Payloads))
	for i, payload := range input.Payloads {
		strs[i] = c.ToString(payload)
	}
	return strs
}

func (c *externalStorageDataConverter) WithWorkflowContext(ctx Context) converter.DataConverter {
	innerConverter := c.innerConverter
	if contextAwareInnerConverter, ok := c.innerConverter.(ContextAware); ok {
		innerConverter = contextAwareInnerConverter.WithWorkflowContext(ctx)
	}

	return &externalStorageDataConverter{
		innerConverter: innerConverter,
		driverMap:      c.driverMap,
		selector:       c.selector,
		activityInfo:   c.activityInfo,
		workflowInfo:   GetWorkflowInfo(ctx),
	}
}

func (c *externalStorageDataConverter) WithContext(ctx context.Context) converter.DataConverter {
	innerConverter := c.innerConverter
	if contextAwareInnerConverter, ok := c.innerConverter.(ContextAware); ok {
		innerConverter = contextAwareInnerConverter.WithContext(ctx)
	}

	activityInfo := c.activityInfo
	if IsActivity(ctx) {
		activityInfo = GetActivityInfo(ctx)
	}

	return &externalStorageDataConverter{
		innerConverter: innerConverter,
		driverMap:      c.driverMap,
		selector:       c.selector,
		activityInfo:   activityInfo,
		workflowInfo:   c.workflowInfo,
	}
}

func (c *externalStorageDataConverter) shouldPassthrough() bool {
	// CONSIDER: Maybe we allow a single driver and no selector to mean "always use this driver".
	// This type of behavior would need to be consistent across all SDKs and well documented.
	return len(c.driverMap) == 0 || c.selector == nil
}

// driverNotFoundError is an error that indicates that the requested driver could not be found.
// This error should be retryable.
type driverNotFoundError struct {
	name string
}

func (err driverNotFoundError) Error() string {
	return fmt.Sprintf("driver %s not found", err.name)
}

func NewDriverNotFoundError(name string) error {
	return driverNotFoundError{name: name}
}
