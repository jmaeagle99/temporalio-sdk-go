package internal

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/extstore"
)

// externalStorageDataConverter is a DataConverter for storing and retrieving payloads to and from external storage.
type externalStorageDataConverter struct {
	innerConverter converter.DataConverter
	driverMap      map[string]extstore.ExternalStorageDriver
	selector       extstore.DriverSelector
}

// Create a data converter that uses the storage provider to store payloads externally. The inner converter is used for converting
// both input values, generated storage claims, and payloads that are passed through.
func NewExternalStorageDataConverter(converter converter.DataConverter, options extstore.ExternalStorageOptions) (converter.DataConverter, error) {
	if len(options.Drivers) == 0 {
		return nil, errors.New("must register at least one driver")
	}
	if options.Selector == nil {
		return nil, errors.New("selector cannot be nil")
	}

	driverMap := make(map[string]extstore.ExternalStorageDriver)

	for _, driver := range options.Drivers {
		name := driver.Name()
		if _, ok := driverMap[name]; ok {
			return nil, errors.New("driver name is not unique")
		}
		driverMap[name] = driver
	}

	return &externalStorageDataConverter{
		innerConverter: converter,
		driverMap:      driverMap,
		selector:       options.Selector,
	}, nil
}

// externalStorageReference represents an externally stored payload.
// It contains information for indicating which driver was used to store the
// payload and the claim that can be used with that driver to retrieve the payload
// at a later time.
type externalStorageReference struct {
	// The name of the storage driver used to store a payload.
	DriverName string

	// The claim associated with the externally stored payload.
	Claim extstore.ExternalStorageClaim
}

func (c *externalStorageDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	valuePayload, err := c.innerConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}

	ctx := context.TODO()
	selectorCtx := extstore.DriverSelectorContext{
		Context: ctx,
	}

	driverName, err := c.selector(selectorCtx, valuePayload)
	if err != nil {
		return nil, err
	}
	if driverName == "" {
		return valuePayload, nil
	}
	driver, ok := c.driverMap[driverName]
	if !ok {
		return nil, NewDriverNotFoundError(driverName)
	}

	claims, err := driver.Store(ctx, []*commonpb.Payload{valuePayload})
	if err != nil {
		return nil, err
	}

	if len(claims) != 1 {
		return nil, fmt.Errorf("reference count didn't match payload count")
	}

	referencePayload, err := c.innerConverter.ToPayload(externalStorageReference{
		DriverName: driverName,
		Claim:      claims[0],
	})
	referencePayload.ExternalPayloads = append(referencePayload.ExternalPayloads, &commonpb.Payload_ExternalPayloadDetails{
		SizeBytes: int64(valuePayload.Size()),
	})
	return referencePayload, nil
}

func (c *externalStorageDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
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

	valuePayload, err := driver.Retrieve(context.TODO(), []extstore.ExternalStorageClaim{reference.Claim})
	if err != nil {
		return err
	}

	return c.innerConverter.FromPayload(valuePayload[0], valuePtr)
}

func (c *externalStorageDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	result := &commonpb.Payloads{}
	// TODO: Parallelize
	for _, v := range value {
		payload, err := c.ToPayload(v)
		if err != nil {
			return nil, err
		}
		result.Payloads = append(result.Payloads, payload)
	}
	return result, nil
}

func (c *externalStorageDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if payloads == nil {
		return nil
	}

	// TODO: Parallelize
	for i, payload := range payloads.GetPayloads() {
		if i >= len(valuePtrs) {
			break
		}
		err := c.FromPayload(payload, valuePtrs[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *externalStorageDataConverter) ToString(input *commonpb.Payload) string {
	return ""
}

func (c *externalStorageDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return []string{}
}

type driverNotFoundError struct {
	name string
}

func (err driverNotFoundError) Error() string {
	return fmt.Sprintf("driver %s not found in storage provider", err.name)
}

func NewDriverNotFoundError(name string) error {
	return driverNotFoundError{name: name}
}
