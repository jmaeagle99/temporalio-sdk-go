package externalstorage

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	ext "go.temporal.io/sdk/externalstorage"
)

// externalStorageDataConverter is a DataConverter for storing and retrieving payloads to and from external storage.
type externalStorageDataConverter struct {
	// The data converter used to convert the storage claim that is produced from storing a payload value.
	outputConverter converter.DataConverter

	// The data converter used to convert the value before storing into external storage.
	valueConverter converter.DataConverter

	// The provider used to store and retrieve payloads from external storage.
	provider ext.ExternalStorageProvider
}

// Create a data converter that uses the storage provider to store payloads externally. The inner converter is used for converting
// both input values, generated storage claims, and payloads that are passed through.
func NewExternalStorageDataConverter(converter converter.DataConverter, provider ext.ExternalStorageProvider) converter.DataConverter {
	return NewExternalStorageDataConverter2(converter, provider, converter)
}

// Create a data converter that uses the storage provider to store payloads externally. The inner converter is used for converting
// both input values whereas the output converter is used for converting storage claims and payloads that are passed through.
// TODO: Better name
func NewExternalStorageDataConverter2(converter converter.DataConverter, provider ext.ExternalStorageProvider, outputConverter converter.DataConverter) converter.DataConverter {
	return &externalStorageDataConverter{
		outputConverter: outputConverter,
		valueConverter:  converter,
		provider:        provider,
	}
}

func (c *externalStorageDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	valuePayload, err := c.valueConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}

	references, err := c.provider.Store(context.TODO(), []*commonpb.Payload{valuePayload})
	if err != nil {
		return nil, err
	}

	if len(references) != 1 {
		return nil, fmt.Errorf("reference count didn't match payload count")
	}

	reference := references[0]
	if reference == nil {
		if c.outputConverter == c.valueConverter {
			return valuePayload, nil
		} else {
			return c.outputConverter.ToPayload(value)
		}
	}

	referencePayload, err := c.outputConverter.ToPayload(reference)
	referencePayload.ExternalPayloads = append(referencePayload.ExternalPayloads, &commonpb.Payload_ExternalPayloadDetails{
		SizeBytes: int64(valuePayload.Size()),
	})
	return referencePayload, nil
}

func (c *externalStorageDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	if len(payload.ExternalPayloads) == 0 {
		// This is not a external payload claim
		return c.valueConverter.FromPayload(payload, valuePtr)
	}

	var reference ext.ExternalStorageReference
	err := c.outputConverter.FromPayload(payload, &reference)
	if err != nil {
		return err
	}

	valuePayload, err := c.provider.Retrieve(context.TODO(), []ext.ExternalStorageReference{reference})
	if err != nil {
		return err
	}

	return c.valueConverter.FromPayload(valuePayload[0], valuePtr)
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
