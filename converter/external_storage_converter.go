package converter

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	externalstorage "go.temporal.io/sdk/externalstorage"
)

// A DataConverter for storing and retrieving payloads to and from external storage.
type externalStorageDataConverter struct {
	// The data converter used to convert the storage claim that is produced from storing a payload value.
	claimConverter DataConverter
	// The data converter used to convert the value before storing into external storage.
	valueConverter DataConverter
	// The provider used to store and retrieve payloads from external storage.
	provider externalstorage.ExternalStorageProvider
}

func NewExternalStorageDataConverter(innerConverter DataConverter, provider externalstorage.ExternalStorageProvider) DataConverter {
	return &externalStorageDataConverter{
		claimConverter: innerConverter,
		valueConverter: innerConverter,
		provider:       provider,
	}
}

var externalDriverKey = "externalDriver"

func (dc *externalStorageDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	value_payload, err := dc.valueConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}

	storageReferences, err := dc.provider.Store(context.TODO(), []*commonpb.Payload{value_payload})
	if err != nil {
		return nil, err
	}

	if len(storageReferences) != 1 {
		return nil, fmt.Errorf("")
	}

	storageReference := storageReferences[0]
	if storageReference == nil {
		return value_payload, nil
	}

	referencePayload, err := dc.claimConverter.ToPayload(storageReference)
	referencePayload.Metadata[externalDriverKey] = []byte(storageReference.Name)
	referencePayload.ExternalPayloads = append(referencePayload.ExternalPayloads, &commonpb.Payload_ExternalPayloadDetails{
		SizeBytes: int64(value_payload.Size()),
	})
	return referencePayload, nil
}

func (dc *externalStorageDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	_, hasDriverName := payload.Metadata[externalDriverKey]
	if !hasDriverName {
		// This is not a external payload claim
		return dc.valueConverter.FromPayload(payload, valuePtr)
	}

	var reference externalstorage.ExternalStorageReference
	err := dc.claimConverter.FromPayload(payload, &reference)
	if err != nil {
		return err
	}

	value_payload, err := dc.provider.Retrieve(context.TODO(), []externalstorage.ExternalStorageReference{reference})
	if err != nil {
		return err
	}

	return dc.valueConverter.FromPayload(value_payload[0], valuePtr)
}

func (dc *externalStorageDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	result := &commonpb.Payloads{}
	for _, v := range value {
		payload, err := dc.ToPayload(v)
		if err != nil {
			return nil, err
		}
		result.Payloads = append(result.Payloads, payload)
	}
	return result, nil
}

func (dc *externalStorageDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if payloads == nil {
		return nil
	}

	for i, payload := range payloads.GetPayloads() {
		if i >= len(valuePtrs) {
			break
		}
		err := dc.FromPayload(payload, valuePtrs[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (dc *externalStorageDataConverter) ToString(input *commonpb.Payload) string {
	return ""
}

func (dc *externalStorageDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return []string{}
}
