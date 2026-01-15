package converter

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

type ExternalStorageContext struct {
	Context       context.Context
	DataConverter DataConverter
}

type ExternalStorageProvider interface {
	Store(context ExternalStorageContext, payload *commonpb.Payload) (*commonpb.Payload, error)
	Retrieve(context ExternalStorageContext, payload *commonpb.Payload) (*commonpb.Payload, error)
}

type ExternalPayloadReference struct {
	Data map[string][]byte
}

type ExternalStorageDriver interface {
	GetName() string
	Store(payload *commonpb.Payload) (ExternalPayloadReference, error)
	Retrieve(reference ExternalPayloadReference) (*commonpb.Payload, error)
}

type ExternalStorageDataConverter struct {
	underlyingDataConverter DataConverter
	provider                ExternalStorageProvider
}

func NewExternalStorageDataConverter(underlyingDataConverter DataConverter, provider ExternalStorageProvider) DataConverter {
	return &ExternalStorageDataConverter{
		underlyingDataConverter: underlyingDataConverter,
		provider:                provider,
	}
}

func (dc *ExternalStorageDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := dc.underlyingDataConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}

	context := ExternalStorageContext{
		Context:       context.TODO(),
		DataConverter: dc.underlyingDataConverter,
	}

	return dc.provider.Store(context, payload)
}

func (dc *ExternalStorageDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	context := ExternalStorageContext{
		Context:       context.TODO(),
		DataConverter: dc.underlyingDataConverter,
	}

	data_payload, err := dc.provider.Retrieve(context, payload)
	if err != nil {
		return err
	}

	return dc.underlyingDataConverter.FromPayload(data_payload, valuePtr)
}

func (dc *ExternalStorageDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	return nil, nil
}

func (dc *ExternalStorageDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	return nil
}

func (dc *ExternalStorageDataConverter) ToString(input *commonpb.Payload) string {
	return ""
}

func (dc *ExternalStorageDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return []string{}
}
