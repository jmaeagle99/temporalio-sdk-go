package converter

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type PayloadHandleDataConverter struct {
	innerConverter converter.DataConverter
}

func NewPayloadHandleDataConverter(innerConverter converter.DataConverter) converter.DataConverter {
	return &PayloadHandleDataConverter{
		innerConverter: innerConverter,
	}
}

func (dc *PayloadHandleDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payloadHandle, ok := value.(converter.PayloadHandle)
	if ok {
		return payloadHandle.Payload()
	}
	return dc.innerConverter.ToPayload(value)
}

func (dc *PayloadHandleDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	payloadHandle, ok := valuePtr.(*converter.PayloadHandle)
	if ok {
		payloadHandle.Initialize(dc.innerConverter, payload)
		return nil
	}
	return dc.innerConverter.FromPayload(payload, valuePtr)
}

func (dc *PayloadHandleDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
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

func (dc *PayloadHandleDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
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

func (dc *PayloadHandleDataConverter) ToString(input *commonpb.Payload) string {
	return dc.innerConverter.ToString(input)
}

func (dc *PayloadHandleDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return dc.innerConverter.ToStrings(input)
}
