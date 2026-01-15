package converter

import (
	commonpb "go.temporal.io/api/common/v1"
)

type PayloadLimit struct {
	Disabled bool
	Size     int
}

type PayloadLimitDataConverter struct {
	parent                    DataConverter
	MemoUploadErrorLimit      *PayloadLimit
	MemoUploadWarningLimit    *PayloadLimit
	PayloadUploadErrorLimit   *PayloadLimit
	PayloadUploadWarningLimit *PayloadLimit
}

func NewPayloadLimitDataConverter(parent DataConverter) PayloadLimitDataConverter {
	return PayloadLimitDataConverter{
		parent: parent,
	}
}

// ToPayload converts single value to payload.
func (dc *PayloadLimitDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := dc.parent.ToPayload(value)
	if err != nil {
		return payload, err
	}
	// TODO: Validate payload size
	return payload, nil
}

// FromPayload converts single value from payload.
func (dc *PayloadLimitDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	return dc.parent.FromPayload(payload, valuePtr)
}

// ToPayloads converts a list of values.
func (dc *PayloadLimitDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	payloads, err := dc.parent.ToPayloads(value)
	if err != nil {
		return payloads, err
	}
	// TODO: Validate payloads size
	return payloads, nil
}

// FromPayloads converts to a list of values of different types.
func (dc *PayloadLimitDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	return dc.parent.FromPayloads(payloads, valuePtrs)
}

// ToString converts payload object into human readable string.
func (dc *PayloadLimitDataConverter) ToString(input *commonpb.Payload) string {
	return dc.parent.ToString(input)
}

// ToStrings converts payloads object into human readable strings.
func (dc *PayloadLimitDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return dc.parent.ToStrings(input)
}
