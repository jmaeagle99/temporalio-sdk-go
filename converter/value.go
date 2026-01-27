package converter

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

type (
	// EncodedValue is used to encapsulate/extract encoded value from workflow/activity.
	EncodedValue interface {
		// HasValue return whether there is value encoded.
		HasValue() bool
		// Get extract the encoded value into strong typed value pointer.
		//
		// Note, values should not be reused for extraction here because merging on
		// top of existing values may result in unexpected behavior similar to
		// json.Unmarshal.
		Get(valuePtr interface{}) error
	}

	// EncodedValues is used to encapsulate/extract encoded one or more values from workflow/activity.
	EncodedValues interface {
		// HasValues return whether there are values encoded.
		HasValues() bool
		// Get extract the encoded values into strong typed value pointers.
		//
		// Note, values should not be reused for extraction here because merging on
		// top of existing values may result in unexpected behavior similar to
		// json.Unmarshal.
		Get(valuePtr ...interface{}) error
	}

	// RawValue is a representation of an unconverted, raw payload.
	//
	// This type can be used as a parameter or return type in workflows and activities to pass through
	// a raw payload. Encoding/decoding of the payload is still done by the system. A RawValue enabled
	// payload converter is required for this.
	RawValue struct {
		payload *commonpb.Payload
	}

	// PayloadHandle is a representation of a payload that allows for defered acquisition of its value.
	//
	// This type can be used as a argument or return type in workflows and activities to pass through
	// a reference to the payload. Encoding/decoding of the payload is defered until retrieving the
	// associated value is explicitly invoked. PayloadHandle enabled payload and data converters are
	// required for this type to be useable.
	PayloadHandle struct {
		converter DataConverter
		payload   *commonpb.Payload
		metadata  *commonpb.Payload
	}
)

// NewRawValue creates a new RawValue instance.
func NewRawValue(payload *commonpb.Payload) RawValue {
	return RawValue{payload: payload}
}

func (v RawValue) Payload() *commonpb.Payload {
	return v.payload
}

func (v RawValue) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("RawValue is not JSON serializable")
}

func (v *RawValue) UnmarshalJSON(b []byte) error {
	return fmt.Errorf("RawValue is not JSON serializable")
}

func (h *PayloadHandle) Initialize(converter DataConverter, valuePayload *commonpb.Payload, metadataPayload *commonpb.Payload) {
	h.converter = converter
	h.payload = valuePayload
	h.metadata = metadataPayload
}

func (h *PayloadHandle) Payload() (*commonpb.Payload, error) {
	return h.payload, nil
}

func (h *PayloadHandle) Value(valuePtr interface{}) error {
	return h.converter.FromPayload(h.payload, valuePtr)
}

func (h *PayloadHandle) Metadata(valuePtr interface{}) error {
	return h.converter.FromPayload(h.metadata, valuePtr)
}
