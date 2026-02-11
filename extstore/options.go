package extstore

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// ExternalStorageDriverContext is a struct that contains contextual information
// that a drivers and the driver selector can use to make informed decisions about
// driver selection and storage metadata.
type ExternalStorageDriverContext struct {
	context.Context
	ActivityInfo any
	WorkflowInfo any
}

// ExternalStorageDriverSelector allows callers to provide custom logic for selecting which
// storage driver to use for each payload. The selector receives a context and
// and the payload to be stored. It should return the name of the driver used
// to store the payload externally, or empty string to pass the payload
// through without external storage.
type ExternalStorageDriverSelector func(ExternalStorageDriverContext, *commonpb.Payload) (ExternalStorageDriver, error)

// ExternalStorageOptions is a struct for providing configuration of external
// storage. It contains a list of ExternalStorageDriver and a selector function
// to determine how to externally store a payload.
type ExternalStorageOptions struct {
	Drivers              []ExternalStorageDriver
	Selector             ExternalStorageDriverSelector
	PayloadSizeThreshold int
	ExternalConverter    converter.DataConverter
}
