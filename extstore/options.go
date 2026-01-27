package extstore

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

// DriverSelectorContext is a struct that contains contextual information
// that a DriverSelector can use to choose the most appropriate driver to
// store a payload.
type DriverSelectorContext struct {
	context.Context
}

// DriverSelector allows callers to provide custom logic for selecting which
// storage driver to use for each payload. The selector receives a context and
// and the payload to be stored. It should return the name of the driver used
// to store the payload externally, or empty string to pass the payload
// through without external storage.
type DriverSelector func(DriverSelectorContext, *commonpb.Payload) (string, error)

// ExternalStorageOptions is a struct for providing configuration of external
// storage. It contains a list of ExternalStorageDriver and a selector function
// to determine how to externally store a payload.
type ExternalStorageOptions struct {
	Drivers  []ExternalStorageDriver
	Selector DriverSelector
}

const DefaultThreshold = 100 * 1024

// NewWithThreshold creates ExternalStorageOptions for a single driver and a threshold
// value over which payloads will be stored using the driver.
func NewWithThreshold(driver ExternalStorageDriver, threshold int) ExternalStorageOptions {
	return ExternalStorageOptions{
		Drivers: []ExternalStorageDriver{driver},
		Selector: func(ctx DriverSelectorContext, payload *commonpb.Payload) (string, error) {
			if payload.Size() > threshold {
				driver.Name()
			}
			return "", nil
		},
	}
}
