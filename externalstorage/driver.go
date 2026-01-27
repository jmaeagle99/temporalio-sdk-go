package externalstorage

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

// ExternalStorageClaim represents a storage claim
// for retrieving a payload at a later time. The data that is stored
// within the claim is driver-dependent and should only be used for retrieving
// the associated payload with the driver.
type ExternalStorageClaim struct {
	// A mapping of any data that the driver would like to use in identifying
	// the associated payload.
	Data map[string][]byte
}

// ExternalStorageDriver defines a storage driver used to store payloads outside
// of Temporal in some external storage system such as AWS S3.
type ExternalStorageDriver interface {
	// Name returns the name of the storage driver. A storage driver may choose to
	// allow this to parameterized upon creation to allow multiple instances of
	// the same driver type to be used by storage providers.
	Name() string

	// Type returns the type of the storage driver. This string should be the same
	// across all instantiations of the same driver struct.
	Type() string

	// Store stores a list of payloads in external storage and returns a storage claim
	// for each uploaded payload.
	Store(ctx context.Context, payloads []*commonpb.Payload) ([]ExternalStorageClaim, error)

	// Retrieve retrieves a list of payloads from external storage that are associated
	// with the provided list of storage claims.
	Retrieve(ctx context.Context, claims []ExternalStorageClaim) ([]*commonpb.Payload, error)
}

type payloadNotFoundError struct {
	driver ExternalStorageDriver
}

func (err payloadNotFoundError) Error() string {
	return fmt.Sprintf("Payload not found for driver %s", err.driver.Name())
}

func NewPayloadNotFoundError(driver ExternalStorageDriver) error {
	return payloadNotFoundError{
		driver: driver,
	}
}
