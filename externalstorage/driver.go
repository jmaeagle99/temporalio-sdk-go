package externalstorage

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

// A struct that storage drivers use to represent a storage claim
// for retrieving a payload at a later time. The data that is stored
// within the claim is driver-dependent and should only be used for retrieving
// the associated payload with the driver.
type ExternalStorageClaim struct {
	// A mapping of any data that the driver would like to use in identifying
	// the associated payload.
	Data map[string][]byte
}

// An interface that represents a storage driver used to store payloads outside
// of Temporal in some external storage system such as AWS S3.
type ExternalStorageDriver interface {
	// Returns the name of the storage driver. A storage driver may choose to
	// allow this to parameterized upon creation to allow multiple instances of
	// the same driver type to be used by storage providers.
	GetName() string
	// Store a list of payloads in external storage and returns a storage claim
	// for each uploaded payload.
	Store(context context.Context, payloads []*commonpb.Payload) ([]ExternalStorageClaim, error)

	// Retrieves a list of paylods from external storage that are associated
	// with the provided list of storage claims.
	Retrieve(context context.Context, claims []ExternalStorageClaim) ([]*commonpb.Payload, error)
}
