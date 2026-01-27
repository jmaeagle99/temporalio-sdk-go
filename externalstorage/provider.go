package externalstorage

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

// ExternalStorageReference represents an externally stored payload.
// It contains information for indicating which driver was used to store the
// payload and the claim that can be used with that driver to retrieve the payload
// at a later time.
type ExternalStorageReference struct {
	// The name of the storage driver used to store a payload.
	Name string

	// The claim associated with the externally stored payload.
	Claim ExternalStorageClaim
}

// ExternalStorageProvider defines an external storage provider. This provider can
// use any means of storing payloads and opt to not store payloads if it is advantageous
// to pass them through.
type ExternalStorageProvider interface {
	// Drivers returns the list of drivers used by the storage provider.
	Drivers() []ExternalStorageDriver

	// Store requests to store a list of payloads in external storage. The provider returns
	// an ExternalPayloadReference when it stores the payload or nil when it declines
	// to store the payload.
	Store(ctx context.Context, payloads []*commonpb.Payload) ([]*ExternalStorageReference, error)

	// Retrieve requests to retrieve a list of payloads associated with the provided payload references.
	Retrieve(ctx context.Context, references []ExternalStorageReference) ([]*commonpb.Payload, error)
}

type driverNotFoundError struct {
	name string
}

func (err driverNotFoundError) Error() string {
	return fmt.Sprintf("driver %s not found in storage provider", err.name)
}

func NewDriverNotFoundError(name string) error {
	return driverNotFoundError{name: name}
}
