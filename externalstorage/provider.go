package externalstorage

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

// A struct that storage providers use to represent an externally stored payload.
// It contains information for indiciating which driver was used to store the
// payload and the claim that can be used with that driver to retrieve the payload
// at a later time.
type ExternalStorageReference struct {
	// The name of the storage driver used to store a payload.
	Name string

	// The claim associated with the externally stored payload.
	Claim ExternalStorageClaim
}

// An interface that represents an external storage provider. This provider can
// use an means of storing payloads and opt to not store payloads if it is advantageous
// to pass them through.
type ExternalStorageProvider interface {
	// Request to store a list of payloads in external storage. The provider returns
	// an ExternalPayloadReference when it stores the payload or nil when it declines
	// to store the payload.
	Store(context context.Context, payloads []*commonpb.Payload) ([]*ExternalStorageReference, error)

	// Request to retrieve a list of payloads associated with the provided payload references.
	Retrieve(context context.Context, references []ExternalStorageReference) ([]*commonpb.Payload, error)
}
