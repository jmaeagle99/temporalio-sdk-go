package externalstorage

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

// ExternalStorageGRPCClientInterceptorOptions holds options for storing payloads
// external of Temporal using an ExternalStorageProvider.
type ExternalStorageGRPCClientInterceptorOptions struct {
	// Required: The external storage provider used to store payloads externally and exchange with claim payloads.
	Provider ExternalStorageProvider

	// Optional: Data converter used to convert claims into payloads. If not set, the SDK's dataconverter will be used.
	ClaimConverter converter.DataConverter
}

// NewExternalStorageGRPCClientInterceptor returns a GRPC Client Interceptor that will store
// payloads external of Temporal using the specified ExternalStorageProvider.
func NewExternalStorageGRPCClientInterceptor(options ExternalStorageGRPCClientInterceptorOptions) (grpc.UnaryClientInterceptor, error) {
	if options.Provider == nil {
		return nil, fmt.Errorf("Provider must be set for this interceptor to function")
	}

	provider := options.Provider
	claimConverter := options.ClaimConverter
	if claimConverter == nil {
		claimConverter = converter.GetDefaultDataConverter()
	}

	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{
		Outbound: &proxy.VisitPayloadsOptions{
			Visitor: func(vpc *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
				references, err := provider.Store(vpc.Context, payloads)
				if err != nil {
					return payloads, err
				}
				if len(references) != len(payloads) {
					return payloads, fmt.Errorf("Reference count is not the same as payload count")
				}
				results := make([]*commonpb.Payload, len(payloads))
				for index, reference := range references {
					if reference == nil {
						results[index] = payloads[index]
					} else {
						referencePayload, err := claimConverter.ToPayload(reference)
						if err != nil {
							return payloads, fmt.Errorf("Unable to convert claim to payload")
						}
						referencePayload.ExternalPayloads = append(referencePayload.ExternalPayloads, &commonpb.Payload_ExternalPayloadDetails{
							SizeBytes: int64(payloads[index].Size()),
						})
						results[index] = referencePayload
					}
				}
				return results, nil
			},
		},
		Inbound: &proxy.VisitPayloadsOptions{
			Visitor: func(vpc *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
				results := make([]*commonpb.Payload, len(payloads))
				references := make([]ExternalStorageReference, len(payloads))
				for index, payload := range payloads {
					if len(payload.ExternalPayloads) == 0 {
						// This is not a external payload claim
						results[index] = payload
					} else {
						err := claimConverter.FromPayload(payload, &references[index])
						if err != nil {
							return payloads, err
						}
					}
				}
				if len(references) > 0 {
					valuePayloads, err := provider.Retrieve(vpc.Context, references)
					if err != nil {
						return payloads, err
					}
					if len(valuePayloads) != len(references) {
						return payloads, fmt.Errorf("Reference count is not the same as payload count")
					}
					for index, valuePayload := range valuePayloads {
						if valuePayload != nil {
							results[index] = valuePayload
						}
					}
				}
				return results, nil
			},
		},
	})
}
