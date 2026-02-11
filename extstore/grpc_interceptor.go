package extstore

import (
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
)

type ExternalStorageGRPCClientInteceptorOptions struct {
	Drivers        []ExternalStorageDriver
	Selector       ExternalStorageDriverSelector
	ExternalCodecs []converter.PayloadCodec
}

// NewExternalStorageGRPCClientInterceptor returns a GRPC Client Interceptor that will store
// payloads external of Temporal using the specified storage options.
func NewExternalStorageGRPCClientInterceptor(options ExternalStorageOptions) (grpc.UnaryClientInterceptor, error) {
	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{})
}
