package extstore

import (
	"go.temporal.io/api/proxy"
	"google.golang.org/grpc"
)

// NewExternalStorageGRPCClientInterceptor returns a GRPC Client Interceptor that will store
// payloads external of Temporal using the specified storage options.
func NewExternalStorageGRPCClientInterceptor(options ExternalStorageOptions) (grpc.UnaryClientInterceptor, error) {
	return proxy.NewPayloadVisitorInterceptor(proxy.PayloadVisitorInterceptorOptions{})
}
