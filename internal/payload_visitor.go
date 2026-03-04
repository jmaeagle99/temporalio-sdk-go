package internal

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
)

type PayloadVisitor interface {
	Visit(ctx *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error)
}
