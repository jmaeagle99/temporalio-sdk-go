package converter

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
)

type PayloadLimitDataConverter struct {
	innerConverter converter.DataConverter
	visitorOptions *proxy.VisitPayloadsOptions
}

type PayloadLimitDataConverterOptions struct {
	DisableErrorLimit  bool
	Logger             log.Logger
	PayloadSizeError   int
	PayloadSizeWarning int
}

type PayloadSizeError struct {
	message string
}

func (e PayloadSizeError) Error() string {
	return e.message
}

func NewPayloadLimitDataConverter(innerConverter converter.DataConverter, options PayloadLimitDataConverterOptions) converter.DataConverter {
	var visitorOptions *proxy.VisitPayloadsOptions
	if !options.DisableErrorLimit || options.PayloadSizeWarning > 0 {
		visitorOptions = &proxy.VisitPayloadsOptions{
			Visitor: func(vpc *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
				var totalSize int
				for _, payload := range payloads {
					if payload != nil {
						totalSize += payload.Size()
					}
				}
				if !options.DisableErrorLimit && totalSize > options.PayloadSizeError {
					return nil, PayloadSizeError{message: "[TMPRL1103] Attempted to upload payloads with size that exceeded the error limit."}
				}
				if totalSize > options.PayloadSizeWarning && options.Logger != nil {
					options.Logger.Warn("[TMPRL1103] Attempted to upload payloads with size that exceeded the warning limit.")
				}
				return payloads, nil
			},
		}
	}
	return &PayloadLimitDataConverter{
		innerConverter: innerConverter,
		visitorOptions: visitorOptions,
	}
}

func (dc *PayloadLimitDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := dc.innerConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}
	if dc.visitorOptions != nil {
		payloads := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{payload},
		}
		err = proxy.VisitPayloads(context.TODO(), payloads, *dc.visitorOptions)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func (dc *PayloadLimitDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	return dc.innerConverter.FromPayload(payload, valuePtr)
}

func (dc *PayloadLimitDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	payloads, err := dc.innerConverter.ToPayloads(value...)
	if err != nil {
		return nil, err
	}
	if dc.visitorOptions != nil {
		err = proxy.VisitPayloads(context.TODO(), payloads, *dc.visitorOptions)
		if err != nil {
			return nil, err
		}
	}
	return payloads, nil
}

func (dc *PayloadLimitDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	return dc.innerConverter.FromPayloads(payloads, valuePtrs...)
}

func (dc *PayloadLimitDataConverter) ToString(input *commonpb.Payload) string {
	return dc.innerConverter.ToString(input)
}

func (dc *PayloadLimitDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return dc.innerConverter.ToStrings(input)
}
