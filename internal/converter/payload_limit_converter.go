package converter

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/proxy"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
)

type PayloadLimitDataConverter struct {
	innerConverter    converter.DataConverter
	checkErrorLimitFn func(int64) error
	options           PayloadLimitDataConverterOptions
}

type PayloadLimitDataConverterOptions struct {
	Logger             log.Logger
	PayloadSizeWarning int
}

type PayloadSizeError struct {
	message string
}

func (e PayloadSizeError) Error() string {
	return e.message
}

type PayloadErrorLimits struct {
	PayloadSizeError int64
}

func NewPayloadLimitDataConverter(innerConverter converter.DataConverter, logger log.Logger) PayloadLimitDataConverter {
	return NewPayloadLimitDataConverterWithOptions(innerConverter, PayloadLimitDataConverterOptions{
		Logger:             logger,
		PayloadSizeWarning: 512 * 1024,
	})
}

func NewPayloadLimitDataConverterWithOptions(innerConverter converter.DataConverter, options PayloadLimitDataConverterOptions) PayloadLimitDataConverter {
	return PayloadLimitDataConverter{
		innerConverter: innerConverter,
		options:        options,
	}
}

func (dc *PayloadLimitDataConverter) SetErrorLimits(errorLimits *PayloadErrorLimits) {
	if errorLimits == nil || errorLimits.PayloadSizeError <= 0 {
		dc.checkErrorLimitFn = nil
	} else {
		dc.checkErrorLimitFn = func(payloadsSize int64) error {
			if payloadsSize > errorLimits.PayloadSizeError {
				return PayloadSizeError{message: "[TMPRL1103] Attempted to upload payloads with size that exceeded the error limit."}
			}
			return nil
		}
	}
}

func (dc PayloadLimitDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := dc.innerConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}
	payloads := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{payload},
	}
	err = proxy.VisitPayloads(context.TODO(), payloads, proxy.VisitPayloadsOptions{
		Visitor: dc.checkPayloadSizeVisitor,
	})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (dc PayloadLimitDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	return dc.innerConverter.FromPayload(payload, valuePtr)
}

func (dc PayloadLimitDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	payloads, err := dc.innerConverter.ToPayloads(value...)
	if err != nil {
		return nil, err
	}
	err = proxy.VisitPayloads(context.TODO(), payloads, proxy.VisitPayloadsOptions{
		Visitor: dc.checkPayloadSizeVisitor,
	})
	if err != nil {
		return nil, err
	}
	return payloads, nil
}

func (dc PayloadLimitDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	return dc.innerConverter.FromPayloads(payloads, valuePtrs...)
}

func (dc PayloadLimitDataConverter) ToString(input *commonpb.Payload) string {
	return dc.innerConverter.ToString(input)
}

func (dc PayloadLimitDataConverter) ToStrings(input *commonpb.Payloads) []string {
	return dc.innerConverter.ToStrings(input)
}

func (dc PayloadLimitDataConverter) checkPayloadSizeVisitor(vpc *proxy.VisitPayloadsContext, payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	var totalSize int64
	for _, payload := range payloads {
		if payload != nil {
			totalSize += int64(payload.Size())
		}
	}
	if dc.checkErrorLimitFn != nil {
		if err := dc.checkErrorLimitFn(totalSize); err != nil {
			return nil, err
		}
	}
	if dc.options.PayloadSizeWarning > 0 && totalSize > int64(dc.options.PayloadSizeWarning) && dc.options.Logger != nil {
		dc.options.Logger.Warn("[TMPRL1103] Attempted to upload payloads with size that exceeded the warning limit.")
	}
	return payloads, nil
}
