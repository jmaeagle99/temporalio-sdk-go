package internal

import (
	"context"
	"sync/atomic"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/log"
)

// Options for when payload sizes exceed limits.
//
// Exposed as: [go.temporal.io/sdk/client.PayloadLimitOptions]
type PayloadLimitOptions struct {
	// The limit (in bytes) at which a memo size warning is logged.
	MemoSizeWarning int
	// The limit (in bytes) at which a payload size warning is logged.
	PayloadSizeWarning int
}

type payloadSizeError struct {
	message string
	size    int64
	limit   int64
}

func (e payloadSizeError) Error() string {
	return e.message
}

type payloadErrorLimits struct {
	MemoSizeError    int64
	PayloadSizeError int64
}

// payloadProcessingDataConverter is a converter that wraps another data converter and applies post conversion operations
// to payloads, such as payload size limit logging and enforcement. Future operations may be added to create an internal
// centralized pipeline of transformations and validations in one converter.
type payloadProcessingDataConverter struct {
	converter.DataConverter
	errorLimits        atomic.Pointer[payloadErrorLimits]
	logger             log.Logger
	memoSizeWarning    int
	payloadSizeWarning int
	panicOnError       bool
}

func newPayloadProcessingDataConverter(
	innerConverter converter.DataConverter,
	logger log.Logger,
	payloadLimitOptions PayloadLimitOptions,
) (converter.DataConverter, func(*payloadErrorLimits)) {
	memoSizeWarning := 2 * kb
	if payloadLimitOptions.MemoSizeWarning != 0 {
		memoSizeWarning = payloadLimitOptions.MemoSizeWarning
	}
	payloadSizeWarning := 512 * kb
	if payloadLimitOptions.PayloadSizeWarning != 0 {
		payloadSizeWarning = payloadLimitOptions.PayloadSizeWarning
	}
	dataConverter := &payloadProcessingDataConverter{
		DataConverter:      innerConverter,
		logger:             logger,
		memoSizeWarning:    memoSizeWarning,
		payloadSizeWarning: payloadSizeWarning,
		panicOnError:       false,
	}
	return dataConverter, dataConverter.SetErrorLimits
}

func (c *payloadProcessingDataConverter) SetErrorLimits(errorLimits *payloadErrorLimits) {
	c.errorLimits.Store(errorLimits)
}

func (c *payloadProcessingDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	if handle, ok := value.(converter.PayloadHandle); ok {
		// Handles already have a payload that can be directly returned
		return handle.Payload(), nil
	}
	payload, err := c.DataConverter.ToPayload(value)
	if err != nil {
		return nil, err
	}
	err = c.checkSize([]*commonpb.Payload{payload})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *payloadProcessingDataConverter) ToPayloads(value ...interface{}) (*commonpb.Payloads, error) {
	if len(value) == 0 {
		return c.DataConverter.ToPayloads(value...)
	}

	// Single value fast path
	if len(value) == 1 {
		payload, err := c.ToPayload(value[0])
		if err != nil {
			return nil, err
		}
		payloads := []*commonpb.Payload{payload}
		if err := c.checkSize(payloads); err != nil {
			return nil, err
		}
		return &commonpb.Payloads{Payloads: payloads}, nil
	}

	finalPayloads := make([]*commonpb.Payload, len(value))
	nonHandleValues := make([]interface{}, 0, len(value))
	nonHandleIndices := make([]int, 0, len(value))

	for i, v := range value {
		if handle, ok := v.(converter.PayloadHandle); ok {
			// Handles already have a payload that can be directly returned
			finalPayloads[i] = handle.Payload()
		} else {
			nonHandleValues = append(nonHandleValues, v)
			nonHandleIndices = append(nonHandleIndices, i)
		}
	}

	// Non-handle values are converted and payloads are written in correct positions
	if len(nonHandleValues) > 0 {
		nonHandlePayloads, err := c.DataConverter.ToPayloads(nonHandleValues...)
		if err != nil {
			return nil, err
		}

		for i, idx := range nonHandleIndices {
			if i < len(nonHandlePayloads.Payloads) {
				finalPayloads[idx] = nonHandlePayloads.Payloads[i]
			}
		}
	}

	err := c.checkSize(finalPayloads)
	if err != nil {
		return nil, err
	}

	return &commonpb.Payloads{Payloads: finalPayloads}, nil
}

func (c *payloadProcessingDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	if handle, ok := valuePtr.(*converter.PayloadHandle); ok {
		handle.Initialize(c.DataConverter, payload)
		return nil
	}
	return c.DataConverter.FromPayload(payload, valuePtr)
}

func (c *payloadProcessingDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if payloads == nil {
		return nil
	}
	if len(valuePtrs) == 0 {
		return c.DataConverter.FromPayloads(payloads, valuePtrs...)
	}

	// Single value fast path
	if len(valuePtrs) == 1 && len(payloads.Payloads) > 0 {
		return c.FromPayload(payloads.Payloads[0], valuePtrs[0])
	}

	nonHandlePayloads := make([]*commonpb.Payload, 0, len(valuePtrs))
	nonHandleValuePtrs := make([]interface{}, 0, len(valuePtrs))

	// Initialize handles with their payloads and the inner converter.
	for i, valuePtr := range valuePtrs {
		if i >= len(payloads.Payloads) {
			break
		}
		if handle, ok := valuePtr.(*converter.PayloadHandle); ok {
			handle.Initialize(c.DataConverter, payloads.Payloads[i])
		} else {
			nonHandlePayloads = append(nonHandlePayloads, payloads.Payloads[i])
			nonHandleValuePtrs = append(nonHandleValuePtrs, valuePtr)
		}
	}

	// Convert all non-handle payloads to their values
	return c.DataConverter.FromPayloads(&commonpb.Payloads{Payloads: nonHandlePayloads}, nonHandleValuePtrs...)
}

func (c *payloadProcessingDataConverter) WithWorkflowContext(ctx Context) converter.DataConverter {
	innerConverter := c.DataConverter
	if contextAwareInnerConverter, ok := c.DataConverter.(ContextAware); ok {
		innerConverter = contextAwareInnerConverter.WithWorkflowContext(ctx)
	}

	newConverter := &payloadProcessingDataConverter{
		DataConverter:      innerConverter,
		logger:             GetLogger(ctx),
		memoSizeWarning:    c.memoSizeWarning,
		payloadSizeWarning: c.payloadSizeWarning,
		panicOnError:       true,
	}
	newConverter.errorLimits.Store(c.errorLimits.Load())
	return newConverter
}

func (c *payloadProcessingDataConverter) WithContext(ctx context.Context) converter.DataConverter {
	logger := c.logger
	if IsActivity(ctx) {
		logger = GetActivityLogger(ctx)
	}

	innerConverter := c.DataConverter
	if contextAwareInnerConverter, ok := c.DataConverter.(ContextAware); ok {
		innerConverter = contextAwareInnerConverter.WithContext(ctx)
	}

	newConverter := &payloadProcessingDataConverter{
		DataConverter:      innerConverter,
		logger:             logger,
		memoSizeWarning:    c.memoSizeWarning,
		payloadSizeWarning: c.payloadSizeWarning,
		panicOnError:       c.panicOnError,
	}
	newConverter.errorLimits.Store(c.errorLimits.Load())
	return newConverter
}

func (c *payloadProcessingDataConverter) CheckMemoSize(memo map[string]*commonpb.Payload) error {
	var totalSize int64
	for _, payload := range memo {
		if payload != nil {
			totalSize += int64(payload.Size())
		}
	}
	errorLimits := c.errorLimits.Load()
	if errorLimits != nil && errorLimits.MemoSizeError > 0 && totalSize > errorLimits.MemoSizeError {
		err := payloadSizeError{
			message: "[TMPRL1103] Attempted to upload memo with size that exceeded the error limit.",
			size:    totalSize,
			limit:   errorLimits.MemoSizeError,
		}
		if c.panicOnError {
			panic(err)
		}
		return err
	}
	if c.memoSizeWarning > 0 && totalSize > int64(c.memoSizeWarning) && c.logger != nil {
		c.logger.Warn(
			"[TMPRL1103] Attempted to upload memo with size that exceeded the warning limit.",
			tagPayloadSize, totalSize,
			tagPayloadSizeLimit, int64(c.memoSizeWarning),
		)
	}
	return nil
}

func (c *payloadProcessingDataConverter) checkSize(payloads []*commonpb.Payload) error {
	var totalSize int64
	for _, payload := range payloads {
		if payload != nil {
			totalSize += int64(payload.Size())
		}
	}
	errorLimits := c.errorLimits.Load()
	if errorLimits != nil && errorLimits.PayloadSizeError > 0 && totalSize > errorLimits.PayloadSizeError {
		err := payloadSizeError{
			message: "[TMPRL1103] Attempted to upload payloads with size that exceeded the error limit.",
			size:    totalSize,
			limit:   errorLimits.PayloadSizeError,
		}
		if c.panicOnError {
			panic(err)
		}
		return err
	}
	if c.payloadSizeWarning > 0 && totalSize > int64(c.payloadSizeWarning) && c.logger != nil {
		c.logger.Warn(
			"[TMPRL1103] Attempted to upload payloads with size that exceeded the warning limit.",
			tagPayloadSize, totalSize,
			tagPayloadSizeLimit, int64(c.payloadSizeWarning),
		)
	}
	return nil
}
