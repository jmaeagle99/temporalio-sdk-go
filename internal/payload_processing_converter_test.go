package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/log"
)

func createProcessingConverter() converter.DataConverter {
	dc, _ := newPayloadProcessingDataConverter(
		converter.GetDefaultDataConverter(),
		&log.NoopLogger{},
		PayloadLimitOptions{},
	)
	return dc
}

func createHandle(t *testing.T, value any) converter.PayloadHandle {
	var handle converter.PayloadHandle
	handle.Initialize(converter.GetDefaultDataConverter(), createPayload(t, value))
	return handle
}

func createPayload(t *testing.T, value any) *commonpb.Payload {
	dc := converter.GetDefaultDataConverter()
	payload, err := dc.ToPayload(value)
	assert.NoError(t, err)
	return payload
}

func toPayloads(payloads ...*commonpb.Payload) *commonpb.Payloads {
	return &commonpb.Payloads{Payloads: payloads}
}

type payloadProcessingTestStruct struct {
	TestField string
}

func TestToPayloadValue(t *testing.T) {
	dc := createProcessingConverter()
	value := 2.71828
	payload, err := dc.ToPayload(value)
	assert.NoError(t, err)
	assert.NotNil(t, payload)
}

func TestToPayloadHandle(t *testing.T) {
	dc := createProcessingConverter()
	handle := createHandle(t, 2.71828)
	payload, err := dc.ToPayload(handle)
	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.Equal(t, handle.Payload(), payload)
}

func TestToPayloadsNoValues(t *testing.T) {
	dc := createProcessingConverter()
	payloads, err := dc.ToPayloads()
	assert.NoError(t, err)
	assert.Nil(t, payloads)
}

func TestToPayloadsSingleValue(t *testing.T) {
	dc := createProcessingConverter()
	value := "some value"
	payloads, err := dc.ToPayloads(value)
	assert.NoError(t, err)
	assert.NotNil(t, payloads)
	assert.Len(t, payloads.Payloads, 1)
}

func TestToPayloadsSingleHandle(t *testing.T) {
	dc := createProcessingConverter()
	handle := createHandle(t, "some value")
	payloads, err := dc.ToPayloads(handle)
	assert.NoError(t, err)
	assert.NotNil(t, payloads)
	assert.Len(t, payloads.Payloads, 1)
	assert.Equal(t, handle.Payload(), payloads.Payloads[0])
}

func TestToPayloadsMultiValue(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "some value"
	value2 := 42
	value3 := true
	payloads, err := dc.ToPayloads(value1, value2, value3)
	assert.NoError(t, err)
	assert.NotNil(t, payloads)
	assert.Len(t, payloads.Payloads, 3)
}

func TestToPayloadsMultiHandle(t *testing.T) {
	dc := createProcessingConverter()
	handle1 := createHandle(t, "some value")
	handle2 := createHandle(t, 42)
	handle3 := createHandle(t, true)
	payloads, err := dc.ToPayloads(handle1, handle2, handle3)
	assert.NoError(t, err)
	assert.NotNil(t, payloads)
	assert.Len(t, payloads.Payloads, 3)
	assert.Equal(t, handle1.Payload(), payloads.Payloads[0])
	assert.Equal(t, handle2.Payload(), payloads.Payloads[1])
	assert.Equal(t, handle3.Payload(), payloads.Payloads[2])
}

func TestToPayloadsValuesAndHandles(t *testing.T) {
	dc := createProcessingConverter()
	handle1 := createHandle(t, "some value")
	value1 := 42
	handle2 := createHandle(t, true)
	value2 := 2.7
	payloads, err := dc.ToPayloads(handle1, value1, handle2, value2)
	assert.NoError(t, err)
	assert.NotNil(t, payloads)
	assert.Len(t, payloads.Payloads, 4)
	assert.Equal(t, handle1.Payload(), payloads.Payloads[0])
	assert.Equal(t, handle2.Payload(), payloads.Payloads[2])
}

func TestFromPayloadValue(t *testing.T) {
	dc := createProcessingConverter()
	value := "another value"
	payload := createPayload(t, value)
	var actual string
	assert.NoError(t, dc.FromPayload(payload, &actual))
	assert.Equal(t, value, actual)
}

func TestFromPayloadHandle(t *testing.T) {
	dc := createProcessingConverter()
	value := "another value"
	payload := createPayload(t, value)
	var actual converter.PayloadHandle
	assert.NoError(t, dc.FromPayload(payload, &actual))
	assert.Equal(t, payload, actual.Payload())
}

func TestFromPayloadsSingleValue(t *testing.T) {
	dc := createProcessingConverter()
	value := "some value"
	payload := createPayload(t, value)
	var actual string
	assert.NoError(t, dc.FromPayloads(toPayloads(payload), &actual))
	assert.Equal(t, value, actual)
}

func TestFromPayloadsSingleHandle(t *testing.T) {
	dc := createProcessingConverter()
	value := "some value"
	payload := createPayload(t, value)
	var actual converter.PayloadHandle
	assert.NoError(t, dc.FromPayloads(toPayloads(payload), &actual))
	assert.Equal(t, payload, actual.Payload())
}

func TestFromPayloadsMultiValue(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "some value"
	payload1 := createPayload(t, value1)
	value2 := 42
	payload2 := createPayload(t, value2)
	value3 := true
	payload3 := createPayload(t, value3)
	var actual1 string
	var actual2 int
	var actual3 bool
	assert.NoError(t, dc.FromPayloads(toPayloads(payload1, payload2, payload3), &actual1, &actual2, &actual3))
	assert.Equal(t, value1, actual1)
	assert.Equal(t, value2, actual2)
	assert.Equal(t, value3, actual3)
}

func TestFromPayloadsMultiHandle(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "some value"
	payload1 := createPayload(t, value1)
	value2 := 42
	payload2 := createPayload(t, value2)
	value3 := true
	payload3 := createPayload(t, value3)
	var actual1 converter.PayloadHandle
	var actual2 converter.PayloadHandle
	var actual3 converter.PayloadHandle
	assert.NoError(t, dc.FromPayloads(toPayloads(payload1, payload2, payload3), &actual1, &actual2, &actual3))
	assert.Equal(t, payload1, actual1.Payload())
	assert.Equal(t, payload2, actual2.Payload())
	assert.Equal(t, payload3, actual3.Payload())
}

func TestFromPayloadsValuesAndHandles(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "some value"
	payload1 := createPayload(t, value1)
	value2 := 42
	payload2 := createPayload(t, value2)
	value3 := true
	payload3 := createPayload(t, value3)
	value4 := 2.7
	payload4 := createPayload(t, value4)
	var actual1 string
	var actual2 converter.PayloadHandle
	var actual3 converter.PayloadHandle
	var actual4 float64
	assert.NoError(t, dc.FromPayloads(toPayloads(payload1, payload2, payload3, payload4), &actual1, &actual2, &actual3, &actual4))
	assert.Equal(t, value1, actual1)
	assert.Equal(t, payload2, actual2.Payload())
	assert.Equal(t, payload3, actual3.Payload())
	assert.Equal(t, value4, actual4)
}

func TestFromPayloadsLessPayloadsThanValues(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "more values"
	payload1 := createPayload(t, value1)
	value2 := payloadProcessingTestStruct{TestField: "test value"}
	payload2 := createPayload(t, value2)
	var actual1 string
	var actual2 payloadProcessingTestStruct
	var actual3 int = 5
	assert.NoError(t, dc.FromPayloads(toPayloads(payload1, payload2), &actual1, &actual2, &actual3))
	assert.Equal(t, value1, actual1)
	assert.Equal(t, value2, actual2)
	assert.Equal(t, 5, actual3)
}

func TestFromPayloadsLessValuesThanPayloads(t *testing.T) {
	dc := createProcessingConverter()
	value1 := "more values"
	payload1 := createPayload(t, value1)
	value2 := payloadProcessingTestStruct{TestField: "test value"}
	payload2 := createPayload(t, value2)
	value3 := 17
	payload3 := createPayload(t, value3)
	var actual1 string
	var actual2 payloadProcessingTestStruct
	assert.NoError(t, dc.FromPayloads(toPayloads(payload1, payload2, payload3), &actual1, &actual2))
	assert.Equal(t, value1, actual1)
	assert.Equal(t, value2, actual2)
}
