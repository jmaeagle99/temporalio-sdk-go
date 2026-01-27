package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/extstore"
	ilog "go.temporal.io/sdk/internal/log"
)

type InMemoryStorageDriver struct {
	nextClaimId int
	mapping     map[string]*commonpb.Payload
}

func NewInMemoryStorageDriver() extstore.ExternalStorageDriver {
	return &InMemoryStorageDriver{
		nextClaimId: 1,
		mapping:     map[string]*commonpb.Payload{},
	}
}

func (driver *InMemoryStorageDriver) Name() string {
	return "in-memory"
}

func (driver *InMemoryStorageDriver) Type() string {
	return "in-memory"
}

func (driver *InMemoryStorageDriver) Store(ctx context.Context, payloads []*commonpb.Payload) ([]extstore.ExternalStorageClaim, error) {
	claims := make([]extstore.ExternalStorageClaim, len(payloads))
	for index, payload := range payloads {
		claimId := fmt.Sprintf("%d", driver.nextClaimId)
		driver.nextClaimId++
		driver.mapping[claimId] = payload
		claims[index] = extstore.ExternalStorageClaim{
			Data: map[string][]byte{"claim-id": []byte(claimId)},
		}
	}
	return claims, nil
}

func (driver *InMemoryStorageDriver) Retrieve(ctx context.Context, claims []extstore.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}

func TestPayloadHandles(t *testing.T) {
	options := extstore.NewWithThreshold(NewInMemoryStorageDriver(), 128)

	extStoreConverter, err := NewExternalStorageDataConverter(
		converter.NewCodecDataConverter(
			converter.GetDefaultDataConverter(),
			converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
		),
		options,
	)

	chainedConverter, _ := NewPayloadLimitDataConverterWithOptions(
		NewPayloadHandleDataConverter(extStoreConverter),
		ilog.NewDefaultLogger(),
		PayloadLimitOptions{
			PayloadSizeWarning: 256,
		},
	)

	expected_value := "I'm a little teapot, short and stout."

	payload, err := chainedConverter.ToPayload(expected_value)
	assert.NoError(t, err)

	var handle converter.PayloadHandle
	err = chainedConverter.FromPayload(payload, &handle)
	assert.NoError(t, err)

	var handle1_value string
	err = handle.Value(&handle1_value)
	assert.NoError(t, err)

	assert.Equal(t, expected_value, handle1_value)

	expected_value2 := "Here is my handle, here is my spout."

	payloads, err := chainedConverter.ToPayloads(expected_value2)
	assert.NoError(t, err)

	var handle2 converter.PayloadHandle
	err = chainedConverter.FromPayloads(payloads, &handle2)
	assert.NoError(t, err)

	var handle2_value string
	err = handle2.Value(&handle2_value)
	assert.NoError(t, err)

	assert.Equal(t, expected_value2, handle2_value)
}
