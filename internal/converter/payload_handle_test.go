package converter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/externalstorage"
	iexternalstorage "go.temporal.io/sdk/internal/externalstorage"
)

type InMemoryStorageDriver struct {
	nextClaimId int
	mapping     map[string]*commonpb.Payload
}

func NewInMemoryStorageDriver() externalstorage.ExternalStorageDriver {
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

func (driver *InMemoryStorageDriver) Store(ctx context.Context, payloads []*commonpb.Payload) ([]externalstorage.ExternalStorageClaim, error) {
	claims := make([]externalstorage.ExternalStorageClaim, len(payloads))
	for index, payload := range payloads {
		claimId := fmt.Sprintf("%d", driver.nextClaimId)
		driver.nextClaimId++
		driver.mapping[claimId] = payload
		claims[index] = externalstorage.ExternalStorageClaim{
			Data: map[string][]byte{"claim-id": []byte(claimId)},
		}
	}
	return claims, nil
}

func (driver *InMemoryStorageDriver) Retrieve(ctx context.Context, claims []externalstorage.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}

func TestPayloadHandles(t *testing.T) {
	chainedConverter := NewPayloadLimitDataConverterWithOptions(
		NewPayloadHandleDataConverter(
			iexternalstorage.NewExternalStorageDataConverter(
				converter.NewCodecDataConverter(
					converter.GetDefaultDataConverter(),
					converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
				),
				externalstorage.NewSingleDriverProviderWithThreshold(
					NewInMemoryStorageDriver(),
					10,
				),
			),
		),
		PayloadLimitDataConverterOptions{
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
	err = handle.Get(&handle1_value)
	assert.NoError(t, err)

	assert.Equal(t, expected_value, handle1_value)

	expected_value2 := "Here is my handle, here is my spout."

	payloads, err := chainedConverter.ToPayloads(expected_value2)
	assert.NoError(t, err)

	var handle2 converter.PayloadHandle
	err = chainedConverter.FromPayloads(payloads, &handle2)
	assert.NoError(t, err)

	var handle2_value string
	err = handle2.Get(&handle2_value)
	assert.NoError(t, err)

	assert.Equal(t, expected_value2, handle2_value)
}
