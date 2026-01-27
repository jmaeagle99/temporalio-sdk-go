package converter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/externalstorage"
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

func (driver *InMemoryStorageDriver) GetName() string {
	return "in-memory"
}

func (driver *InMemoryStorageDriver) Store(context context.Context, payloads []*commonpb.Payload) ([]externalstorage.ExternalStorageClaim, error) {
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

func (driver *InMemoryStorageDriver) Retrieve(context context.Context, claims []externalstorage.ExternalStorageClaim) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, len(claims))
	for index, claim := range claims {
		payloads[index] = driver.mapping[string(claim.Data["claim-id"])]
	}
	return payloads, nil
}

type SingleStorageProvider struct {
	driver    externalstorage.ExternalStorageDriver
	threshold int
}

func NewSimpleStorageProvider(driver externalstorage.ExternalStorageDriver, threshold int) externalstorage.ExternalStorageProvider {
	return &SingleStorageProvider{
		driver:    driver,
		threshold: threshold,
	}
}

func (provider *SingleStorageProvider) Store(context context.Context, payloads []*commonpb.Payload) ([]*externalstorage.ExternalStorageReference, error) {
	references := make([]*externalstorage.ExternalStorageReference, len(payloads))
	for index, payload := range payloads {
		if payload.Size() > provider.threshold {
			claim, err := provider.driver.Store(context, []*commonpb.Payload{payload})
			if err != nil {
				return nil, err
			}
			references[index] = &externalstorage.ExternalStorageReference{
				Name:  provider.driver.GetName(),
				Claim: claim[0],
			}
		}
	}
	return references, nil
}

func (provider *SingleStorageProvider) Retrieve(context context.Context, references []externalstorage.ExternalStorageReference) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, 0, len(references))
	for _, refernce := range references {
		stored_payloads, err := provider.driver.Retrieve(context, []externalstorage.ExternalStorageClaim{refernce.Claim})
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, stored_payloads...)
	}
	return payloads, nil
}

func TestPayloadHandles(t *testing.T) {
	chainedConverter := NewPayloadLimitDataConverter(
		NewPayloadHandleDataConverter(
			converter.NewExternalStorageDataConverter(
				converter.NewCodecDataConverter(
					converter.GetDefaultDataConverter(),
					converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
				),
				NewSimpleStorageProvider(
					NewInMemoryStorageDriver(),
					10,
				),
			),
		),
		PayloadLimitDataConverterOptions{
			DisableErrorLimit:  false,
			PayloadSizeError:   1024,
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
