package externalstorage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	ext "go.temporal.io/sdk/externalstorage"
	iext "go.temporal.io/sdk/internal/externalstorage"
	"go.temporal.io/sdk/test/externalstorage/inmemory"
)

func TestSelectableDriverProvider(t *testing.T) {

	// Example selectable provider setup
	largeOptions := inmemory.Options{Suffix: "large"}
	mediumOptions := inmemory.Options{Suffix: "medium"}

	provider, err := ext.NewSelectableDriverProvider(
		[]ext.ExternalStorageDriver{
			inmemory.New(largeOptions),
			inmemory.New(mediumOptions),
		},
		func(ctx context.Context, drivers map[string]ext.ExternalStorageDriver, payloads []*commonpb.Payload) ([]ext.SelectableStorageProviderSelection, error) {
			results := make([]ext.SelectableStorageProviderSelection, len(payloads))
			for index, payload := range payloads {
				size := payload.Size()
				result := ext.SelectableStorageProviderSelection{
					Payload: payload,
				}
				if size > 10240 {
					result.Driver = drivers[inmemory.Name(largeOptions)]
				} else if size > 1024 {
					result.Driver = drivers[inmemory.Name(mediumOptions)]
				}
				results[index] = result
			}
			return results, nil
		},
	)
	assert.NoError(t, err)

	// Example construction of converter
	compositeConverter := iext.NewExternalStorageDataConverter2(
		converter.GetDefaultDataConverter(),
		provider,
		converter.NewCodecDataConverter(
			converter.GetDefaultDataConverter(),
			converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
		),
	)

	// Validation
	smallData := strings.Repeat("a", 128)
	smallPayload, err := compositeConverter.ToPayload(smallData)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(smallPayload.ExternalPayloads))

	mediumData := strings.Repeat("a", 2*1024)
	mediumPayload, err := compositeConverter.ToPayload(mediumData)
	assert.NoError(t, err)

	assertClaimCheck(t, mediumPayload, inmemory.Name(mediumOptions))

	largeData := strings.Repeat("a", 12*1024)
	largePayload, err := compositeConverter.ToPayload(largeData)
	assert.NoError(t, err)

	assertClaimCheck(t, largePayload, inmemory.Name(largeOptions))
}

func assertClaimCheck(t *testing.T, payload *commonpb.Payload, expectedName string) {
	assert.True(t, len(payload.ExternalPayloads) > 0)
	var reference ext.ExternalStorageReference
	c := converter.NewCodecDataConverter(
		converter.GetDefaultDataConverter(),
		converter.NewZlibCodec(converter.ZlibCodecOptions{AlwaysEncode: true}),
	)
	err := c.FromPayload(payload, &reference)
	assert.NoError(t, err)
	assert.Equal(t, expectedName, reference.Name)
}
