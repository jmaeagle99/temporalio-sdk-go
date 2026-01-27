package externalstorage

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

type singleStorageProvider struct {
	driver    ExternalStorageDriver
	threshold int
}

func NewSingleDriverProvider(driver ExternalStorageDriver) ExternalStorageProvider {
	return &singleStorageProvider{
		driver:    driver,
		threshold: 128 * 1024,
	}
}

func NewSingleDriverProviderWithThreshold(driver ExternalStorageDriver, threshold int) ExternalStorageProvider {
	return &singleStorageProvider{
		driver:    driver,
		threshold: threshold,
	}
}

func (provider *singleStorageProvider) Drivers() []ExternalStorageDriver {
	return []ExternalStorageDriver{provider.driver}
}

func (provider *singleStorageProvider) Store(ctx context.Context, payloads []*commonpb.Payload) ([]*ExternalStorageReference, error) {
	references := make([]*ExternalStorageReference, len(payloads))
	for index, payload := range payloads {
		if payload.Size() > provider.threshold {
			claim, err := provider.driver.Store(ctx, []*commonpb.Payload{payload})
			if err != nil {
				return nil, err
			}
			references[index] = &ExternalStorageReference{
				Name:  provider.driver.Name(),
				Claim: claim[0],
			}
		}
	}
	return references, nil
}

func (provider *singleStorageProvider) Retrieve(ctx context.Context, references []ExternalStorageReference) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, 0, len(references))
	// TODO: Parallelize
	for _, reference := range references {
		storedPayloads, err := provider.driver.Retrieve(ctx, []ExternalStorageClaim{reference.Claim})
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, storedPayloads...)
	}
	return payloads, nil
}
