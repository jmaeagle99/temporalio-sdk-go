package externalstorage

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

type singleStorageProvider struct {
	driver    ExternalStorageDriver
	threshold int
}

func NewSingleDriverProvider(driver ExternalStorageDriver, threshold int) ExternalStorageProvider {
	return &singleStorageProvider{
		driver:    driver,
		threshold: threshold,
	}
}

func (provider *singleStorageProvider) Store(context context.Context, payloads []*commonpb.Payload) ([]*ExternalStorageReference, error) {
	references := make([]*ExternalStorageReference, len(payloads))
	for index, payload := range payloads {
		if payload.Size() > provider.threshold {
			claim, err := provider.driver.Store(context, []*commonpb.Payload{payload})
			if err != nil {
				return nil, err
			}
			references[index] = &ExternalStorageReference{
				Name:  provider.driver.GetName(),
				Claim: claim[0],
			}
		}
	}
	return references, nil
}

func (provider *singleStorageProvider) Retrieve(context context.Context, references []ExternalStorageReference) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, 0, len(references))
	for _, refernce := range references {
		stored_payloads, err := provider.driver.Retrieve(context, []ExternalStorageClaim{refernce.Claim})
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, stored_payloads...)
	}
	return payloads, nil
}
