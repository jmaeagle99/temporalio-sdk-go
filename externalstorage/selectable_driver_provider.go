package externalstorage

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
)

// SelectableStorageProviderSelection is the result of the selector function, allowing the function
// to return a driver that should be used to store the payload. The selector function shall always
// set the Payload field. The selector can optionally set the Driver field; if set, the driver will
// be used to store the payload, but if not set, the payload will be passed through without being
// externally stored.
type SelectableStorageProviderSelection struct {
	Payload *commonpb.Payload
	Driver  ExternalStorageDriver
}

// SelectableStorageProviderDriveSelector allows callers to provide a mapping of storage drivers and payloads to this function so that the
// function can choose which driver for storing each payload. The selector method may choose not use a driver and only provide the payload
// in the corresponding result to indicate that the payload should be passed through.
type SelectableStorageProviderDriverSelector func(context.Context, map[string]ExternalStorageDriver, []*commonpb.Payload) ([]SelectableStorageProviderSelection, error)

type selectableStorageProvider struct {
	drivers    map[string]ExternalStorageDriver
	selectorFn SelectableStorageProviderDriverSelector
}

// Create a storage provider that allows selecting an appropriate driver for each payload that should be stored externally.
func NewSelectableDriverProvider(drivers []ExternalStorageDriver, selectorFn SelectableStorageProviderDriverSelector) (ExternalStorageProvider, error) {
	driverMap := make(map[string]ExternalStorageDriver)

	for _, driver := range drivers {
		name := driver.Name()
		if _, ok := driverMap[name]; ok {
			return nil, fmt.Errorf("blah")
		}
		driverMap[name] = driver
	}

	return &selectableStorageProvider{
		drivers:    driverMap,
		selectorFn: selectorFn,
	}, nil
}

func (p *selectableStorageProvider) Drivers() []ExternalStorageDriver {
	drivers := make([]ExternalStorageDriver, 0, len(p.drivers))
	for _, driver := range p.drivers {
		drivers = append(drivers, driver)
	}
	return drivers
}

func (p *selectableStorageProvider) Store(ctx context.Context, payloads []*commonpb.Payload) ([]*ExternalStorageReference, error) {
	selectorResults, err := p.selectorFn(ctx, p.drivers, payloads)
	if err != nil {
		return nil, err
	}
	providerResults := make([]*ExternalStorageReference, len(payloads))
	// TODO: Parallelize
	for index, result := range selectorResults {
		if result.Driver != nil {
			claim, err := result.Driver.Store(ctx, []*commonpb.Payload{result.Payload})
			if err != nil {
				return nil, err
			}
			providerResults[index] = &ExternalStorageReference{
				Name:  result.Driver.Name(),
				Claim: claim[0],
			}
		}
	}
	return providerResults, nil
}

func (p *selectableStorageProvider) Retrieve(ctx context.Context, references []ExternalStorageReference) ([]*commonpb.Payload, error) {
	payloads := make([]*commonpb.Payload, 0, len(references))
	// TODO: Parallelize
	for _, reference := range references {
		driver, ok := p.drivers[reference.Name]
		if !ok {
			return nil, NewDriverNotFoundError(reference.Name)
		}
		storedPayloads, err := driver.Retrieve(ctx, []ExternalStorageClaim{reference.Claim})
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, storedPayloads...)
	}
	return payloads, nil
}
