package extstoreredis

import "go.temporal.io/sdk/extstore"

// Options for constructing an Redis storage driver
type DriverOptions struct {
}

func NewDriver(options DriverOptions) (extstore.ExternalStorageDriver, error) {
	return nil, nil
}
