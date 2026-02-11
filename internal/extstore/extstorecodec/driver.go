package extstorecodec

import (
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/extstore"
)

type DriverOptions struct {
	Driver extstore.ExternalStorageDriver
	Codecs []converter.PayloadCodec
}

func NewDriver(options DriverOptions) (extstore.ExternalStorageDriver, error) {
	return nil, nil
}
