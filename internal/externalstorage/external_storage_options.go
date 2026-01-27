package externalstorage

import (
	"go.temporal.io/sdk/converter"
	ext "go.temporal.io/sdk/externalstorage"
)

type ExternalStorageOptions struct {
	Provider        ext.ExternalStorageProvider
	OutputConverter converter.DataConverter
}
