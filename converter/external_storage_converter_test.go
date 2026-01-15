package converter_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

type SingleExternalStorageProvider struct {
	Driver                converter.ExternalStorageDriver
	ExternalSizeThreshold int
}

func (p *SingleExternalStorageProvider) Store(ctx converter.ExternalStorageContext, payload *commonpb.Payload) (*commonpb.Payload, error) {
	if payload.Size() <= p.ExternalSizeThreshold {
		return payload, nil
	}

	reference, err := p.Driver.Store(payload)
	if err != nil {
		return nil, err
	}

	claim_payload, err := ctx.DataConverter.ToPayload(reference)
	if err != nil {
		return nil, err
	}

	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(payload.Size()))

	claim_payload.Metadata["external"] = []byte("")
	claim_payload.Metadata["external.size"] = sizeBytes
	claim_payload.Metadata["external.drivername"] = []byte(p.Driver.GetName())

	return claim_payload, nil
}

func (p SingleExternalStorageProvider) Retrieve(ctx converter.ExternalStorageContext, payload *commonpb.Payload) (*commonpb.Payload, error) {
	if _, ok := payload.Metadata["external"]; !ok {
		// Payload was not stored in external storage but passed through
		return payload, nil
	}

	driverName, ok := payload.Metadata["external.drivername"]
	if !ok {
		return nil, fmt.Errorf("Payload storage driver metadata not found.")
	}

	if !bytes.Equal(driverName, []byte(p.Driver.GetName())) {
		return nil, fmt.Errorf("Payload storage driver does not match.")
	}

	var reference converter.ExternalPayloadReference
	err := ctx.DataConverter.FromPayload(payload, &reference)
	if err != nil {
		return nil, err
	}

	stored_payload, err := p.Driver.Retrieve(reference)
	if err != nil {
		return nil, err
	}

	return stored_payload, nil
}

type MyStorageDriver struct {
	storage     map[string]*commonpb.Payload
	nextClaimId int
}

func NewMyStorageDriver() converter.ExternalStorageDriver {
	return &MyStorageDriver{
		storage:     make(map[string]*commonpb.Payload),
		nextClaimId: 1,
	}
}

func (d *MyStorageDriver) GetName() string {
	return "MyStorageDriver"
}

func (d *MyStorageDriver) Store(payload *commonpb.Payload) (converter.ExternalPayloadReference, error) {
	claim_id := fmt.Sprintf("%d", d.nextClaimId)
	d.nextClaimId++
	d.storage[claim_id] = payload
	return converter.ExternalPayloadReference{
		Data: map[string][]byte{
			"claim_id": []byte(claim_id),
		},
	}, nil
}

func (d *MyStorageDriver) Retrieve(reference converter.ExternalPayloadReference) (*commonpb.Payload, error) {
	claimId := string(reference.Data["claim_id"])
	return d.storage[claimId], nil
}

func SimpleWorkflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("HelloWorld workflow started", "name", name)

	var result string
	err := workflow.ExecuteActivity(ctx, SimpleActivity, name).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	logger.Info("HelloWorld workflow completed.", "result", result)

	return result, nil
}

func SimpleActivity(ctx context.Context, name string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Activity", "name", name)
	return "Hello " + name + "!", nil
}

func TestExternalStorageDataConverter(t *testing.T) {
	converterWithStorage := converter.NewExternalStorageDataConverter(
		converter.GetDefaultDataConverter(),
		&SingleExternalStorageProvider{
			Driver:                NewMyStorageDriver(),
			ExternalSizeThreshold: 0,
		},
	)

	value := "Hello, world!"
	claim_payload, err := converterWithStorage.ToPayload(value)
	if err == nil {
		fmt.Println("Claim Payload:", claim_payload)
	}
	var retrieved_value string
	err = converterWithStorage.FromPayload(claim_payload, &retrieved_value)
	if err == nil {
		fmt.Println("Retrieved Payload:", retrieved_value)
	}
}
