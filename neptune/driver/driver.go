package driver

import (
	"context"
	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	gremgo "github.com/ONSdigital/gremgo-neptune"
)

// Type check to ensure that NeptuneDriver implements the driver.Driver interface
var _ driver.Driver = (*NeptuneDriver)(nil)

type NeptuneDriver struct {
	Pool NeptunePool // Defined with an interface to support mocking.
}

func New(ctx context.Context, dbAddr string, errs chan error) (*NeptuneDriver, error) {
	pool := gremgo.NewPoolWithDialerCtx(ctx, dbAddr, errs)
	return &NeptuneDriver{
		Pool: pool,
	}, nil
}

func (n *NeptuneDriver) Close(ctx context.Context) error {
	n.Pool.Close()
	return nil
}
