package driver

import (
	"context"
	"crypto/tls"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	gremgo "github.com/ONSdigital/gremgo-neptune"
)

// Type check to ensure that NeptuneDriver implements the driver.Driver interface
var _ driver.Driver = (*NeptuneDriver)(nil)

type NeptuneDriver struct {
	Pool NeptunePool // Defined with an interface to support mocking.
}

func New(ctx context.Context, dbAddr string, errs chan error, tlsSkip bool) (*NeptuneDriver, error) {
	tConf := &tls.Config{InsecureSkipVerify: tlsSkip}
	pool := gremgo.NewPoolWithDialerCtx(ctx, dbAddr, errs, gremgo.SetTLSClientConfig(tConf))
	return &NeptuneDriver{
		Pool: pool,
	}, nil
}

func (n *NeptuneDriver) Close(ctx context.Context) error {
	n.Pool.Close()
	return nil
}
