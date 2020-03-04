package driver

import (
	"context"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

const (
	serviceName = "neptune"
	pingStmt    = "g.V().limit(1)"
	msgHealthy  = "Neptune is healthy"
)

// Healthcheck calls neptune to check its health status
func (n *NeptuneDriver) Healthcheck() (s string, err error) {
	if _, err = n.Pool.Get(pingStmt, nil, nil); err != nil {
		return serviceName, err
	}
	return serviceName, nil
}

// Checker hecks health of Neo4j and updates the provided CheckState accordingly
func (n *NeptuneDriver) Checker(ctx context.Context, state *health.CheckState) error {

	// Perform healthcheck
	_, err := n.Healthcheck()

	// All errors are mapped to Critical status
	if err != nil {
		state.Update(health.StatusCritical, err.Error(), 0)
		return nil
	}

	// Success healthcheck is mapped to OK status
	state.Update(health.StatusOK, msgHealthy, 0)
	return nil
}
