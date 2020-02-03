package neo4jdriver

import (
	"context"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

// ServiceName : neo4j
const ServiceName = "neo4j"

// MsgHealthy Check message returned when Neo4j is healthy
const MsgHealthy = "Neo4j is healthy"

const pingStmt = "MATCH (i) RETURN i LIMIT 1"

// Healthcheck calls neo4j to check its health status.
func (n *NeoDriver) Healthcheck() (string, error) {
	conn, err := n.pool.OpenPool()
	if err != nil {
		return ServiceName, err
	}
	defer conn.Close()

	rows, err := conn.QueryNeo(pingStmt, nil)

	if err != nil {
		return ServiceName, err
	}
	defer rows.Close()

	return ServiceName, nil
}

// Checker : Check health of Neo4j and updates the provided CheckState object
func (n *NeoDriver) Checker(ctx context.Context, state CheckState) error {

	// Perform healthcheck
	_, err := n.Healthcheck()

	// All errors are mapped to Critical status
	if err != nil {
		state.Update(health.StatusCritical, err.Error(), 0)
		return nil
	}

	// Success healthcheck is mapped to OK status
	state.Update(health.StatusOK, MsgHealthy, 0)
	return nil
}
