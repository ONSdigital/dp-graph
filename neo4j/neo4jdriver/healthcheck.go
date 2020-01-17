package neo4jdriver

import (
	"context"
	"time"

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

// Checker : Check health of Neo4j and return it inside a Check structure
func (n *NeoDriver) Checker(ctx context.Context) (*health.Check, error) {
	_, err := n.Healthcheck()
	currentTime := time.Now().UTC()
	n.Check.LastChecked = &currentTime
	if err != nil {
		n.Check.LastFailure = &currentTime
		n.Check.Status = health.StatusCritical
		n.Check.Message = err.Error()
		return n.Check, err
	}
	n.Check.LastSuccess = &currentTime
	n.Check.Status = health.StatusOK
	n.Check.Message = MsgHealthy
	return n.Check, nil
}
