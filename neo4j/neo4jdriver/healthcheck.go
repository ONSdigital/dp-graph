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

// minTime : Oldest time for Check structure.
var minTime = time.Unix(0, 0)

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
func (n *NeoDriver) Checker(ctx *context.Context) (*health.Check, error) {
	_, err := n.Healthcheck()
	if err != nil {
		return getCheck(ctx, health.StatusCritical, err.Error()), err
	}
	return getCheck(ctx, health.StatusOK, MsgHealthy), nil
}

// getCheck : Create a Check structure and populate it according the status and message
func getCheck(ctx *context.Context, status, message string) *health.Check {

	currentTime := time.Now().UTC()

	check := &health.Check{
		Name:        ServiceName,
		Status:      status,
		Message:     message,
		LastChecked: currentTime,
		LastSuccess: minTime,
		LastFailure: minTime,
	}

	switch status {
	case health.StatusOK:
		check.StatusCode = 200
		check.LastSuccess = currentTime
	case health.StatusWarning:
		check.StatusCode = 429
		check.LastFailure = currentTime
	default:
		check.Status = health.StatusCritical
		check.StatusCode = 500
		check.LastFailure = currentTime
	}

	return check
}
