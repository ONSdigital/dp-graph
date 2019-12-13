package neo4jdriver

import (
	"context"
	"time"

	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

// ServiceName : neo4j
const ServiceName = "neo4j"
const pingStmt = "MATCH (i) RETURN i LIMIT 1"

// StatusDescription : Map of descriptions by status
var StatusDescription = map[string]string{
	health.StatusOK:       "Everything is ok",
	health.StatusWarning:  "Things are degraded, but at least partially functioning",
	health.StatusCritical: "The checked functionality is unavailable or non-functioning",
}

// UnixTime : Oldest time for Check structure. TODO why don't we use time 0 (1 Jan 1970)?
var UnixTime = time.Unix(1494505756, 0)

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
		return getCheck(ctx, 500), err
	}
	return getCheck(ctx, 200), nil
}

// getCheck : Create a Check structure and populate it according to the code
func getCheck(ctx *context.Context, code int) *health.Check {

	currentTime := time.Now().UTC()

	check := &health.Check{
		Name:        ServiceName,
		StatusCode:  code,
		LastChecked: currentTime,
		LastSuccess: UnixTime,
		LastFailure: UnixTime,
	}

	switch code {
	case 200:
		check.Message = StatusDescription[health.StatusOK]
		check.Status = health.StatusOK
		check.LastSuccess = currentTime
	case 429:
		check.Message = StatusDescription[health.StatusWarning]
		check.Status = health.StatusWarning
		check.LastFailure = currentTime
	default:
		check.Message = StatusDescription[health.StatusCritical]
		check.Status = health.StatusCritical
		check.LastFailure = currentTime
	}

	return check
}
