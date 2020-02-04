package neo4jdriver_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-graph/neo4j/internal"
	"github.com/ONSdigital/dp-graph/neo4j/neo4jdriver"
	driver "github.com/ONSdigital/dp-graph/neo4j/neo4jdriver"
	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

// mock func for successful call to bolt.Conn.Close
var closeSuccess = func() error {
	return nil
}

// mock func for successful call to bolt.Conn.QueryNeo
var queryNeoSuccess = func(query string, params map[string]interface{}) (bolt.Rows, error) {
	return &internal.BoltRowsMock{
		CloseFunc: closeSuccess,
	}, nil
}

// mock func for failed call to bolt.Conn.QueryNeo
var queryNeoFail = func(query string, params map[string]interface{}) (bolt.Rows, error) {
	return nil, errors.New("An open statement already exists")
}

func TestNeo4jHealthOK(t *testing.T) {
	Convey("Given that Neo4J is healthy", t, func() {

		// mock successful bolt.Conn with successful Query
		connBoltNoErr := &internal.BoltConnMock{
			CloseFunc:    closeSuccess,
			QueryNeoFunc: queryNeoSuccess,
		}

		// mock pool with successful bolt.Conn
		mockPool := &internal.ClosableDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return connBoltNoErr, nil
			},
		}
		d := driver.NewWithPool(mockPool)

		// CheckState for test validation
		checkState := health.NewCheckState(neo4jdriver.ServiceName)

		Convey("Checker updates the CheckState to a successful state", func() {
			d.Checker(context.Background(), checkState)
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(connBoltNoErr.QueryNeoCalls()), ShouldEqual, 1)
			So(checkState.Status(), ShouldEqual, health.StatusOK)
			So(checkState.Message(), ShouldEqual, neo4jdriver.MsgHealthy)
			So(checkState.StatusCode(), ShouldEqual, 0)
		})
	})
}

func TestNeo4jHealthNotReachable(t *testing.T) {
	Convey("Given that Neo4j is not reachable", t, func() {

		// mock pool with unsuccessful bolt.Conn
		mockPool := &internal.ClosableDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return nil, errors.New("Driver pool has been closed")
			},
		}
		d := driver.NewWithPool(mockPool)

		// CheckState for test validation
		checkState := health.NewCheckState(neo4jdriver.ServiceName)

		Convey("Checker updates the CheckState to a critical state with the relevant error message", func() {
			d.Checker(context.Background(), checkState)
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "Driver pool has been closed")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})
	})
}

func TestNeo4jHealthQueryFailed(t *testing.T) {
	Convey("Given that Neo4j is reachable but queries fail", t, func() {

		// mock successful bolt.Conn with failed Query
		connBoltErrQuery := &internal.BoltConnMock{
			CloseFunc:    closeSuccess,
			QueryNeoFunc: queryNeoFail,
		}

		// mock pool with failed query
		mockPool := &internal.ClosableDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return connBoltErrQuery, nil
			},
		}
		d := driver.NewWithPool(mockPool)

		// CheckState for test validation
		checkState := health.NewCheckState(neo4jdriver.ServiceName)

		Convey("Checker updates the CheckState to a critical state with the relevant error message", func() {
			d.Checker(context.Background(), checkState)
			So(len(mockPool.OpenPoolCalls()), ShouldEqual, 1)
			So(len(connBoltErrQuery.QueryNeoCalls()), ShouldEqual, 1)
			So(checkState.Status(), ShouldEqual, health.StatusCritical)
			So(checkState.Message(), ShouldEqual, "An open statement already exists")
			So(checkState.StatusCode(), ShouldEqual, 0)
		})
	})
}
