package neo4jdriver_test

import (
	"testing"
	"time"

	"github.com/ONSdigital/dp-graph/neo4j/internal"
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
		var connBoltNoErr = &internal.BoltConnMock{
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

		Convey("Checker returns a successful Check structure", func() {
			timeBeforeCall := time.Now().UTC()
			check, err := d.Checker(nil)
			timeAfterCall := time.Now().UTC()
			So(err, ShouldBeNil)
			So(check.Name, ShouldEqual, driver.ServiceName)
			So(check.Status, ShouldEqual, health.StatusOK)
			So(check.StatusCode, ShouldEqual, 200)
			So(check.Message, ShouldEqual, driver.StatusDescription[health.StatusOK])
			So(check.LastChecked, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
			So(check.LastSuccess, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
			So(check.LastFailure, ShouldHappenOnOrBefore, driver.UnixTime)
			So(len(connBoltNoErr.QueryNeoCalls()), ShouldEqual, 1)
		})
	})
}

func TestNeo4jHealthNotReacheable(t *testing.T) {
	Convey("Given that Neo4j is not reachable", t, func() {

		// mock pool with unsuccessful bolt.Conn
		mockPool := &internal.ClosableDriverPoolMock{
			OpenPoolFunc: func() (bolt.Conn, error) {
				return nil, errors.New("Driver pool has been closed")
			},
		}
		d := driver.NewWithPool(mockPool)

		Convey("Checker returns a critical Check structure", func() {
			timeBeforeCall := time.Now().UTC()
			check, err := d.Checker(nil)
			timeAfterCall := time.Now().UTC()
			So(err, ShouldNotBeNil)
			So(check.Name, ShouldEqual, driver.ServiceName)
			So(check.Status, ShouldEqual, health.StatusCritical)
			So(check.StatusCode, ShouldEqual, 500)
			So(check.Message, ShouldEqual, driver.StatusDescription[health.StatusCritical])
			So(check.LastChecked, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
			So(check.LastSuccess, ShouldHappenOnOrBefore, driver.UnixTime)
			So(check.LastFailure, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
		})
	})
}

func TestNeo4jHealthQueryFailed(t *testing.T) {
	Convey("Given that Neo4j is reacheble but queries fail", t, func() {

		// mock successful bolt.Conn with failed Query
		var connBoltErrQuery = &internal.BoltConnMock{
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

		Convey("Checker returns a critical Check structure", func() {
			timeBeforeCall := time.Now().UTC()
			check, err := d.Checker(nil)
			timeAfterCall := time.Now().UTC()
			So(err, ShouldNotBeNil)
			So(check.Name, ShouldEqual, driver.ServiceName)
			So(check.Status, ShouldEqual, health.StatusCritical)
			So(check.StatusCode, ShouldEqual, 500)
			So(check.Message, ShouldEqual, driver.StatusDescription[health.StatusCritical])
			So(check.LastChecked, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
			So(check.LastSuccess, ShouldHappenOnOrBefore, driver.UnixTime)
			So(check.LastFailure, ShouldHappenOnOrBetween, timeBeforeCall, timeAfterCall)
			So(len(connBoltErrQuery.QueryNeoCalls()), ShouldEqual, 1)
		})
	})
}
