package neo4j

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neo4j/internal"
	"github.com/ONSdigital/dp-graph/v2/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/v2/neo4j/query"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
)

var dimension = &models.Dimension{
	DimensionID: "Sex",
	Option:      "Male",
}

var expectedDimension = &models.Dimension{
	DimensionID: "Sex",
	Option:      "Male",
	NodeID:      "1234",
}

func Test_InsertDimension(t *testing.T) {
	Convey("Given an empty neo4j mock", t, func() {
		neo4jMock := &internal.Neo4jDriverMock{}
		db := &Neo4j{neo4jMock, 5, 30}

		Convey("When Insert is invoked with an empty instanceID", func() {
			dim, err := db.InsertDimension(context.Background(), map[string]string{}, &sync.Mutex{}, "", dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, "instance id is required but was empty")
				So(dim, ShouldBeNil)
			})
		})

		Convey("When Insert is invoked with a nil cache map", func() {
			dim, err := db.InsertDimension(context.Background(), nil, &sync.Mutex{}, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, "no cache map provided to InsertDimension")
				So(dim, ShouldBeNil)
			})
		})

		Convey("When Insert is invoked with a nil cache mutex", func() {
			dim, err := db.InsertDimension(context.Background(), map[string]string{}, nil, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, "no cache mutex provided to InsertDimension")
				So(dim, ShouldBeNil)
			})
		})

		Convey("When Insert is invoked with a nil dimension", func() {
			dim, err := db.InsertDimension(context.Background(), map[string]string{}, &sync.Mutex{}, instanceID, nil)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, "dimension is required but was nil")
				So(dim, ShouldBeNil)
			})
		})
	})

	Convey("Given a dimension type that has already been processed", t, func() {
		nodeID := new(string)
		neo4jMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, b bool) error {
				*nodeID = "1234"
				return nil
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		constraintsCache := map[string]string{"_" + instanceID + "_" + dimension.DimensionID: ""}
		constraintsCacheMutex := &sync.Mutex{}

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, constraintsCacheMutex, instanceID, dimension)
			dim.NodeID = *nodeID

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldResemble, expectedDimension)
				So(err, ShouldEqual, nil)
			})

			Convey("And neo4j.ReadWithParams is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ReadWithParamsCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, instanceID, dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{"value": dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neo4jMock.ExecCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension type that has not already been processed", t, func() {
		nodeID := new(string)

		neo4jMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, b bool) error {
				*nodeID = "1234"
				return nil
			},
			ExecFunc: func(q string, p map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		constraintsCache := map[string]string{"_differentID_" + dimension.DimensionID: ""}
		constraintsCacheMutex := &sync.Mutex{}

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, constraintsCacheMutex, instanceID, dimension)
			dim.NodeID = *nodeID

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldResemble, expectedDimension)
				So(err, ShouldEqual, nil)
			})

			Convey("And neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, "Sex")
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, map[string]any(nil))
			})

			Convey("And neo4j.ReadWithParams is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ReadWithParamsCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, instanceID, dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{"value": dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a create unique constraint returns an error", t, func() {
		neo4jMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, b bool) error {
				return nil
			},
			ExecFunc: func(q string, p map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		constraintsCache := map[string]string{}
		constraintsCacheMutex := &sync.Mutex{}

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, constraintsCacheMutex, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, (*models.Dimension)(nil))
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neoClient.Exec returned an error").Error())
			})

			Convey("And neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, map[string]any(nil))
			})

			Convey("And there is no other calls to neo4j", func() {
				So(len(neo4jMock.ReadWithParamsCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given neo4j.ReadWithParams returns an error", t, func() {
		neo4jMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, b bool) error {
				return errorMock
			},
			ExecFunc: func(q string, p map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		constraintsCache := map[string]string{"_" + instanceID + "_" + dimension.DimensionID: dimension.DimensionID}
		constraintsCacheMutex := &sync.Mutex{}

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, constraintsCacheMutex, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, (*models.Dimension)(nil))
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neoClient.ReadWithParams returned an error").Error())
			})

			Convey("And neo4j.ReadWithParams is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ReadWithParamsCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, "instanceID", dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)

				expectedParams := map[string]interface{}{"value": dimension.Option}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})

			Convey("And there is no other calls to neo4j", func() {
				So(len(neo4jMock.ExecCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestCacheDimension(t *testing.T) {
	ctx := context.Background()
	dimensionLabel := "_inst_dim"

	Convey("Given 2 concurrent go-routines that try to cache a dimension", t, func() {
		cache := make(map[string]string)
		cacheMutex := &sync.Mutex{}

		wg := &sync.WaitGroup{}
		created1 := false
		created2 := false

		wg.Add(1)
		go func() {
			defer wg.Done()
			created1 = cacheDimension(ctx, cache, cacheMutex, dimensionLabel)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			created2 = cacheDimension(ctx, cache, cacheMutex, dimensionLabel)
		}()

		wg.Wait()

		Convey("Then the dimension was cached, created only by one of the two callers", func() {
			So(cache, ShouldResemble, map[string]string{
				dimensionLabel: dimensionLabel,
			})
			So(created1 || created2, ShouldBeTrue) // at least one created the value
			So(created1, ShouldNotEqual, created2) // only one created the value
		})
	})
}
