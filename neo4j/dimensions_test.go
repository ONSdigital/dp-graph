package neo4j

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/models"
	"github.com/ONSdigital/dp-graph/neo4j/internal"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
)

// var instance = &models.Instance{InstanceID: instanceID}
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

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, instanceID, dimension)
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

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, instanceID, dimension)
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
				So(calls[0].Params, ShouldEqual, nil)
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

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neoClient.Exec returned an error").Error())
			})

			Convey("And neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, dimension.DimensionID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
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

		Convey("When Insert is invoked", func() {
			dim, err := db.InsertDimension(context.Background(), constraintsCache, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(dim, ShouldEqual, nil)
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
