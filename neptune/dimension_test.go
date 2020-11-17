package neptune

import (
	"context"
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/gremgo-neptune"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNeptuneDB_InsertDimension(t *testing.T) {

	createPoolMock := func(expectedVertices []graphson.Vertex) *internal.NeptunePoolMock {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]graphson.Vertex, error) {
				return expectedVertices, nil
			},
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, nil
			},
		}
		return poolMock
	}

	createDimension := func() *models.Dimension {
		return &models.Dimension{
			DimensionID: "dimID",
			Option:      "option",
			NodeID:      "nodeID",
		}
	}

	createVertices := func() []graphson.Vertex {
		expectedVertex := graphson.Vertex{
			Type: "",
			Value: graphson.VertexValue{
				ID:         "123",
				Label:      "",
				Properties: nil,
			},
		}
		expectedVertices := []graphson.Vertex{expectedVertex}
		return expectedVertices
	}

	ctx := context.Background()
	instanceID := "instanceID"

	Convey("Given a empty instance ID value", t, func() {

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		db := mockDB(createPoolMock(createVertices()))
		instanceID := ""

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "instance id is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given a nil dimension value", t, func() {
		uniqueDimensions := map[string]string{}
		db := mockDB(createPoolMock(createVertices()))
		var dimension *models.Dimension

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension is required but was nil")
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given an empty dimension ID", t, func() {
		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		db := mockDB(createPoolMock(createVertices()))

		dimension.DimensionID = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension id is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given an empty dimension option value", t, func() {
		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		db := mockDB(createPoolMock(createVertices()))

		dimension.Option = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension value is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given an empty dimension ID and option value", t, func() {
		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		db := mockDB(createPoolMock(createVertices()))

		dimension.DimensionID = ""
		dimension.Option = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given a dimension already exists", t, func() {

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		expectedDimID := fmt.Sprintf("_%s_%s_%s", instanceID, dimension.DimensionID, dimension.Option)
		poolMock := createPoolMock(createVertices())
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return []string{expectedDimID}, nil
		}
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the existing dimension is deleted from the graph DB", func() {
				So(len(poolMock.ExecuteCalls()), ShouldBeGreaterThan, 0)
				expectedDropDimStmt := "g.V('_instanceID_dimID_option').bothE().drop().iterate();g.V('_instanceID_dimID_option').drop()"
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedDropDimStmt)
			})

			Convey("Then the new dimension ID is returned", func() {
				So(err, ShouldBeNil)
				So(insertedDimension, ShouldNotBeNil)
				So(insertedDimension.NodeID, ShouldEqual, expectedDimID)
			})
		})
	})

	Convey("Given an error on dimension lookup", t, func() {

		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ")

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return nil, expectedErr
		}
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, expectedErr)
				So(insertedDimension, ShouldBeNil)
			})
		})
	})

	Convey("Given a dimension to insert", t, func() {

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the graph DB is queried to see if the dimension exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				expectedGetDimStmt := "g.V('_instanceID_dimID_option').id()"
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the graph DB is called to insert the dimension", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 2)
				expectedCreateDimStmt := "g.addV('_instanceID_dimID').property(id, '_instanceID_dimID_option').property('value',\"option\")"
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedCreateDimStmt)
				expectedCreateDimEdgeStmt := "g.V('_instanceID_Instance').as('inst').V('_instanceID_dimID_option').addE('HAS_DIMENSION').to('inst')"
				So(poolMock.ExecuteCalls()[1].Query, ShouldEqual, expectedCreateDimEdgeStmt)
			})

			Convey("Then the new dimension ID is returned", func() {
				So(err, ShouldBeNil)
				So(insertedDimension, ShouldNotBeNil)
				expectedDimID := "_instanceID_dimID_option"
				So(insertedDimension.NodeID, ShouldEqual, expectedDimID)
			})
		})
	})
}
