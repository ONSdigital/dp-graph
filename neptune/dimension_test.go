package neptune

import (
	"context"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNeptuneDB_InsertDimension(t *testing.T) {

	ctx := context.Background()
	instanceID := "instanceID"

	Convey("Given a empty instance ID value", t, func() {

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		db := createMockDB(createVertices())
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
		db := createMockDB(createVertices())
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
		db := createMockDB(createVertices())

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
		db := createMockDB(createVertices())

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
		db := createMockDB(createVertices())

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

	Convey("Given a dimension to insert", t, func() {

		uniqueDimensions := map[string]string{}
		dimension := createDimension()
		expectedVertices := createVertices()
		db := createMockDB(expectedVertices)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, instanceID, dimension)

			Convey("Then the dimension ID is returned", func() {
				So(err, ShouldBeNil)
				So(insertedDimension, ShouldNotBeNil)
				So(insertedDimension.NodeID, ShouldEqual, expectedVertices[0].GetID())
			})
		})
	})
}

func createMockDB(expectedVertices []graphson.Vertex) *NeptuneDB {
	poolMock := &internal.NeptunePoolMock{
		GetFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]graphson.Vertex, error) {
			return expectedVertices, nil
		},
	}
	db := mockDB(poolMock)
	return db
}

func createDimension() *models.Dimension {
	return &models.Dimension{
		DimensionID: "dimID",
		Option:      "option",
		NodeID:      "nodeID",
	}
}

func createVertices() []graphson.Vertex {
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
