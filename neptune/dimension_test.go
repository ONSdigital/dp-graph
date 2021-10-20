package neptune

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-graph/v3/models"
	"github.com/ONSdigital/dp-graph/v3/neptune/internal"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/gremgo-neptune"

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
	expectedGetDimStmt := "g.V('_instanceID_dimID_option').id()"
	expectedCreateDimStmt := "g.addV('_instanceID_dimID').property(id, '_instanceID_dimID_option').property('value',\"option\")"
	expectedCreateDimEdgeStmt := "g.V('_instanceID_Instance').as('inst').V('_instanceID_dimID_option').addE('HAS_DIMENSION').to('inst')"

	Convey("Given an empty Neptune mock", t, func() {
		db := mockDB(nil)
		dimension := createDimension()

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
				So(err.Error(), ShouldEqual, "no uniqueDimensions (cache) map provided to InsertDimension")
				So(dim, ShouldBeNil)
			})
		})

		Convey("When Insert is invoked with a nil cache mutex", func() {
			dim, err := db.InsertDimension(context.Background(), map[string]string{}, nil, instanceID, dimension)

			Convey("Then the expected error is returned with a nil dimension", func() {
				So(err.Error(), ShouldEqual, "no uniqueDimensions (cache) mutex provided to InsertDimension")
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

	Convey("Given a empty instance ID value", t, func() {

		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)
		instanceID := ""

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "instance id is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a nil dimension value", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)
		var dimension *models.Dimension

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension is required but was nil")
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty dimension ID", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)

		dimension.DimensionID = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension id is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty dimension option value", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)

		dimension.Option = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension value is required but was empty")
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty dimension ID and option value", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)

		dimension.DimensionID = ""
		dimension.Option = ""

		Convey("When InsertDimension is called", func() {
			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension already exists", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		expectedDimID := fmt.Sprintf("_%s_%s_%s", instanceID, dimension.DimensionID, dimension.Option)
		poolMock := createPoolMock(createVertices())
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return []string{expectedDimID}, nil
		}
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the existing dimension is deleted from the graph DB", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 3)
				expectedDropDimStmt := "g.V('_instanceID_dimID_option').bothE().drop().iterate();g.V('_instanceID_dimID_option').drop()"
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedDropDimStmt)
			})

			Convey("Then the graph DB is called to insert the dimension", func() {
				So(poolMock.ExecuteCalls()[1].Query, ShouldEqual, expectedCreateDimStmt)
				So(poolMock.ExecuteCalls()[2].Query, ShouldEqual, expectedCreateDimEdgeStmt)
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
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return nil, expectedErr
		}
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the graph DB is queried to see if the dimension exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, expectedErr)
				So(insertedDimension, ShouldBeNil)
			})

			Convey("Then the graph DB is not called to insert the dimension", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a dimension to insert", t, func() {
		uniqueDimensions := map[string]string{}
		uniqueDimensionsMutex := &sync.Mutex{}
		dimension := createDimension()
		poolMock := createPoolMock(createVertices())
		db := mockDB(poolMock)

		Convey("When InsertDimension is called", func() {

			insertedDimension, err := db.InsertDimension(ctx, uniqueDimensions, uniqueDimensionsMutex, instanceID, dimension)

			Convey("Then the graph DB is queried to see if the dimension exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the graph DB is called to insert the dimension", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 2)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedCreateDimStmt)
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
