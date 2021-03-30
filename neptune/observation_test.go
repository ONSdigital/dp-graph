package neptune

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/v2/models"

	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/dp-graph/v2/observation"
	"github.com/ONSdigital/gremgo-neptune"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_buildObservationsQuery(t *testing.T) {

	Convey("Given an empty filter job", t, func() {
		instanceID := "888"
		filter := &observation.DimensionFilters{}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(instanceID, filter)

			Convey("Then the resulting query portion should return all observations", func() {
				So(result, ShouldEqual, fmt.Sprintf(query.GetAllObservationsPart, instanceID))
			})
		})
	})

	Convey("Given an filter job with 1 dimension and 1 option", t, func() {
		instanceID := "888"
		filter := &observation.DimensionFilters{
			Dimensions: []*observation.Dimension{
				{Name: "age", Options: []string{"30"}},
			},
		}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(instanceID, filter)

			Convey("Then the resulting query portion should filter to relevant observations", func() {
				So(result, ShouldEqual, "g.V().hasId('_888_age_30').in('isValueOf')")
			})
		})
	})

	Convey("Given an filter job with multiple dimensions and options", t, func() {
		instanceID := "888"
		filter := &observation.DimensionFilters{
			Dimensions: []*observation.Dimension{
				{Name: "age", Options: []string{"29", "30", "31"}},
				{Name: "sex", Options: []string{"male", "female", "all"}},
				{Name: "geography", Options: []string{"K0001", "K0002", "K0003"}},
			},
		}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(instanceID, filter)

			Convey("Then the resulting query portion should filter to relevant observations", func() {
				expectedQuery := `g.V().hasId('_888_geography_K0001','_888_geography_K0002','_888_geography_K0003').in('isValueOf').where(out('isValueOf').hasId('_888_age_29','_888_age_30','_888_age_31','_888_sex_male','_888_sex_female','_888_sex_all').fold().count(local).is_(2))`
				So(result, ShouldEqual, expectedQuery)
			})
		})
	})
}

func Test_StreamCSVRows(t *testing.T) {

	Convey("Given a store with a mock DB connection and an empty filter job", t, func() {
		poolMock := &internal.NeptunePoolMock{
			OpenStreamCursorFunc: func(ctx context.Context, query string, bindings map[string]string, rebindings map[string]string) (*gremgo.Stream, error) {
				return &gremgo.Stream{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When StreamCSVRows is called", func() {
			stream, err := db.StreamCSVRows(ctx, "", "", nil, nil)

			Convey("Then an error is returned", func() {
				So(stream, ShouldBeNil)
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, ErrInvalidFilter)
			})
		})
	})

	Convey("Given a store with a mock DB connection and a valid filter job", t, func() {
		poolMock := &internal.NeptunePoolMock{
			OpenStreamCursorFunc: func(ctx context.Context, query string, bindings map[string]string, rebindings map[string]string) (*gremgo.Stream, error) {
				return &gremgo.Stream{}, nil
			},
		}
		db := mockDB(poolMock)

		instanceID := "888"
		filter := &observation.DimensionFilters{
			Dimensions: []*observation.Dimension{
				{Name: "age", Options: []string{"29", "30", "31"}},
				{Name: "sex", Options: []string{"male", "female", "all"}},
			},
		}

		Convey("When StreamCSVRows is called", func() {
			stream, err := db.StreamCSVRows(ctx, instanceID, "", filter, nil)

			Convey("Then no error should be returned", func() {
				So(stream, ShouldNotBeNil)
				So(err, ShouldBeNil)
			})
		})
	})
}

func Test_escapeSingleQuotes(t *testing.T) {

	Convey("Given a value with a single quote", t, func() {

		value := "carl's"
		expected := "carl\\'s"

		Convey("When escapeSingleQuotes is called", func() {
			actual := escapeSingleQuotes(value)

			Convey("Then single quote is escaped", func() {
				So(actual, ShouldEqual, expected)
			})
		})
	})

	Convey("Given a value with multiple single quotes", t, func() {

		value := "83.8,,1999,1999,E07000146,King's Lynn and West Norfolk,68IMP,68IMP : Owner-occupiers' imputed rental,chained-volume-measures-index,Chained volume measures index"
		expected := "83.8,,1999,1999,E07000146,King\\'s Lynn and West Norfolk,68IMP,68IMP : Owner-occupiers\\' imputed rental,chained-volume-measures-index,Chained volume measures index"

		Convey("When escapeSingleQuotes is called", func() {
			actual := escapeSingleQuotes(value)

			Convey("Then each single quote is correctly escaped", func() {
				So(actual, ShouldEqual, expected)
			})
		})
	})
}

func Test_InsertObservationBatch(t *testing.T) {

	ctx := context.Background()
	instanceID := "instanceID"

	observations := []*models.Observation{
		{
			Row:        "row,content,1",
			RowIndex:   1,
			InstanceID: instanceID,
			DimensionOptions: []*models.DimensionOption{
				{DimensionName: "age", Name: "29"},
				{DimensionName: "sex", Name: "male"},
			},
		}, {
			Row:        "row,content,2",
			RowIndex:   2,
			InstanceID: instanceID,
			DimensionOptions: []*models.DimensionOption{
				{DimensionName: "age", Name: "30"},
				{DimensionName: "sex", Name: "male"},
			},
		},
	}

	expectedObsQuery := "g.V('_instanceID_observation_1','_instanceID_observation_2').id()"
	expectedObsEdgeQuery := "g.V('_obs_1','_obs_2').bothE().id()"
	expectedObsDeleteStmt := "g.E('_edge_1','_edge_2').drop().iterate();g.V('_obs_1','_obs_2').drop()"
	expectedObsCreateStmt := "g.addV('_instanceID_observation').property(id, '_instanceID_observation_1').property(single, 'value', 'row,content,1').addV('_instanceID_observation').property(id, '_instanceID_observation_2').property(single, 'value', 'row,content,2')"
	expectedObsEdgeCreateStmt := "g.V('_instanceID_age_29').as('_instanceID_age_29').V('_instanceID_sex_male').as('_instanceID_sex_male').V('_instanceID_age_30').as('_instanceID_age_30').V('_instanceID_observation_1').addE('isValueOf').to('_instanceID_age_29').V('_instanceID_observation_1').addE('isValueOf').to('_instanceID_sex_male').V('_instanceID_observation_2').addE('isValueOf').to('_instanceID_age_30').V('_instanceID_observation_2').addE('isValueOf').to('_instanceID_sex_male')"

	Convey("Given an error is returned when attempting to get existing observations", t, func() {

		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ")

		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return nil, expectedErr
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "failed to remove existing observations: "+expectedErr.Error())
			})
		})
	})

	Convey("Given some observations already exist", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				if strings.Contains(query, ".bothE().id()") {
					return []string{"_edge_1", "_edge_2"}, nil
				}
				return []string{"_obs_1", "_obs_2"}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the expected get / delete statements are executed", func() {
				So(err, ShouldBeNil)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedObsQuery)
				So(poolMock.GetStringListCalls()[1].Query, ShouldEqual, expectedObsEdgeQuery)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedObsDeleteStmt)
			})
		})
	})

	Convey("Given no observations already exist", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the delete statements are not executed", func() {
				So(err, ShouldBeNil)
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedObsQuery)
				So(poolMock.ExecuteCalls()[0].Query, ShouldNotEqual, expectedObsDeleteStmt)
			})
		})
	})

	Convey("Given a new batch of observations to import", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the expected observation insert queries are executed", func() {
				So(err, ShouldBeNil)
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedObsCreateStmt)
				So(poolMock.ExecuteCalls()[1].Query, ShouldEqual, expectedObsEdgeCreateStmt)
			})
		})
	})

	Convey("Given an error is returned from inserting observation nodes", t, func() {
		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ")

		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, expectedErr
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "failed to add observation nodes: "+expectedErr.Error())
			})
		})
	})

	Convey("Given an error is returned from inserting observation edges", t, func() {
		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ")

		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				if strings.Contains(query, ".addV(") {
					// then it's the first query, to insert observation nodes
					return []gremgo.Response{}, nil
				}

				return []gremgo.Response{}, expectedErr
			},
		}
		db := mockDB(poolMock)

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(ctx, 0, instanceID, observations, nil)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "failed to add observation edges: "+expectedErr.Error())
			})
		})
	})
}

func Test_sortDimensions(t *testing.T) {

	Convey("Given an empty list of dimensions", t, func() {
		var dimensionOptions []*observation.Dimension

		Convey("When sortDimensions is called", func() {
			result := sortDimensions(dimensionOptions)

			Convey("Then the same list is returned", func() {
				So(result, ShouldEqual, dimensionOptions)
			})
		})
	})

	Convey("Given a list of dimensions with 1 dimension", t, func() {

		dimensionOptions := []*observation.Dimension{
			{Name: "age", Options: []string{"30"}},
		}

		Convey("When sortDimensions is called", func() {
			result := sortDimensions(dimensionOptions)

			Convey("Then the same list is returned", func() {
				So(result, ShouldResemble, dimensionOptions)
			})
		})
	})

	Convey("Given a list of dimensions with 1 geography dimension", t, func() {

		dimensionOptions := []*observation.Dimension{
			{Name: "geography", Options: []string{"K0001", "K0002", "K0003"}},
		}

		Convey("When sortDimensions is called", func() {
			result := sortDimensions(dimensionOptions)

			Convey("Then the same list is returned", func() {
				So(result, ShouldResemble, dimensionOptions)
			})
		})
	})

	Convey("Given a list of dimensions without a geography dimension", t, func() {
		dimensionOptions := []*observation.Dimension{
			{Name: "age", Options: []string{"29", "30", "31"}},
			{Name: "sex", Options: []string{"male", "female", "all"}},
			{Name: "time", Options: []string{"2004", "2005", "2006"}},
		}

		Convey("When sortDimensions is called", func() {
			result := sortDimensions(dimensionOptions)

			Convey("Then the same list is returned", func() {
				So(result, ShouldResemble, dimensionOptions)
			})
		})
	})

	Convey("Given a list of dimensions with a geography dimension", t, func() {
		dimensionOptions := []*observation.Dimension{
			{Name: "age", Options: []string{"29", "30", "31"}},
			{Name: "sex", Options: []string{"male", "female", "all"}},
			{Name: "geography", Options: []string{"K0001", "K0002", "K0003"}},
		}

		expectedDimensionOptions := []*observation.Dimension{
			{Name: "geography", Options: []string{"K0001", "K0002", "K0003"}},
			{Name: "age", Options: []string{"29", "30", "31"}},
			{Name: "sex", Options: []string{"male", "female", "all"}},
		}

		Convey("When sortDimensions is called", func() {
			result := sortDimensions(dimensionOptions)

			Convey("Then the list is returned with the geography dimension first", func() {
				So(result, ShouldResemble, expectedDimensionOptions)
			})
		})
	})
}
