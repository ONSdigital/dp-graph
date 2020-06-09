package neptune

import (
	"context"
	"fmt"
	"testing"

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
				So(result, ShouldEqual, ".V().has('_888_age','value',within('30')).in('isValueOf')")
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
				expectedQuery := `.V().has('_888_age','value',within('29','30','31')).in('isValueOf').` +
					`match(` +
					`__.as('row').out('isValueOf').has('_888_sex','value',within('male','female','all')),` +
					`__.as('row').out('isValueOf').has('_888_geography','value',within('K0001','K0002','K0003')))`
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
			stream, err := db.StreamCSVRows(nil, "", "", nil, nil)

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
			stream, err := db.StreamCSVRows(nil, instanceID, "", filter, nil)

			Convey("Then no error should be returned", func() {
				So(stream, ShouldNotBeNil)
				So(err, ShouldBeNil)
			})
		})
	})

}
