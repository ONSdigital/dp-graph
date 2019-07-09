package neptune

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/neptune/internal"
	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/gremgo-neptune"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_buildObservationsQuery(t *testing.T) {

	Convey("Given an empty filter job", t, func() {
		filter := &observation.Filter{
			InstanceID: "888",
		}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(filter)

			Convey("Then the resulting query portion should return all observations", func() {
				So(result, ShouldContainSubstring, fmt.Sprintf(query.GetAllObservationsPart, filter.InstanceID))
				So(result, ShouldNotContainSubstring, fmt.Sprintf(query.GetObservationsPart, filter.InstanceID))
			})
		})
	})

	Convey("Given an filter job with 1 dimension and 1 option", t, func() {
		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"30"}},
			},
		}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(filter)

			Convey("Then the resulting query portion should filter to relevant observations", func() {
				So(result, ShouldNotContainSubstring, fmt.Sprintf(query.GetAllObservationsPart, filter.InstanceID))
				So(result, ShouldContainSubstring, fmt.Sprintf(query.GetObservationsPart, filter.InstanceID))
				So(result, ShouldContainSubstring, fmt.Sprintf(
					query.GetObservationDimensionPart,
					filter.InstanceID,
					filter.DimensionFilters[0].Name,
					filter.DimensionFilters[0].Options[0]),
				)
				So(strings.Count(result, "__.as('row').out("), ShouldEqual, 1)
			})
		})
	})

	Convey("Given an filter job with multiple dimensions and options", t, func() {
		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30", "31"}},
				{Name: "sex", Options: []string{"male", "female", "all"}},
			},
		}

		Convey("When buildObservationsQuery is called", func() {
			result := buildObservationsQuery(filter)

			Convey("Then the resulting query portion should filter to relevant observations", func() {
				So(result, ShouldNotContainSubstring, fmt.Sprintf(query.GetAllObservationsPart, filter.InstanceID))
				So(result, ShouldContainSubstring, fmt.Sprintf(query.GetObservationsPart, filter.InstanceID))
				So(strings.Count(result, "__.as('row').out("), ShouldEqual, 2)
			})
		})
	})
}

func Test_StreamCSVRows(t *testing.T) {

	Convey("Given a store with a mock DB connection and an empty filter job", t, func() {
		poolMock := &internal.NeptunePoolMock{
			OpenCursorCtxFunc: func(ctx context.Context, query string, bindings map[string]string, rebindings map[string]string) (*gremgo.Cursor, error) {
				return &gremgo.Cursor{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When StreamCSVRows is called", func() {
			stream, err := db.StreamCSVRows(nil, nil, nil)

			Convey("Then an error is returned", func() {
				So(stream, ShouldBeNil)
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, ErrEmptyFilter)
			})
		})
	})

	Convey("Given a store with a mock DB connection and a valid filter job", t, func() {
		poolMock := &internal.NeptunePoolMock{
			OpenCursorCtxFunc: func(ctx context.Context, query string, bindings map[string]string, rebindings map[string]string) (*gremgo.Cursor, error) {
				return &gremgo.Cursor{}, nil
			},
		}
		db := mockDB(poolMock)

		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30", "31"}},
				{Name: "sex", Options: []string{"male", "female", "all"}},
			},
		}

		Convey("When StreamCSVRows is called", func() {
			stream, err := db.StreamCSVRows(nil, filter, nil)

			Convey("Then an error is returned", func() {
				So(stream, ShouldNotBeNil)
				So(err, ShouldBeNil)
			})
		})
	})

}
