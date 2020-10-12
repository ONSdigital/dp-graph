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
				expectedQuery := `g.V().hasId('_888_age_29','_888_age_30','_888_age_31').in('isValueOf')` +
					`.where(out('isValueOf').hasId('_888_sex_male','_888_sex_female','_888_sex_all','_888_geography_K0001','_888_geography_K0002','_888_geography_K0003')` +
					`.fold().count(local).is_(2))`
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
