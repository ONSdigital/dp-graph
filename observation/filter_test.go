package observation

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var filter = DimensionFilters{
	Dimensions: nil,
	Published:  &Published,
}

func TestFilter_IsEmpty(t *testing.T) {
	Convey("Given dimensionFilters is nil", t, func() {
		filter.Dimensions = nil

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters is empty", t, func() {
		filter.Dimensions = []*Dimension{}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters contains only empty values", t, func() {
		filter.Dimensions = []*Dimension{
			{
				Options: []string{""},
				Name:    "",
			},
		}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeTrue)
		})
	})

	Convey("Given dimensionFilters contains non empty values", t, func() {
		filter.Dimensions = []*Dimension{
			{
				Options: []string{"JAN"},
				Name:    "Time",
			},
		}

		Convey("The IsEmpty returns true", func() {
			So(filter.IsEmpty(), ShouldBeFalse)
		})
	})
}
