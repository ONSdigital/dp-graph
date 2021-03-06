package neptune

import (
	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNeptuneDB_getVertex(t *testing.T) {

	Convey("Given a mocked neptune DB that returns an empty vertex array", t, func() {

		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnZeroVertices}
		db := mockDB(poolMock)

		Convey("When getVertex is called", func() {
			_, err := db.getVertex("gremlin statement")

			Convey("Then ErrNotFound is returned", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})
}
