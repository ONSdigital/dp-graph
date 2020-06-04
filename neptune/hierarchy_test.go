package neptune

import (
	"context"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/gremgo-neptune"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNeptuneDB_SetHasData(t *testing.T) {

	Convey("Given a mocked neptune DB that returns a code list", t, func() {

		ctx := context.Background()
		attempt := 1
		instanceID := "instanceID"
		dimensionName := "dimensionName"

		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnCodesList,
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When SetHasData is called", func() {
			err := db.SetHasData(ctx, attempt, instanceID, dimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to set the hasData property", func() {
				expectedQuery := `g.V().hasLabel('_hierarchy_node_instanceID_dimensionName').as('v').has('code',within(["123","456","789"])).property(single,'hasData',true)`
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}
