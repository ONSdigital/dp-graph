package neptune

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/neptune/driver"
	"github.com/ONSdigital/dp-graph/neptune/internal"
)

// returnOne is a mock implementation for a NeptunePool method.
var returnOne = func(q string, bindings, rebindings map[string]string) (
	i int64, err error) {
	return 1, nil
}

func TestGetCodeListInNonErrorScenario(t *testing.T) {
	Convey("When the database says the CodeList ID exists", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: returnOne}
		driver := driver.NeptuneDriver{Pool: poolMock}
		db := &NeptuneDB{
			NeptuneDriver: driver,
			maxAttempts:   5,
			timeout:       30,
		}
		Convey("And GetCodeList() is called", func() {
			codeListID := "arbitrary"
			codeList, err := db.GetCodeList(context.Background(), codeListID)
			Convey("The driver GetCount function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := "g.V().hasLabel('_code_list_arbitrary').count()"
					So(calls[0].Q, ShouldEqual, expectedQry)
				})
			})
			Convey("And no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("And a non nil structure returned", func() {
				So(codeList, ShouldNotBeNil)
			})
		})
	},
	)
}
