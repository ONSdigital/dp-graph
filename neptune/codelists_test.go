package neptune

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/neptune/driver"
	"github.com/ONSdigital/dp-graph/neptune/internal"
)

func TestMockDBCompiles(t *testing.T) {
	Convey("Something", t, func() {
		poolMock := &internal.NeptunePoolMock{}
		driver := driver.NeptuneDriver{poolMock}
		db := &NeptuneDB{driver, 5, 30}
		_ = db
	},
	)
}
