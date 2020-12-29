package neptune

import (
	"github.com/ONSdigital/dp-graph/v2/neptune/driver"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"time"
)

// mockDB provides a NeptuneDB, into which you can pass a mocked
// NeptunePoolMock implementation, and thus write tests that bypass real
// database communication.
func mockDB(poolMock *internal.NeptunePoolMock) *NeptuneDB {
	driver := driver.NeptuneDriver{Pool: poolMock}
	db := &NeptuneDB{driver, 5, time.Millisecond, 30, 25000, 150, 150}
	return db
}
