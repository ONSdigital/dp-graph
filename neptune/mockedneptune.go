package neptune

import (
	"time"

	"github.com/ONSdigital/dp-graph/v3/neptune/driver"
	"github.com/ONSdigital/dp-graph/v3/neptune/internal"
)

// mockDB provides a NeptuneDB, into which you can pass a mocked
// NeptunePoolMock implementation, and thus write tests that bypass real
// database communication.
func mockDB(poolMock *internal.NeptunePoolMock) *NeptuneDB {
	driver := driver.NeptuneDriver{Pool: poolMock}
	db := &NeptuneDB{driver, 5, time.Millisecond, 30, 25000, 150, 150}
	return db
}
