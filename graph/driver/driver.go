package driver

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-import-api/models"
)

type Driver interface {
	// Open returns a new connection to the database.
	// The name is a string in a driver-specific format.
	//
	// Open may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Open(name string) (Conn, error)
}

var ErrBadConn = errors.New("driver: bad connection")

type Pinger interface {
	Ping(ctx context.Context) error
}

// type ExecerContext interface {
// 	ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)
// }

type Conn interface {
	// // Prepare returns a prepared statement, bound to this connection.
	// Prepare(query string) (Stmt, error)

	// Close invalidates and potentially stops any current
	// prepared statements and transactions, marking this
	// connection as no longer in use.
	//
	// Because the sql package maintains a free pool of
	// connections and only calls Close when there's a surplus of
	// idle connections, it shouldn't be necessary for drivers to
	// do their own connection caching.
	Close() error

	//	Exec(resultCapture interface{}, query string, args ...interface{}) error

	// // Begin starts and returns a new transaction.
	// //
	// // Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
	// Begin() (Tx, error)
}

type Connector interface {
	// Connect returns a connection to the database.
	// Connect may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The provided context.Context is for dialing purposes only
	// (see net.DialContext) and should not be stored or used for
	// other purposes.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Connect(context.Context) (Conn, error)

	// Driver returns the underlying Driver of the Connector,
	// mainly to maintain compatibility with the Driver method
	// on sql.DB.
	Driver() Driver
}

type CodeListMapper interface {
	Map(i interface{}) (*models.CodeList, error)
}
