// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package internal

import (
	"github.com/ONSdigital/dp-graph/v3/neo4j/neo4jdriver"
	"sync"
)

var (
	lockBoltRowsMockAll      sync.RWMutex
	lockBoltRowsMockClose    sync.RWMutex
	lockBoltRowsMockColumns  sync.RWMutex
	lockBoltRowsMockMetadata sync.RWMutex
	lockBoltRowsMockNextNeo  sync.RWMutex
)

// Ensure, that BoltRowsMock does implement neo4jdriver.BoltRows.
// If this is not the case, regenerate this file with moq.
var _ neo4jdriver.BoltRows = &BoltRowsMock{}

// BoltRowsMock is a mock implementation of neo4jdriver.BoltRows.
//
//     func TestSomethingThatUsesBoltRows(t *testing.T) {
//
//         // make and configure a mocked neo4jdriver.BoltRows
//         mockedBoltRows := &BoltRowsMock{
//             AllFunc: func() ([][]interface{}, map[string]interface{}, error) {
// 	               panic("mock out the All method")
//             },
//             CloseFunc: func() error {
// 	               panic("mock out the Close method")
//             },
//             ColumnsFunc: func() []string {
// 	               panic("mock out the Columns method")
//             },
//             MetadataFunc: func() map[string]interface{} {
// 	               panic("mock out the Metadata method")
//             },
//             NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
// 	               panic("mock out the NextNeo method")
//             },
//         }
//
//         // use mockedBoltRows in code that requires neo4jdriver.BoltRows
//         // and then make assertions.
//
//     }
type BoltRowsMock struct {
	// AllFunc mocks the All method.
	AllFunc func() ([][]interface{}, map[string]interface{}, error)

	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// ColumnsFunc mocks the Columns method.
	ColumnsFunc func() []string

	// MetadataFunc mocks the Metadata method.
	MetadataFunc func() map[string]interface{}

	// NextNeoFunc mocks the NextNeo method.
	NextNeoFunc func() ([]interface{}, map[string]interface{}, error)

	// calls tracks calls to the methods.
	calls struct {
		// All holds details about calls to the All method.
		All []struct {
		}
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// Columns holds details about calls to the Columns method.
		Columns []struct {
		}
		// Metadata holds details about calls to the Metadata method.
		Metadata []struct {
		}
		// NextNeo holds details about calls to the NextNeo method.
		NextNeo []struct {
		}
	}
}

// All calls AllFunc.
func (mock *BoltRowsMock) All() ([][]interface{}, map[string]interface{}, error) {
	if mock.AllFunc == nil {
		panic("BoltRowsMock.AllFunc: method is nil but BoltRows.All was just called")
	}
	callInfo := struct {
	}{}
	lockBoltRowsMockAll.Lock()
	mock.calls.All = append(mock.calls.All, callInfo)
	lockBoltRowsMockAll.Unlock()
	return mock.AllFunc()
}

// AllCalls gets all the calls that were made to All.
// Check the length with:
//     len(mockedBoltRows.AllCalls())
func (mock *BoltRowsMock) AllCalls() []struct {
} {
	var calls []struct {
	}
	lockBoltRowsMockAll.RLock()
	calls = mock.calls.All
	lockBoltRowsMockAll.RUnlock()
	return calls
}

// Close calls CloseFunc.
func (mock *BoltRowsMock) Close() error {
	if mock.CloseFunc == nil {
		panic("BoltRowsMock.CloseFunc: method is nil but BoltRows.Close was just called")
	}
	callInfo := struct {
	}{}
	lockBoltRowsMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockBoltRowsMockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedBoltRows.CloseCalls())
func (mock *BoltRowsMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	lockBoltRowsMockClose.RLock()
	calls = mock.calls.Close
	lockBoltRowsMockClose.RUnlock()
	return calls
}

// Columns calls ColumnsFunc.
func (mock *BoltRowsMock) Columns() []string {
	if mock.ColumnsFunc == nil {
		panic("BoltRowsMock.ColumnsFunc: method is nil but BoltRows.Columns was just called")
	}
	callInfo := struct {
	}{}
	lockBoltRowsMockColumns.Lock()
	mock.calls.Columns = append(mock.calls.Columns, callInfo)
	lockBoltRowsMockColumns.Unlock()
	return mock.ColumnsFunc()
}

// ColumnsCalls gets all the calls that were made to Columns.
// Check the length with:
//     len(mockedBoltRows.ColumnsCalls())
func (mock *BoltRowsMock) ColumnsCalls() []struct {
} {
	var calls []struct {
	}
	lockBoltRowsMockColumns.RLock()
	calls = mock.calls.Columns
	lockBoltRowsMockColumns.RUnlock()
	return calls
}

// Metadata calls MetadataFunc.
func (mock *BoltRowsMock) Metadata() map[string]interface{} {
	if mock.MetadataFunc == nil {
		panic("BoltRowsMock.MetadataFunc: method is nil but BoltRows.Metadata was just called")
	}
	callInfo := struct {
	}{}
	lockBoltRowsMockMetadata.Lock()
	mock.calls.Metadata = append(mock.calls.Metadata, callInfo)
	lockBoltRowsMockMetadata.Unlock()
	return mock.MetadataFunc()
}

// MetadataCalls gets all the calls that were made to Metadata.
// Check the length with:
//     len(mockedBoltRows.MetadataCalls())
func (mock *BoltRowsMock) MetadataCalls() []struct {
} {
	var calls []struct {
	}
	lockBoltRowsMockMetadata.RLock()
	calls = mock.calls.Metadata
	lockBoltRowsMockMetadata.RUnlock()
	return calls
}

// NextNeo calls NextNeoFunc.
func (mock *BoltRowsMock) NextNeo() ([]interface{}, map[string]interface{}, error) {
	if mock.NextNeoFunc == nil {
		panic("BoltRowsMock.NextNeoFunc: method is nil but BoltRows.NextNeo was just called")
	}
	callInfo := struct {
	}{}
	lockBoltRowsMockNextNeo.Lock()
	mock.calls.NextNeo = append(mock.calls.NextNeo, callInfo)
	lockBoltRowsMockNextNeo.Unlock()
	return mock.NextNeoFunc()
}

// NextNeoCalls gets all the calls that were made to NextNeo.
// Check the length with:
//     len(mockedBoltRows.NextNeoCalls())
func (mock *BoltRowsMock) NextNeoCalls() []struct {
} {
	var calls []struct {
	}
	lockBoltRowsMockNextNeo.RLock()
	calls = mock.calls.NextNeo
	lockBoltRowsMockNextNeo.RUnlock()
	return calls
}
