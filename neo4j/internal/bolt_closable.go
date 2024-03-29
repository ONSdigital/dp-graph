// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package internal

import (
	"github.com/ONSdigital/dp-graph/v2/neo4j/neo4jdriver"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"sync"
)

// Ensure, that ClosableDriverPoolMock does implement neo4jdriver.ClosableDriverPool.
// If this is not the case, regenerate this file with moq.
var _ neo4jdriver.ClosableDriverPool = &ClosableDriverPoolMock{}

// ClosableDriverPoolMock is a mock implementation of neo4jdriver.ClosableDriverPool.
//
// 	func TestSomethingThatUsesClosableDriverPool(t *testing.T) {
//
// 		// make and configure a mocked neo4jdriver.ClosableDriverPool
// 		mockedClosableDriverPool := &ClosableDriverPoolMock{
// 			CloseFunc: func() error {
// 				panic("mock out the Close method")
// 			},
// 			OpenPoolFunc: func() (bolt.Conn, error) {
// 				panic("mock out the OpenPool method")
// 			},
// 		}
//
// 		// use mockedClosableDriverPool in code that requires neo4jdriver.ClosableDriverPool
// 		// and then make assertions.
//
// 	}
type ClosableDriverPoolMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func() error

	// OpenPoolFunc mocks the OpenPool method.
	OpenPoolFunc func() (bolt.Conn, error)

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
		}
		// OpenPool holds details about calls to the OpenPool method.
		OpenPool []struct {
		}
	}
	lockClose    sync.RWMutex
	lockOpenPool sync.RWMutex
}

// Close calls CloseFunc.
func (mock *ClosableDriverPoolMock) Close() error {
	if mock.CloseFunc == nil {
		panic("ClosableDriverPoolMock.CloseFunc: method is nil but ClosableDriverPool.Close was just called")
	}
	callInfo := struct {
	}{}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc()
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedClosableDriverPool.CloseCalls())
func (mock *ClosableDriverPoolMock) CloseCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// OpenPool calls OpenPoolFunc.
func (mock *ClosableDriverPoolMock) OpenPool() (bolt.Conn, error) {
	if mock.OpenPoolFunc == nil {
		panic("ClosableDriverPoolMock.OpenPoolFunc: method is nil but ClosableDriverPool.OpenPool was just called")
	}
	callInfo := struct {
	}{}
	mock.lockOpenPool.Lock()
	mock.calls.OpenPool = append(mock.calls.OpenPool, callInfo)
	mock.lockOpenPool.Unlock()
	return mock.OpenPoolFunc()
}

// OpenPoolCalls gets all the calls that were made to OpenPool.
// Check the length with:
//     len(mockedClosableDriverPool.OpenPoolCalls())
func (mock *ClosableDriverPoolMock) OpenPoolCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockOpenPool.RLock()
	calls = mock.calls.OpenPool
	mock.lockOpenPool.RUnlock()
	return calls
}
