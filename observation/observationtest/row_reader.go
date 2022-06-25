// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package observationtest

import (
	"context"
	"github.com/ONSdigital/dp-graph/v2/observation"
	"sync"
)

// Ensure, that StreamRowReaderMock does implement observation.StreamRowReader.
// If this is not the case, regenerate this file with moq.
var _ observation.StreamRowReader = &StreamRowReaderMock{}

// StreamRowReaderMock is a mock implementation of observation.StreamRowReader.
//
// 	func TestSomethingThatUsesStreamRowReader(t *testing.T) {
//
// 		// make and configure a mocked observation.StreamRowReader
// 		mockedStreamRowReader := &StreamRowReaderMock{
// 			CloseFunc: func(contextMoqParam context.Context) error {
// 				panic("mock out the Close method")
// 			},
// 			ReadFunc: func() (string, error) {
// 				panic("mock out the Read method")
// 			},
// 		}
//
// 		// use mockedStreamRowReader in code that requires observation.StreamRowReader
// 		// and then make assertions.
//
// 	}
type StreamRowReaderMock struct {
	// CloseFunc mocks the Close method.
	CloseFunc func(contextMoqParam context.Context) error

	// ReadFunc mocks the Read method.
	ReadFunc func() (string, error)

	// calls tracks calls to the methods.
	calls struct {
		// Close holds details about calls to the Close method.
		Close []struct {
			// ContextMoqParam is the contextMoqParam argument value.
			ContextMoqParam context.Context
		}
		// Read holds details about calls to the Read method.
		Read []struct {
		}
	}
	lockClose sync.RWMutex
	lockRead  sync.RWMutex
}

// Close calls CloseFunc.
func (mock *StreamRowReaderMock) Close(contextMoqParam context.Context) error {
	if mock.CloseFunc == nil {
		panic("StreamRowReaderMock.CloseFunc: method is nil but StreamRowReader.Close was just called")
	}
	callInfo := struct {
		ContextMoqParam context.Context
	}{
		ContextMoqParam: contextMoqParam,
	}
	mock.lockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	mock.lockClose.Unlock()
	return mock.CloseFunc(contextMoqParam)
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedStreamRowReader.CloseCalls())
func (mock *StreamRowReaderMock) CloseCalls() []struct {
	ContextMoqParam context.Context
} {
	var calls []struct {
		ContextMoqParam context.Context
	}
	mock.lockClose.RLock()
	calls = mock.calls.Close
	mock.lockClose.RUnlock()
	return calls
}

// Read calls ReadFunc.
func (mock *StreamRowReaderMock) Read() (string, error) {
	if mock.ReadFunc == nil {
		panic("StreamRowReaderMock.ReadFunc: method is nil but StreamRowReader.Read was just called")
	}
	callInfo := struct {
	}{}
	mock.lockRead.Lock()
	mock.calls.Read = append(mock.calls.Read, callInfo)
	mock.lockRead.Unlock()
	return mock.ReadFunc()
}

// ReadCalls gets all the calls that were made to Read.
// Check the length with:
//     len(mockedStreamRowReader.ReadCalls())
func (mock *StreamRowReaderMock) ReadCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockRead.RLock()
	calls = mock.calls.Read
	mock.lockRead.RUnlock()
	return calls
}
