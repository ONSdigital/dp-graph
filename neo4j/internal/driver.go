// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package internal

import (
	"context"
	"github.com/ONSdigital/dp-graph/v3/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/v3/neo4j/neo4jdriver"
	"github.com/ONSdigital/dp-healthcheck/v2/healthcheck"
	"github.com/ONSdigital/golang-neo4j-bolt-driver"
	"sync"
)

var (
	lockNeo4jDriverMockChecker        sync.RWMutex
	lockNeo4jDriverMockClose          sync.RWMutex
	lockNeo4jDriverMockCount          sync.RWMutex
	lockNeo4jDriverMockExec           sync.RWMutex
	lockNeo4jDriverMockHealthcheck    sync.RWMutex
	lockNeo4jDriverMockRead           sync.RWMutex
	lockNeo4jDriverMockReadWithParams sync.RWMutex
	lockNeo4jDriverMockStreamRows     sync.RWMutex
)

// Ensure, that Neo4jDriverMock does implement neo4jdriver.Neo4jDriver.
// If this is not the case, regenerate this file with moq.
var _ neo4jdriver.Neo4jDriver = &Neo4jDriverMock{}

// Neo4jDriverMock is a mock implementation of neo4jdriver.Neo4jDriver.
//
//     func TestSomethingThatUsesNeo4jDriver(t *testing.T) {
//
//         // make and configure a mocked neo4jdriver.Neo4jDriver
//         mockedNeo4jDriver := &Neo4jDriverMock{
//             CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error {
// 	               panic("mock out the Checker method")
//             },
//             CloseFunc: func(ctx context.Context) error {
// 	               panic("mock out the Close method")
//             },
//             CountFunc: func(query string) (int64, error) {
// 	               panic("mock out the Count method")
//             },
//             ExecFunc: func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
// 	               panic("mock out the Exec method")
//             },
//             HealthcheckFunc: func() (string, error) {
// 	               panic("mock out the Healthcheck method")
//             },
//             ReadFunc: func(query string, mapp mapper.ResultMapper, single bool) error {
// 	               panic("mock out the Read method")
//             },
//             ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
// 	               panic("mock out the ReadWithParams method")
//             },
//             StreamRowsFunc: func(query string) (*neo4jdriver.BoltRowReader, error) {
// 	               panic("mock out the StreamRows method")
//             },
//         }
//
//         // use mockedNeo4jDriver in code that requires neo4jdriver.Neo4jDriver
//         // and then make assertions.
//
//     }
type Neo4jDriverMock struct {
	// CheckerFunc mocks the Checker method.
	CheckerFunc func(ctx context.Context, state *healthcheck.CheckState) error

	// CloseFunc mocks the Close method.
	CloseFunc func(ctx context.Context) error

	// CountFunc mocks the Count method.
	CountFunc func(query string) (int64, error)

	// ExecFunc mocks the Exec method.
	ExecFunc func(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Result, error)

	// HealthcheckFunc mocks the Healthcheck method.
	HealthcheckFunc func() (string, error)

	// ReadFunc mocks the Read method.
	ReadFunc func(query string, mapp mapper.ResultMapper, single bool) error

	// ReadWithParamsFunc mocks the ReadWithParams method.
	ReadWithParamsFunc func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error

	// StreamRowsFunc mocks the StreamRows method.
	StreamRowsFunc func(query string) (*neo4jdriver.BoltRowReader, error)

	// calls tracks calls to the methods.
	calls struct {
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// State is the state argument value.
			State *healthcheck.CheckState
		}
		// Close holds details about calls to the Close method.
		Close []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
		}
		// Count holds details about calls to the Count method.
		Count []struct {
			// Query is the query argument value.
			Query string
		}
		// Exec holds details about calls to the Exec method.
		Exec []struct {
			// Query is the query argument value.
			Query string
			// Params is the params argument value.
			Params map[string]interface{}
		}
		// Healthcheck holds details about calls to the Healthcheck method.
		Healthcheck []struct {
		}
		// Read holds details about calls to the Read method.
		Read []struct {
			// Query is the query argument value.
			Query string
			// Mapp is the mapp argument value.
			Mapp mapper.ResultMapper
			// Single is the single argument value.
			Single bool
		}
		// ReadWithParams holds details about calls to the ReadWithParams method.
		ReadWithParams []struct {
			// Query is the query argument value.
			Query string
			// Params is the params argument value.
			Params map[string]interface{}
			// Mapp is the mapp argument value.
			Mapp mapper.ResultMapper
			// Single is the single argument value.
			Single bool
		}
		// StreamRows holds details about calls to the StreamRows method.
		StreamRows []struct {
			// Query is the query argument value.
			Query string
		}
	}
}

// Checker calls CheckerFunc.
func (mock *Neo4jDriverMock) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("Neo4jDriverMock.CheckerFunc: method is nil but Neo4jDriver.Checker was just called")
	}
	callInfo := struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}{
		Ctx:   ctx,
		State: state,
	}
	lockNeo4jDriverMockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	lockNeo4jDriverMockChecker.Unlock()
	return mock.CheckerFunc(ctx, state)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedNeo4jDriver.CheckerCalls())
func (mock *Neo4jDriverMock) CheckerCalls() []struct {
	Ctx   context.Context
	State *healthcheck.CheckState
} {
	var calls []struct {
		Ctx   context.Context
		State *healthcheck.CheckState
	}
	lockNeo4jDriverMockChecker.RLock()
	calls = mock.calls.Checker
	lockNeo4jDriverMockChecker.RUnlock()
	return calls
}

// Close calls CloseFunc.
func (mock *Neo4jDriverMock) Close(ctx context.Context) error {
	if mock.CloseFunc == nil {
		panic("Neo4jDriverMock.CloseFunc: method is nil but Neo4jDriver.Close was just called")
	}
	callInfo := struct {
		Ctx context.Context
	}{
		Ctx: ctx,
	}
	lockNeo4jDriverMockClose.Lock()
	mock.calls.Close = append(mock.calls.Close, callInfo)
	lockNeo4jDriverMockClose.Unlock()
	return mock.CloseFunc(ctx)
}

// CloseCalls gets all the calls that were made to Close.
// Check the length with:
//     len(mockedNeo4jDriver.CloseCalls())
func (mock *Neo4jDriverMock) CloseCalls() []struct {
	Ctx context.Context
} {
	var calls []struct {
		Ctx context.Context
	}
	lockNeo4jDriverMockClose.RLock()
	calls = mock.calls.Close
	lockNeo4jDriverMockClose.RUnlock()
	return calls
}

// Count calls CountFunc.
func (mock *Neo4jDriverMock) Count(query string) (int64, error) {
	if mock.CountFunc == nil {
		panic("Neo4jDriverMock.CountFunc: method is nil but Neo4jDriver.Count was just called")
	}
	callInfo := struct {
		Query string
	}{
		Query: query,
	}
	lockNeo4jDriverMockCount.Lock()
	mock.calls.Count = append(mock.calls.Count, callInfo)
	lockNeo4jDriverMockCount.Unlock()
	return mock.CountFunc(query)
}

// CountCalls gets all the calls that were made to Count.
// Check the length with:
//     len(mockedNeo4jDriver.CountCalls())
func (mock *Neo4jDriverMock) CountCalls() []struct {
	Query string
} {
	var calls []struct {
		Query string
	}
	lockNeo4jDriverMockCount.RLock()
	calls = mock.calls.Count
	lockNeo4jDriverMockCount.RUnlock()
	return calls
}

// Exec calls ExecFunc.
func (mock *Neo4jDriverMock) Exec(query string, params map[string]interface{}) (golangNeo4jBoltDriver.Result, error) {
	if mock.ExecFunc == nil {
		panic("Neo4jDriverMock.ExecFunc: method is nil but Neo4jDriver.Exec was just called")
	}
	callInfo := struct {
		Query  string
		Params map[string]interface{}
	}{
		Query:  query,
		Params: params,
	}
	lockNeo4jDriverMockExec.Lock()
	mock.calls.Exec = append(mock.calls.Exec, callInfo)
	lockNeo4jDriverMockExec.Unlock()
	return mock.ExecFunc(query, params)
}

// ExecCalls gets all the calls that were made to Exec.
// Check the length with:
//     len(mockedNeo4jDriver.ExecCalls())
func (mock *Neo4jDriverMock) ExecCalls() []struct {
	Query  string
	Params map[string]interface{}
} {
	var calls []struct {
		Query  string
		Params map[string]interface{}
	}
	lockNeo4jDriverMockExec.RLock()
	calls = mock.calls.Exec
	lockNeo4jDriverMockExec.RUnlock()
	return calls
}

// Healthcheck calls HealthcheckFunc.
func (mock *Neo4jDriverMock) Healthcheck() (string, error) {
	if mock.HealthcheckFunc == nil {
		panic("Neo4jDriverMock.HealthcheckFunc: method is nil but Neo4jDriver.Healthcheck was just called")
	}
	callInfo := struct {
	}{}
	lockNeo4jDriverMockHealthcheck.Lock()
	mock.calls.Healthcheck = append(mock.calls.Healthcheck, callInfo)
	lockNeo4jDriverMockHealthcheck.Unlock()
	return mock.HealthcheckFunc()
}

// HealthcheckCalls gets all the calls that were made to Healthcheck.
// Check the length with:
//     len(mockedNeo4jDriver.HealthcheckCalls())
func (mock *Neo4jDriverMock) HealthcheckCalls() []struct {
} {
	var calls []struct {
	}
	lockNeo4jDriverMockHealthcheck.RLock()
	calls = mock.calls.Healthcheck
	lockNeo4jDriverMockHealthcheck.RUnlock()
	return calls
}

// Read calls ReadFunc.
func (mock *Neo4jDriverMock) Read(query string, mapp mapper.ResultMapper, single bool) error {
	if mock.ReadFunc == nil {
		panic("Neo4jDriverMock.ReadFunc: method is nil but Neo4jDriver.Read was just called")
	}
	callInfo := struct {
		Query  string
		Mapp   mapper.ResultMapper
		Single bool
	}{
		Query:  query,
		Mapp:   mapp,
		Single: single,
	}
	lockNeo4jDriverMockRead.Lock()
	mock.calls.Read = append(mock.calls.Read, callInfo)
	lockNeo4jDriverMockRead.Unlock()
	return mock.ReadFunc(query, mapp, single)
}

// ReadCalls gets all the calls that were made to Read.
// Check the length with:
//     len(mockedNeo4jDriver.ReadCalls())
func (mock *Neo4jDriverMock) ReadCalls() []struct {
	Query  string
	Mapp   mapper.ResultMapper
	Single bool
} {
	var calls []struct {
		Query  string
		Mapp   mapper.ResultMapper
		Single bool
	}
	lockNeo4jDriverMockRead.RLock()
	calls = mock.calls.Read
	lockNeo4jDriverMockRead.RUnlock()
	return calls
}

// ReadWithParams calls ReadWithParamsFunc.
func (mock *Neo4jDriverMock) ReadWithParams(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
	if mock.ReadWithParamsFunc == nil {
		panic("Neo4jDriverMock.ReadWithParamsFunc: method is nil but Neo4jDriver.ReadWithParams was just called")
	}
	callInfo := struct {
		Query  string
		Params map[string]interface{}
		Mapp   mapper.ResultMapper
		Single bool
	}{
		Query:  query,
		Params: params,
		Mapp:   mapp,
		Single: single,
	}
	lockNeo4jDriverMockReadWithParams.Lock()
	mock.calls.ReadWithParams = append(mock.calls.ReadWithParams, callInfo)
	lockNeo4jDriverMockReadWithParams.Unlock()
	return mock.ReadWithParamsFunc(query, params, mapp, single)
}

// ReadWithParamsCalls gets all the calls that were made to ReadWithParams.
// Check the length with:
//     len(mockedNeo4jDriver.ReadWithParamsCalls())
func (mock *Neo4jDriverMock) ReadWithParamsCalls() []struct {
	Query  string
	Params map[string]interface{}
	Mapp   mapper.ResultMapper
	Single bool
} {
	var calls []struct {
		Query  string
		Params map[string]interface{}
		Mapp   mapper.ResultMapper
		Single bool
	}
	lockNeo4jDriverMockReadWithParams.RLock()
	calls = mock.calls.ReadWithParams
	lockNeo4jDriverMockReadWithParams.RUnlock()
	return calls
}

// StreamRows calls StreamRowsFunc.
func (mock *Neo4jDriverMock) StreamRows(query string) (*neo4jdriver.BoltRowReader, error) {
	if mock.StreamRowsFunc == nil {
		panic("Neo4jDriverMock.StreamRowsFunc: method is nil but Neo4jDriver.StreamRows was just called")
	}
	callInfo := struct {
		Query string
	}{
		Query: query,
	}
	lockNeo4jDriverMockStreamRows.Lock()
	mock.calls.StreamRows = append(mock.calls.StreamRows, callInfo)
	lockNeo4jDriverMockStreamRows.Unlock()
	return mock.StreamRowsFunc(query)
}

// StreamRowsCalls gets all the calls that were made to StreamRows.
// Check the length with:
//     len(mockedNeo4jDriver.StreamRowsCalls())
func (mock *Neo4jDriverMock) StreamRowsCalls() []struct {
	Query string
} {
	var calls []struct {
		Query string
	}
	lockNeo4jDriverMockStreamRows.RLock()
	calls = mock.calls.StreamRows
	lockNeo4jDriverMockStreamRows.RUnlock()
	return calls
}
