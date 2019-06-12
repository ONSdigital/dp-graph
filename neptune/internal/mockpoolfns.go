package internal

/*
This module provides a handful of mock convenience functions that can be
used to inject behaviour into NeptunePoolMock.
*/

import (
	"errors"
)

// ReturnOne is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 1.
var ReturnOne = func(q string, bindings, rebindings map[string]string) (
	i int64, err error) {
	return 1, nil
}

// ReturnZero is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 0.
var ReturnZero = func(q string, bindings, rebindings map[string]string) (
	i int64, err error) {
	return 0, nil
}

// ReturnMalformedRequestErr is a mock implementation for NeptunePool.GetCount()
// that always returns an error that is judged to be not transient by
// neptune.isTransientError
var ReturnMalformedRequestErr = func(q string, bindings, rebindings map[string]string) (
	i int64, err error) {
	return -1, errors.New(" MALFORMED REQUEST ")
}
