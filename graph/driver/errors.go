package driver

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when the result set from the database held 0 records
var ErrNotFound = errors.New("not found")

// ErrMultipleFound is returned when the result set from the database holds
// more than one error, inside a call that requires exactly one.
var ErrMultipleFound = errors.New("multiple found where should be one")

// ErrNotImplemented is returned when a method is called but the driver does not implement it
var ErrNotImplemented = errors.New("method not implemented by driver")

// ErrAttemptsExceededLimit is returned when the number of attempts has reaced
// the maximum permitted
type ErrAttemptsExceededLimit struct {
	WrappedErr error
}

func (e ErrAttemptsExceededLimit) Error() string {
	return fmt.Sprintf("number of attempts to execute statement exceeded: %s", e.WrappedErr.Error())
}

// ErrNonRetriable is returned when the wrapped error type is not retriable
type ErrNonRetriable struct {
	WrappedErr error
}

func (e ErrNonRetriable) Error() string {
	return fmt.Sprintf("received a non retriable error from neo4j: %s", e.WrappedErr.Error())
}
