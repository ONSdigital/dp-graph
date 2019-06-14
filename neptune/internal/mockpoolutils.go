package internal

import (
	"fmt"

	"github.com/gedge/graphson"
)

/*
This module provides a handful of mock convenience functions that can be
used to inject behaviour into NeptunePoolMock.
*/

import (
	"errors"
)

// ReturnOne is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 1.
var ReturnOne = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 1, nil
}

// ReturnTwo is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 2.
var ReturnTwo = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 2, nil
}

// ReturnZero is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 0.
var ReturnZero = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 0, nil
}

// ReturnMalformedRequestErr is a mock implementation for NeptunePool.GetCount()
// that always returns an error that is judged to be not transient by
// neptune.isTransientError
var ReturnMalformedRequestErr = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return -1, errors.New(" MALFORMED REQUEST ")
}

// ReturnThreeCodeLists is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s)
var ReturnThreeCodeLists = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	const codeListLabel = "_code_list"
	for i := 0; i < 3; i++ {
		listID := fmt.Sprintf("listID_%d", i)
		gv := graphson.GenericValue{Type: "string", Value: listID}
		pv := graphson.VertexPropertyValue{
			ID:    gv,
			Label: "unused-label",
			Value: listID,
		}
		vertexProperty := graphson.VertexProperty{Type: "string", Value: pv}
		vertexProperties := []graphson.VertexProperty{vertexProperty}
		vertexValue := graphson.VertexValue{
			ID:         listID,
			Label:      codeListLabel,
			Properties: map[string][]graphson.VertexProperty{"unused-key": vertexProperties},
		}
		vertex := graphson.Vertex{Type: codeListLabel, Value: vertexValue}
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}
