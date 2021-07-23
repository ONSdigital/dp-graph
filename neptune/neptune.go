package neptune

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	neptune "github.com/ONSdigital/dp-graph/v2/neptune/driver"
	"github.com/ONSdigital/dp-graph/v2/retry"
	"github.com/ONSdigital/graphson"
	gremgo "github.com/ONSdigital/gremgo-neptune"
	"github.com/ONSdigital/log.go/v2/log"
)

type NeptuneDB struct {
	neptune.NeptuneDriver

	maxAttempts     int
	retryTime       time.Duration
	timeout         int
	batchSizeReader int
	batchSizeWriter int
	maxWorkers      int
}

func New(dbAddr string, size, timeout, retries, batchSizeReader, batchSizeWriter, maxWorkers int, retryTime time.Duration, errs chan error) (n *NeptuneDB, err error) {
	// set defaults if not provided
	if size == 0 {
		size = 30
	}
	if timeout == 0 {
		timeout = 30
	}
	if retries == 0 {
		retries = 5
	}
	if retryTime == 0 {
		retryTime = 20 * time.Millisecond
	}
	if batchSizeReader == 0 {
		batchSizeReader = 25000
	}
	if batchSizeWriter == 0 {
		batchSizeWriter = 150
	}
	if maxWorkers == 0 {
		maxWorkers = 150
	}

	var d *neptune.NeptuneDriver
	if d, err = neptune.New(context.Background(), dbAddr, errs); err != nil {
		return
	}

	// seed for sleepy() below
	rand.Seed(time.Now().Unix())

	n = &NeptuneDB{
		*d,
		1 + retries,
		retryTime,
		timeout,
		batchSizeReader,
		batchSizeWriter,
		maxWorkers,
	}
	return
}

func (n *NeptuneDB) getVertices(gremStmt string) (vertices []graphson.Vertex, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "getVertices", "statement": statementSummary(gremStmt), "attempt": 1}

	doer := func() (interface{}, error) {
		return n.Pool.Get(gremStmt, nil, nil)
	}

	res, err := n.attemptNeptuneRequest(ctx, doer, logData)
	if err != nil {
		return
	}

	var ok bool
	if vertices, ok = res.([]graphson.Vertex); !ok {
		err = errors.New("cannot cast Get results to []Vertex")
		log.Error(ctx, "cast", err, logData)
	}
	return
}

func (n *NeptuneDB) getStringList(gremStmt string) (strings []string, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "getStringList", "statement": statementSummary(gremStmt), "attempt": 1}

	doer := func() (interface{}, error) {
		return n.Pool.GetStringList(gremStmt, nil, nil)
	}

	res, err := n.attemptNeptuneRequest(ctx, doer, nil)
	if err != nil {
		return
	}

	var ok bool
	if strings, ok = res.([]string); !ok {
		err = errors.New("cannot cast GetStringList results to []String")
		log.Error(ctx, "cast", err, logData)
	}
	return
}

func (n *NeptuneDB) getVertex(gremStmt string) (vertex graphson.Vertex, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "getVertex", "statement": gremStmt}

	var vertices []graphson.Vertex
	if vertices, err = n.getVertices(gremStmt); err != nil {
		log.Error(ctx, "get", err, logData)
		return
	}
	if len(vertices) == 0 {
		err = driver.ErrNotFound
		log.Error(ctx, "vertex not found", err, logData)
		return
	}
	if len(vertices) != 1 {
		err = errors.New("expected only one vertex")
		log.Error(ctx, "more than one vertex found when only 1 is expected", err, logData)
		return
	}
	return vertices[0], nil
}

func (n *NeptuneDB) getEdges(gremStmt string) (edges []graphson.Edge, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "getEdges", "statement": statementSummary(gremStmt), "attempt": 1}

	doer := func() (interface{}, error) {
		return n.Pool.GetE(gremStmt, nil, nil)
	}

	res, err := n.attemptNeptuneRequest(ctx, doer, nil)
	if err != nil {
		return
	}

	var ok bool
	if edges, ok = res.([]graphson.Edge); !ok {
		err = errors.New("cannot cast GetE results to []Edge")
		log.Error(ctx, "cast", err, logData)
	}
	return
}

func (n *NeptuneDB) exec(gremStmt string) (gremgoRes []gremgo.Response, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "n.exec", "statement": statementSummary(gremStmt)}

	doer := func() (interface{}, error) {
		return n.Pool.Execute(gremStmt, nil, nil)
	}

	res, err := n.attemptNeptuneRequest(ctx, doer, nil)
	if err != nil {
		return
	}

	var ok bool
	if gremgoRes, ok = res.([]gremgo.Response); !ok {
		err = errors.New("cannot cast results to []gremgo.Response")
		log.Error(ctx, "cast", err, logData)
	}

	return
}

func (n *NeptuneDB) getNumber(gremStmt string) (count int64, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "n.getNumber", "statement": statementSummary(gremStmt), "attempt": 1}

	doer := func() (interface{}, error) {
		return n.Pool.GetCount(gremStmt, nil, nil)
	}

	res, err := n.attemptNeptuneRequest(ctx, doer, nil)
	if err != nil {
		return
	}

	var ok bool
	if count, ok = res.(int64); !ok {
		err = errors.New("cannot cast count results to int64")
		log.Error(ctx, "cast", err, logData)
	}

	return
}

func (n *NeptuneDB) attemptNeptuneRequest(ctx context.Context, doer retry.Doer, logData log.Data) (res interface{}, err error) {
	res, err = retry.Do(
		ctx,
		doer,
		isTransientError,
		n.maxAttempts,
		n.retryTime,
	)
	if err != nil {
		log.Error(ctx, "maxAttempts reached", err, logData)
		return nil, err
	}
	return
}

func isTransientError(err error) bool {
	if strings.Contains(err.Error(), " MALFORMED REQUEST ") ||
		strings.Contains(err.Error(), " INVALID REQUEST ARGUMENTS ") {
		return false
	}
	return true
}
