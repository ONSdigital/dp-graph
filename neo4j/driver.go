package neo4j

import (
	"io"
	"strconv"

	dpbolt "github.com/ONSdigital/dp-bolt/bolt"
	"github.com/ONSdigital/dp-graph/graph/driver"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/ian-kent/go-log/log"
	"github.com/pkg/errors"
)

//import("graph/driver")

type Neo4j struct {
	//	url    string
	pool bolt.ClosableDriverPool
	//	mapper *mapper.Mapper
}

var ErrNotFound = errors.New("not found")

func New(dbAddr, poolSize string) (d *Neo4j, err error) {
	size, err := strconv.Atoi(poolSize)
	if err != nil {
		return nil, err
	}

	pool, err := bolt.NewClosableDriverPool(dbAddr, size)
	if err != nil {
		log.Error(err, nil)
		return nil, err
	}

	return &Neo4j{
		//	codeListLabel: codelistLabel,
		pool: pool,
		// mapper: &mapper.Mapper{
		// 	Host:           apiURL,
		// 	DatasetAPIHost: datasetAPIurl,
		// },
	}, nil
}

func (d *Neo4j) Open(name string) (driver.Conn, error) {
	return d.pool.OpenPool()
	// if err != nil {
	// 	return nil, err
	// }
	//
	// return co, nil
}

func (n *Neo4j) open() (*Conn, error) {
	conn, err := n.Open("")
	c, ok := conn.(Conn)
	if err != nil || !ok {
		defer conn.Close()

		if !ok {
			return nil, errors.New("not a valid connection type")
		}
		return nil, err
	}
	return &c, nil
}

type Conn struct {
	conn bolt.Conn
}

func (c Conn) Close() error {
	return c.conn.Close()
}

func (n *Neo4j) exec(query string, mapper dpbolt.ResultMapper, single bool) error {
	//	var c Conn
	c, err := n.open()
	defer c.Close()

	//	var rows *bolt.Result
	rows, err := c.conn.QueryNeo(query, nil)
	if err != nil {
		return errors.WithMessage(err, "error executing neo4j query")
	}
	defer rows.Close()

	index := 0
	numOfResults := 0
results:
	for {
		data, meta, nextNeoErr := rows.NextNeo()
		if nextNeoErr != nil {
			if nextNeoErr != io.EOF {
				return errors.WithMessage(nextNeoErr, "extractResults: rows.NextNeo() return unexpected error")
			}
			break results
		}

		numOfResults++
		if single && index > 0 {
			return errors.WithMessage(err, "non unique results")
		}

		if mapper != nil {
			if err := mapper(&dpbolt.Result{Data: data, Meta: meta, Index: index}); err != nil {
				return errors.WithMessage(err, "mapResult returned an error")
			}
		}
		index++
	}

	if numOfResults == 0 {
		return ErrNotFound
	}

	return nil
}
