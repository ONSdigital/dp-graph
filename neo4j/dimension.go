package neo4j

import (
	"context"
	"fmt"
	"sync"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/v2/neo4j/query"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

// Type check to ensure that Neo4j implements the driver.Dimension interface
var _ driver.Dimension = (*Neo4j)(nil)

// InsertDimension node to neo4j and create a unique constraint on the dimension
// label & value if one does not already exist, return dimension with new node ID
func (n *Neo4j) InsertDimension(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, instanceID string, d *models.Dimension) (*models.Dimension, error) {
	if len(instanceID) == 0 {
		return nil, errors.New("instance id is required but was empty")
	}
	if cache == nil {
		return nil, errors.New("no cache map provided to InsertDimension")
	}
	if cacheMutex == nil {
		return nil, errors.New("no cache mutex provided to InsertDimension")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	// cache dimension and createUniqueConstraint only if the cache value was added now
	dimensionLabel := fmt.Sprintf("_%s_%s", instanceID, d.DimensionID)
	if created := cacheDimension(ctx, cache, cacheMutex, dimensionLabel); created {
		if err := n.createUniqueConstraint(ctx, instanceID, d.DimensionID); err != nil {
			return nil, err
		}
	}

	if err := n.insertDimension(ctx, instanceID, d); err != nil {
		return nil, err
	}

	return d, nil
}

// cacheDimension adds an entry to the cache for the provided instance and dimension, only if it does not exist
// This method is concurrency safe, as it does the check+update after acquiring an exclusive lock
// It returns true if a new entry was created to the cache, or false if it already existed before the call.
func cacheDimension(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, dimensionLabel string) bool {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if _, exists := cache[dimensionLabel]; !exists {
		cache[dimensionLabel] = dimensionLabel
		return true
	}
	return false
}

func (n *Neo4j) createUniqueConstraint(ctx context.Context, instanceID, dimensionID string) error {
	stmt := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, dimensionID)

	if _, err := n.Exec(stmt, nil); err != nil {
		return errors.Wrap(err, "neoClient.Exec returned an error")
	}

	log.Info(ctx, "successfully created unique constraint on dimension", log.Data{"dimension_id": dimensionID})
	return nil
}

func (n *Neo4j) insertDimension(ctx context.Context, instanceID string, d *models.Dimension) error {
	logData := log.Data{
		"dimension_id": d.DimensionID,
		"value":        d.Option,
	}

	var err error
	params := map[string]interface{}{"value": d.Option}
	logData["params"] = params

	stmt := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, instanceID, d.DimensionID)
	logData["statement"] = stmt

	nodeID := new(string)
	if err = n.ReadWithParams(stmt, params, mapper.GetNodeID(nodeID), true); err != nil {
		return errors.Wrap(err, "neoClient.ReadWithParams returned an error")
	}

	d.NodeID = *nodeID
	return nil
}
