package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/models"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

// InsertDimension node to neo4j and create a unique constraint on the dimension
// label & value if one does not already exist, return dimension with new node ID
func (n *Neo4j) InsertDimension(ctx context.Context, cache map[string]string, instanceID string, d *models.Dimension) (*models.Dimension, error) {
	if len(instanceID) == 0 {
		return nil, errors.New("instance id is required but was empty")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	dimensionLabel := fmt.Sprintf("_%s_%s", instanceID, d.DimensionID)

	if _, exists := cache[dimensionLabel]; !exists {

		if err := n.createUniqueConstraint(ctx, instanceID, d.DimensionID); err != nil {
			return nil, err
		}
		cache[dimensionLabel] = dimensionLabel
	}

	if err := n.insertDimension(ctx, instanceID, d); err != nil {
		return nil, err
	}

	return d, nil
}

func (n *Neo4j) createUniqueConstraint(ctx context.Context, instanceID, dimensionID string) error {
	stmt := fmt.Sprintf(query.CreateDimensionConstraint, instanceID, dimensionID)

	if _, err := n.Exec(stmt, nil); err != nil {
		return errors.Wrap(err, "neoClient.Exec returned an error")
	}

	log.Event(ctx, "successfully created unique constraint on dimension", log.INFO, log.Data{"dimension_id": dimensionID})
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
