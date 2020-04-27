package neptune

import (
	"context"
	"fmt"
	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/pkg/errors"
)

// Type check to ensure that NeptuneDB implements the driver.Dimension interface
var _ driver.Dimension = (*NeptuneDB)(nil)

// InsertDimension node to neptune and create relationships to the instance node.
// Where nodes and relationships already exist, ensure they are upserted.
func (n *NeptuneDB) InsertDimension(ctx context.Context, uniqueDimensions map[string]string, instanceID string, d *models.Dimension) (*models.Dimension, error) {
	if len(instanceID) == 0 {
		return nil, errors.New("instance id is required but was empty")
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}

	dimensionLabel := fmt.Sprintf("_%s_%s", instanceID, d.DimensionID)

	res, err := n.getVertex(fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, d.DimensionID, d.Option, instanceID, d.DimensionID, d.Option, instanceID))
	if err != nil {
		return nil, err
	}

	d.NodeID = res.GetID()

	if _, ok := uniqueDimensions[dimensionLabel]; !ok {
		uniqueDimensions[dimensionLabel] = dimensionLabel
	}

	return d, nil
}
