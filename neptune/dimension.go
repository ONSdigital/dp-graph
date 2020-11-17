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

	dimID := fmt.Sprintf("_%s_%s_%s", instanceID, d.DimensionID, d.Option)

	err := n.removeExistingDimension(dimID)
	if err != nil {
		return nil, err
	}

	err = n.createDimension(instanceID, d, dimID)
	if err != nil {
		return nil, err
	}

	d.NodeID = dimID
	dimensionLabel := fmt.Sprintf("_%s_%s", instanceID, d.DimensionID)

	if _, ok := uniqueDimensions[dimensionLabel]; !ok {
		uniqueDimensions[dimensionLabel] = dimensionLabel
	}

	return d, nil
}

func (n *NeptuneDB) createDimension(instanceID string, d *models.Dimension, dimID string) error {

	createDim := fmt.Sprintf(query.CreateDimension, instanceID, d.DimensionID, dimID, d.Option)
	if _, err := n.exec(createDim); err != nil {
		return err
	}

	createDimEdge := fmt.Sprintf(query.CreateDimensionToInstanceRelationship, instanceID, dimID)
	if _, err := n.exec(createDimEdge); err != nil {
		return err
	}

	return nil
}

func (n *NeptuneDB) removeExistingDimension(dimID string) error {

	getDim := fmt.Sprintf(query.GetDimension, dimID)

	existingDimIDs, err := n.getStringList(getDim)
	if err != nil {
		return err
	}

	if len(existingDimIDs) > 0 {
		dropDim := fmt.Sprintf(query.DropDimensionRelationships, dimID)
		dropDim += fmt.Sprintf(query.DropDimension, dimID)

		if _, err := n.exec(dropDim); err != nil {
			return err
		}
	}

	return nil
}
