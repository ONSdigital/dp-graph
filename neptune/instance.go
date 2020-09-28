package neptune

import (
	"context"
	"fmt"
	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"strings"

	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	gremgo "github.com/ONSdigital/gremgo-neptune"
	"github.com/ONSdigital/log.go/log"
	"github.com/pkg/errors"
)

// Type check to ensure that NeptuneDB implements the driver.Instance interface
var _ driver.Instance = (*NeptuneDB)(nil)

const codeListNotFoundFmt = "VertexStep(OUT,[usedBy],vertex), HasStep([~label.eq(_code_list_%s)"

// CountInsertedObservations returns the current number of observations relating to a given instance
func (n *NeptuneDB) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return n.getNumber(fmt.Sprintf(query.CountObservations, instanceID))
}

// AddVersionDetailsToInstance updates an instance node to contain details of which
// dataset, edition and version the instance will also be known by
func (n *NeptuneDB) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	data := log.Data{
		"instance_id": instanceID,
		"dataset_id":  datasetID,
		"edition":     edition,
		"version":     version,
	}

	q := fmt.Sprintf(query.AddVersionDetailsToInstance, instanceID, datasetID, edition, version)

	if _, err := n.exec(q); err != nil {
		log.Event(ctx, "neptune exec failed on AddVersionDetailsToInstance", log.ERROR, data, log.Error(err))
		return err
	}
	return nil
}

// SetInstanceIsPublished sets a flag on an instance node to indicate the published state
func (n *NeptuneDB) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	data := log.Data{
		"instance_id": instanceID,
	}

	q := fmt.Sprintf(query.SetInstanceIsPublished, instanceID)

	if _, err := n.exec(q); err != nil {
		log.Event(ctx, "neptune exec failed on SetInstanceIsPublished", log.ERROR, data, log.Error(err))
		return err
	}
	return nil
}

// CreateInstanceConstraint is not needed for the neptune implementation, as constraints are
// not a neptune construct
func (n *NeptuneDB) CreateInstanceConstraint(ctx context.Context, instanceID string) error {
	return nil
}

// CreateInstance will check if an instance node already exists and create one from
// the provided details if one does not exist
func (n *NeptuneDB) CreateInstance(ctx context.Context, instanceID string, csvHeaders []string) error {
	if len(instanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}

	data := log.Data{
		"instance_id": instanceID,
	}

	exists, err := n.InstanceExists(ctx, instanceID)
	if err != nil {
		return err
	}

	if exists {
		log.Event(ctx, "instance already exists in neptune", log.WARN, data)
		return nil
	}

	create := fmt.Sprintf(query.CreateInstance, instanceID, instanceID, strings.Join(csvHeaders, ","))
	if _, err := n.exec(create); err != nil {
		log.Event(ctx, "neptune exec failed on CreateInstance", log.ERROR, data, log.Error(err))
		return err
	}
	return nil
}

// AddDimensions list to the specified instance node
func (n *NeptuneDB) AddDimensions(ctx context.Context, instanceID string, dimensions []interface{}) error {

	data := log.Data{
		"instance_id": instanceID,
	}

	q := fmt.Sprintf(query.AddInstanceDimensionsPart, instanceID)
	for _, d := range dimensions {
		q += fmt.Sprintf(query.AddInstanceDimensionsPropertyPart, d.(string))
	}

	if _, err := n.exec(q); err != nil {
		log.Event(ctx, "neptune exec failed on AddDimensions", log.ERROR, data, log.Error(err))
		return err
	}

	return nil
}

// CreateCodeRelationship links an instance to a code for the given dimension option
func (n *NeptuneDB) CreateCodeRelationship(ctx context.Context, instanceID, codeListID, code string) error {
	if len(instanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}

	if len(code) == 0 {
		return errors.New("error creating relationship from instance to code: code is required but was empty")
	}

	data := log.Data{
		"instance_id": instanceID,
		"code_list":   codeListID,
		"code":        code,
	}

	createRelationships := fmt.Sprintf(query.CreateInstanceToCodeRelationship, instanceID, code, codeListID)
	if res, err := n.exec(createRelationships); err != nil {
		if len(res) > 0 && res[0].Status.Code == gremgo.StatusScriptEvaluationError &&
			strings.Contains(res[0].Status.Message, fmt.Sprintf(codeListNotFoundFmt, codeListID)) {

			return errors.Wrapf(err, "error creating relationship from instance to code: code or code list not found: %+v", data)
		}
		log.Event(ctx, "neptune exec failed on CreateCodeRelationship", log.ERROR, data, log.Error(err))
		return err
	}

	return nil
}

// InstanceExists returns true if an instance already exists with the provided id
func (n *NeptuneDB) InstanceExists(ctx context.Context, instanceID string) (bool, error) {
	data := log.Data{
		"instance_id": instanceID,
	}

	exists := fmt.Sprintf(query.CheckInstance, instanceID)
	count, err := n.getNumber(exists)
	if err != nil {
		log.Event(ctx, "neptune getNumber failed to check if instance exists", log.ERROR, data, log.Error(err))
		return false, err
	}

	if count == 0 {
		return false, nil
	}

	return true, nil
}
