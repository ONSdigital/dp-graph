package neptune

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/dp-graph/v2/observation"
	"github.com/ONSdigital/log.go/log"
)

// Type check to ensure that NeptuneDB implements the driver.Observation interface
var _ driver.Observation = (*NeptuneDB)(nil)

// ErrInvalidFilter is returned if the provided filter is nil.
var ErrInvalidFilter = errors.New("nil filter cannot be processed")

// TODO: this global state is only used for metrics in InsertObservationBatch,
// not used in actual code flow, but should be revisited before production use
var batchCount = 0
var totalTime time.Time

// StreamCSVRows returns a reader allowing individual CSV rows to be read.
// Rows returned can be limited, to stop this pass in nil. If filter.DimensionFilters
// is nil, empty or contains only empty values then a StreamRowReader for the entire dataset will be returned.
func (n *NeptuneDB) StreamCSVRows(ctx context.Context, instanceID, filterID string, filter *observation.DimensionFilters, limit *int) (observation.StreamRowReader, error) {
	if filter == nil {
		return nil, ErrInvalidFilter
	}

	q := fmt.Sprintf(query.GetInstanceHeaderPart, instanceID)
	headerReader, err := n.Pool.OpenStreamCursor(ctx, q, nil, nil)
	if err != nil {
		return nil, err
	}

	q = buildObservationsQuery(instanceID, filter)
	q += query.GetObservationValuesPart

	if limit != nil {
		q += fmt.Sprintf(query.LimitPart, *limit)
	}

	observationReader, err := n.Pool.OpenStreamCursor(ctx, q, nil, nil)
	if err != nil {
		return nil, err
	}

	return observation.NewCompositeRowReader(headerReader, observationReader), nil
}

func buildObservationsQuery(instanceID string, f *observation.DimensionFilters) string {
	if f.IsEmpty() {
		return fmt.Sprintf(query.GetAllObservationsPart, instanceID)
	}

	var q string
	additionalDimensions := 0
	var additionalDimensionOptions []string

	for i, dim := range f.Dimensions {
		if len(dim.Options) == 0 {
			continue
		}

		optionIdPrefix := fmt.Sprintf("_%s_%s_", instanceID, dim.Name)
		var optionIdList []string

		for _, opt := range dim.Options {
			optionId := optionIdPrefix + opt
			optionIdList = append(optionIdList, fmt.Sprintf("'%s'", optionId))
		}

		// the first dimension filtered independently of the rest to reduce the set of observations to filter, improving performance
		if i == 0 {
			q = fmt.Sprintf(query.GetFirstDimensionPart, strings.Join(optionIdList, ","))
			continue
		}

		additionalDimensions++
		additionalDimensionOptions = append(additionalDimensionOptions, optionIdList...)
	}

	// only filter on additional dimensions if they are defined.
	if additionalDimensions > 0 {
		q += fmt.Sprintf(query.GetAdditionalDimensionsPart, strings.Join(additionalDimensionOptions, ","), additionalDimensions)
	}

	return q
}

// InsertObservationBatch creates a batch query based on a provided list of
// observations and attempts to insert them by bulk to the database
func (n *NeptuneDB) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionNodeIDs map[string]string) error {
	if len(observations) == 0 {
		log.Event(ctx, "no observations in batch", log.INFO, log.Data{"instance_ID": instanceID})
		return nil
	}

	bID := batchCount
	batchCount++
	batchStart := time.Now()
	if totalTime.IsZero() {
		totalTime = batchStart
	} else {
		log.Event(ctx, "opening batch", log.INFO, log.Data{"size": len(observations), "batchID": bID})
	}

	var obsIDs []string
	var dimIDs []string
	obsIDMap := map[string]*models.Observation{}
	dimIDMap := map[string]struct{}{}

	for _, obs := range observations {
		obs.Row = escapeSingleQuotes(obs.Row)
		obsID := fmt.Sprintf(`_%s_observation_%d`, instanceID, obs.RowIndex)
		obsIDMap[obsID] = obs
		obsIDs = append(obsIDs, obsID)

		for _, dim := range obs.DimensionOptions {
			dimID := createDimensionId(dim, instanceID)

			_, ok := dimIDMap[dimID]
			if !ok {
				dimIDs = append(dimIDs, dimID)
			}

			dimIDMap[dimID] = struct{}{}
		}
	}

	err := n.removeExistingObservations(obsIDs)
	if err != nil {
		return errors.Wrap(err, "failed to remove existing observations")
	}

	err = n.addObservationNodes(obsIDs, obsIDMap, instanceID)
	if err != nil {
		return errors.Wrap(err, "failed to add observation nodes")
	}

	err = n.addObservationEdges(dimIDs, obsIDs, obsIDMap, instanceID)
	if err != nil {
		return errors.Wrap(err, "failed to add observation edges")
	}

	log.Event(ctx, "batch complete", log.INFO, log.Data{"batchID": bID, "elapsed": time.Since(totalTime), "batchTime": time.Since(batchStart)})
	return nil
}

func (n *NeptuneDB) addObservationEdges(dimIDs []string, obsIDs []string, obsIDMap map[string]*models.Observation, instanceID string) error {
	insertObsEdgesStmt := "g"

	// add lookups for dimension nodes
	for _, dimID := range dimIDs {
		insertObsEdgesStmt += fmt.Sprintf(query.DimensionLookupPart, dimID, dimID)
	}

	for _, obsID := range obsIDs {
		for _, dim := range obsIDMap[obsID].DimensionOptions {
			dimID := createDimensionId(dim, instanceID)
			insertObsEdgesStmt += fmt.Sprintf(query.AddObservationEdgePart, obsID, dimID)
		}
	}

	if _, err := n.exec(insertObsEdgesStmt); err != nil {
		return err
	}
	return nil
}

func createDimensionId(dim *models.DimensionOption, instanceID string) string {
	dimName := strings.ToLower(dim.DimensionName)
	dimID := "_" + instanceID + "_" + dimName + "_" + dim.Name
	return dimID
}

// addObservationNodes creates graph DB nodes for the given observations and instance ID
func (n *NeptuneDB) addObservationNodes(obsIDs []string, obsIDMap map[string]*models.Observation, instanceID string) error {

	insertObsStmt := "g"
	for _, obsID := range obsIDs {
		obs := obsIDMap[obsID]
		insertObsStmt += fmt.Sprintf(query.CreateObservationPart, instanceID, obsID, obs.Row)
	}
	if _, err := n.exec(insertObsStmt); err != nil {
		return err
	}
	return nil
}

// removeExistingObservations removes existing observations for the given id's
func (n *NeptuneDB) removeExistingObservations(obsIDs []string) error {

	// query for existing observations to drop - most likely there will be none
	queryExistingObservations := fmt.Sprintf(query.GetObservations, `'`+strings.Join(obsIDs, `','`)+`'`)
	existingObsIDs, err := n.getStringList(queryExistingObservations)
	if err != nil {
		return err
	}

	if len(existingObsIDs) > 0 {

		existingObsIDsJoined := `'` + strings.Join(existingObsIDs, `','`) + `'`

		queryExistingObservationEdges := fmt.Sprintf(query.GetObservationsEdges, existingObsIDsJoined)
		existingObsEdgeIDs, err := n.getStringList(queryExistingObservationEdges)
		if err != nil {
			return err
		}

		var removeObsStmt string

		if len(existingObsEdgeIDs) > 0 {
			removeObsStmt += fmt.Sprintf(query.DropObservationEdges, `'`+strings.Join(existingObsEdgeIDs, `','`)+`'`)
		}

		removeObsStmt += fmt.Sprintf(query.DropObservations, existingObsIDsJoined)
		_, err = n.exec(removeObsStmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func escapeSingleQuotes(input string) string {
	return strings.Replace(input, "'", "\\'", -1)
}
