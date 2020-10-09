package neptune

import (
	"context"
	"fmt"
	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"strings"
	"time"

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

	q += buildObservationsQuery(instanceID, filter)
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

	var create string
	for _, o := range observations {
		o.Row = escapeSingleQuotes(o.Row)
		create += fmt.Sprintf(query.DropObservationRelationships, instanceID, o.Row)
		create += fmt.Sprintf(query.DropObservation, instanceID, o.Row)
		create += fmt.Sprintf(query.CreateObservationPart, instanceID, o.Row, o.RowIndex)
		for _, d := range o.DimensionOptions {
			dimensionName := strings.ToLower(d.DimensionName)
			dimensionLookup := instanceID + "_" + dimensionName + "_" + d.Name

			nodeID, ok := dimensionNodeIDs[dimensionLookup]
			if !ok {
				return fmt.Errorf("no nodeID [%s] found in dimension map", dimensionLookup)
			}

			create += fmt.Sprintf(query.AddObservationRelationshipPart, nodeID)
		}

		create = strings.TrimSuffix(create, ".outV()")
		create += ".iterate();"
	}

	create = strings.TrimSuffix(create, ".iterate();")
	if _, err := n.exec(create); err != nil {
		return err
	}

	log.Event(ctx, "batch complete", log.INFO, log.Data{"batchID": bID, "elapsed": time.Since(totalTime), "batchTime": time.Since(batchStart)})
	return nil
}

func escapeSingleQuotes(input string) string {
	for i, c := range input {
		if string(c) == "'" {
			input = input[:i] + "\\" + input[i:]
		}
	}
	return input
}
