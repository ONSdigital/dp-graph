package neptune

import (
	"context"
	"fmt"
	"strings"

	"github.com/ONSdigital/dp-graph/neptune/query"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
)

func (n *NeptuneDB) StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error) {
	q := fmt.Sprintf(query.GetInstanceHeader, filter.InstanceID)

	var obsQuery string
	if filter.IsEmpty() {
		obsQuery = fmt.Sprintf(query.GetAllObservations, filter.InstanceID)
	} else {
		obsQuery = buildObservationsQuery(filter)
	}

	q += obsQuery
	if limit != nil {
		q += fmt.Sprintf(query.LimitPart, *limit)
	}

	return n.Pool.OpenCursorCtx(ctx, q, nil, nil)
}

func buildObservationsQuery(f *observation.Filter) string {
	q := fmt.Sprintf(query.GetObservationsPart, f.InstanceID)

	for _, dim := range f.DimensionFilters {
		if len(dim.Options) == 0 {
			continue
		}

		for i, opt := range dim.Options {
			dim.Options[i] = fmt.Sprintf("'%s'", opt)
		}

		q += fmt.Sprintf(query.GetObservationDimensionPart, f.InstanceID, dim.Name, strings.Join(dim.Options, ",")) + ","
	}

	q = strings.Trim(q, ",")
	q += query.GetObservationSelectRowPart
	return q
}

func (n *NeptuneDB) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error {
	return nil
}
