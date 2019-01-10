package graph

import (
	"context"

	"github.com/ONSdigital/dp-code-list-api/models"
)

func (db *DB) GetList(ctx context.Context, apiHost, id string) (*models.CodeList, error) {
	return db.driver.GetCodeList(ctx, apiHost, id)
}
