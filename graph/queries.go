package graph

import (
	"context"

	"github.com/ONSdigital/dp-code-list-api/models"
)

func (db *DB) GetList(ctx context.Context, id string) (*models.CodeList, error) {
	return drive.GetCodeList(ctx, id)
}
