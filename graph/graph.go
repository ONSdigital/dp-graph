package graph

import (
	"github.com/ONSdigital/dp-graph/config"
	"github.com/ONSdigital/dp-graph/graph/driver"
)

type DB struct {
	driver driver.Driver
}

func New() *DB {
	cfg, err := config.Get()

	return &DB{
		driver: cfg.driver,
	}
}
