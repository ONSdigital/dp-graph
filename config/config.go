package config

import (
	"errors"
	"time"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/mock"
	"github.com/ONSdigital/dp-graph/v2/neo4j"
	"github.com/ONSdigital/dp-graph/v2/neptune"
	"github.com/kelseyhightower/envconfig"
)

// Configuration allows environment variables to be read and sent to the
// relevant driver for further setup
type Configuration struct {
	DriverChoice    string        `envconfig:"GRAPH_DRIVER_TYPE"`
	DatabaseAddress string        `envconfig:"GRAPH_ADDR" json:"-"`
	PoolSize        int           `envconfig:"GRAPH_POOL_SIZE"`
	MaxRetries      int           `envconfig:"MAX_RETRIES"`
	RetryTime       time.Duration `envconfig:"RETRY_TIME"`
	QueryTimeout    int           `envconfig:"GRAPH_QUERY_TIMEOUT"`
	Neptune         NeptuneConfig

	Driver driver.Driver
}

// NeptuneConfig defines the neptune-specific configuration
type NeptuneConfig struct {
	BatchSizeReader int  `envconfig:"NEPTUNE_BATCH_SIZE_READER"`
	BatchSizeWriter int  `envconfig:"NEPTUNE_BATCH_SIZE_WRITER"`
	MaxWorkers      int  `envconfig:"NEPTUNE_MAX_WORKERS"`
	TLSSkipVerify   bool `envconfig:"NEPTUNE_TLS_SKIP_VERIFY"`
}

var cfg *Configuration

// Get reads config and returns the configured instantiated driver
func Get(errs chan error) (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		DriverChoice: "",
		Neptune: NeptuneConfig{
			BatchSizeReader: 25000,
			BatchSizeWriter: 150,
			MaxWorkers:      150,
			TLSSkipVerify:   false,
		},
	}

	err := envconfig.Process("", cfg)

	var d driver.Driver

	switch cfg.DriverChoice {
	case "neo4j":
		d, err = neo4j.New(cfg.DatabaseAddress, cfg.PoolSize, cfg.QueryTimeout, cfg.MaxRetries)
		if err != nil {
			return nil, err
		}
	case "neptune":
		d, err = neptune.New(
			cfg.DatabaseAddress,
			cfg.PoolSize,
			cfg.QueryTimeout,
			cfg.MaxRetries,
			cfg.Neptune.BatchSizeReader,
			cfg.Neptune.BatchSizeWriter,
			cfg.Neptune.MaxWorkers,
			cfg.Neptune.TLSSkipVerify,
			cfg.RetryTime,
			errs)
		if err != nil {
			return nil, err
		}
	case "mock":
		d = &mock.Mock{}
	default:
		return nil, errors.New("driver type config not provided")
	}

	cfg.Driver = d

	return cfg, nil
}
