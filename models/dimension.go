package models

import (
	"errors"
)

// Dimension struct encapsulating Dimension details.
type Dimension struct {
	DimensionID string
	Option      string
	NodeID      string
}

// Validate checks the dimension object
func (d *Dimension) Validate() error {
	if d == nil {
		return errors.New("dimension is required but was nil")
	}
	if len(d.DimensionID) == 0 && len(d.Option) == 0 {
		return errors.New("dimension invalid: both dimension.dimension_id and dimension.value are required but were both empty")
	}
	if len(d.DimensionID) == 0 {
		return errors.New("dimension id is required but was empty")
	}
	if len(d.Option) == 0 {
		return errors.New("dimension value is required but was empty")
	}
	return nil
}

// Instance struct to hold instance information.
type Instance struct {
	InstanceID string        `json:"id,omitempty"`
	CSVHeader  []string      `json:"headers"`
	Dimensions []interface{} `json:"-"`
}

// Validate checks that the instance ID is not empty
func (i *Instance) Validate() error {
	if i == nil {
		return errors.New("instance is required but was nil")
	}

	if len(i.InstanceID) == 0 {
		return errors.New("instance id is required but was empty")
	}
	return nil
}
