package models

import (
	"errors"
	"fmt"
)

// Editions represents the editions response model
type Editions struct {
	Items      []Edition `json:"items"`
	Count      int       `json:"count"`
	Offset     int       `json:"offset"`
	Limit      int       `json:"limit"`
	TotalCount int       `json:"total_count"`
}

// Edition represents a single edition response model
type Edition struct {
	Edition string        `json:"edition"`
	Label   string        `json:"label"`
	Links   *EditionLinks `json:"links"`
}

// EditionLinks represents the links returned for a specific edition
type EditionLinks struct {
	Self     *Link `json:"self"`
	Editions *Link `json:"editions"`
	Codes    *Link `json:"codes"`
}

// UpdateLinks updates the links for an Edition struct with the provided codeListID, returning any parsing error while trying to update.
func (e *Edition) UpdateLinks(codeListID, url string) (err error) {
	if e.Links == nil || e.Links.Self == nil || e.Links.Self.ID == "" {
		return errors.New("unable to create links - edition id not provided")
	}

	id := e.Links.Self.ID
	e.Links.Self, err = CreateLink(id, fmt.Sprintf(editionURI, codeListID, id), url)
	if err != nil {
		return err
	}

	e.Links.Editions, err = CreateLink("", fmt.Sprintf(editionsURI, codeListID), url)
	if err != nil {
		return err
	}

	e.Links.Codes, err = CreateLink("", fmt.Sprintf(codesURI, codeListID, id), url)
	if err != nil {
		return err
	}

	return nil
}
