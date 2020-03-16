package models

import (
	"errors"
	"fmt"
)

// CodeListResults contains an array of code lists which can be paginated
type CodeListResults struct {
	Items      []CodeList `json:"items"`
	Count      int        `json:"count"`
	Offset     int        `json:"offset"`
	Limit      int        `json:"limit"`
	TotalCount int        `json:"total_count"`
}

// CodeList containing links to all possible codes
type CodeList struct {
	ID    string        `json:"-"`
	Links *CodeListLink `json:"links,omitempty"`
}

// CodeListLink contains links for a code list resource
type CodeListLink struct {
	Self     *Link `json:"self,omitempty"`
	Editions *Link `json:"editions,omitempty"`
}

// UpdateLinks updates the links for a CodeList struct with the provided url, returning any parsing error while trying to update.
func (c *CodeList) UpdateLinks(url string) (err error) {
	if c.Links == nil || c.Links.Self == nil || c.Links.Self.ID == "" {
		return errors.New("unable to create links - codelist id not provided")
	}

	id := c.Links.Self.ID
	c.Links.Self, err = CreateLink(id, fmt.Sprintf(codeListURI, id), url)
	if err != nil {
		return err
	}

	c.Links.Editions, err = CreateLink("", fmt.Sprintf(editionsURI, id), url)
	if err != nil {
		return err
	}

	return nil
}
