package models

const codelistFormat = "%s/code-lists/%s/codes"
const rootFormat = "%s/hierarchies/%s/%s"

// CodelistURL set by main() to make accessible to all models users
var CodelistURL string

// HierarchyResponse models a node in the hierarchy
type HierarchyResponse struct {
	ID           string              `json:"-"`
	Label        string              `json:"label"`
	Children     []*HierarchyElement `json:"children,omitempty"`
	NoOfChildren int64               `json:"no_of_children,omitempty"`
	Links        map[string]Link     `json:"links,omitempty"`
	HasData      bool                `json:"has_data"`
	Breadcrumbs  []*HierarchyElement `json:"breadcrumbs,omitempty"`
}

// HierarchyElement is a item in a list within a Response
type HierarchyElement struct {
	ID           string          `json:"-"`
	Label        string          `json:"label"`
	NoOfChildren int64           `json:"no_of_children,omitempty"`
	Links        map[string]Link `json:"links,omitempty"`
	HasData      bool            `json:"has_data"`
}
