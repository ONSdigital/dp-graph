package models

// CodelistURL set by main() to make accessible to all models users
var CodelistURL string

// HierarchyResponse models a node in the hierarchy
type HierarchyResponse struct {
	ID           string
	Label        string
	Children     []*HierarchyElement
	NoOfChildren int64
	HasData      bool
	Breadcrumbs  []*HierarchyElement
}

// HierarchyElement is a item in a list within a Response
type HierarchyElement struct {
	ID           string
	Label        string
	NoOfChildren int64
	HasData      bool
}
