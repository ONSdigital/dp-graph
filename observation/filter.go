package observation

// Boolean indicators for publish flag
var (
	Published   = true
	Unpublished = false
)

// DimensionFilters represents a list of dimension filters
type DimensionFilters struct {
	Dimensions []*Dimension
}

// Dimension represents an object containing a list of dimension values and the dimension name
type Dimension struct {
	Name    string
	Options []string
}

// Downloads represent a list of download types
// type Downloads struct {
// 	CSV *DownloadItem `json:"csv,omitempty"`
// 	XLS *DownloadItem `json:"xls,omitempty"`
// }

// DownloadItem represents an object containing download details
// type DownloadItem struct {
// 	HRef    string `json:"href,omitempty"`
// 	Private string `json:"private,omitempty"`
// 	Public  string `json:"public,omitempty"`
// 	Size    string `json:"size,omitempty"`
// }

// IsEmpty return true if DimensionFilters is nil, empty or contains only empty values
func (d DimensionFilters) IsEmpty() bool {
	if len(d.Dimensions) == 0 {
		return true
	}

	for _, o := range d.Dimensions {
		if o.Name != "" && len(o.Options) > 0 {
			// return at the first non empty option
			return false
		}
	}

	return true
}
