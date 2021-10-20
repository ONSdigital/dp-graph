package mapper

import (
	"strings"

	"github.com/ONSdigital/dp-graph/v3/graph/driver"
	"github.com/ONSdigital/dp-graph/v3/models"
)

//CodeLists returns a dpbolt.ResultMapper which converts a dpbolt.Result to models.CodeLists
func CodeLists(codeLists *models.CodeListResults) ResultMapper {
	return func(r *Result) error {
		var codeListID string
		for _, v := range r.Data[0].([]interface{}) {
			label := v.(string)
			if strings.Contains(label, "_code_list_") {
				// retrieve the codelist id from label
				codeListID = strings.Replace(label, "_code_list_", "", -1)
				break
			}
		}

		codeLists.Items = append(codeLists.Items, models.CodeList{
			ID: codeListID,
		})

		return nil
	}
}

//CodeList returns a dpbolt.ResultMapper which converts a dpbolt.Result to a model.CodeList
func CodeList(codeList *models.CodeList, id string) ResultMapper {
	return func(r *Result) error {
		if len(r.Data) == 0 {
			return driver.ErrNotFound
		}

		codeList.ID = id

		return nil
	}
}
