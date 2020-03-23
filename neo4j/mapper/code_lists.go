package mapper

import (
	"strings"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/models"
)

//CodeLists returns a dpbolt.ResultMapper which converts a dpbolt.Result to models.CodeLists
func CodeLists(codeLists *models.CodeListResults) ResultMapper {
	return func(r *Result) error {
		var id string
		for _, v := range r.Data[0].([]interface{}) {
			s := v.(string)
			if strings.Contains(s, "_code_list_") {
				id = strings.Replace(s, "_code_list_", "", -1)
				break
			}
		}

		codeLists.Items = append(codeLists.Items, models.CodeList{
			ID: id,
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

		codeList = &models.CodeList{
			ID: id,
		}

		return nil
	}
}
