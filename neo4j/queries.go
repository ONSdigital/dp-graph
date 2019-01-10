package neo4j

import (
	"context"
	"fmt"

	dpbolt "github.com/ONSdigital/dp-bolt/bolt"
	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/go-ns/log"
)

const (
	getCodeListsQuery       = "MATCH (i) WHERE i:_%s%s RETURN distinct labels(i) as labels"
	getCodeListQuery        = "MATCH (i:_code_list:`_%s_%s`) RETURN i"
	codeListExistsQuery     = "MATCH (cl:_code_list:`_%s_%s`) RETURN count(*)"
	getCodeListEditionQuery = "MATCH (i:_code_list:`_%s_%s` {edition:" + `"%s"` + "}) RETURN i"
	countEditions           = "MATCH (cl:_code_list:`_%s_%s`) WHERE cl.edition = %q RETURN count(*)"
	getCodesQuery           = "MATCH (c:_code) -[r:usedBy]->(cl:_code_list: `_%s_%s`) WHERE cl.edition = %q RETURN c, r"
	getCodeQuery            = "MATCH (c:_code) -[r:usedBy]->(cl:_code_list: `_%s_%s`) WHERE cl.edition = %q AND c.value = %q RETURN c, r"
	getCodeDatasets         = "MATCH (d)<-[inDataset]-(c:_code)-[r:usedBy]->(cl:_code_list:`_code_list_%s`) WHERE (cl.edition=" + `"%s"` + ") AND (c.value=" + `"%s"` + ") AND (d.is_published=true) RETURN d,r"
)

func (n *Neo4j) GetCodeList(ctx context.Context, apiHost, code string) (*models.CodeList, error) {
	log.InfoCtx(ctx, "about to query neo4j for code list", log.Data{"code_list_id": code})

	query := fmt.Sprintf(codeListExistsQuery, "code_list", code)
	//typically mapper would pass in the &models.Thing which the results get written to
	//but this seems quite obscured/not sure i like this pattern
	//	_, mapper := mapper.GetCount()

	mapper := func(r *dpbolt.Result) error {
		return nil
	}

	if err := n.exec(query, mapper, true); err != nil {
		//includes not found/404 responses
		return nil, err
	}

	// from a Neo4j POV Codelists are't actually a thing a codeList exists if there is 1 or more edition nodes.
	return &models.CodeList{
		Links: models.CodeListLink{
			Self: &models.Link{
				ID:   code,
				Href: fmt.Sprintf("%s/code-lists/%s", apiHost, code),
			},
			Editions: &models.Link{
				Href: fmt.Sprintf("%s/code-lists/%s/editions", apiHost, code),
			},
		},
	}, nil
}
