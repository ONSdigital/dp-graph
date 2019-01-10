package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
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

func (n *Neo4j) GetCodeList(ctx context.Context, code string) (*models.CodeList, error) {
	log.InfoCtx(ctx, "about to query neo4j for code list", log.Data{"code_list_id": code})

	query := fmt.Sprintf(codeListExistsQuery, "code_list", code)
	//typically mapper would pass in the &models.Thing which the results get written to
	//but this seems quite obscured/not sure i like this pattern
	_, mapper := mapper.GetCount()

	err := n.exec(query, mapper, true)

	if errs != nil {
		return nil, err
	}

	// from a Neo4j POV Codelists are't actually a thing a codeList exists if there is 1 or more edition nodes.
	return &models.CodeList{
		Links: models.CodeListLink{
			Self: &models.Link{
				ID:   code,
				Href: fmt.Sprintf("%s/code-lists/%s", "api.gov", code),
			},
			Editions: &models.Link{
				Href: fmt.Sprintf("%s/code-lists/%s/editions", "api.gov", code),
			},
		},
	}, nil
}
