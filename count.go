package elastic_wrapper

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/count"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type CountResponse struct {
	Count  int64 `json:"count"`
	Shards struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
}

func (c *CountResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, c)
}

// CountByIndex https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
func (es *ElasticsearchEx) CountByIndex(ctx context.Context, index string, query *types.Query) (int64, error) {
	req := es.Core.Count().Index(index)
	if query != nil {
		req.Request(&count.Request{Query: query})
	}
	var rsp CountResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return rsp.Count, err
}

func (es *ElasticsearchEx) CountAllByIndex(ctx context.Context, index string) (int64, error) {
	return es.CountByIndex(ctx, index, nil)
}
