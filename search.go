package elastic_wrapper

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"

	"github.com/ttys3/elastic-wrapper-go/sort"
)

type SearchResponse[T any] struct {
	UntypedSearchResponse[T, NeverUnmarshal, []any]
}

// SearchResponseCustom custom search response with sort type support decode int64 > math.MaxFloat64
type SearchResponseCustom[T any] struct {
	UntypedSearchResponse[T, NeverUnmarshal, *sort.SortType]
}

type SearchAggResponse[Doc, Agg any] struct {
	UntypedSearchResponse[Doc, Agg, []any]
}

type UntypedSearchResponse[Doc, Agg, Sort any] struct {
	ScrollID     string `json:"_scroll_id"`
	Took         int    `json:"took"`
	TimedOut     bool   `json:"timed_out"`
	Aggregations Agg    `json:"aggregations"`
	Shards       struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index     string              `json:"_index"`
			Id        string              `json:"_id"`
			Score     *float64            `json:"_score"`
			Source    Doc                 `json:"_source"` // 文档
			Fields    map[string][]any    `json:"fields"`
			Sort      Sort                `json:"sort"`
			Highlight map[string][]string `json:"highlight"`
		} `json:"hits"`
	} `json:"hits"`
}

func (resp *UntypedSearchResponse[_, _, _]) FromJSON(buf []byte) error {
	return FromJSONImplDefault(buf, resp)
}

func (resp *UntypedSearchResponse[T, _, _]) Sources() []T {
	if len(resp.Hits.Hits) == 0 {
		return nil
	}
	sources := make([]T, len(resp.Hits.Hits))
	for idx := range resp.Hits.Hits {
		sources[idx] = resp.Hits.Hits[idx].Source
	}
	return sources
}

func (resp *UntypedSearchResponse[_, _, Sort]) LastSort() (s Sort) {
	n := len(resp.Hits.Hits)
	if n == 0 {
		return
	}
	s = resp.Hits.Hits[n-1].Sort
	return
}

// SearchByIndex Returns results matching a query.
// https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
// Index A comma-separated list of index names to search; use `_all` or empty string
// to perform the operation on all indices
func (es *ElasticsearchEx) SearchByIndex(ctx context.Context, index string, searchRequest *search.Request, dest FromJSON) error {
	req := es.Core.Search().Index(index).Request(searchRequest)
	err := doGetResponse[ErrGeneric](ctx, req, dest)
	return err
}

// SearchByIndexRaw use this if you want to use your own pre-baked JSON queries using templates or even a specific encoder
// raw query like:
//
//	{
//	 "query": {
//	   "term": {
//	     "user.id": {
//	       "value": "kimchy",
//	       "boost": 1.0
//	     }
//	   }
//	 }
//	}
func (es *ElasticsearchEx) SearchByIndexRaw(ctx context.Context, index string, sr []byte, dest FromJSON) error {
	req := es.Core.Search().Index(index).Raw(sr)
	err := doGetResponse[ErrGeneric](ctx, req, dest)
	return err
}

type Pit struct {
	ID        string `json:"id"`
	KeepAlive string `json:"keep_alive"`
}

// SearchByIndexPaginated searchAfter like: "search_after": [1463538857, "654323"]
// sort like: [ {"date": "asc"}, {"tie_breaker_id": "asc"} ]
// https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html#search-after
func (es *ElasticsearchEx) SearchByIndexPaginated(ctx context.Context, index string, searchRequest *search.Request, dest FromJSON,
	size int64, searchAfter types.SortResults, sorts types.Sort,
) error {
	if len(searchAfter) > 0 {
		searchRequest.SearchAfter = searchAfter
	}
	if len(sorts) > 0 {
		searchRequest.Sort = sorts
	}

	req := es.Core.Search().Index(index).Request(searchRequest)
	err := doGetResponse[ErrGeneric](ctx, req, dest)
	return err
}

type NeverUnmarshal struct{}

func (NeverUnmarshal) UnmarshalJSON([]byte) error { return nil }
func (NeverUnmarshal) FromJSON([]byte) error      { return nil }
