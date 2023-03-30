package elastic_wrapper

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

type IndexCreateResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index"`
}

func (i *IndexCreateResponse) FromJSON(bytes []byte) error {
	return json.Unmarshal(bytes, i)
}

type ErrIndexCreateFailed struct {
	TheError struct {
		RootCause []struct {
			Type      string `json:"type"`
			Reason    string `json:"reason"`
			IndexUuid string `json:"index_uuid"`
			Index     string `json:"index"`
		} `json:"root_cause"`
		Type      string `json:"type"`
		Reason    string `json:"reason"`
		IndexUuid string `json:"index_uuid"`
		Index     string `json:"index"`
	} `json:"error"`
	Status int `json:"status"`
}

func (ice ErrIndexCreateFailed) Error() string {
	return fmt.Sprintf("create index failed, reason: %s, code: %v", ice.TheError.Reason, ice.Status)
}

// IndexCreate creates an index with the specified name and mapping. error is nil if create OK
// http 400 if exists or index name invalid, 200 OK
// response {"acknowledged":true,"shards_acknowledged":true,"index":"demo_keyword_index"}
// err response {"error":{"root_cause":[{"type":"invalid_index_name_exception","reason":"Invalid index name [@%21&%2A%23], must be lowercase","index_uuid":"_na_","index":"@%21&%2A%23"}],"type":"invalid_index_name_exception","reason":"Invalid index name [@%21&%2A%23], must be lowercase","index_uuid":"_na_","index":"@%21&%2A%23"},"status":400}
func (es *ElasticsearchEx) IndexCreate(ctx context.Context, indexName string, indexCreateReq *create.Request) (*IndexCreateResponse, error) {
	var rsp IndexCreateResponse
	req := es.Indices.Create(indexName).
		Request(indexCreateReq).
		Timeout("30s").
		WaitForActiveShards("1")

	err := doGetResponse[ErrIndexCreateFailed](ctx, req, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) IndexCreateSimple(ctx context.Context, indexName string, mappings *types.TypeMapping) (*IndexCreateResponse, error) {
	req := create.NewRequest()
	req.Mappings = mappings
	return es.IndexCreate(ctx, indexName, req)
}

func (es *ElasticsearchEx) IndexCreateRaw(ctx context.Context, indexName string, indexCreateReq []byte) (*IndexCreateResponse, error) {
	var rsp IndexCreateResponse
	req := es.Indices.Create(indexName).
		Raw(indexCreateReq).
		Timeout("30s").
		WaitForActiveShards("1")

	err := doGetResponse[ErrIndexCreateFailed](ctx, req, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) IndexDelete(ctx context.Context, indexName string) (bool, error) {
	return es.Indices.Delete(indexName).IsSuccess(ctx)
}

func (es *ElasticsearchEx) IndexExists(ctx context.Context, indexName string) (bool, error) {
	return es.Indices.Exists(indexName).IsSuccess(ctx)
}

type IndexInfo struct {
	Aliases  map[string]any `json:"aliases"`
	Mappings map[string]any `json:"mappings"`
	Settings map[string]any `json:"settings"`
}

type IndexInfoResponse map[string]IndexInfo

func (i *IndexInfoResponse) FromJSON(bytes []byte) error {
	return json.Unmarshal(bytes, i)
}

func (es *ElasticsearchEx) IndexGet(ctx context.Context, indexName string) (*IndexInfoResponse, error) {
	req := es.Indices.Get(indexName)
	var rsp IndexInfoResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}

type IndexStats struct {
	Shards  map[string]any `json:"_shards"`
	All     map[string]any `json:"_all"`
	Indices map[string]any `json:"indices"`
}

func (i *IndexStats) FromJSON(bytes []byte) error {
	return json.Unmarshal(bytes, i)
}

func (es *ElasticsearchEx) IndexStats(ctx context.Context, indexName string) (*IndexStats, error) {
	req := es.Indices.Stats().Index(indexName)
	var rsp IndexStats
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}
