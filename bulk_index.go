package elastic_wrapper

import (
	"bytes"
	"context"
	"fmt"

	"github.com/olivere/ndjson"

	"github.com/ttys3/elastic-wrapper-go/bulk_index"
)

type Document interface {
	GetID() string
}

// {"took":38,"errors":false,"items":[
// {"index":{"_index":"test-bulk-example","_type":"_doc","_id":"1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}},
// {"index":{"_index":"test-bulk-example","_type":"_doc","_id":"2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},
// {"index":{"_index":"test-bulk-example","_type":"_doc","_id":"3","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":2,"_primary_term":1,"status":201}}
// ]}

type BuilkIndexResponse struct {
	Took   int  `json:"took"`
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			Index   string `json:"_index"`
			Type    string `json:"_type"`
			Id      string `json:"_id"`
			Version int    `json:"_version"`
			Result  string `json:"result"`
			Shards  struct {
				Total      int `json:"total"`
				Successful int `json:"successful"`
				Failed     int `json:"failed"`
			} `json:"_shards"`
			SeqNo       int `json:"_seq_no"`
			PrimaryTerm int `json:"_primary_term"`
			Status      int `json:"status"`
		} `json:"index"`
	} `json:"items"`
}

func (b *BuilkIndexResponse) FromJSON(i []byte) error {
	return FromJSONImplDefault(i, b)
}

// BulkIndexError
// {"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"The bulk request must be terminated by a newline [\\n]"}],
// "type":"illegal_argument_exception","reason":"The bulk request must be terminated by a newline [\\n]"},"status":400}
type BulkIndexError struct {
	TheError struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"root_cause"`
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
	Status int `json:"status"`
}

func (e BulkIndexError) Error() string {
	return fmt.Sprintf("bulk index error: %s, code: %v", e.TheError.Reason, e.Status)
}

type BulkIndexerStats struct {
	NumFailed   uint64
	NumIndexed  uint64
	NumRequests uint64
}

type Meta struct {
	Index string `json:"_index,omitempty"`
	ID    string `json:"_id"`
}

type ActionAndMeta map[string]Meta

func (es *ElasticsearchEx) BulkIndexOrCreate(ctx context.Context, action, indexName string, items []Document) error {
	if action != "index" && action != "create" {
		return fmt.Errorf("action must be index or create")
	}
	buf := bytes.NewBuffer(nil)
	w := ndjson.NewWriter(buf)
	for _, a := range items {
		// action_and_meta_data\n
		err := w.Encode(ActionAndMeta{action: Meta{ID: a.GetID()}})
		if err != nil {
			return fmt.Errorf("cannot encode action_and_meta_data %s: %s", a.GetID(), err)
		}
		// source\n
		err = w.Encode(a)
		if err != nil {
			return fmt.Errorf("cannot encode document %s: %s", a.GetID(), err)
		}
		if err != nil {
			return err
		}
	}

	builkIndex := bulk_index.NewBulkIndexFunc(es.TypedClient)

	req := builkIndex(indexName).Raw(buf.Bytes())
	var rsp BuilkIndexResponse
	return doGetResponse[BulkIndexError](ctx, req, &rsp)
}

func (es *ElasticsearchEx) BulkIndex(ctx context.Context, indexName string, items []Document) error {
	return es.BulkIndexOrCreate(ctx, "index", indexName, items)
}

func (es *ElasticsearchEx) BulkCreate(ctx context.Context, indexName string, items []Document) error {
	return es.BulkIndexOrCreate(ctx, "create", indexName, items)
}

func (es *ElasticsearchEx) BulkUpdate(ctx context.Context, indexName string, updates map[string]map[string]any) error {
	action := "update"
	buf := bytes.NewBuffer(nil)
	w := ndjson.NewWriter(buf)
	for id, doc := range updates {
		// action_and_meta_data\n
		err := w.Encode(ActionAndMeta{action: Meta{ID: id}})
		if err != nil {
			return fmt.Errorf("cannot encode action_and_meta_data %s: %s", id, err)
		}
		// { "doc" : {"field2" : "value2"} }\n
		err = w.Encode(map[string]any{"doc": doc})
		if err != nil {
			return fmt.Errorf("cannot encode document %s: %s", id, err)
		}
	}

	builkIndex := bulk_index.NewBulkIndexFunc(es.TypedClient)

	req := builkIndex(indexName).Raw(buf.Bytes())
	var rsp BuilkIndexResponse
	return doGetResponse[BulkIndexError](ctx, req, &rsp)
}

func (es *ElasticsearchEx) BulkDelete(ctx context.Context, indexName string, ids []string) error {
	action := "delete"
	buf := bytes.NewBuffer(nil)
	w := ndjson.NewWriter(buf)
	for _, id := range ids {
		// action_and_meta_data\n
		err := w.Encode(ActionAndMeta{action: Meta{ID: id}})
		if err != nil {
			return fmt.Errorf("cannot encode action_and_meta_data %s: %s", id, err)
		}
	}

	builkIndex := bulk_index.NewBulkIndexFunc(es.TypedClient)

	req := builkIndex(indexName).Raw(buf.Bytes())
	var rsp BuilkIndexResponse
	return doGetResponse[BulkIndexError](ctx, req, &rsp)
}
