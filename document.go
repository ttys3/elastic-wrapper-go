package elastic_wrapper

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/index"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/update"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"
	"google.golang.org/protobuf/proto"
)

type ErrorDocIndex struct {
	TheError struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"root_cause"`
		Type     string `json:"type"`
		Reason   string `json:"reason"`
		CausedBy struct {
			Type     string `json:"type"`
			Reason   string `json:"reason"`
			CausedBy struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"caused_by"`
		} `json:"caused_by"`
	} `json:"error"`
	Status int `json:"status"`
}

func (e ErrorDocIndex) Error() string {
	return fmt.Sprintf("error index document, reason: %s, code: %v, root_cause: %v",
		e.TheError.Reason, e.Status, e.TheError.RootCause)
}

type DocCreateResponse struct {
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
}

// FromJSON impl FromJSON for DocCreateResponse
func (r *DocCreateResponse) FromJSON(buf []byte) error {
	return FromJSONImplDefault(buf, r)
}

// DocCreate Creates a new document in the index
func (es *ElasticsearchEx) DocCreate(ctx context.Context, docCreate *create.Create) (*DocCreateResponse, error) {
	var rsp DocCreateResponse
	// Returns a 409 response when a document with a same ID already exists in the index
	err := doGetResponse[ErrorDocIndex](ctx, docCreate, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) DocCreateSimple(ctx context.Context, index, id string, body interface{}) (*DocCreateResponse, error) {
	return es.DocCreate(ctx, es.Create(index, id).Request(body))
}

// DocCreateRefresh https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
// This should ONLY be done after careful thought and verification that it does not lead to poor performance, both from an indexing and a search standpoint.
func (es *ElasticsearchEx) DocCreateRefresh(ctx context.Context, index, id string, body interface{}, r refresh.Refresh) (*DocCreateResponse, error) {
	return es.DocCreate(ctx, es.Create(index, id).Request(body).Refresh(r))
}

func (es *ElasticsearchEx) DocCreateRaw(ctx context.Context, index, id string, body []byte) (*DocCreateResponse, error) {
	return es.DocCreate(ctx, es.Create(index, id).Raw(body))
}

// DocIndex Creates or updates a document in an index
func (es *ElasticsearchEx) DocIndex(ctx context.Context, docIndex *index.Index) (*DocCreateResponse, error) {
	var rsp DocCreateResponse
	// Returns a 409 response when a document with a same ID already exists in the index
	err := doGetResponse[ErrorDocIndex](ctx, docIndex, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) DocIndexSimple(ctx context.Context, index, id string, body interface{}) (*DocCreateResponse, error) {
	return es.DocIndex(ctx, es.Index(index).Request(body).Id(id))
}

type ErrorDocGet struct{}

func (e ErrorDocGet) Error() string {
	return fmt.Sprintf("error getting document")
}

type DocGetResponse[S any] struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Found       bool   `json:"found"`
	Source      S      `json:"_source"`
}

// FromJSON impl FromJSON for DocGetResponse
func (resp *DocGetResponse[any]) FromJSON(buf []byte) error {
	return FromJSONImplDefault(buf, resp)
}

type ErrGetDoc struct {
	Index string `json:"_index"`
	Type  string `json:"_type"`
	Id    string `json:"_id"`
	Found bool   `json:"found"`
}

func (e ErrGetDoc) Error() string {
	return "document not found"
}

// DocGet get document by id
// 404 if doc not found
func (es *ElasticsearchEx) DocGet(ctx context.Context, index, id string, result FromJSON) error {
	return doGetResponse[ErrGetDoc](ctx, es.Get(index, id), result)
}

type DocDeleteResponse struct {
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
}

func (d *DocDeleteResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, d)
}

// DocDelete delete document by id
func (es *ElasticsearchEx) DocDelete(ctx context.Context, index, id string) (*DocDeleteResponse, error) {
	var rsp DocDeleteResponse
	err := doGetResponse[ErrorDocGet](ctx, es.Delete(index, id), &rsp)
	return &rsp, err
}

type ErrorDocUpdate struct {
	TheError struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"root_cause"`
		Type     string `json:"type"`
		Reason   string `json:"reason"`
		CausedBy struct {
			Type        string   `json:"type"`
			Reason      string   `json:"reason"`
			ScriptStack []string `json:"script_stack"`
			Script      string   `json:"script"`
			Lang        string   `json:"lang"`
			Position    struct {
				Offset int `json:"offset"`
				Start  int `json:"start"`
				End    int `json:"end"`
			} `json:"position"`
			CausedBy struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"caused_by"`
		} `json:"caused_by"`
	} `json:"error"`
	Status int `json:"status"`
}

func (e ErrorDocUpdate) Error() string {
	position := fmt.Sprintf("offset: %d start-end: %d-%d", e.TheError.CausedBy.Position.Offset, e.TheError.CausedBy.Position.Start, e.TheError.CausedBy.Position.End)
	return fmt.Sprintf("code: %v, error: %s, caused_by: %v, postion: %v, script_stack:%+v",
		e.Status, e.TheError.Reason, e.TheError.CausedBy, position, e.TheError.CausedBy.ScriptStack)
}

type DocUpdateResponse struct{}

func (d *DocUpdateResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, d)
}

// DocUpdate update document by id
// using DocIndex if you want to do full update (which is actually reindex)
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
// Enables you to script document updates. The script can update, delete, or skip modifying the document.
// The update API also supports passing a partial document, which is merged into the existing document.
// To fully replace an existing document, use the index API.
// The _source field must be enabled to use update. In addition to _source, you can access the following variables
// through the ctx map: _index, _type, _id, _version, _routing, and _now (the current timestamp).
func (es *ElasticsearchEx) DocUpdate(ctx context.Context, index, id string, updateReq *update.Request) (*DocUpdateResponse, error) {
	var rsp DocUpdateResponse
	err := doGetResponse[ErrorDocUpdate](ctx, es.Update(index, id).Request(updateReq), &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) DocUpdateRetryOnConflict(ctx context.Context, index, id string, updateReq *update.Request) (*DocUpdateResponse, error) {
	var rsp DocUpdateResponse
	err := doGetResponse[ErrorDocUpdate](ctx, es.Update(index, id).Request(updateReq).RetryOnConflict(3), &rsp)
	return &rsp, err
}

// DocUpdateSimple A partial update to an existing document by id
func (es *ElasticsearchEx) DocUpdateSimple(ctx context.Context, index, id string, doc interface{}) (*DocUpdateResponse, error) {
	return es.DocUpdate(ctx, index, id, &update.Request{Doc: doc})
}

func (es *ElasticsearchEx) DocUpdateSimpleRetryOnConflict(ctx context.Context, index, id string, doc interface{}) (*DocUpdateResponse, error) {
	return es.DocUpdateRetryOnConflict(ctx, index, id, &update.Request{Doc: doc})
}

// DocUpdateScript A partial update to an existing document by using scripts
// TODO using stored script https://www.elastic.co/guide/en/elasticsearch/reference/7.17/modules-scripting-using.html#script-stored-scripts
// https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-scripting-using.html#script-stored-scripts
// https://www.elastic.co/guide/en/elasticsearch/painless/7.17/painless-execute-api.html#painless-execute-api-request-body
func (es *ElasticsearchEx) DocUpdateScript(ctx context.Context, index, id, source string, params map[string]interface{}) (*DocUpdateResponse, error) {
	// DocAsUpsert?
	// ScriptedUpsert ?
	script := types.NewInlineScript()
	script.Source = source
	if len(params) > 0 {
		script.Params = params
	}

	ts := types.Script(script)
	return es.DocUpdate(ctx, index, id, &update.Request{Script: &ts})
}

var counterTmpl = `
if (ctx._source.{{.FieldName}} == null) {
	ctx._source.{{.FieldName}} = params.{{.CountParamName}};
} else {
	ctx._source.{{.FieldName}} += params.{{.CountParamName}};
}
`

var counterTemplate = template.Must(template.New("counter_tmpl").Parse(counterTmpl))

func (es *ElasticsearchEx) DocUpdateCounter(ctx context.Context, index, id string, fieldsIncrMap map[string]int64) (*DocUpdateResponse, error) {
	// DocAsUpsert?
	// ScriptedUpsert ?

	// POST my-index-000001/_update/1
	// {
	//	"script" : {
	//	"source": "ctx._source.counter += params.count",
	//		"lang": "painless",
	//		"params" : {
	//		"count" : 4
	//	}
	// }
	// }

	params := make(map[string]interface{})
	var sb strings.Builder
	for field, incr := range fieldsIncrMap {
		countParamName := fmt.Sprintf("count_%s", field)
		// sb.WriteString(fmt.Sprintf("ctx._source.%s += params.%s;", field, countParamName))
		counterTemplate.Execute(&sb, map[string]interface{}{
			"FieldName":      field,
			"CountParamName": countParamName,
		})
		params[countParamName] = incr
	}

	script := types.NewInlineScript()
	script.Source = sb.String()
	script.Lang = &scriptlanguage.Painless
	script.Params = params
	theScript := types.Script(script)

	updateReq := update.NewRequest()
	updateReq.Script = &theScript
	updateReq.ScriptedUpsert = proto.Bool(true)
	// Upsert: [UpdateRequest] upsert doesn't support values of type: VALUE_BOOLEAN
	// DocAsUpsert: Validation Failed: 1: doc must be specified if doc_as_upsert is enabled
	return es.DocUpdate(ctx, index, id, updateReq)
}

func (es *ElasticsearchEx) DocUpdateCounterSimple(ctx context.Context, index, id string, delta int64, fields ...string) (*DocUpdateResponse, error) {
	fieldsIncrMap := make(map[string]int64)
	for _, field := range fields {
		fieldsIncrMap[field] = delta
	}

	return es.DocUpdateCounter(ctx, index, id, fieldsIncrMap)
}
