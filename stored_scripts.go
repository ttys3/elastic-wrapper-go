package elastic_wrapper

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/putscript"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"
)

type PutStoredScriptResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

func (p *PutStoredScriptResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, &p)
}

// PutStoredScript create or update stored script
func (es *ElasticsearchEx) PutStoredScript(ctx context.Context, id string, storedScript *types.StoredScript) (*PutStoredScriptResponse, error) {
	request := putscript.NewRequest()
	request.Script = *storedScript
	req := es.PutScript(id).Request(request)
	var rsp PutStoredScriptResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) PutStoredScriptSimple(ctx context.Context, id, script string) (*PutStoredScriptResponse, error) {
	storedScript := types.NewStoredScript()
	storedScript.Source = script
	storedScript.Lang = scriptlanguage.Painless
	return es.PutStoredScript(ctx, id, storedScript)
}

type GetScriptLanguagesExResponse struct {
	TypesAllowed     []string `json:"types_allowed"`
	LanguageContexts []struct {
		Language string   `json:"language"`
		Contexts []string `json:"contexts"`
	} `json:"language_contexts"`
}

func (g *GetScriptLanguagesExResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, &g)
}

// GetScriptLanguagesEx Get script languages API
// https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-script-languages-api.html
func (es *ElasticsearchEx) GetScriptLanguagesEx(ctx context.Context) (*GetScriptLanguagesExResponse, error) {
	req := es.GetScriptLanguages()

	var rsp GetScriptLanguagesExResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}

type GetScriptContextsExResponse struct {
	Contexts []struct {
		Name    string `json:"name"`
		Methods []struct {
			Name       string `json:"name"`
			ReturnType string `json:"return_type"`
			Params     []struct {
				Type string `json:"type"`
				Name string `json:"name"`
			} `json:"params"`
		} `json:"methods"`
	} `json:"contexts"`
}

func (g *GetScriptContextsExResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, &g)
}

// GetScriptContextsEx Get script contexts API
// https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-script-contexts-api.html
func (es *ElasticsearchEx) GetScriptContextsEx(ctx context.Context) (*GetScriptContextsExResponse, error) {
	req := es.GetScriptContext()
	var rsp GetScriptContextsExResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}

type DeleteStoredScriptResponse struct {
	Acknowledged bool `json:"acknowledged"`
}

func (d *DeleteStoredScriptResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, &d)
}

func (es *ElasticsearchEx) DeleteStoredScript(ctx context.Context, id string) (*DeleteStoredScriptResponse, error) {
	req := es.DeleteScript(id)
	var rsp DeleteStoredScriptResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}

type GetStoredScriptResponse struct {
	Id     string `json:"_id"`
	Found  bool   `json:"found"`
	Script struct {
		Lang   string `json:"lang"`
		Source string `json:"source"`
	} `json:"script"`
}

func (g *GetStoredScriptResponse) FromJSON(bytes []byte) error {
	return FromJSONImplDefault(bytes, &g)
}

// GetStoredScript Get stored script API
// https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-stored-script-api.html
func (es *ElasticsearchEx) GetStoredScript(ctx context.Context, id string) (*GetStoredScriptResponse, error) {
	req := es.GetScript(id)
	var rsp GetStoredScriptResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}
