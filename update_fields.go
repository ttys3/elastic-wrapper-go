package elastic_wrapper

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"

	"github.com/ttys3/elastic-wrapper-go/scripts"
)

type UpdateFieldsScript struct {
	script *types.StoredScript
}

func NewUpdateFieldsScript() *UpdateFieldsScript {
	return &UpdateFieldsScript{
		script: &types.StoredScript{
			Lang:   scriptlanguage.Painless,
			Source: scripts.UpdateTypesScript,
		},
	}
}

func (s UpdateFieldsScript) Script(params map[string]any) *types.Script {
	var sc types.Script = types.StoredScriptId{
		Params: params,
		Id:     scripts.UpdateTypesScriptName,
	}
	return &sc
}

// InitScript: create script if script not found
func (s UpdateFieldsScript) InitScript(ctx context.Context, es *ElasticsearchEx) (bool, error) {
	script := types.NewStoredScript()
	script.Lang = s.script.Lang
	script.Source = s.script.Source
	resp, err := es.PutStoredScript(ctx, scripts.UpdateTypesScriptName, script)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

var _fieldScript = NewUpdateFieldsScript()

func ParamScript[Field scripts.UpdateField | *scripts.UpdateField, Slice ~[]Field](fields Slice) *types.Script {
	return _fieldScript.Script(map[string]interface{}{"fields": fields})
}
