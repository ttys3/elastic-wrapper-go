package elastic_wrapper

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/scriptspainlessexecute"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scriptlanguage"
)

type ScriptsPainlessExecuteResponse struct {
	Result string `json:"result"`
}

func (s *ScriptsPainlessExecuteResponse) FromJSON(i []byte) error {
	return FromJSONImplDefault(i, s)
}

type ErrScriptsPainlessExecuteResponse struct {
	TheError struct {
		CausedBy struct {
			Reason string `json:"reason"`
			Type   string `json:"type"`
		} `json:"caused_by"`
		Lang     string `json:"lang"`
		Position struct {
			End    int `json:"end"`
			Offset int `json:"offset"`
			Start  int `json:"start"`
		} `json:"position"`
		Reason    string `json:"reason"`
		RootCause []struct {
			Lang     string `json:"lang"`
			Position struct {
				End    int `json:"end"`
				Offset int `json:"offset"`
				Start  int `json:"start"`
			} `json:"position"`
			Reason      string   `json:"reason"`
			Script      string   `json:"script"`
			ScriptStack []string `json:"script_stack"`
			Type        string   `json:"type"`
		} `json:"root_cause"`
		Script      string   `json:"script"`
		ScriptStack []string `json:"script_stack"`
		Type        string   `json:"type"`
	} `json:"error"`
	Status int `json:"status"`
}

func (e ErrScriptsPainlessExecuteResponse) Error() string {
	position := fmt.Sprintf("offset: %d start-end: %d-%d", e.TheError.Position.Offset, e.TheError.Position.Start, e.TheError.Position.End)
	return fmt.Sprintf("code: %v, error: %s, caused_by: %s, postion: %v, script_stack:%+v",
		e.Status, e.TheError.Reason, e.TheError.CausedBy, position, e.TheError.ScriptStack)
}

// ExecutePainless es Painless execute API
func (es *ElasticsearchEx) ExecutePainless(ctx context.Context, execReq *scriptspainlessexecute.Request) (*ScriptsPainlessExecuteResponse, error) {
	req := es.ScriptsPainlessExecute().Request(execReq)
	var rsp ScriptsPainlessExecuteResponse
	err := doGetResponse[ErrScriptsPainlessExecuteResponse](ctx, req, &rsp)
	return &rsp, err
}

func (es *ElasticsearchEx) ExecutePainlessSimple(ctx context.Context, script string) (*ScriptsPainlessExecuteResponse, error) {
	execReq := scriptspainlessexecute.NewRequest()
	inlineScript := types.NewInlineScript()
	inlineScript.Lang = &scriptlanguage.Painless
	inlineScript.Source = script
	execReq.Script = inlineScript
	req := es.ScriptsPainlessExecute().Request(execReq)
	var rsp ScriptsPainlessExecuteResponse
	err := doGetResponse[ErrScriptsPainlessExecuteResponse](ctx, req, &rsp)
	return &rsp, err
}
