package elastic_wrapper

import (
	"context"
	"testing"
)

func TestPutStoredScript(t *testing.T) {
	// create stored script
	es := newClient(t)

	rsp, err := es.GetScriptLanguagesEx(context.Background())
	if err != nil {
		t.Fatalf("GetScriptLanguagesEx failed: %v", err)
	}
	t.Logf("GetScriptLanguagesEx: %v", rsp)

	ctxRsp, ctxErr := es.GetScriptContextsEx(context.Background())
	if ctxErr != nil {
		t.Fatalf("GetScriptContextsEx failed: %v", ctxErr)
	}
	t.Logf("GetScriptContextsEx: %v", ctxRsp)

	scriptID := "test_script_return_1"

	t.Cleanup(func() {
		rsp, err := es.DeleteStoredScript(context.Background(), scriptID)
		if err != nil {
			t.Fatalf("DeleteStoredScript failed: %v", err)
		}
		t.Logf("DeleteStoredScript: %v", rsp)
	})

	putRsp, putErr := es.PutStoredScriptSimple(context.Background(), scriptID, "return 1")
	if putErr != nil {
		t.Fatalf("PutStoredScriptSimple failed: %v", putErr)
	}
	t.Logf("PutStoredScriptSimple: %v", putRsp)

	// get stored script
	getRsp, getErr := es.GetStoredScript(context.Background(), scriptID)
	if getErr != nil {
		t.Fatalf("GetStoredScript failed: %v", getErr)
	}
	t.Logf("GetStoredScript: %v", getRsp)
}
