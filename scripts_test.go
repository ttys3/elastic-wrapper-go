package elastic_wrapper

import (
	"context"
	"testing"
)

// test ExecutePainless
func TestExecutePainlessSimpleNumberPlus(t *testing.T) {
	es := newClient(t)
	ctx := context.Background()
	rsp, err := es.ExecutePainlessSimple(ctx, "1+1")
	if err != nil {
		t.Errorf("ExecutePainlessSimple failed: %v", err)
	}
	t.Logf("ExecutePainlessSimple: %v", rsp)
	if rsp.Result != "2" {
		t.Errorf("ExecutePainlessSimple result is not 2")
	}
}

func TestExecutePainlessSimpleBoolean(t *testing.T) {
	es := newClient(t)
	ctx := context.Background()
	rsp, err := es.ExecutePainlessSimple(ctx, "return true;")
	if err != nil {
		t.Errorf("ExecutePainlessSimple failed: %v", err)
	}
	t.Logf("ExecutePainlessSimple: %v", rsp)
	if rsp.Result != "true" {
		t.Errorf("ExecutePainlessSimple result is not true")
	}
}

func TestExecutePainlessMustFail(t *testing.T) {
	es := newClient(t)
	ctx := context.Background()
	rsp, err := es.ExecutePainlessSimple(ctx, "return true; return false;")
	if err == nil {
		t.Errorf("ExecutePainlessSimple should fail but not: rsp=%v", rsp)
	} else {
		t.Logf("ExecutePainlessSimple failed as expected: %v", rsp)
	}
}
