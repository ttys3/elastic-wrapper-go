package elastic_wrapper

import (
	"context"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
)

// test count
func TestCountByIndex(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_count_by_index"

	t.Cleanup(func() {
		deleted, err := es.IndexDelete(context.Background(), demoIndex)
		if err != nil {
			t.Fatalf("delete index failed, err=%v", err)
		}
		if !deleted {
			t.Fatalf("delete index failed, deleted=%v", deleted)
		}
	})
	rsp, err := es.IndexCreateSimple(context.Background(), demoIndex, &types.TypeMapping{
		Properties: map[string]types.Property{
			"content": types.NewTextProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	docRsp, err := es.DocCreateRefresh(context.Background(), demoIndex, "1", map[string]interface{}{
		"content": "hello",
	}, refresh.True)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	docRsp, err = es.DocCreateRefresh(context.Background(), demoIndex, "2", map[string]interface{}{
		"content": "world",
	}, refresh.True)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	docRsp, err = es.DocCreateRefresh(context.Background(), demoIndex, "3", map[string]interface{}{
		"content": "better world",
	}, refresh.True)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	// logger.SetLevel(logger.DebugLevel)
	// get the document
	total, err := es.CountByIndex(context.Background(), demoIndex, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("total=%+v", total)
	if total != 3 {
		t.Fatalf("total=%d, expected=3", total)
	}

	total, err = es.CountByIndex(context.Background(), demoIndex, &types.Query{Match: map[string]types.MatchQuery{"content": {Query: "world"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("total=%+v", total)
	if total != 2 {
		t.Fatalf("total=%d, expected=2", total)
	}
}
