package elastic_wrapper

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
)

func TestMget(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_mget"

	t.Cleanup(func() {
		deleted, err := es.IndexDelete(context.Background(), demoIndex)
		if err != nil {
			t.Fatalf("delete index failed, err=%v", err)
		}
		if !deleted {
			t.Fatalf("delete index failed, deleted=%v", deleted)
		}
	})
	rsp, err := es.IndexCreateSimple(context.Background(), demoIndex, &types.TypeMapping{Properties: map[string]types.Property{"content": types.NewKeywordProperty()}})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	n := 10
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, strconv.Itoa(i))
		rsp, err := es.DocCreateRefresh(context.Background(), demoIndex, strconv.Itoa(i), map[string]interface{}{
			"content": fmt.Sprintf("test doc %d", i),
		}, refresh.True)
		if err != nil {
			t.Fatalf("create doc failed, err=%v", err)
		}
		t.Logf("rsp=%+v", rsp)
	}

	type DemoDoc struct {
		Content string `json:"content"`
	}
	// get the document
	var mGetRsp DocsResponse[DemoDoc]
	err = es.GetDocumentByIDs(context.Background(), demoIndex, ids, &mGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mGetRsp=%+v", mGetRsp)

	for _, doc := range mGetRsp.Docs {
		t.Logf("doc=%+v", doc.Source.Content)
	}

	if len(mGetRsp.Docs) != n {
		t.Fatalf("mget failed, len(mGetRsp.Docs)=%d, n=%d", len(mGetRsp.Docs), n)
	}
}
