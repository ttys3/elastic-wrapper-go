package elastic_wrapper

import (
	"context"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// test TestIndexCreate Must Success
func TestIndexCreateMustSuccess(t *testing.T) {
	es := newClient(t)
	demoIndex := "demo_keyword_index"

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
			"content": types.NewKeywordProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)
}

func TestIndexCreateRawMustSuccess(t *testing.T) {
	es := newClient(t)
	demoIndex := "demo_keyword_index"

	t.Cleanup(func() {
		deleted, err := es.IndexDelete(context.Background(), demoIndex)
		if err != nil {
			t.Fatalf("delete index failed, err=%v", err)
		}
		if !deleted {
			t.Fatalf("delete index failed, deleted=%v", deleted)
		}
	})

	payload := `{
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "field1": { "type": "text" }
    }
  }
}`
	rsp, err := es.IndexCreateRaw(context.Background(), demoIndex, []byte(payload))
	if err != nil {
		t.Fatalf("IndexCreateRaw failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	// test index exists
	exists, err := es.IndexExists(context.Background(), demoIndex)
	if err != nil {
		t.Fatalf("IndexExists failed, err=%v", err)
	}
	if !exists {
		t.Fatalf("IndexExists failed, exists=%v", exists)
	}

	// test get index info
	info, err := es.IndexGet(context.Background(), demoIndex)
	if err != nil {
		t.Fatalf("IndexGet failed, err=%v", err)
	}
	t.Logf("info=%+v", info)

	// test get index stats
	stats, err := es.IndexStats(context.Background(), demoIndex)
	if err != nil {
		t.Fatalf("IndexStats failed, err=%v", err)
	}
	t.Logf("stats=%+v", stats)
}

// test TestIndexCreateMustFailEs8 Must Fail
// func TestIndexCreateMustFailEs8(t *testing.T) {
// 	es := newClient(t)
// 	// es 7.x will success, the name becomes `@%21&%2A%23`
// 	rsp, err := es.IndexCreateSimple(context.Background(), "@!&*#", &types.TypeMapping{
// 		Properties: map[string]types.Property{"content": types.NewKeywordProperty()},
// 	})
// 	if err != nil {
// 		t.Logf("create simple index failed as expected, err=%v", err)
// 	} else {
// 		t.Fatalf("create simple index should fail, but not, rsp=%+v", rsp)
// 	}
// 	t.Logf("rsp=%+v", rsp)
// 	if !strings.Contains(err.Error(), "Invalid index name") {
// 		t.Errorf("err should contains 'Invalid index name', but not, err=%v", err)
// 	}
// }

func TestIndexCreateMustFailEs7And8(t *testing.T) {
	es := newClient(t)
	index := "test_index_create_must_fail"
	rsp, err := es.IndexCreateSimple(context.Background(), index, &types.TypeMapping{
		// no type for content
		Properties: map[string]types.Property{"content": types.KeywordProperty{}},
	})
	if err != nil {
		t.Logf("create simple index failed as expected, err=%v", err)
	} else {
		t.Fatalf("create simple index should fail, but not, rsp=%+v", rsp)
	}
	t.Cleanup(func() {
		if rsp, err := es.IndexDelete(context.Background(), index); err != nil {
			t.Logf("delete index failed, err=%v", err)
		} else {
			t.Logf("delete index success, rsp=%+v", rsp)
		}
	})
	t.Logf("rsp=%+v", rsp)
	if !strings.Contains(err.Error(), "Failed to parse mapping [_doc]: No type specified for field") {
		t.Errorf("err should contains 'Invalid index name', but not, err=%v", err)
	}
}
