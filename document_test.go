package elastic_wrapper

import (
	"context"
	"errors"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// test document create
func TestDocCreate(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_create"

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

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", map[string]interface{}{
		"content": "world",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	// test conflict
	docCreateConflictRsp, docCreateConflictErr := es.DocCreateSimple(context.Background(), demoIndex, "1", map[string]interface{}{
		"content": "hello",
	})
	if err != nil && !errors.Is(err, ErrConflict) {
		t.Errorf("docCreateConflictErr=%v, docCreateConflictRsp=%+v", docCreateConflictErr, docCreateConflictRsp)
	}
	t.Logf("docCreateConflictRsp=%+v err=%v", docCreateConflictRsp, docCreateConflictErr)

	type DemoDoc struct {
		Content string `json:"content"`
	}
	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)

	// delete the document
	docDelRsp, err := es.DocDelete(context.Background(), demoIndex, "1")
	if err != nil {
		t.Errorf("delete document failed, err=%v", err)
	}
	t.Logf("docDelRsp=%+v", docDelRsp)

	// get the document again, it should not exists any more
	var docGetRspAgain DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRspAgain)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("get document failed, err=%v", err)
	}
	t.Logf("docGetRspAgain=%+v err=%v", docGetRspAgain, err)
}

// test document update
func TestDocUpdate(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update"

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

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", map[string]interface{}{
		"content": "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	type DemoDoc struct {
		Content string `json:"content"`
	}

	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)
	if docGetRsp.Source.Content != "hello" {
		t.Fatalf("docGetRsp.Source.Content=%s", docGetRsp.Source.Content)
	}

	// test update
	updateRsp, updateErr := es.DocIndexSimple(context.Background(), demoIndex, "1", map[string]interface{}{
		"content": "world",
	})
	if updateErr != nil && !errors.Is(updateErr, ErrNotFound) {
		t.Errorf("updateRsp=%v, updateErr=%+v", updateRsp, updateErr)
	}
	t.Logf("updateRsp=%+v updateErr=%v", updateRsp, updateErr)

	// get the document
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)
	if docGetRsp.Source.Content != "world" {
		t.Fatalf("docGetRsp.Source.Content=%s", docGetRsp.Source.Content)
	}
}

// test document update not exists
func TestDocUpdateNotFound(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update_not_found"

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

	// test update not exits document
	updateRsp, updateErr := es.DocUpdateSimple(context.Background(), demoIndex, "2", map[string]interface{}{
		"content": "world",
	})
	if updateErr != nil && errors.Is(updateErr, ErrNotFound) {
		t.Logf("fail as expected updateRsp=%v, updateErr=%+v", updateRsp, updateErr)
	} else {
		t.Fatalf("this should fail, but not, updateRsp=%v, updateErr=%+v", updateRsp, updateErr)
	}
}

// test DocUpdateCounter
func TestDocUpdateCounter(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update_counter_simple"

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
			"moment_count":  types.NewIntegerNumberProperty(),
			"like_count":    types.NewIntegerNumberProperty(),
			"comment_count": types.NewIntegerNumberProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	origDoc := map[string]int64{
		"moment_count":  1,
		"like_count":    2,
		"comment_count": 3,
	}

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", origDoc)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	type DemoDoc struct {
		MomentCount  int64 `json:"moment_count"`
		LikeCount    int64 `json:"like_count"`
		CommentCount int64 `json:"comment_count"`
	}

	incr := map[string]int64{
		"moment_count":  1,
		"like_count":    2,
		"comment_count": 3,
	}

	// test update counter
	ctUpRsp, ctUpErr := es.DocUpdateCounter(context.Background(), demoIndex, "1", incr)
	if ctUpErr != nil {
		t.Fatalf("ctUpErr=%+v", ctUpErr)
	}
	t.Logf("ctUpRsp=%+v", ctUpRsp)

	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)

	if docGetRsp.Source.MomentCount != origDoc["moment_count"]+incr["moment_count"] {
		t.Fatalf("docGetRsp.Source.MomentCount=%d", docGetRsp.Source.MomentCount)
	}
	if docGetRsp.Source.LikeCount != origDoc["like_count"]+incr["like_count"] {
		t.Fatalf("docGetRsp.Source.LikeCount=%d", docGetRsp.Source.LikeCount)
	}
	if docGetRsp.Source.CommentCount != origDoc["comment_count"]+incr["comment_count"] {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}
}

// test DocUpdateCounterSimple
func TestDocUpdateCounterSimple(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update_counter_simple"

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
			"moment_count":  types.NewIntegerNumberProperty(),
			"like_count":    types.NewIntegerNumberProperty(),
			"comment_count": types.NewIntegerNumberProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	type DemoDoc struct {
		MomentCount  int `json:"moment_count"`
		LikeCount    int `json:"like_count"`
		CommentCount int `json:"comment_count"`
	}

	origDoc := &DemoDoc{
		MomentCount:  1,
		LikeCount:    2,
		CommentCount: 3,
	}

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", origDoc)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	// test update counter
	ctUpRsp, ctUpErr := es.DocUpdateCounterSimple(context.Background(), demoIndex, "1", 1, "moment_count", "like_count", "comment_count")
	if ctUpErr != nil {
		t.Fatalf("ctUpErr=%+v", ctUpErr)
	}
	t.Logf("ctUpRsp=%+v", ctUpRsp)

	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)

	if docGetRsp.Source.MomentCount != origDoc.MomentCount+1 {
		t.Fatalf("docGetRsp.Source.MomentCount=%d", docGetRsp.Source.MomentCount)
	}
	if docGetRsp.Source.LikeCount != origDoc.LikeCount+1 {
		t.Fatalf("docGetRsp.Source.LikeCount=%d", docGetRsp.Source.LikeCount)
	}
	if docGetRsp.Source.CommentCount != origDoc.CommentCount+1 {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}
}

func TestDocUpdateCounterSimpleNoInitValueFields(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update_counter_simple_no_init_value_fields"

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
			"moment_count":  types.NewIntegerNumberProperty(),
			"like_count":    types.NewIntegerNumberProperty(),
			"comment_count": types.NewIntegerNumberProperty(),
			"other_count":   types.NewIntegerNumberProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	type DemoDoc struct {
		MomentCount  int `json:"moment_count"`
		LikeCount    int `json:"like_count"`
		CommentCount int `json:"comment_count"`
		OtherCount   int `json:"other_count"`
	}

	origDoc := &DemoDoc{
		MomentCount:  1,
		LikeCount:    2,
		CommentCount: 3,
	}

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", origDoc)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	// test update counter
	ctUpRsp, ctUpErr := es.DocUpdateCounterSimple(context.Background(), demoIndex, "1", 1, "moment_count", "like_count", "other_count")
	if ctUpErr != nil {
		t.Fatalf("ctUpErr=%+v", ctUpErr)
	}
	t.Logf("ctUpRsp=%+v", ctUpRsp)

	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)

	if docGetRsp.Source.MomentCount != origDoc.MomentCount+1 {
		t.Fatalf("docGetRsp.Source.MomentCount=%d", docGetRsp.Source.MomentCount)
	}
	if docGetRsp.Source.LikeCount != origDoc.LikeCount+1 {
		t.Fatalf("docGetRsp.Source.LikeCount=%d", docGetRsp.Source.LikeCount)
	}
	if docGetRsp.Source.CommentCount != origDoc.CommentCount {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}

	if docGetRsp.Source.OtherCount != origDoc.OtherCount+1 {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}
}

func TestDocUpdateCounterSimpleNoInitValueFieldsWithOmitempty(t *testing.T) {
	es := newClient(t)

	demoIndex := "test_doc_update_counter_simple_no_init_value_fields"

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
			"moment_count":  types.NewIntegerNumberProperty(),
			"like_count":    types.NewIntegerNumberProperty(),
			"comment_count": types.NewIntegerNumberProperty(),
			"other_count":   types.NewIntegerNumberProperty(),
		},
	})
	if err != nil {
		t.Fatalf("create simple index failed, err=%v", err)
	}
	t.Logf("rsp=%+v", rsp)

	type DemoDoc struct {
		MomentCount  int `json:"moment_count,omitempty"`
		LikeCount    int `json:"like_count,omitempty"`
		CommentCount int `json:"comment_count,omitempty"`
		OtherCount   int `json:"other_count,omitempty"`
	}

	origDoc := &DemoDoc{
		MomentCount:  1,
		LikeCount:    2,
		CommentCount: 3,
	}

	docRsp, err := es.DocCreateSimple(context.Background(), demoIndex, "1", origDoc)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docRsp=%+v", docRsp)

	// test update counter
	ctUpRsp, ctUpErr := es.DocUpdateCounterSimple(context.Background(), demoIndex, "1", 1, "moment_count", "like_count", "other_count")
	if ctUpErr != nil {
		t.Fatalf("ctUpErr=%+v", ctUpErr)
	}
	t.Logf("ctUpRsp=%+v", ctUpRsp)

	// get the document
	var docGetRsp DocGetResponse[DemoDoc]
	err = es.DocGet(context.Background(), demoIndex, "1", &docGetRsp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("docGetRsp=%+v", docGetRsp)

	if docGetRsp.Source.MomentCount != origDoc.MomentCount+1 {
		t.Fatalf("docGetRsp.Source.MomentCount=%d", docGetRsp.Source.MomentCount)
	}
	if docGetRsp.Source.LikeCount != origDoc.LikeCount+1 {
		t.Fatalf("docGetRsp.Source.LikeCount=%d", docGetRsp.Source.LikeCount)
	}
	if docGetRsp.Source.CommentCount != origDoc.CommentCount {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}

	if docGetRsp.Source.OtherCount != origDoc.OtherCount+1 {
		t.Fatalf("docGetRsp.Source.CommentCount=%d", docGetRsp.Source.CommentCount)
	}
}
