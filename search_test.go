package elastic_wrapper

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/operator"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"log"
	"reflect"
	"testing"

	"github.com/ttys3/tracing-go"
)

func TestElasticsearchEx_SearchByIndex(t *testing.T) {
	es := newClient(t)

	type args struct {
		ctx   context.Context
		index string
		sr    *search.Request
	}

	getCtx := func(name string) context.Context {
		ctx, _ := tracing.Start(context.Background(), name)
		log.Printf("trace id: %s", tracing.TraceID(ctx))
		return ctx
	}

	// must use float64 for int decode in golang
	testDoc := map[string]any{"content": "this is ok.. kill it", "user_id": float64(10000)}

	tests := []struct {
		name    string
		args    args
		want    map[string]any
		wantErr bool
	}{
		{
			name: "match-query",
			args: args{
				ctx:   getCtx("match-query"),
				index: "demo",
				sr: &search.Request{
					Query: &types.Query{
						Match: map[string]types.MatchQuery{
							"content": {Query: "kill", Operator: &operator.And},
						},
					},
				},
			},
			want:    testDoc,
			wantErr: false,
		},
	}
	got := SearchResponse[map[string]any]{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Cleanup(func() {
				es.IndexDelete(tt.args.ctx, tt.args.index)
			})
			// create index
			_, err := es.IndexCreateRaw(tt.args.ctx, tt.args.index, []byte(`{"mappings":{"properties":{"content":{"type":"text"}}}}`))
			if err != nil {
				t.Fatalf("IndexCreateRaw() error = %v", err)
			}

			// add test doc and force refresh now
			_, err = es.DocCreateRefresh(tt.args.ctx, tt.args.index, "aaa", map[string]any{"content": "this is ok.. kill it", "user_id": 10000}, refresh.True)
			// wait index
			// time.Sleep(time.Second)

			err = es.SearchByIndex(tt.args.ctx, tt.args.index, tt.args.sr, &got)
			if (err != nil) != tt.wantErr || len(got.Hits.Hits) == 0 {
				t.Errorf("SearchByIndex() error = %v, wantErr %v, got.Hits.Hits=%v", err, tt.wantErr, got.Hits.Hits)
				return
			}
			if !reflect.DeepEqual(got.Hits.Hits[0].Source, tt.want) {
				t.Errorf("SearchByIndex() got = %v, want %v", got.Hits.Hits[0].Source, tt.want)
			}
		})
	}
}
