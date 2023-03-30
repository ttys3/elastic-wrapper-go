package elastic_wrapper

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
)

type article struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Published time.Time `json:"published"`
	Author    author    `json:"author"`
}

func (a *article) GetID() string {
	return strconv.Itoa(a.ID)
}

type author struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func TestElasticsearchEx_BulkIndex(t *testing.T) {
	indexName := "test-bulk-example"
	numItems := 10000
	var articles []Document
	names := []string{"Alice", "John", "Mary"}
	for i := 1; i <= numItems; i++ {
		articles = append(articles, &article{
			ID:        i,
			Title:     strings.Join([]string{"Title", strconv.Itoa(i)}, " "),
			Body:      "Lorem ipsum dolor sit amet...",
			Published: time.Now().Round(time.Second).UTC().AddDate(0, 0, i),
			Author: author{
				FirstName: names[rand.Intn(len(names))],
				LastName:  "Smith",
			},
		})
	}
	t.Logf("→ Generated %s articles", humanize.Comma(int64(len(articles))))

	es := newClient(t)

	ctx := context.Background()
	if exists, err := es.IndexExists(ctx, indexName); err == nil && exists {
		if deleted, err := es.IndexDelete(ctx, indexName); err != nil {
			t.Fatalf("→ Failed to delete index %s: %v", indexName, err)
		} else {
			t.Logf("→ Deleted index %v", deleted)
		}
	}
	// Re-create the index
	res, err := es.Client.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}
	res.Body.Close()

	start := time.Now().UTC()

	err = es.BulkIndex(context.Background(), indexName, articles)
	if err != nil {
		t.Fatalf("→ Failed to bulk index: %v", err)
	}
	t.Logf("→ Bulk indexed %s articles in %s", humanize.Comma(int64(len(articles))), time.Since(start))
}

func TestElasticsearchEx_BulkUpdate(t *testing.T) {
	indexName := "test-bulk-update-example"
	numItems := 3
	var articles []Document
	names := []string{"Alice", "John", "Mary"}
	for i := 1; i <= numItems; i++ {
		articles = append(articles, &article{
			ID:    i,
			Title: strings.Join([]string{"Title", strconv.Itoa(i)}, " "),
			Body:  "Lorem ipsum dolor sit amet...",
			Author: author{
				FirstName: names[rand.Intn(len(names))],
				LastName:  "Smith",
			},
		})
	}
	t.Logf("→ Generated %s articles", humanize.Comma(int64(len(articles))))

	es := newClient(t)

	ctx := context.Background()
	if exists, err := es.IndexExists(ctx, indexName); err == nil && exists {
		if deleted, err := es.IndexDelete(ctx, indexName); err != nil {
			t.Fatalf("→ Failed to delete index %s: %v", indexName, err)
		} else {
			t.Logf("→ Deleted index %v", deleted)
		}
	}
	// Re-create the index
	res, err := es.Client.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}
	res.Body.Close()

	start := time.Now().UTC()

	err = es.BulkIndex(context.Background(), indexName, articles)
	if err != nil {
		t.Fatalf("→ Failed to bulk index: %v", err)
	}
	t.Logf("→ Bulk indexed %s articles in %s", humanize.Comma(int64(len(articles))), time.Since(start))

	updates := map[string]map[string]any{
		"1": {
			"body": "hello",
		},
		"2": {
			"body": "world",
		},
		"3": {
			"body": "ninja",
		},
	}

	start = time.Now().UTC()
	err = es.BulkUpdate(context.Background(), indexName, updates)
	if err != nil {
		t.Fatalf("→ Failed to bulk index: %v", err)
	}
	t.Logf("→ Bulk updated %s articles in %s", humanize.Comma(int64(len(articles))), time.Since(start))

	var result DocsResponse[article]
	err = es.GetDocumentByIDs(context.Background(), indexName, []string{"1", "2", "3"}, &result)
	if err != nil {
		t.Fatalf("→ Failed to mget: %v", err)
	}
	for _, r := range result.Docs {
		t.Logf("→ %v", r)
		if r.Source.Body != updates[strconv.Itoa(r.Source.ID)]["body"] {
			t.Errorf("→ Failed to update %v", r.Source.ID)
		}
	}
}
