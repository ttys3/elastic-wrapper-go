package elastic_wrapper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type MgetRequest struct {
	IDs []string `json:"ids"` // document ids to get
}

func isSuccessEsapi(res *esapi.Response) bool {
	// the same logic as es SDK `IsSuccess` method
	if res != nil && res.StatusCode >= 200 && res.StatusCode < 300 {
		return true
	}
	return false
}

type _docResponse[T any] struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Found       bool   `json:"found"`
	Source      T      `json:"_source"`
}

type DocsResponse[T any] struct {
	Docs []_docResponse[T] `json:"docs"`
}

func (m *DocsResponse[T]) GetSources() []T {
	docs := make([]T, 0, len(m.Docs))
	for idx := range m.Docs {
		if m.Docs[idx].Found {
			docs = append(docs, m.Docs[idx].Source)
		}
	}
	return docs
}

func (m *DocsResponse[T]) FromJSON(i []byte) error {
	return FromJSONImplDefault(i, m)
}

var _ FromJSON = (*DocsResponse[any])(nil)

func (es *ElasticsearchEx) GetDocumentByIDs(ctx context.Context, index string, ids []string, dest FromJSON) error {
	// es.Client.Mget
	payload, err := json.Marshal(&MgetRequest{IDs: ids})
	if err != nil {
		return err
	}

	r := esapi.MgetRequest{Body: bytes.NewReader(payload), Index: index}
	res, err := r.Do(ctx, es.Client.Transport)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if !isSuccessEsapi(res) {
		if res.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if res.StatusCode == http.StatusConflict {
			return ErrConflict
		}
		var errMget ErrGeneric
		err = json.Unmarshal(body, &errMget)
		if err != nil {
			return fmt.Errorf("request failed, code=%d, body=%s", res.StatusCode, string(body))
		}
		return errMget
	}

	return dest.FromJSON(body)
}
