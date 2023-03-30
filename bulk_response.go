package elastic_wrapper

import "encoding/json"

type BulkOperateResponse struct {
	Errors bool               `json:"errors"`
	Items  []BulkResponseItem `json:"items"`
	Took   int64              `json:"took"`
}

func (b *BulkOperateResponse) FromJSON(buf []byte) error {
	return json.Unmarshal(buf, b)
}

func (b BulkOperateResponse) ErrorItems() []BulkResponseItem {
	if !b.Errors {
		return nil
	}
	var items []BulkResponseItem
	for _, item := range b.Items {
		if successCode(item.Detail.Status) {
			continue
		}
		items = append(items, item)
	}
	return items
}

type BulkResponseItem struct {
	Type   string             `json:"-"` // bulk operate type is one of "create", "index", "update", "delete"
	Detail BulkResponseDetail `json:"-"`
}

func (item *BulkResponseItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]*BulkResponseDetail{item.Type: &item.Detail})
}

func (item *BulkResponseItem) UnmarshalJSON(buf []byte) error {
	var m map[string]BulkResponseDetail
	err := json.Unmarshal(buf, &m)
	if err != nil {
		return err
	}
	for t, detail := range m {
		item.Type, item.Detail = t, detail
	}
	return nil
}

type BulkResponseDetail struct {
	ID          string      `json:"_id"`
	Index       string      `json:"_index"`
	PrimaryTerm *int64      `json:"_primary_term,omitempty"`
	SeqNo       *int64      `json:"_seq_no,omitempty"`
	Shards      *BulkShards `json:"_shards,omitempty"`
	Type        string      `json:"_type"`
	Version     *int64      `json:"_version,omitempty"`
	Result      *string     `json:"result,omitempty"`
	Status      int         `json:"status"`
	Error       *BulkError  `json:"error,omitempty"`
}

type BulkError struct {
	Index     string `json:"index"`
	IndexUUID string `json:"index_uuid"`
	Reason    string `json:"reason"`
	Shard     string `json:"shard"`
	Type      string `json:"type"`
}

type BulkShards struct {
	Failed     int64 `json:"failed"`
	Successful int64 `json:"successful"`
	Total      int64 `json:"total"`
}
