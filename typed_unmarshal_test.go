package elastic_wrapper

import (
	"encoding/json"
	"strconv"
)

type SortUnmarshaler []SortUnmarshalItem

func NewSortUnmarshaler(ts string) *SortUnmarshaler {
	items := make(SortUnmarshaler, 0, len(ts))
	for idx := range ts {
		items = append(items, SortUnmarshalItem{kind: ts[idx]})
	}
	return &items
}

func (s *SortUnmarshaler) Values() []any {
	if s == nil {
		return nil
	}
	sli := make([]any, 0, len(*s))
	for _, item := range *s {
		sli = append(sli, item.value)
	}
	return sli
}

type SortUnmarshalItem struct {
	kind  byte // i: int64, f: float64, s: string, else: any
	value any
}

func (s *SortUnmarshalItem) Value() any {
	return s.value
}

func (s *SortUnmarshalItem) Int() int64 {
	i, _ := s.value.(int64)
	return i
}

func (s *SortUnmarshalItem) Float() float64 {
	f, _ := s.value.(float64)
	return f
}

func (s *SortUnmarshalItem) Str() string {
	str, _ := s.value.(string)
	return str
}

func (s *SortUnmarshalItem) UnmarshalJSON(buf []byte) error {
	switch s.kind {
	case 's':
		s.value = string(buf)
	case 'f':
		f, err := strconv.ParseFloat(string(buf), 64)
		if err != nil {
			return err
		}
		s.value = f
	case 'i':
		i, err := strconv.ParseInt(string(buf), 10, 64)
		if err != nil {
			return err
		}
		s.value = i
	default:
		err := json.Unmarshal(buf, &s.value)
		if err != nil {
			return err
		}
	}
	return nil
}

type SearchResponseSort[Src any] struct {
	ts string
	UntypedSearchResponse[Src, NeverUnmarshal, json.RawMessage]
}

func (resp *SearchResponseSort[_]) LastSort() []any {
	n := len(resp.Hits.Hits)
	if n == 0 {
		return nil
	}
	sort := NewSortUnmarshaler(resp.ts)
	json.Unmarshal(resp.Hits.Hits[n-1].Sort, sort)
	return sort.Values()
}
