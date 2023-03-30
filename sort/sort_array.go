package sort

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type SortType struct {
	sorts []any
}

func (st *SortType) Values() []any {
	if st == nil || len(st.sorts) == 0 {
		return nil
	}
	return st.sorts
}

func (st *SortType) Len() int {
	return len(st.sorts)
}

func (st *SortType) Push(ele any) *SortType {
	st.sorts = append(st.sorts, ele)
	return st
}

// MarshalJSON implement json.Marshaler for SortType
func (st SortType) MarshalJSON() ([]byte, error) {
	return json.Marshal(st.sorts)
}

// UnmarshalJSON implement json.Unmarshaler for SortType
func (st *SortType) UnmarshalJSON(data []byte) error {
	// [ 1676432653945685122, "xxxxxxxx", true, 3.14, "888.6379"]

	de := json.NewDecoder(bytes.NewReader(data))
	de.UseNumber()
	err := de.Decode(&st.sorts)
	if err != nil {
		return err
	}

	for idx, vv := range st.sorts {
		switch v := vv.(type) {
		case json.Number:
			if strings.Contains(v.String(), ".") {
				f64, err := v.Float64()
				if err != nil {
					return fmt.Errorf("unmarshal sort data idex=%v value=%s convert to float64 failed: %v",
						idx, v.String(), err)
				}
				// float64
				st.sorts[idx] = f64
			} else {
				// int64
				i64, err := v.Int64()
				if err != nil {
					return fmt.Errorf("unmarshal sort data idex=%v value=%s convert to int64 failed: %v",
						idx, v.String(), err)
				}
				st.sorts[idx] = i64
			}
		default:
			// do nothing
		}
	}
	return nil
}
