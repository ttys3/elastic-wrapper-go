package elastic_wrapper

import "fmt"

type IgnoreResponse struct{}

func (IgnoreResponse) FromJSON(bs []byte) error {
	if len(bs) == 0 {
		return fmt.Errorf("empty response")
	}
	return nil
}
