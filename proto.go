package elastic_wrapper

import "encoding/json"

type FromJSON interface {
	FromJSON([]byte) error
}

// FromJSONImplDefault is just a convenient func to avoid import json package everywhere
func FromJSONImplDefault(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
