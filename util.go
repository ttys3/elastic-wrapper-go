package elastic_wrapper

import (
	"io"

	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// NewJSONReader re-export esutil.NewJSONReader to avoid client code import esutil
// encodes v into JSON and returns it as an io.Reader.
func NewJSONReader(v interface{}) io.Reader {
	return esutil.NewJSONReader(v)
}
