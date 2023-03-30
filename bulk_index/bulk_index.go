// Package bulk_index implement bulk index documents api.
package bulk_index

import (
	gobytes "bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
)

const (
	actionMask = iota + 1

	indexMask
)

// ErrBuildPath is returned in case of missing parameters within the build of the request.
var ErrBuildPath = errors.New("cannot build path, check for missing path parameters")

type BulkIndex struct {
	transport elastictransport.Interface

	headers http.Header
	values  url.Values
	path    url.URL

	buf *gobytes.Buffer

	req interface{}
	raw []byte

	paramSet int

	index string
}

// NewBulkIndex type alias for index.
type NewBulkIndex func(index string) *BulkIndex

// NewBulkIndexFunc returns a new instance of BulkIndex with the provided transport.
// Used in the index of the library this allows to retrieve every apis in once place.
func NewBulkIndexFunc(tp elastictransport.Interface) NewBulkIndex {
	return func(index string) *BulkIndex {
		n := New(tp)

		n.Index(index)

		return n
	}
}

// New Creates a new bulk document index req.
//
// https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html
func New(tp elastictransport.Interface) *BulkIndex {
	r := &BulkIndex{
		transport: tp,
		values:    make(url.Values),
		headers:   make(http.Header),
		buf:       gobytes.NewBuffer(nil),
	}

	return r
}

// Raw takes a json payload as input which is then passed to the http.Request
// If specified Raw takes precedence on Request method.
func (r *BulkIndex) Raw(raw []byte) *BulkIndex {
	r.raw = raw

	return r
}

// Request allows to set the request property with the appropriate payload.
func (r *BulkIndex) Request(req interface{}) *BulkIndex {
	r.req = req

	return r
}

// HttpRequest returns the http.Request object built from the
// given parameters.
func (r *BulkIndex) HttpRequest(ctx context.Context) (*http.Request, error) {
	var path strings.Builder
	var method string
	var req *http.Request

	var err error

	if r.raw != nil {
		r.buf.Write(r.raw)
	} else if r.req != nil {
		data, err := json.Marshal(r.req)
		if err != nil {
			return nil, fmt.Errorf("could not serialise request for BulkIndex: %w", err)
		}

		r.buf.Write(data)
	}

	r.path.Scheme = "http"
	method = http.MethodPost

	// POST /_bulk
	// POST /<target>/_bulk
	// target data stream, index, or index alias
	switch {
	case r.paramSet == indexMask:
		path.WriteString("/")
		path.WriteString(url.PathEscape(r.index))
	}

	path.WriteString("/")
	path.WriteString("_bulk")

	r.path.Path = path.String()
	r.path.RawQuery = r.values.Encode()

	if r.path.Path == "" {
		return nil, ErrBuildPath
	}

	if ctx != nil {
		req, err = http.NewRequestWithContext(ctx, method, r.path.String(), r.buf)
	} else {
		req, err = http.NewRequest(method, r.path.String(), r.buf)
	}

	req.Header = r.headers.Clone()

	if req.Header.Get("Content-Type") == "" {
		if r.buf.Len() > 0 {
			req.Header.Set("Content-Type", "application/x-ndjson")
		}
	}

	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/vnd.elasticsearch+json;compatible-with=8")
	}

	if err != nil {
		return req, fmt.Errorf("could not build http.Request: %w", err)
	}

	return req, nil
}

// Do runs the http.Request through the provided transport.
func (r BulkIndex) Do(ctx context.Context) (*http.Response, error) {
	req, err := r.HttpRequest(ctx)
	if err != nil {
		return nil, err
	}

	res, err := r.transport.Perform(req)
	if err != nil {
		return nil, fmt.Errorf("an error happened during the BulkIndex query execution: %w", err)
	}

	return res, nil
}

// Header set a key, value pair in the BulkIndex headers map.
func (r *BulkIndex) Header(key, value string) *BulkIndex {
	r.headers.Set(key, value)

	return r
}

// Index The name of the index
// API Name: index
func (r *BulkIndex) Index(v string) *BulkIndex {
	r.paramSet |= indexMask
	r.index = v

	return r
}
