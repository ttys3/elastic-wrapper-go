package elastic_wrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func bodyClose(res *http.Response) {
	if res != nil && res.Body != nil {
		res.Body.Close()
	}
}

func isSuccess(res *http.Response) bool {
	// the same logic as es SDK `IsSuccess` method
	if res != nil {
		return successCode(res.StatusCode)
	}
	return false
}

func successCode(code int) bool {
	return code >= 200 && code < 300
}

type HttpRequest interface {
	Do(ctx context.Context) (*http.Response, error)
}

type ESRequest interface {
	Do(ctx context.Context, transport esapi.Transport) (*esapi.Response, error)
}

type ErrGeneric map[string]any

func (e ErrGeneric) Error() string {
	str, _ := json.Marshal(e)
	return string(str)
}

type ResponseGeneric struct{}

func (r *ResponseGeneric) FromJSON(buf []byte) error {
	return json.Unmarshal(buf, r)
}

func doGetResponse[E error](ctx context.Context, req HttpRequest, rsp FromJSON) error {
	res, err := req.Do(ctx)
	defer bodyClose(res)

	if err != nil {
		return err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	// logger.WithTraceID(ctx).Debugw("doGetResponse dump elastic body", "body", string(body))

	if !isSuccess(res) {
		if res.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if res.StatusCode == http.StatusConflict {
			return ErrConflict
		}
		var respErr E
		err = json.Unmarshal(body, &respErr)
		if err != nil {
			return fmt.Errorf("request failed, code=%d, body=%s", res.StatusCode, string(body))
		}
		return respErr
	}
	return rsp.FromJSON(body)
}

func handleESRequestIgnoreResponse(ctx context.Context, transport esapi.Transport, req ESRequest) error {
	return handleESRequest(ctx, transport, req, IgnoreResponse{})
}

func handleESRequest(ctx context.Context, transport esapi.Transport, req ESRequest, dest FromJSON) error {
	// do request
	res, err := req.Do(ctx, transport)
	if err != nil {
		return fmt.Errorf("error on do request: %w", err)
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	// handle status code
	err = NewResponseStatusError(res.StatusCode)
	if err != nil {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("fail response: %s, code: %w", body, err)
	}
	if _, ok := dest.(IgnoreResponse); ok {
		return nil
	}
	// handle response body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("error on read response body: %w, code: %v", err, res.StatusCode)
	}
	return dest.FromJSON(body)
}
