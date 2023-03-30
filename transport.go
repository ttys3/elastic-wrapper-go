package elastic_wrapper

import (
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/propagation"
)

const defaultDialTimeout = 10 * time.Second

func NewTracingTransportDefault(dialTimeout time.Duration, CACert []byte, v7Compatible bool) (http.RoundTripper, error) {
	return NewTracingTransport(GetTransport(dialTimeout), CACert, v7Compatible)
}

func GetTransport(dialTimeout time.Duration) *http.Transport {
	if dialTimeout == 0 {
		dialTimeout = defaultDialTimeout
	}
	// from http.DefaultTransport with slight modification
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout, // stdlib default is 30
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// NewTracingTransport uses *http.Transport which in stdlib and impl http.RoundTripper
func NewTracingTransport(transport *http.Transport, CACert []byte, v7Compatible bool) (http.RoundTripper, error) {
	if len(CACert) > 0 {
		httpTransport := transport.Clone()
		httpTransport.TLSClientConfig.RootCAs = x509.NewCertPool()

		if ok := httpTransport.TLSClientConfig.RootCAs.AppendCertsFromPEM(CACert); !ok {
			return nil, errors.New("unable to add CA certificate")
		}
		return &addTraceparent{
			inner:        httpTransport,
			v7Compatible: v7Compatible,
		}, nil
	}

	return &addTraceparent{
		inner:        transport,
		v7Compatible: v7Compatible,
	}, nil
}

type addTraceparent struct {
	inner        http.RoundTripper
	v7Compatible bool
}

var prop = propagation.TraceContext{}

func (tr *addTraceparent) RoundTrip(r *http.Request) (*http.Response, error) {
	// https://www.w3.org/TR/trace-context/#traceparent-header
	// inject traceparent header into request
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.4/api-conventions.html#traceparent
	prop.Inject(r.Context(), propagation.HeaderCarrier(r.Header))

	if tr.v7Compatible {
		r.Header.Set("Content-Type", "application/json")
	}

	rsp, err := tr.inner.RoundTrip(r)

	if rsp != nil && rsp.Header != nil && tr.v7Compatible {
		// bypass genuineCheckHeader check in the SDK for tencent cloud low version 7.x es
		rsp.Header.Set("X-Elastic-Product", "Elasticsearch")
	}

	return rsp, err
}
