package elastic_wrapper

import (
	"context"
	"fmt"
	"net/http"
	"time"

	elasticsearchv8 "github.com/elastic/go-elasticsearch/v8"
)

type ElasticsearchEx struct {
	*elasticsearchv8.TypedClient                         // the Typed API
	Client                       *elasticsearchv8.Client // the Functional Options API, currently Bulk Indexer is only available in this API
	options                      builderOptions

	UpdateFieldsScript *UpdateFieldsScript
}

type builderOptions struct {
	cfg          *elasticsearchv8.Config
	v7Compatible bool
	dialTimeout  time.Duration
	scripts      map[string]string // id -> script map

	caCert        []byte
	username      string
	password      string
	esAddresses   []string
	disableRetry  bool
	maxRetries    int   // defaultMaxRetries    = 3
	retryOnStatus []int // defaultRetryOnStatus = [...]int{502, 503, 504}
	retryOnError  func(request *http.Request, err error) bool
	retryBackoff  func(attempt int) time.Duration
}

func (es *ElasticsearchEx) GetTransport() http.RoundTripper {
	return es.options.cfg.Transport
}

func (es *ElasticsearchEx) registerScripts(ctx context.Context) error {
	if len(es.options.scripts) == 0 {
		return nil
	}

	for id, script := range es.options.scripts {
		_, err := es.PutStoredScriptSimple(ctx, id, script)
		if err != nil {
			return fmt.Errorf("failed to register script %s: %w", id, err)
		}
	}
	return nil
}

func New(opts ...Option) (*ElasticsearchEx, error) {
	es := &ElasticsearchEx{}
	for _, o := range opts {
		o(&es.options)
	}

	if es.options.cfg == nil {
		es.options.cfg = &elasticsearchv8.Config{
			Addresses: es.options.esAddresses,
			Username:  es.options.username,
			Password:  es.options.password,
			CACert:    es.options.caCert,

			// If DisableRetry is true, then RetryOnStatus, RetryOnError, MaxRetries, and RetryBackoff will be ignored.
			DisableRetry: es.options.disableRetry,

			// if MaxRetries = 0, it will set defaultMaxRetries = 3
			MaxRetries: es.options.maxRetries,

			// If RetryOnStatus is nil, then the defaults will be used:  502 (Bad Gateway), 503 (Service Unavailable), 504 (Gateway Timeout)
			RetryOnStatus: es.options.retryOnStatus,

			// RetryOnError if nil, default is: func(request *http.Request, err error) bool { !errors.Is(err, io.EOF) }
			RetryOnError: es.options.retryOnError,

			// Delay the retry if a backoff function is configured
			RetryBackoff: es.options.retryBackoff,
		}
	}

	if len(es.options.cfg.Addresses) == 0 {
		return nil, fmt.Errorf("no elasticsearch server address provided")
	}

	var err error
	es.options.cfg.EnableMetrics = true
	// es.options.cfg.EnableDebugLogger = true
	// es.options.cfg.EnableCompatibilityMode = false

	caCert := es.options.caCert
	if caCert == nil {
		caCert = es.options.cfg.CACert
	}

	// using our custom transport to inject tracing header
	es.options.cfg.Transport, err = NewTracingTransportDefault(es.options.dialTimeout, caCert, es.options.v7Compatible)
	if err != nil {
		return nil, err
	}
	// force es CACert to nil, it has bug handling cert in custom transport, we must hacking here
	es.options.cfg.CACert = nil
	typedClient, err := elasticsearchv8.NewTypedClient(*es.options.cfg)
	if err != nil {
		return nil, err
	}
	client, err := elasticsearchv8.NewClient(*es.options.cfg)
	if err != nil {
		return nil, err
	}

	err = es.registerScripts(context.Background())
	if err != nil {
		return nil, err
	}

	return &ElasticsearchEx{
		TypedClient: typedClient,
		Client:      client,
	}, nil
}

type Option func(*builderOptions)

func WithUsername(username string) Option {
	return func(o *builderOptions) {
		o.username = username
	}
}

func WithPassword(password string) Option {
	return func(o *builderOptions) {
		o.password = password
	}
}

func WithCACert(cert []byte) Option {
	return func(o *builderOptions) {
		o.caCert = cert
	}
}

func WithAddresses(addresses []string) Option {
	return func(o *builderOptions) {
		o.esAddresses = addresses
	}
}

func WithConfig(cfg *elasticsearchv8.Config) Option {
	return func(o *builderOptions) {
		o.cfg = cfg
	}
}

func WithV7Compatible(v7Compatible bool) Option {
	return func(o *builderOptions) {
		o.v7Compatible = v7Compatible
	}
}

func WithScripts(scriptMap map[string]string) Option {
	return func(o *builderOptions) {
		o.scripts = scriptMap
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(o *builderOptions) {
		o.dialTimeout = timeout
	}
}

func WithDisableRetry(disableRetry bool) Option {
	return func(o *builderOptions) {
		o.disableRetry = disableRetry
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(o *builderOptions) {
		o.maxRetries = maxRetries
	}
}

func WithRetryOnStatus(retryOnStatus []int) Option {
	return func(o *builderOptions) {
		o.retryOnStatus = retryOnStatus
	}
}

func WithRetryOnError(retryOnError func(request *http.Request, err error) bool) Option {
	return func(o *builderOptions) {
		o.retryOnError = retryOnError
	}
}

func WithRetryBackoff(retryBackoff func(attempt int) time.Duration) Option {
	return func(o *builderOptions) {
		o.retryBackoff = retryBackoff
	}
}
