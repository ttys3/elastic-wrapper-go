package elastic_wrapper

import (
	"context"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	elasticsearchv8 "github.com/elastic/go-elasticsearch/v8"

	"github.com/ttys3/tracing-go"
)

func newClient(t *testing.T) *ElasticsearchEx {
	_, err := tracing.InitProvider(context.Background(), tracing.WithStdoutTrace())
	if err != nil {
		t.Fatalf("init tracer failed: %v", err)
	}
	t.Cleanup(func() {
		// shutdown(context.Background())
	})
	cert := []byte("")
	if certEnv := os.Getenv("TEST_CA_CERT_FILE"); certEnv != "" {
		var err error
		cert, err = os.ReadFile(certEnv)
		if err != nil {
			t.Fatalf("read cert file failed, file=%v err=%v", certEnv, err)
		}
	}

	esAddr := "https://localhost:9200"
	if esAddrEnv := os.Getenv("TEST_ES_ADDR"); esAddrEnv != "" {
		esAddr = esAddrEnv
	}

	username := "elastic"
	password := "elastic"
	if usernameEnv := os.Getenv("TEST_ES_USERNAME"); usernameEnv != "" {
		username = usernameEnv
	}
	if passwordEnv := os.Getenv("TEST_ES_PASSWORD"); passwordEnv != "" {
		password = passwordEnv
	}

	cfg := elasticsearchv8.Config{
		Addresses: []string{
			esAddr,
		},
		Username: username,
		Password: password,
		CACert:   cert,
	}
	es, err := New(WithConfig(&cfg), WithV7Compatible(true))
	if err != nil {
		t.Fatalf("new es client failed, err=%v", err)
	}
	return es
}

func TestDoGetResponceTimeoutRetry(t *testing.T) {
	tp := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 3 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	// RetryOnError default is:
	// func(request *http.Request, err error) bool {
	//				return !errors.Is(err, io.EOF)
	//			}
	tc, err := elasticsearchv8.NewTypedClient(elasticsearchv8.Config{
		Addresses:     []string{"http://1.0.0.1:9200"},
		Transport:     tp,
		RetryOnStatus: []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		MaxRetries:    3,
		DisableRetry:  false, // If DisableRetry is true, then RetryOnStatus, RetryOnError, MaxRetries, and RetryBackoff will be ignored.
	})
	if err != nil {
		t.Errorf("new typed client failed, err=%v", err)
	}
	es := &ElasticsearchEx{
		TypedClient: tc,
	}
	err = es.DocGet(context.Background(), "test", "1234567890", nil)
	if err == nil {
		t.Errorf("get doc should failed")
	}
	t.Logf("got expected err=%v", err)
}
