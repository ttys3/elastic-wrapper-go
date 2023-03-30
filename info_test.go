package elastic_wrapper

import (
	"context"
	"testing"
)

func TestElasticsearchEx_GetClusterInfo(t *testing.T) {
	es := newClient(t)
	info, err := es.GetClusterInfo(context.Background())
	if err != nil {
		t.Errorf("GetClusterInfo() error = %v", err)
		return
	}
	t.Logf("info: %+v", info)
}

func TestElasticsearchEx_Ping(t *testing.T) {
	es := newClient(t)
	err := es.Ping(context.Background())
	if err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}
