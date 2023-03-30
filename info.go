package elastic_wrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ttys3/tracing-go"
)

func (es *ElasticsearchEx) Ping(ctx context.Context) error {
	ctx, span := tracing.Start(ctx, "Ping")
	defer span.End()

	success, err := es.Core.Ping().IsSuccess(ctx)
	if err != nil {
		return err
	}
	if !success {
		return fmt.Errorf("ping failed, success: %v", success)
	}
	return nil
}

// ClusterInfoResponse is the response of ClusterInfo
/*
{
  "name" : "es01",
  "cluster_name" : "docker-cluster",
  "cluster_uuid" : "222qE0-PTtOtjmE7avMOgg",
  "version" : {
    "number" : "8.4.3",
    "build_flavor" : "default",
    "build_type" : "docker",
    "build_hash" : "42f05b9372a9a4a470db3b52817899b99a76ee73",
    "build_date" : "2022-10-04T07:17:24.662462378Z",
    "build_snapshot" : false,
    "lucene_version" : "9.3.0",
    "minimum_wire_compatibility_version" : "7.17.0",
    "minimum_index_compatibility_version" : "7.0.0"
  },
  "tagline" : "You Know, for Search"
}
*/
type ClusterInfoResponse struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	ClusterUuid string `json:"cluster_uuid"`
	Version     struct {
		Number                           string    `json:"number"`
		BuildFlavor                      string    `json:"build_flavor"`
		BuildType                        string    `json:"build_type"`
		BuildHash                        string    `json:"build_hash"`
		BuildDate                        time.Time `json:"build_date"`
		BuildSnapshot                    bool      `json:"build_snapshot"`
		LuceneVersion                    string    `json:"lucene_version"`
		MinimumWireCompatibilityVersion  string    `json:"minimum_wire_compatibility_version"`
		MinimumIndexCompatibilityVersion string    `json:"minimum_index_compatibility_version"`
	} `json:"version"`
	Tagline string `json:"tagline"`
}

func (c *ClusterInfoResponse) FromJSON(bytes []byte) error {
	return json.Unmarshal(bytes, c)
}

func (es *ElasticsearchEx) GetClusterInfo(ctx context.Context) (*ClusterInfoResponse, error) {
	ctx, span := tracing.Start(ctx, "GetClusterInfo")
	defer span.End()

	req := es.Core.Info()
	var rsp ClusterInfoResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	if err != nil {
		return nil, err
	}
	return &rsp, nil
}
