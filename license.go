package elastic_wrapper

import (
	"context"
	"encoding/json"
	"time"
)

/*
GetLicenseResponse is the response of License

	{
	  "license" : {
	    "status" : "active",
	    "uid" : "ab06dd7b-c292-4bd3-836e-81e6a633be07",
	    "type" : "basic",
	    "issue_date" : "2022-10-19T02:20:57.432Z",
	    "issue_date_in_millis" : 1666146057432,
	    "max_nodes" : 1000,
	    "max_resource_units" : null,
	    "issued_to" : "docker-cluster",
	    "issuer" : "elasticsearch",
	    "start_date_in_millis" : -1
	  }
	}
*/
type GetLicenseResponse struct {
	License struct {
		Status            string      `json:"status"`
		Uid               string      `json:"uid"`
		Type              string      `json:"type"`
		IssueDate         time.Time   `json:"issue_date"`
		IssueDateInMillis int64       `json:"issue_date_in_millis"`
		MaxNodes          int         `json:"max_nodes"`
		MaxResourceUnits  interface{} `json:"max_resource_units"`
		IssuedTo          string      `json:"issued_to"`
		Issuer            string      `json:"issuer"`
		StartDateInMillis int         `json:"start_date_in_millis"`
	} `json:"license"`
}

func (g *GetLicenseResponse) FromJSON(bytes []byte) error {
	return json.Unmarshal(bytes, g)
}

func (es *ElasticsearchEx) GetLicenseInfo(ctx context.Context) (*GetLicenseResponse, error) {
	req := es.License.Get()
	var rsp GetLicenseResponse
	err := doGetResponse[ErrGeneric](ctx, req, &rsp)
	return &rsp, err
}
