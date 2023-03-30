package elastic_wrapper

import (
	"context"
	"testing"
)

func TestElasticsearchEx_GetLicenseInfo(t *testing.T) {
	es := newClient(t)
	info, err := es.GetLicenseInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("info=%+v", info)
	// raw from response is "2022-10-19T02:20:57.432Z"
	// go "2022-10-19 02:20:57.432 +0000 UTC"
	// if info.License.IssueDate.String() != "2022-10-19 02:20:57.432 +0000 UTC" {
	//	t.Fatalf("issue date not match, expect=%s, actual=%s", "2022-10-19 02:20:57.432 +0000 UTC", info.License.IssueDate.String())
	// }

	// for tencent cloud:
	// &{License:{Status:active Uid:330047c2-025d-4de8-99db-0ec16af46ea2 Type:platinum IssueDate:2022-04-13 00:00:00 +0000 UTC IssueDateInMillis:1649808000000
	// MaxNodes:1 MaxResourceUnits:<nil> IssuedTo:Tencent Holdings Limited Issuer:API StartDateInMillis:1559347200000}}
	if info.License.Issuer != "elasticsearch" && info.License.Issuer != "API" {
		t.Fatalf("issuer not match, expect=%s, actual=%s", "elasticsearch", info.License.Issuer)
	}
}
