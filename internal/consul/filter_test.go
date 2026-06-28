/*
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 	http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package consul

import (
	"testing"

	"github.com/ks-tool/yellow-pages/internal/model"
)

func TestFilterSubset(t *testing.T) {
	t.Parallel()
	entry := model.ServiceEntry{
		Service: model.ServiceInstance{
			Name: "web",
			Tags: []string{"v1", "primary"},
			Meta: map[string]string{"env": "prod", "team": "core"},
		},
		Health: model.HealthPassing,
	}

	cases := []struct {
		filter string
		want   bool
	}{
		{`ServiceTags contains "v1"`, true},
		{`ServiceTags contains "v2"`, false},
		{`"primary" in ServiceTags`, true},
		{`ServiceMeta.env == "prod"`, true},
		{`ServiceMeta.env != "prod"`, false},
		{`ServiceMeta.team == "core"`, true},
		{`Checks.Status == "passing"`, true},
		{`Checks.Status == "critical"`, false},
		{`ServiceTags contains "v1" and ServiceMeta.env == "prod"`, true},
		{`ServiceTags contains "v2" or ServiceMeta.env == "prod"`, true},
		{`not ServiceTags contains "v2"`, true},
		{`ServiceTags contains "v1" and not ServiceMeta.env == "dev"`, true},
		{`(ServiceTags contains "v2" or ServiceMeta.env == "prod") and Checks.Status == "passing"`, true},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			t.Parallel()
			f, err := parseFilter(tc.filter)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.filter, err)
			}
			if got := f(viewOf(entry)); got != tc.want {
				t.Errorf("%q = %v, want %v", tc.filter, got, tc.want)
			}
		})
	}
}

func TestFilterParseError(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{`ServiceTags`, `ServiceMeta.env ~~ "x"`, `( ServiceTags contains "v1"`} {
		if _, err := parseFilter(bad); err == nil {
			t.Errorf("parseFilter(%q) = nil error, want error", bad)
		}
	}
}
