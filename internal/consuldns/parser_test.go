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

package consuldns

import "testing"

func TestParseName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want parsedQuery
	}{
		{"web.service.consul.", parsedQuery{kind: kindService, service: "web"}},
		{"web.service.dc1.dc.consul.", parsedQuery{kind: kindService, service: "web", datacenter: "dc1"}},
		{"web.service.dc1.consul.", parsedQuery{kind: kindService, service: "web", datacenter: "dc1"}},
		{"v1.web.service.consul.", parsedQuery{kind: kindService, service: "web", tag: "v1"}},
		{"_web._v2.service.consul.", parsedQuery{kind: kindService, service: "web", tag: "v2"}},
		{"_web._v2.service.dc1.dc.consul.", parsedQuery{kind: kindService, service: "web", tag: "v2", datacenter: "dc1"}},
		{"_web._tcp.consul.", parsedQuery{kind: kindService, service: "web", tag: "tcp"}},
		{"node-a.node.consul.", parsedQuery{kind: kindNode, node: "node-a"}},
		{"node-a.node.dc1.dc.consul.", parsedQuery{kind: kindNode, node: "node-a", datacenter: "dc1"}},
		{"web.connect.consul.", parsedQuery{kind: kindUnknown}},
		{"web.query.consul.", parsedQuery{kind: kindUnknown}},
		{"example.com.", parsedQuery{kind: kindUnknown}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseName(tc.name, "consul.")
			if got != tc.want {
				t.Errorf("parseName(%q) = %+v, want %+v", tc.name, got, tc.want)
			}
		})
	}
}
