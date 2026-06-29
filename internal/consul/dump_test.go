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
	"context"
	"net/http"
	"testing"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// divergeDumper reports a per-seed divergence: two seeds disagree on web.
type divergeDumper struct{}

func (divergeDumper) Dump(_ context.Context, _ model.Query) map[string][]model.ServiceEntry {
	entry := func(node string) model.ServiceEntry {
		return model.ServiceEntry{Node: model.Node{ID: node}, Service: model.ServiceInstance{ID: "web", Name: "web"}}
	}
	return map[string][]model.ServiceEntry{
		"seed-a:9900": {entry("agent-1"), entry("agent-2")},
		"seed-b:9900": {entry("agent-1")}, // missing agent-2: divergence
	}
}

func TestRegistryDumpShowsPerSeedDivergence(t *testing.T) {
	t.Parallel()
	h, _, _, _ := newHandlerWith(t, Options{Dumper: divergeDumper{}})

	got := decode[map[string][]struct {
		Node    model.Node
		Service model.ServiceInstance
	}](t, do(t, h, http.MethodGet, "/v1/internal/registry-dump?service=web", ""))

	if len(got["seed-a:9900"]) != 2 || len(got["seed-b:9900"]) != 1 {
		t.Errorf("registry-dump did not reveal divergence: %+v", got)
	}
}

func TestRegistryDumpLocalFallback(t *testing.T) {
	t.Parallel()
	h, _, _, _ := newHandlerWith(t, Options{}) // no Dumper -> local fallback
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web","Port":80}`)

	got := decode[map[string][]struct {
		Service model.ServiceInstance
	}](t, do(t, h, http.MethodGet, "/v1/internal/registry-dump?service=web", ""))
	if len(got["local"]) != 1 {
		t.Errorf("local registry-dump = %+v, want one web", got)
	}
}
