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
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/store"
)

// testRegistry backs the handler with a Store and tracks hosted services.
type testRegistry struct {
	st     *store.Memory
	node   model.Node
	hosted map[string]model.ServiceInstance
}

func newRegistry(st *store.Memory, node model.Node) *testRegistry {
	return &testRegistry{st: st, node: node, hosted: map[string]model.ServiceInstance{}}
}

func (r *testRegistry) RegisterServices(_ context.Context, reg model.Registration) error {
	reg.Node = r.node
	for _, s := range reg.Services {
		r.hosted[s.ID] = s
	}
	return r.st.Register(reg)
}

func (r *testRegistry) RemoveService(_ context.Context, id string) error {
	delete(r.hosted, id)
	return r.st.DeregisterService(r.node.ID, id)
}

func (r *testRegistry) Resolve(_ context.Context, q model.Query) (model.LookupResult, error) {
	return r.st.Lookup(q), nil
}

func (r *testRegistry) Hosted() []model.ServiceInstance {
	out := make([]model.ServiceInstance, 0, len(r.hosted))
	for _, s := range r.hosted {
		out = append(out, s)
	}
	return out
}

func newHandler(t *testing.T) (http.Handler, *testRegistry, *store.Memory) {
	t.Helper()
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
	node := model.Node{ID: "agent-1", Name: "node-a", Address: "10.0.0.5", Datacenter: "dc1"}
	reg := newRegistry(st, node)
	h := NewHandler(reg, NodeInfo{
		ID: "agent-1", Name: "node-a", Datacenter: "dc1", Address: "10.0.0.5", Version: "1.2.3", Seeds: []string{"10.0.0.9:9900"},
	}, func() uint64 { return 42 }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h, reg, st
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v\nbody: %s", rec.Result().Status, err, rec.Body.String())
	}
	return v
}

const registerBody = `{
  "Name": "web", "Port": 8080, "Address": "10.0.0.5", "Tags": ["v1","primary"],
  "Meta": {"env": "prod"},
  "Check": {"TTL": "30s", "HTTP": "http://x/health"},
  "Connect": {"SidecarService": {}},
  "Weights": {"Passing": 5, "Warning": 1}
}`

func TestRegisterLenientAndCatalogFlat(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)

	// Lenient: unknown fields (Check.HTTP, Connect) are ignored.
	if rec := do(t, h, http.MethodPut, "/v1/agent/service/register", registerBody); rec.Code != http.StatusOK {
		t.Fatalf("register status = %d, body %s", rec.Code, rec.Body.String())
	}

	rec := do(t, h, http.MethodGet, "/v1/catalog/service/web", "")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	got := decode[[]catalogService](t, rec)
	if len(got) != 1 {
		t.Fatalf("catalog/service = %d entries, want 1", len(got))
	}
	e := got[0]
	if e.ServiceName != "web" || e.ServiceID != "web" || e.ServicePort != 8080 {
		t.Errorf("flat entry = %+v", e)
	}
	if e.ServiceAddress != "10.0.0.5" || e.Node != "node-a" || e.Datacenter != "dc1" {
		t.Errorf("flat node/address = %+v", e)
	}
	if e.ServiceWeights.Passing != 5 || e.ServiceWeights.Warning != 1 {
		t.Errorf("weights = %+v, want 5/1", e.ServiceWeights)
	}
	if len(e.ServiceTags) != 2 {
		t.Errorf("tags = %v", e.ServiceTags)
	}
}

func TestHealthNestedSchemaAndChecks(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", registerBody)

	got := decode[[]healthServiceEntry](t, do(t, h, http.MethodGet, "/v1/health/service/web", ""))
	if len(got) != 1 {
		t.Fatalf("health/service = %d, want 1", len(got))
	}
	e := got[0]
	if e.Service.Service != "web" || e.Service.ID != "web" || e.Service.Port != 8080 {
		t.Errorf("nested service = %+v", e.Service)
	}
	if e.Node.Node != "node-a" || e.Node.Datacenter != "dc1" {
		t.Errorf("nested node = %+v", e.Node)
	}
	if e.Service.Weights.Passing != 5 {
		t.Errorf("weights not filled: %+v", e.Service.Weights)
	}
	var hasSerf, hasSvc bool
	for _, c := range e.Checks {
		if c.CheckID == "serfHealth" {
			hasSerf = true
		}
		if c.CheckID == "service:web" && c.Status == "passing" {
			hasSvc = true
		}
	}
	if !hasSerf || !hasSvc {
		t.Errorf("synthetic checks missing: %+v", e.Checks)
	}
}

func TestEmptyServiceReturns200EmptyArray(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	rec := do(t, h, http.MethodGet, "/v1/health/service/absent", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("body = %q, want []", rec.Body.String())
	}
}

func TestServiceAddressFallsBackToNode(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	// No service Address: catalog ServiceAddress falls back to the node address.
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"api","Port":9000}`)
	got := decode[[]catalogService](t, do(t, h, http.MethodGet, "/v1/catalog/service/api", ""))
	if len(got) != 1 || got[0].ServiceAddress != "10.0.0.5" {
		t.Errorf("ServiceAddress = %q, want node fallback 10.0.0.5", got[0].ServiceAddress)
	}
	if got[0].ServiceWeights.Passing != 1 || got[0].ServiceWeights.Warning != 1 {
		t.Errorf("default weights = %+v, want 1/1", got[0].ServiceWeights)
	}
}

func TestMaintenanceVisibleButNotPassing(t *testing.T) {
	t.Parallel()
	h, _, st := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", registerBody)
	if err := st.SetMaintenance("agent-1", "web", true); err != nil {
		t.Fatal(err)
	}

	// Visible without ?passing, with a _service_maintenance critical check.
	all := decode[[]healthServiceEntry](t, do(t, h, http.MethodGet, "/v1/health/service/web", ""))
	if len(all) != 1 {
		t.Fatalf("maintenance instance not visible: %d", len(all))
	}
	var hasMaint bool
	for _, c := range all[0].Checks {
		if strings.HasPrefix(c.CheckID, "_service_maintenance") && c.Status == "critical" {
			hasMaint = true
		}
	}
	if !hasMaint {
		t.Errorf("missing _service_maintenance critical check: %+v", all[0].Checks)
	}

	// Excluded by ?passing.
	passing := decode[[]healthServiceEntry](t, do(t, h, http.MethodGet, "/v1/health/service/web?passing", ""))
	if len(passing) != 0 {
		t.Errorf("?passing must exclude maintenance, got %d", len(passing))
	}
}

func TestStatusLeaderAndAgentSelf(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)

	rec := do(t, h, http.MethodGet, "/v1/status/leader", "")
	if rec.Header().Get("X-Consul-KnownLeader") != "true" {
		t.Errorf("X-Consul-KnownLeader = %q", rec.Header().Get("X-Consul-KnownLeader"))
	}
	leader := decode[string](t, rec)
	if leader == "" || !strings.HasSuffix(leader, ":8300") {
		t.Errorf("leader = %q, want non-empty addr:8300", leader)
	}

	self := decode[agentSelf](t, do(t, h, http.MethodGet, "/v1/agent/self", ""))
	if self.Config.Datacenter != "dc1" || self.Config.NodeName != "node-a" || self.Config.Version != "1.2.3" {
		t.Errorf("agent/self Config = %+v", self.Config)
	}
}

func TestCatalogServicesAndAgentServices(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", registerBody)

	services := decode[map[string][]string](t, do(t, h, http.MethodGet, "/v1/catalog/services", ""))
	if tags, ok := services["web"]; !ok || len(tags) != 2 {
		t.Errorf("catalog/services = %+v", services)
	}

	agentSvcs := decode[map[string]agentService](t, do(t, h, http.MethodGet, "/v1/agent/services", ""))
	if s, ok := agentSvcs["web"]; !ok || s.Service != "web" || s.Port != 8080 {
		t.Errorf("agent/services = %+v", agentSvcs)
	}
}
