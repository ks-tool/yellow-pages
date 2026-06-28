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
	"net/http"
	"strings"
	"testing"
)

func healthGet(t *testing.T, h http.Handler, path string) []healthServiceEntry {
	t.Helper()
	return decode[[]healthServiceEntry](t, do(t, h, http.MethodGet, path, ""))
}

func TestRegisterWithChecksLenient(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	body := `{"Name":"web","Port":80,"Checks":[{"TTL":"45s","HTTP":"http://x"}],"Connect":{}}`
	if rec := do(t, h, http.MethodPut, "/v1/agent/service/register", body); rec.Code != http.StatusOK {
		t.Fatalf("register with Checks array = %d", rec.Code)
	}
	if got := healthGet(t, h, "/v1/health/service/web"); len(got) != 1 {
		t.Errorf("service not registered: %d", len(got))
	}
}

func TestCheckPassFailIsolated(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web","Port":80}`)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"api","Port":81}`)

	// Fail only web's check.
	if rec := do(t, h, http.MethodPut, "/v1/agent/check/fail/service:web", ""); rec.Code != http.StatusOK {
		t.Fatalf("check/fail = %d", rec.Code)
	}

	if got := healthGet(t, h, "/v1/health/service/web?passing"); len(got) != 0 {
		t.Errorf("failed web still in ?passing: %d", len(got))
	}
	if got := healthGet(t, h, "/v1/health/service/web"); len(got) != 1 || got[0].Checks[1].Status != "critical" {
		t.Errorf("web should be visible critical: %+v", got)
	}
	if got := healthGet(t, h, "/v1/health/service/api?passing"); len(got) != 1 {
		t.Errorf("neighbour api affected by web fail: %d", len(got))
	}

	// A pass revives web.
	do(t, h, http.MethodPut, "/v1/agent/check/pass/service:web", "")
	if got := healthGet(t, h, "/v1/health/service/web?passing"); len(got) != 1 {
		t.Errorf("check/pass did not revive web: %d", len(got))
	}
}

func TestServiceMaintenanceEndpoint(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web","Port":80}`)

	if rec := do(t, h, http.MethodPut, "/v1/agent/service/maintenance/web?enable=true", ""); rec.Code != http.StatusOK {
		t.Fatalf("maintenance enable = %d", rec.Code)
	}
	got := healthGet(t, h, "/v1/health/service/web")
	if len(got) != 1 {
		t.Fatalf("maintenance instance not visible: %d", len(got))
	}
	var hasMaint bool
	for _, c := range got[0].Checks {
		if strings.HasPrefix(c.CheckID, "_service_maintenance") {
			hasMaint = true
		}
	}
	if !hasMaint {
		t.Error("missing _service_maintenance check")
	}
	if p := healthGet(t, h, "/v1/health/service/web?passing"); len(p) != 0 {
		t.Errorf("maintenance instance in ?passing: %d", len(p))
	}

	do(t, h, http.MethodPut, "/v1/agent/service/maintenance/web?enable=false", "")
	if p := healthGet(t, h, "/v1/health/service/web?passing"); len(p) != 1 {
		t.Errorf("maintenance disable did not restore web: %d", len(p))
	}
}

func TestHealthStateCritical(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web","Port":80}`)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"api","Port":81}`)
	do(t, h, http.MethodPut, "/v1/agent/check/fail/service:web", "")

	crit := decode[[]healthCheck](t, do(t, h, http.MethodGet, "/v1/health/state/critical", ""))
	if len(crit) != 1 || crit[0].ServiceName != "web" {
		t.Errorf("state/critical = %+v, want only web", crit)
	}
	pass := decode[[]healthCheck](t, do(t, h, http.MethodGet, "/v1/health/state/passing", ""))
	if len(pass) != 1 || pass[0].ServiceName != "api" {
		t.Errorf("state/passing = %+v, want only api", pass)
	}
}

func TestAgentChecksAndHealthByName(t *testing.T) {
	t.Parallel()
	h, _, _ := newHandler(t)
	do(t, h, http.MethodPut, "/v1/agent/service/register", `{"Name":"web","Port":80}`)

	checks := decode[map[string]healthCheck](t, do(t, h, http.MethodGet, "/v1/agent/checks", ""))
	if _, ok := checks["service:web"]; !ok {
		t.Errorf("agent/checks missing service:web: %+v", checks)
	}

	if rec := do(t, h, http.MethodGet, "/v1/agent/health/service/name/web?format=text", ""); rec.Body.String() != "passing" {
		t.Errorf("health by name text = %q, want passing", rec.Body.String())
	}
	if rec := do(t, h, http.MethodGet, "/v1/agent/health/service/name/absent", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown service health = %d, want 404", rec.Code)
	}
}
