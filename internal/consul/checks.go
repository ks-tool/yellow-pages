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

	"github.com/ks-tool/yellow-pages/internal/model"
)

// checkAccept accepts a check register/deregister without error (the TTL bridge
// is implicit in the service's lease; active checks are accepted, not run).
func (h *Handler) checkAccept(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// checkUpdate bridges /v1/agent/check/{pass,warn,fail,update}/:check_id to the
// per-service lease: pass/warn/update renew the service, fail forces it critical.
// The check id maps to a service id (the "service:" prefix is stripped).
func (h *Handler) checkUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	serviceID := serviceFromCheckID(r.PathValue("checkID"))
	verb := pathVerb(r.URL.Path)

	var err error
	if verb == "fail" {
		err = h.reg.FailService(r.Context(), serviceID)
	} else {
		err = h.reg.RenewService(r.Context(), serviceID)
	}
	if err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// serviceMaintenance toggles a service's maintenance flag (?enable=true|false).
func (h *Handler) serviceMaintenance(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	enable := r.URL.Query().Get("enable") == "true"
	if err := h.reg.SetMaintenance(r.Context(), r.PathValue("serviceID"), enable); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// agentChecks synthesises one check per local service from its current health.
func (h *Handler) agentChecks(w http.ResponseWriter, r *http.Request) {
	out := map[string]healthCheck{}
	for _, e := range h.localEntries(r) {
		for _, c := range synthChecks(e) {
			out[c.CheckID] = c
		}
	}
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, out)
}

// agentHealthByName derives the aggregated status of a named local service.
// ?format=text returns the bare status; otherwise a small JSON object. 404 when
// the service is not registered on this agent.
func (h *Handler) agentHealthByName(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var matched []model.ServiceEntry
	for _, e := range h.localEntries(r) {
		if e.Service.Name == name {
			matched = append(matched, e)
		}
	}
	if len(matched) == 0 {
		http.Error(w, "ServiceNotFound", http.StatusNotFound)
		return
	}
	status := aggregateStatus(matched)
	if r.URL.Query().Get("format") == "text" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(status))
		return
	}
	h.writeJSON(w, h.indexFor(model.Query{Name: name}), model.ConsistencyDefault, 0,
		map[string]string{"AggregatedStatus": status})
}

// healthChecks renders one synthetic service check per merged instance.
func (h *Handler) healthChecks(w http.ResponseWriter, r *http.Request) {
	entries, idx, mode, age, ok := h.read(w, r)
	if !ok {
		return
	}
	out := []healthCheck{}
	for _, e := range entries {
		out = append(out, serviceCheck(e))
	}
	h.writeJSON(w, idx, mode, age, out)
}

// healthState traverses all services and returns the synthetic checks whose
// status matches {state} (passing|warning|critical|any). critical includes
// maintenance and failed instances — real not-passing visibility.
func (h *Handler) healthState(w http.ResponseWriter, r *http.Request) {
	state := r.PathValue("state")
	res, _, err := h.reg.Resolve(r.Context(), model.Query{Datacenter: r.URL.Query().Get("dc")}, readMode(r))
	if err != nil {
		h.fail(w, err)
		return
	}
	out := []healthCheck{}
	for _, e := range res.Entries {
		c := serviceCheck(e)
		if state == "any" || c.Status == state {
			out = append(out, c)
		}
	}
	h.writeJSON(w, h.indexFor(model.Query{}), readMode(r), 0, out)
}

// --- helpers ---

func (h *Handler) localEntries(r *http.Request) []model.ServiceEntry {
	res, _, err := h.reg.Resolve(r.Context(), model.Query{}, model.ConsistencyDefault)
	if err != nil {
		return nil
	}
	out := make([]model.ServiceEntry, 0, len(res.Entries))
	for _, e := range res.Entries {
		if e.Node.ID == h.info.ID {
			out = append(out, e)
		}
	}
	return out
}

// serviceCheck is the single service-level synthetic check for an entry, with
// maintenance reflected as critical.
func serviceCheck(e model.ServiceEntry) healthCheck {
	status := healthStatus(e.Health)
	if e.Maintenance {
		status = "critical"
	}
	return healthCheck{
		Node: e.Node.DisplayName(), CheckID: "service:" + e.Service.ID, Name: "Service '" + e.Service.Name + "' check",
		Status: status, ServiceID: e.Service.ID, ServiceName: e.Service.Name,
	}
}

func aggregateStatus(entries []model.ServiceEntry) string {
	status := "passing"
	for _, e := range entries {
		s := healthStatus(e.Health)
		if e.Maintenance || s == "critical" {
			return "critical"
		}
		if s == "warning" {
			status = "warning"
		}
	}
	return status
}

func serviceFromCheckID(checkID string) string {
	return strings.TrimPrefix(checkID, "service:")
}

func pathVerb(path string) string {
	// .../check/<verb>/<id>
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "check" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}
