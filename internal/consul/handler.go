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
	"encoding/json" // the Consul wire is JSON; yaml.v3 cannot emit it. stdlib only.
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
)

// Registry is the cluster surface the Consul adapter projects: agent-scoped
// writes plus merged reads, in domain terms. The agent's Proxy satisfies it
// structurally; tests can back it with a Store.
type Registry interface {
	RegisterServices(ctx context.Context, reg model.Registration) error
	RemoveService(ctx context.Context, serviceID string) error
	Resolve(ctx context.Context, q model.Query) (model.LookupResult, error)
	Hosted() []model.ServiceInstance
}

// NodeInfo is this agent's static identity, rendered into /v1/agent/self,
// /v1/status/* and the Node fields of catalog/health responses.
type NodeInfo struct {
	ID         string
	Name       string
	Datacenter string
	Address    string
	Version    string
	Seeds      []string // seed addresses, rendered as addr:8300 peers
}

// serverPort is the shim Raft/serf server port Consul clients expect in peers
// and leader addresses (there is no Raft; it is a stable cosmetic value).
const serverPort = "8300"

// Handler implements the Consul-compatible HTTP API.
type Handler struct {
	reg   Registry
	info  NodeInfo
	index func() uint64
	log   *slog.Logger
}

// NewHandler builds the Consul HTTP handler. index supplies X-Consul-Index
// (the agent's synthesised monotonic index); nil yields a constant 1.
func NewHandler(reg Registry, info NodeInfo, index func() uint64, log *slog.Logger) http.Handler {
	if index == nil {
		index = func() uint64 { return 1 }
	}
	if log == nil {
		log = slog.Default()
	}
	h := &Handler{reg: reg, info: info, index: index, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/agent/service/register", h.registerService)
	mux.HandleFunc("PUT /v1/agent/service/deregister/{serviceID}", h.deregisterService)
	mux.HandleFunc("GET /v1/agent/services", h.agentServices)
	mux.HandleFunc("GET /v1/agent/self", h.agentSelf)
	mux.HandleFunc("GET /v1/catalog/services", h.catalogServices)
	mux.HandleFunc("GET /v1/catalog/service/{service}", h.catalogService)
	mux.HandleFunc("GET /v1/catalog/nodes", h.catalogNodes)
	mux.HandleFunc("GET /v1/catalog/datacenters", h.catalogDatacenters)
	mux.HandleFunc("GET /v1/health/service/{service}", h.healthService)
	mux.HandleFunc("GET /v1/status/leader", h.statusLeader)
	mux.HandleFunc("GET /v1/status/peers", h.statusPeers)
	return mux
}

func (h *Handler) registerService(w http.ResponseWriter, r *http.Request) {
	var in registerInput
	dec := json.NewDecoder(r.Body) // lenient: unknown fields are ignored
	if err := dec.Decode(&in); err != nil {
		http.Error(w, "invalid register body", http.StatusBadRequest)
		return
	}
	if in.Name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	id := in.ID
	if id == "" {
		id = in.Name
	}
	svc := model.ServiceInstance{
		ID:      id,
		Name:    in.Name,
		Address: in.Address,
		Port:    clampPort(in.Port),
		Tags:    in.Tags,
		Meta:    in.Meta,
		Weights: weightsToModel(in.Weights),
		TTL:     ttlFromCheck(in.Check),
	}
	reg := model.Registration{Node: h.node(), Services: []model.ServiceInstance{svc}, Generation: 1}
	if err := h.reg.RegisterServices(r.Context(), reg); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deregisterService(w http.ResponseWriter, r *http.Request) {
	if err := h.reg.RemoveService(r.Context(), r.PathValue("serviceID")); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) agentServices(w http.ResponseWriter, r *http.Request) {
	out := make(map[string]agentService)
	for _, s := range h.reg.Hosted() {
		out[s.ID] = agentService{
			ID: s.ID, Service: s.Name, Tags: orEmpty(s.Tags), Meta: s.Meta,
			Port: int(s.Port), Address: s.Address, Weights: modelWeights(s.Weights),
		}
	}
	h.writeJSON(w, r, out)
}

func (h *Handler) agentSelf(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, r, agentSelf{
		Config: agentSelfConfig{Datacenter: h.info.Datacenter, NodeName: h.info.Name, Version: h.info.Version},
		Member: agentMember{Name: h.info.Name, Addr: h.info.Address, Port: portNum(serverPort)},
	})
}

func (h *Handler) catalogServices(w http.ResponseWriter, r *http.Request) {
	dc := r.URL.Query().Get("dc")
	res, err := h.reg.Resolve(r.Context(), model.Query{Datacenter: dc})
	if err != nil {
		h.fail(w, err)
		return
	}
	out := map[string][]string{}
	for _, e := range res.Entries {
		out[e.Service.Name] = mergeTags(out[e.Service.Name], e.Service.Tags)
	}
	h.writeJSON(w, r, out)
}

func (h *Handler) catalogService(w http.ResponseWriter, r *http.Request) {
	q := h.query(r)
	res, err := h.reg.Resolve(r.Context(), q)
	if err != nil {
		h.fail(w, err)
		return
	}
	out := make([]catalogService, 0, len(res.Entries))
	for _, e := range res.Entries {
		out = append(out, h.toCatalog(e))
	}
	h.writeJSON(w, r, out)
}

func (h *Handler) healthService(w http.ResponseWriter, r *http.Request) {
	q := h.query(r)
	res, err := h.reg.Resolve(r.Context(), q)
	if err != nil {
		h.fail(w, err)
		return
	}
	entries := res.Entries
	if r.URL.Query().Has("passing") {
		entries = health.Filter(entries, health.FilterOptions{OnlyPassing: true})
	}
	out := make([]healthServiceEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, h.toHealth(e))
	}
	h.writeJSON(w, r, out)
}

func (h *Handler) catalogNodes(w http.ResponseWriter, r *http.Request) {
	res, err := h.reg.Resolve(r.Context(), model.Query{})
	if err != nil {
		h.fail(w, err)
		return
	}
	seen := map[string]struct{}{}
	out := []node{}
	for _, e := range res.Entries {
		if _, ok := seen[e.Node.ID]; ok {
			continue
		}
		seen[e.Node.ID] = struct{}{}
		out = append(out, toNode(e.Node))
	}
	h.writeJSON(w, r, out)
}

func (h *Handler) catalogDatacenters(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, r, []string{h.info.Datacenter})
}

func (h *Handler) statusLeader(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, r, h.leaderAddr())
}

func (h *Handler) statusPeers(w http.ResponseWriter, r *http.Request) {
	peers := []string{}
	for _, s := range h.info.Seeds {
		peers = append(peers, hostPortWithServerPort(s))
	}
	if len(peers) == 0 {
		peers = append(peers, h.leaderAddr())
	}
	h.writeJSON(w, r, peers)
}

// --- helpers ---

func (h *Handler) query(r *http.Request) model.Query {
	q := model.Query{
		Name:       r.PathValue("service"),
		Datacenter: r.URL.Query().Get("dc"),
	}
	if tags, ok := r.URL.Query()["tag"]; ok {
		q.Tags = tags
	}
	return q
}

func (h *Handler) node() model.Node {
	return model.Node{ID: h.info.ID, Name: h.info.Name, Address: h.info.Address, Datacenter: h.info.Datacenter}
}

func (h *Handler) leaderAddr() string {
	addr := h.info.Address
	if addr == "" {
		addr = "127.0.0.1"
	}
	return addr + ":" + serverPort
}

func (h *Handler) toCatalog(e model.ServiceEntry) catalogService {
	return catalogService{
		ID: e.Node.ID, Node: nodeName(e.Node), Address: e.Node.Address, Datacenter: e.Node.Datacenter,
		NodeMeta: e.Node.Meta, TaggedAddresses: e.Node.TaggedAddresses,
		ServiceID: e.Service.ID, ServiceName: e.Service.Name,
		ServiceAddress: serviceAddr(e), ServicePort: int(e.Service.Port),
		ServiceTags: orEmpty(e.Service.Tags), ServiceMeta: e.Service.Meta,
		ServiceWeights: modelWeights(e.Service.Weights),
		CreateIndex:    e.CreateIndex, ModifyIndex: e.ModifyIndex,
	}
}

func (h *Handler) toHealth(e model.ServiceEntry) healthServiceEntry {
	return healthServiceEntry{
		Node: toNode(e.Node),
		Service: healthService{
			ID: e.Service.ID, Service: e.Service.Name, Tags: orEmpty(e.Service.Tags),
			Address: serviceAddr(e), Meta: e.Service.Meta, Port: int(e.Service.Port),
			Weights: modelWeights(e.Service.Weights),
		},
		Checks: synthChecks(e),
	}
}

// synthChecks builds the serfHealth node check, the per-service check, and a
// _service_maintenance critical check when the instance is in maintenance.
func synthChecks(e model.ServiceEntry) []healthCheck {
	nodeStatus := "passing"
	svcStatus := healthStatus(e.Health)
	if e.Health == model.HealthCritical {
		// An expired (critical) lease also fails the node liveness check.
		nodeStatus = "critical"
	}
	checks := []healthCheck{
		{Node: nodeName(e.Node), CheckID: "serfHealth", Name: "Serf Health Status", Status: nodeStatus},
		{
			Node: nodeName(e.Node), CheckID: "service:" + e.Service.ID, Name: "Service '" + e.Service.Name + "' check",
			Status: svcStatus, ServiceID: e.Service.ID, ServiceName: e.Service.Name,
		},
	}
	if e.Maintenance {
		checks = append(checks, healthCheck{
			Node: nodeName(e.Node), CheckID: "_service_maintenance:" + e.Service.ID, Name: "Service Maintenance Mode",
			Status: "critical", ServiceID: e.Service.ID, ServiceName: e.Service.Name,
		})
	}
	return checks
}

func (h *Handler) writeJSON(w http.ResponseWriter, _ *http.Request, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Consul-Index", strconv.FormatUint(h.indexValue(), 10))
	w.Header().Set("X-Consul-KnownLeader", "true")
	w.Header().Set("X-Consul-LastContact", "0")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.log.Warn("consul: encode response", "error", err)
	}
}

func (h *Handler) indexValue() uint64 {
	if i := h.index(); i > 0 {
		return i
	}
	return 1
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	h.log.Warn("consul: request failed", "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func toNode(n model.Node) node {
	return node{ID: n.ID, Node: nodeName(n), Address: n.Address, Datacenter: n.Datacenter, Meta: n.Meta, TaggedAddresses: n.TaggedAddresses}
}

func nodeName(n model.Node) string {
	if n.Name != "" {
		return n.Name
	}
	return n.ID
}

func serviceAddr(e model.ServiceEntry) string {
	if e.Service.Address != "" {
		return e.Service.Address
	}
	return e.Node.Address
}

func healthStatus(s model.HealthState) string {
	switch s {
	case model.HealthWarning:
		return "warning"
	case model.HealthCritical:
		return "critical"
	default:
		return "passing"
	}
}

func modelWeights(w model.Weights) weights {
	w = w.OrDefault()
	return weights{Passing: w.Passing, Warning: w.Warning}
}

func weightsToModel(w *weights) model.Weights {
	if w == nil {
		return model.Weights{}
	}
	return model.Weights{Passing: w.Passing, Warning: w.Warning}
}

func ttlFromCheck(c *checkInput) time.Duration {
	if c == nil || c.TTL == "" {
		return 0
	}
	d, err := time.ParseDuration(c.TTL)
	if err != nil || d < 0 {
		return 0
	}
	return d
}

func mergeTags(into, add []string) []string {
	seen := map[string]struct{}{}
	for _, t := range into {
		seen[t] = struct{}{}
	}
	for _, t := range add {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			into = append(into, t)
		}
	}
	sort.Strings(into)
	return orEmpty(into)
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func clampPort(p int) uint16 {
	switch {
	case p < 0:
		return 0
	case p > 65535:
		return 65535
	default:
		return uint16(p) //nolint:gosec // bounded to [0,65535] above
	}
}

func portNum(s string) int { n, _ := strconv.Atoi(s); return n }

func hostPortWithServerPort(seed string) string {
	host := seed
	if i := lastColon(seed); i >= 0 {
		host = seed[:i]
	}
	return host + ":" + serverPort
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}
