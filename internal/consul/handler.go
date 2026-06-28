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
	"strings"
	"time"

	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/watch"
)

// Registry is the cluster surface the Consul adapter projects. Resolve returns
// the merged entries and the age of the cache entry they came from. The check
// bridge maps Consul TTL checks to per-service lease operations.
type Registry interface {
	RegisterServices(ctx context.Context, reg model.Registration) error
	RemoveService(ctx context.Context, serviceID string) error
	Resolve(ctx context.Context, q model.Query, mode model.Consistency) (model.LookupResult, time.Duration, error)
	Hosted() []model.ServiceInstance
	// RenewService refreshes a service's lease (check pass/warn/update).
	RenewService(ctx context.Context, serviceID string) error
	// FailService forces a service critical (check fail).
	FailService(ctx context.Context, serviceID string) error
	// SetMaintenance toggles a service's maintenance drain flag.
	SetMaintenance(ctx context.Context, serviceID string, enabled bool) error
}

// NodeInfo is this agent's static identity.
type NodeInfo struct {
	ID         string
	Name       string
	Datacenter string
	Address    string
	Version    string
	Seeds      []string
}

const (
	serverPort        = "8300"
	defaultWait       = 5 * time.Minute
	maxWait           = 10 * time.Minute
	defaultMaxWaiters = 256
)

// Options configures the Consul HTTP handler.
type Options struct {
	Registry Registry
	Info     NodeInfo
	// Watcher enables blocking queries and the per-query X-Consul-Index.
	Watcher *watch.Watcher
	// Identity resolves ACL tokens; Authz enforces write ownership when set to
	// enforce. Both optional (acl.mode=disabled/allow accept any token).
	Identity cred.Identity
	Authz    *cred.Authorizer
	// MaxWaiters caps concurrent blocking queries (0 = default 256).
	MaxWaiters int
	Log        *slog.Logger
}

// Handler implements the Consul-compatible HTTP API.
type Handler struct {
	reg      Registry
	info     NodeInfo
	watcher  *watch.Watcher
	identity cred.Identity
	authz    *cred.Authorizer
	waiters  chan struct{}
	log      *slog.Logger
}

// NewHandler builds the Consul HTTP handler from opts.
func NewHandler(opts Options) http.Handler {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.MaxWaiters <= 0 {
		opts.MaxWaiters = defaultMaxWaiters
	}
	h := &Handler{
		reg:      opts.Registry,
		info:     opts.Info,
		watcher:  opts.Watcher,
		identity: opts.Identity,
		authz:    opts.Authz,
		waiters:  make(chan struct{}, opts.MaxWaiters),
		log:      opts.Log,
	}

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
	mux.HandleFunc("GET /v1/health/checks/{service}", h.healthChecks)
	mux.HandleFunc("GET /v1/health/state/{state}", h.healthState)
	mux.HandleFunc("GET /v1/status/leader", h.statusLeader)
	mux.HandleFunc("GET /v1/status/peers", h.statusPeers)

	// Check bridge: Consul TTL checks map to per-service lease operations.
	mux.HandleFunc("PUT /v1/agent/check/register", h.checkAccept)
	mux.HandleFunc("PUT /v1/agent/check/deregister/{checkID}", h.checkAccept)
	mux.HandleFunc("PUT /v1/agent/check/pass/{checkID}", h.checkUpdate)
	mux.HandleFunc("PUT /v1/agent/check/warn/{checkID}", h.checkUpdate)
	mux.HandleFunc("PUT /v1/agent/check/fail/{checkID}", h.checkUpdate)
	mux.HandleFunc("PUT /v1/agent/check/update/{checkID}", h.checkUpdate)
	mux.HandleFunc("GET /v1/agent/checks", h.agentChecks)
	mux.HandleFunc("PUT /v1/agent/service/maintenance/{serviceID}", h.serviceMaintenance)
	mux.HandleFunc("GET /v1/agent/health/service/name/{name}", h.agentHealthByName)
	return mux
}

// --- write paths (authz-enforced) ---

func (h *Handler) registerService(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	var in registerInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil { // lenient: unknown fields ignored
		http.Error(w, "invalid register body", http.StatusBadRequest)
		return
	}
	if in.Name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	reg := model.Registration{Node: h.node(), Services: []model.ServiceInstance{inputToService(in)}, Generation: 1}
	if err := h.reg.RegisterServices(r.Context(), reg); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) deregisterService(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	if err := h.reg.RemoveService(r.Context(), r.PathValue("serviceID")); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- read paths (blocking, filter, consistency) ---

func (h *Handler) catalogService(w http.ResponseWriter, r *http.Request) {
	entries, idx, mode, age, ok := h.read(w, r)
	if !ok {
		return
	}
	out := make([]catalogService, 0, len(entries))
	for _, e := range entries {
		out = append(out, h.toCatalog(e))
	}
	h.writeJSON(w, idx, mode, age, out)
}

func (h *Handler) healthService(w http.ResponseWriter, r *http.Request) {
	entries, idx, mode, age, ok := h.read(w, r)
	if !ok {
		return
	}
	if r.URL.Query().Has("passing") {
		entries = health.Filter(entries, health.FilterOptions{OnlyPassing: true})
	}
	out := make([]healthServiceEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, h.toHealth(e))
	}
	h.writeJSON(w, idx, mode, age, out)
}

func (h *Handler) catalogServices(w http.ResponseWriter, r *http.Request) {
	entries, idx, mode, age, ok := h.read(w, r)
	if !ok {
		return
	}
	out := map[string][]string{}
	for _, e := range entries {
		out[e.Service.Name] = mergeTags(out[e.Service.Name], e.Service.Tags)
	}
	h.writeJSON(w, idx, mode, age, out)
}

func (h *Handler) catalogNodes(w http.ResponseWriter, r *http.Request) {
	entries, idx, mode, age, ok := h.read(w, r)
	if !ok {
		return
	}
	seen := map[string]struct{}{}
	out := []node{}
	for _, e := range entries {
		if _, dup := seen[e.Node.ID]; dup {
			continue
		}
		seen[e.Node.ID] = struct{}{}
		out = append(out, toNode(e.Node))
	}
	h.writeJSON(w, idx, mode, age, out)
}

// read performs the common blocking + filtered + consistency-aware resolution.
// It returns the entries, the per-query index, the read mode, the cache age and
// ok=false when a response (error / 429) was already written.
func (h *Handler) read(w http.ResponseWriter, r *http.Request) ([]model.ServiceEntry, uint64, model.Consistency, time.Duration, bool) {
	q := h.query(r)
	mode := readMode(r)

	filter, err := compileFilter(r)
	if err != nil {
		http.Error(w, "invalid filter: "+err.Error(), http.StatusBadRequest)
		return nil, 0, mode, 0, false
	}

	minIndex, wait := blockParams(r)
	if minIndex > 0 && h.watcher != nil {
		if !h.acquireWaiter() {
			http.Error(w, "too many blocking queries", http.StatusTooManyRequests)
			return nil, 0, mode, 0, false
		}
		h.watcher.WaitForChange(r.Context(), q, minIndex, wait)
		h.releaseWaiter()
	}

	res, age, err := h.reg.Resolve(r.Context(), q, mode)
	if err != nil {
		h.fail(w, err)
		return nil, 0, mode, 0, false
	}
	entries := res.Entries
	if filter != nil {
		entries = applyFilter(entries, filter)
	}
	return entries, h.indexFor(q), mode, age, true
}

func (h *Handler) agentServices(w http.ResponseWriter, _ *http.Request) {
	out := make(map[string]agentService)
	for _, s := range h.reg.Hosted() {
		out[s.ID] = agentService{
			ID: s.ID, Service: s.Name, Tags: orEmpty(s.Tags), Meta: s.Meta,
			Port: int(s.Port), Address: s.Address, Weights: modelWeights(s.Weights),
		}
	}
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, out)
}

func (h *Handler) agentSelf(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, agentSelf{
		Config: agentSelfConfig{Datacenter: h.info.Datacenter, NodeName: h.info.Name, Version: h.info.Version},
		Member: agentMember{Name: h.info.Name, Addr: h.info.Address, Port: portNum(serverPort)},
	})
}

func (h *Handler) catalogDatacenters(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, []string{h.info.Datacenter})
}

func (h *Handler) statusLeader(w http.ResponseWriter, _ *http.Request) {
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, h.leaderAddr())
}

func (h *Handler) statusPeers(w http.ResponseWriter, _ *http.Request) {
	peers := []string{}
	for _, s := range h.info.Seeds {
		peers = append(peers, hostPortWithServerPort(s))
	}
	if len(peers) == 0 {
		peers = append(peers, h.leaderAddr())
	}
	h.writeJSON(w, h.indexFor(model.Query{}), model.ConsistencyDefault, 0, peers)
}

// --- helpers ---

func (h *Handler) acquireWaiter() bool {
	select {
	case h.waiters <- struct{}{}:
		return true
	default:
		return false
	}
}

func (h *Handler) releaseWaiter() { <-h.waiters }

func (h *Handler) authorizeWrite(w http.ResponseWriter, r *http.Request) bool {
	if h.authz == nil || !h.authz.Enforcing() {
		return true // disabled/allow: accept the token, do not enforce
	}
	p := h.identity.PrincipalForToken(tokenFromHTTP(r))
	if err := h.authz.Authorize(p, h.info.ID); err != nil {
		http.Error(w, "permission denied", http.StatusForbidden)
		return false
	}
	return true
}

func (h *Handler) indexFor(q model.Query) uint64 {
	if h.watcher != nil {
		return h.watcher.CurrentIndex(q)
	}
	return 1
}

func (h *Handler) query(r *http.Request) model.Query {
	q := model.Query{Name: r.PathValue("service"), Datacenter: r.URL.Query().Get("dc")}
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

func (h *Handler) writeJSON(w http.ResponseWriter, index uint64, mode model.Consistency, age time.Duration, v any) {
	if index < 1 {
		index = 1
	}
	lastContact := "0"
	if mode == model.ConsistencyStale {
		lastContact = strconv.FormatInt(age.Milliseconds(), 10)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Consul-Index", strconv.FormatUint(index, 10))
	w.Header().Set("X-Consul-KnownLeader", "true")
	w.Header().Set("X-Consul-LastContact", lastContact)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.log.Warn("consul: encode response", "error", err)
	}
}

func (h *Handler) fail(w http.ResponseWriter, err error) {
	h.log.Warn("consul: request failed", "error", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
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

func synthChecks(e model.ServiceEntry) []healthCheck {
	nodeStatus := "passing"
	if e.Health == model.HealthCritical {
		nodeStatus = "critical"
	}
	checks := []healthCheck{
		{Node: nodeName(e.Node), CheckID: "serfHealth", Name: "Serf Health Status", Status: nodeStatus},
		{
			Node: nodeName(e.Node), CheckID: "service:" + e.Service.ID, Name: "Service '" + e.Service.Name + "' check",
			Status: healthStatus(e.Health), ServiceID: e.Service.ID, ServiceName: e.Service.Name,
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

// --- request parsing ---

func readMode(r *http.Request) model.Consistency {
	q := r.URL.Query()
	switch {
	case q.Has("consistent"):
		return model.ConsistencyConsistent
	case q.Has("stale"):
		return model.ConsistencyStale
	default:
		return model.ConsistencyDefault
	}
}

func blockParams(r *http.Request) (minIndex uint64, wait time.Duration) {
	q := r.URL.Query()
	if v := q.Get("index"); v != "" {
		minIndex, _ = strconv.ParseUint(v, 10, 64)
	}
	wait = defaultWait
	if v := q.Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			wait = d
		}
	}
	if wait > maxWait {
		wait = maxWait
	}
	// Consul adds wait/16 jitter; we clamp deterministically (jitter would only
	// spread load and is unnecessary for correctness).
	return minIndex, wait
}

func compileFilter(r *http.Request) (filterExpr, error) {
	f := r.URL.Query().Get("filter")
	if strings.TrimSpace(f) == "" {
		return nil, nil
	}
	return parseFilter(f)
}

func applyFilter(entries []model.ServiceEntry, f filterExpr) []model.ServiceEntry {
	out := make([]model.ServiceEntry, 0, len(entries))
	for _, e := range entries {
		if f(viewOf(e)) {
			out = append(out, e)
		}
	}
	return out
}

func tokenFromHTTP(r *http.Request) string {
	if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(a, "Bearer "))
	}
	if t := r.Header.Get("X-Consul-Token"); t != "" {
		return t
	}
	return r.URL.Query().Get("token")
}

// --- model -> consul render helpers ---

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

// inputToService converts a Consul register/service-def input to a domain service.
func inputToService(in registerInput) model.ServiceInstance {
	id := in.ID
	if id == "" {
		id = in.Name
	}
	return model.ServiceInstance{
		ID: id, Name: in.Name, Address: in.Address, Port: clampPort(in.Port),
		Tags: in.Tags, Meta: in.Meta, Weights: weightsToModel(in.Weights), TTL: in.firstTTL(),
	}
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
	if d, err := time.ParseDuration(c.TTL); err == nil && d >= 0 {
		return d
	}
	return 0
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
	if i := strings.LastIndexByte(seed, ':'); i >= 0 {
		host = seed[:i]
	}
	return host + ":" + serverPort
}
