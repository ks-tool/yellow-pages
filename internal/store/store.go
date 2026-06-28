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

// Package store is the registry held by seeds: the single contract for reading
// and mutating the cluster's nodes and services. The in-memory implementation
// keys the registry by agent id, holds each service as a sub-record with its OWN
// ttl/last_seen lease, stamps last_seen server-side (client clocks are not
// trusted), treats the client-supplied generation as the data version, and keeps
// a monotonic registry index (resumable across restarts) plus per-entry
// CreateIndex/ModifyIndex. Expired records linger for a grace window — visible as
// critical — before GC reclaims them. Visibility filtering and cross-seed merge
// are NOT done here; they live in internal/health so every surface shares them.
package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
)

// Sentinel errors returned by the store. The native gRPC server maps these to
// status codes (NotFound, InvalidArgument) at the boundary.
var (
	// ErrNotFound is returned when a node or service does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrInvalid is returned when a registration is malformed (missing node id
	// or a service without a name).
	ErrInvalid = errors.New("store: invalid registration")
	// ErrCapacity is returned when a new registration would exceed MaxServices.
	ErrCapacity = errors.New("store: registry at capacity")
)

// Store is the registry contract. Reads (Lookup) return raw matching entries
// with health derived from the lease; cross-seed merge and ?passing-style
// visibility filtering are applied by the caller through internal/health.
type Store interface {
	// Register creates or updates a node and its services. last_seen is stamped
	// server-side; generation is taken as-is (the client-supplied data version).
	// It is idempotent: re-registering identical data only refreshes the lease.
	Register(reg model.Registration) error
	// Renew refreshes the per-service lease of the given node without changing
	// generation. An empty serviceIDs renews all of the node's services.
	Renew(nodeID string, serviceIDs []string) error
	// Deregister removes a node and all its services.
	Deregister(nodeID string) error
	// DeregisterService removes a single service from a node.
	DeregisterService(nodeID, serviceID string) error
	// SetMaintenance toggles the drain flag of a service: it stays visible with
	// a maintenance marker but is excluded from ?passing and DNS.
	SetMaintenance(nodeID, serviceID string, enabled bool) error
	// Fail forces a service critical (Consul check/fail), kept visible; a Renew
	// clears it.
	Fail(nodeID, serviceID string) error
	// Lookup returns the entries whose service Name matches the query exactly,
	// filtered by datacenter and tags, with health derived from the lease.
	Lookup(q model.Query) model.LookupResult
	// GC reconciles time-based health transitions (advancing the index when a
	// service expires or revives) and removes records past their grace window.
	// It returns the number of removed services.
	GC() int
	// Index returns the current registry high-watermark.
	Index() uint64
	// Size returns the number of service instances held.
	Size() int
}

// Options configures a Memory store.
type Options struct {
	// Clock is the time seam; defaults to clock.System().
	Clock clock.Clock
	// DefaultTTL applies when a registered service omits its ttl. Default 30s.
	DefaultTTL time.Duration
	// MinTTL / MaxTTL clamp the effective per-service ttl (0 disables a bound).
	MinTTL time.Duration
	MaxTTL time.Duration
	// GracePeriod keeps an expired service visible as critical before GC removes
	// it (Consul's DeregisterCriticalServiceAfter analogue). Default 0.
	GracePeriod time.Duration
	// StartIndex resumes the monotonic index from a persisted high-watermark so
	// the index never regresses across a restart (the "epoch"). Default 0.
	StartIndex uint64
	// MaxServices caps the number of service instances (0 = unlimited). A new
	// registration past the cap is rejected with ErrCapacity (write DoS guard).
	MaxServices int
	// OnChange, when set, receives the change events of each mutation (and GC)
	// after the store lock is released. It is the seam the Watcher builds on (M8).
	OnChange func([]model.ChangeEvent)
}

const defaultTTL = 30 * time.Second

// service is one stored service instance with its lease and registry indexes.
type service struct {
	def         model.ServiceInstance // Address/Port/Tags/Meta/Weights/TTL/Generation/LastSeen
	maintenance bool
	failed      bool              // a Consul check/fail: forced critical, still visible
	lastState   model.HealthState // last reconciled lease state (for transition detection)
	createIndex uint64
	modifyIndex uint64
}

// node is an agent and its services, keyed by service id.
type node struct {
	meta     model.Node
	services map[string]*service
}

// Memory is the in-memory Store implementation.
type Memory struct {
	mu     sync.Mutex
	clk    clock.Clock
	opts   Options
	nodes  map[string]*node               // agentId -> node
	byName map[string]map[svcRef]struct{} // serviceName -> set of (node,service)
	index  uint64
}

type svcRef struct{ node, service string }

// compile-time assertion that Memory satisfies the Store contract.
var _ Store = (*Memory)(nil)

// NewMemory builds an empty in-memory store from opts.
func NewMemory(opts Options) *Memory {
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = defaultTTL
	}
	return &Memory{
		clk:    opts.Clock,
		opts:   opts,
		nodes:  make(map[string]*node),
		byName: make(map[string]map[svcRef]struct{}),
		index:  opts.StartIndex,
	}
}

// Index returns the current registry high-watermark.
func (m *Memory) Index() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.index
}

// Size returns the number of service instances held.
func (m *Memory) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sizeLocked()
}

func (m *Memory) sizeLocked() int {
	n := 0
	for _, node := range m.nodes {
		n += len(node.services)
	}
	return n
}

// next advances and returns the monotonic index. Callers hold m.mu.
func (m *Memory) next() uint64 {
	m.index++
	return m.index
}

func (m *Memory) clampTTL(d time.Duration) time.Duration {
	if d <= 0 {
		d = m.opts.DefaultTTL
	}
	if m.opts.MinTTL > 0 && d < m.opts.MinTTL {
		d = m.opts.MinTTL
	}
	if m.opts.MaxTTL > 0 && d > m.opts.MaxTTL {
		d = m.opts.MaxTTL
	}
	return d
}

// Register creates or updates the node and its services.
func (m *Memory) Register(reg model.Registration) error {
	if reg.Node.ID == "" {
		return ErrInvalid
	}
	for i := range reg.Services {
		if reg.Services[i].Name == "" {
			return ErrInvalid
		}
	}

	m.mu.Lock()
	now := m.clk.Now()
	size := m.sizeLocked()

	n, ok := m.nodes[reg.Node.ID]
	if !ok {
		n = &node{meta: reg.Node, services: make(map[string]*service)}
		m.nodes[reg.Node.ID] = n
	} else {
		n.meta = reg.Node // node-scoped attributes are mutable data
	}

	var events []model.ChangeEvent
	for _, in := range reg.Services {
		def := in
		if def.ID == "" {
			def.ID = def.Name // Consul: service id defaults to name
		}
		def.TTL = m.clampTTL(def.TTL)
		def.LastSeen = now
		def.Generation = reg.Generation

		svc, exists := n.services[def.ID]
		switch {
		case !exists:
			if m.opts.MaxServices > 0 && size >= m.opts.MaxServices {
				m.mu.Unlock()
				m.emit(events)
				return ErrCapacity
			}
			size++
			idx := m.next()
			svc = &service{def: def, lastState: model.HealthPassing, createIndex: idx, modifyIndex: idx}
			n.services[def.ID] = svc
			m.indexName(reg.Node.ID, def)
			events = append(events, m.putEvent(n.meta, svc))
		case changed(svc.def, def):
			// A definitional change (endpoint/tags/meta/weights/ttl/generation)
			// advances the index; the lease is refreshed and the state reset.
			oldName := svc.def.Name
			svc.def = def
			svc.lastState = model.HealthPassing
			svc.modifyIndex = m.next()
			if oldName != def.Name {
				m.unindexName(reg.Node.ID, oldName, def.ID)
				m.indexName(reg.Node.ID, def)
			}
			events = append(events, m.putEvent(n.meta, svc))
		default:
			// Identical data: a pure lease refresh, like Renew. No index change,
			// no event; GC reconciles any liveness transition.
			svc.def.LastSeen = now
		}
	}

	m.mu.Unlock()
	m.emit(events)
	return nil
}

// changed reports whether the stored definition differs from the incoming one in
// any field that should advance the index. LastSeen is excluded (lease-only).
func changed(a, b model.ServiceInstance) bool {
	if a.Name != b.Name || a.Address != b.Address || a.Port != b.Port ||
		a.TTL != b.TTL || a.Generation != b.Generation || a.Weights != b.Weights {
		return true
	}
	return !equalStrings(a.Tags, b.Tags) || !equalMap(a.Meta, b.Meta)
}

// Renew refreshes the per-service lease without changing generation or the index.
func (m *Memory) Renew(nodeID string, serviceIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	n, ok := m.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	now := m.clk.Now()
	if len(serviceIDs) == 0 {
		for _, svc := range n.services {
			svc.def.LastSeen = now
			svc.failed = false // a pass/renew revives a failed check
		}
		return nil
	}
	for _, id := range serviceIDs {
		svc, ok := n.services[id]
		if !ok {
			return ErrNotFound
		}
		svc.def.LastSeen = now
		svc.failed = false
	}
	return nil
}

// Fail forces a service critical (a Consul check/fail) while keeping it visible.
// A subsequent Renew (check/pass) clears it.
func (m *Memory) Fail(nodeID, serviceID string) error {
	m.mu.Lock()
	n, ok := m.nodes[nodeID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	svc, ok := n.services[serviceID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	if svc.failed {
		m.mu.Unlock()
		return nil
	}
	svc.failed = true
	svc.modifyIndex = m.next()
	event := m.putEvent(n.meta, svc)
	m.mu.Unlock()
	m.emit([]model.ChangeEvent{event})
	return nil
}

// Deregister removes a node and all of its services.
func (m *Memory) Deregister(nodeID string) error {
	m.mu.Lock()
	n, ok := m.nodes[nodeID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	var events []model.ChangeEvent
	for id, svc := range n.services {
		svc.modifyIndex = m.next()
		events = append(events, m.deleteEvent(n.meta, svc))
		m.unindexName(nodeID, svc.def.Name, id)
	}
	delete(m.nodes, nodeID)
	m.mu.Unlock()
	m.emit(events)
	return nil
}

// DeregisterService removes a single service from a node, dropping the node when
// it becomes empty.
func (m *Memory) DeregisterService(nodeID, serviceID string) error {
	m.mu.Lock()
	n, ok := m.nodes[nodeID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	svc, ok := n.services[serviceID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	svc.modifyIndex = m.next()
	event := m.deleteEvent(n.meta, svc)
	delete(n.services, serviceID)
	m.unindexName(nodeID, svc.def.Name, serviceID)
	if len(n.services) == 0 {
		delete(m.nodes, nodeID)
	}
	m.mu.Unlock()
	m.emit([]model.ChangeEvent{event})
	return nil
}

// SetMaintenance toggles a service's drain flag, advancing the index.
func (m *Memory) SetMaintenance(nodeID, serviceID string, enabled bool) error {
	m.mu.Lock()
	n, ok := m.nodes[nodeID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	svc, ok := n.services[serviceID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	if svc.maintenance == enabled {
		m.mu.Unlock()
		return nil // idempotent: no change, no index movement
	}
	svc.maintenance = enabled
	svc.modifyIndex = m.next()
	event := m.putEvent(n.meta, svc)
	m.mu.Unlock()
	m.emit([]model.ChangeEvent{event})
	return nil
}

// Lookup returns matching entries with lease-derived health. An empty Query.Name
// lists every service (the catalog/list path). Past-grace records are omitted
// even before GC physically removes them.
func (m *Memory) Lookup(q model.Query) model.LookupResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clk.Now()
	out := []model.ServiceEntry{}
	visit := func(n *node, svc *service) {
		if q.Datacenter != "" && n.meta.Datacenter != q.Datacenter {
			return
		}
		if !svc.def.MatchesTags(q.Tags) {
			return
		}
		state, expired := m.lease(svc, now)
		if expired {
			return // past grace: invisible, awaiting GC
		}
		out = append(out, m.entry(n.meta, svc, state))
	}

	if q.Name == "" {
		for _, n := range m.nodes {
			for _, svc := range n.services {
				visit(n, svc)
			}
		}
	} else {
		for ref := range m.byName[q.Name] {
			if n := m.nodes[ref.node]; n != nil {
				if svc := n.services[ref.service]; svc != nil {
					visit(n, svc)
				}
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Node.ID != out[j].Node.ID {
			return out[i].Node.ID < out[j].Node.ID
		}
		return out[i].Service.ID < out[j].Service.ID
	})
	return model.LookupResult{Entries: out, Index: m.index}
}

// GC reconciles lease transitions and reaps past-grace records. Callers run it
// periodically (the GC-loop Component, M6).
func (m *Memory) GC() int {
	m.mu.Lock()
	now := m.clk.Now()
	var events []model.ChangeEvent
	removed := 0
	for nodeID, n := range m.nodes {
		for id, svc := range n.services {
			state, expired := m.lease(svc, now)
			switch {
			case expired:
				svc.modifyIndex = m.next()
				events = append(events, m.deleteEvent(n.meta, svc))
				delete(n.services, id)
				m.unindexName(nodeID, svc.def.Name, id)
				removed++
			case state != svc.lastState:
				svc.lastState = state
				svc.modifyIndex = m.next()
				events = append(events, m.putEvent(n.meta, svc))
			}
		}
		if len(n.services) == 0 {
			delete(m.nodes, nodeID)
		}
	}
	m.mu.Unlock()
	m.emit(events)
	return removed
}

// lease derives the health state of svc at now and whether it is past its grace
// window (and so should be reaped). Callers hold m.mu.
func (m *Memory) lease(svc *service, now time.Time) (state model.HealthState, expired bool) {
	if svc.failed {
		return model.HealthCritical, false // check/fail: forced critical, kept visible
	}
	age := now.Sub(svc.def.LastSeen)
	switch {
	case age <= svc.def.TTL:
		return model.HealthPassing, false
	case age <= svc.def.TTL+m.opts.GracePeriod:
		return model.HealthCritical, false // suspect: recently expired, still visible
	default:
		return model.HealthCritical, true // expired past grace
	}
}

func (m *Memory) entry(meta model.Node, svc *service, state model.HealthState) model.ServiceEntry {
	return model.ServiceEntry{
		Node:        meta,
		Service:     svc.def,
		Health:      state,
		Maintenance: svc.maintenance,
		CreateIndex: svc.createIndex,
		ModifyIndex: svc.modifyIndex,
	}
}

func (m *Memory) putEvent(meta model.Node, svc *service) model.ChangeEvent {
	state, _ := m.lease(svc, m.clk.Now())
	return model.ChangeEvent{Type: model.ChangePut, Entry: m.entry(meta, svc, state), Index: svc.modifyIndex}
}

func (m *Memory) deleteEvent(meta model.Node, svc *service) model.ChangeEvent {
	return model.ChangeEvent{
		Type:  model.ChangeDelete,
		Entry: m.entry(meta, svc, model.HealthCritical),
		Index: svc.modifyIndex,
	}
}

func (m *Memory) emit(events []model.ChangeEvent) {
	if len(events) == 0 || m.opts.OnChange == nil {
		return
	}
	m.opts.OnChange(events)
}

func (m *Memory) indexName(nodeID string, def model.ServiceInstance) {
	set := m.byName[def.Name]
	if set == nil {
		set = make(map[svcRef]struct{})
		m.byName[def.Name] = set
	}
	set[svcRef{node: nodeID, service: def.ID}] = struct{}{}
}

func (m *Memory) unindexName(nodeID, name, serviceID string) {
	set := m.byName[name]
	if set == nil {
		return
	}
	delete(set, svcRef{node: nodeID, service: serviceID})
	if len(set) == 0 {
		delete(m.byName, name)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
