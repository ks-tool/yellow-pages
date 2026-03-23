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

package node

import (
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/proto/gen"
)

// AgentRecord holds the information about a registered agent.
type AgentRecord struct {
	Info     *discovery.Agent
	Services []*discovery.Service
	LastSeen time.Time
	TTL      time.Duration
}

// Registry maintains the list of agents and their services.
type Registry struct {
	mu                sync.RWMutex
	agentsByID        map[string]*AgentRecord
	serviceIndex      map[string][]*AgentRecord  // service name -> agents
	serviceAgentIndex map[string]map[string]bool // service name -> agent IDs for fast lookup
	ttl               time.Duration
}

// NewRegistry creates a new registry with the default TTL.
func NewRegistry(defaultTTL time.Duration) *Registry {
	return &Registry{
		agentsByID:        make(map[string]*AgentRecord),
		serviceIndex:      make(map[string][]*AgentRecord),
		serviceAgentIndex: make(map[string]map[string]bool),
		ttl:               defaultTTL,
	}
}

// Register adds or updates an agent.
// ttlSeconds is the TTL in seconds. If lastSeenNano > 0, it is used as the last seen time.
func (r *Registry) Register(agent *discovery.Agent, services []*discovery.Service, ttlSeconds int64, lastSeenNano int64) {
	now := time.Now()
	if lastSeenNano > 0 {
		now = time.Unix(0, lastSeenNano)
	}
	ttlDur := time.Duration(ttlSeconds) * time.Second

	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.agentsByID[agent.Id]; ok {
		r.removeFromIndex(old)
	}

	rec := &AgentRecord{
		Info:     agent,
		Services: services,
		LastSeen: now,
		TTL:      ttlDur,
	}
	r.agentsByID[agent.Id] = rec
	r.addToIndex(rec)
}

// Heartbeat updates the last seen time and optionally the services of an agent.
// Returns true if the agent exists, false otherwise.
func (r *Registry) Heartbeat(agentID string, services []*discovery.Service, ttlSeconds int64, lastSeenNano int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.agentsByID[agentID]
	if !ok {
		return false
	}
	now := time.Now()
	if lastSeenNano > 0 {
		now = time.Unix(0, lastSeenNano)
	}
	rec.LastSeen = now
	if ttlSeconds > 0 {
		rec.TTL = time.Duration(ttlSeconds) * time.Second
	}
	if services != nil {
		r.removeFromIndex(rec)
		rec.Services = services
		r.addToIndex(rec)
	}
	return true
}

// Deregister removes an agent from the registry.
func (r *Registry) Deregister(agentID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.agentsByID[agentID]; ok {
		r.removeFromIndex(rec)
		delete(r.agentsByID, agentID)
	}
}

// GetAgentsForService returns agents that provide the given service.
// filters are applied to the service metadata (key‑value). Only agents that have
// at least one service matching the name and all filters are returned.
// The returned agents are deduplicated by agent ID, keeping the one with the most recent LastSeen.
func (r *Registry) GetAgentsForService(serviceName string, filters map[string]string) []*AgentRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	agentsMap := make(map[string]*AgentRecord) // deduplicate by agent id, keep newest
	for _, rec := range r.serviceIndex[serviceName] {
		if time.Since(rec.LastSeen) > rec.TTL {
			continue
		}
		if !matchFilters(rec, serviceName, filters) {
			continue
		}
		if existing, ok := agentsMap[rec.Info.Id]; !ok || rec.LastSeen.After(existing.LastSeen) {
			agentsMap[rec.Info.Id] = rec
		}
	}
	agents := make([]*AgentRecord, 0, len(agentsMap))
	for _, rec := range agentsMap {
		agents = append(agents, rec)
	}
	return agents
}

// Cleanup removes expired agents.
func (r *Registry) Cleanup() {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, rec := range r.agentsByID {
		if now.Sub(rec.LastSeen) > rec.TTL {
			r.removeFromIndex(rec)
			delete(r.agentsByID, id)
		}
	}
}

// addToIndex adds an agent record to the service index.
func (r *Registry) addToIndex(rec *AgentRecord) {
	for _, svc := range rec.Services {
		r.serviceIndex[svc.Name] = append(r.serviceIndex[svc.Name], rec)
		if r.serviceAgentIndex[svc.Name] == nil {
			r.serviceAgentIndex[svc.Name] = make(map[string]bool)
		}
		r.serviceAgentIndex[svc.Name][rec.Info.Id] = true
	}
}

// removeFromIndex removes an agent record from the service index.
func (r *Registry) removeFromIndex(rec *AgentRecord) {
	for _, svc := range rec.Services {
		slice := r.serviceIndex[svc.Name]
		// Rebuild slice without this agent (efficient for typical small sizes)
		newSlice := make([]*AgentRecord, 0, len(slice)-1)
		for _, a := range slice {
			if a.Info.Id != rec.Info.Id {
				newSlice = append(newSlice, a)
			}
		}
		if len(newSlice) == 0 {
			delete(r.serviceIndex, svc.Name)
			delete(r.serviceAgentIndex, svc.Name)
		} else {
			r.serviceIndex[svc.Name] = newSlice
			delete(r.serviceAgentIndex[svc.Name], rec.Info.Id)
		}
	}
}

// matchFilters checks whether the agent record has a service with the given name
// and all filters match the service metadata.
func matchFilters(rec *AgentRecord, serviceName string, filters map[string]string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, svc := range rec.Services {
		if svc.Name == serviceName {
			ok := true
			for k, v := range filters {
				if svc.Metadata[k] != v {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
	}
	return false
}
