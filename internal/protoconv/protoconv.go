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

// Package protoconv converts between the discovery.v1 wire types and the
// internal/model domain types. It is the single boundary where proto leaks are
// allowed; the domain stays free of grpc/protobuf imports.
package protoconv

import (
	"math"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// --- Node ---

// NodeToProto converts a domain Node to its wire form.
func NodeToProto(n model.Node) *discoveryv1.Node {
	return &discoveryv1.Node{
		Id:              n.ID,
		Name:            n.Name,
		Address:         n.Address,
		Datacenter:      n.Datacenter,
		Meta:            copyMap(n.Meta),
		TaggedAddresses: copyMap(n.TaggedAddresses),
	}
}

// NodeFromProto converts a wire Node to the domain form.
func NodeFromProto(p *discoveryv1.Node) model.Node {
	if p == nil {
		return model.Node{}
	}
	return model.Node{
		ID:              p.GetId(),
		Name:            p.GetName(),
		Address:         p.GetAddress(),
		Datacenter:      p.GetDatacenter(),
		Meta:            copyMap(p.GetMeta()),
		TaggedAddresses: copyMap(p.GetTaggedAddresses()),
	}
}

// --- Weights ---

// WeightsToProto converts domain Weights to the wire form (always non-nil).
func WeightsToProto(w model.Weights) *discoveryv1.Weights {
	return &discoveryv1.Weights{Passing: w.Passing, Warning: w.Warning}
}

// WeightsFromProto converts wire Weights to the domain form.
func WeightsFromProto(p *discoveryv1.Weights) model.Weights {
	if p == nil {
		return model.Weights{}
	}
	return model.Weights{Passing: p.GetPassing(), Warning: p.GetWarning()}
}

// --- Service (definition) ---

// ServiceToProto converts a domain ServiceInstance definition to the wire form.
// Runtime fields (Generation, LastSeen) live on ServiceEntry, not Service.
func ServiceToProto(s model.ServiceInstance) *discoveryv1.Service {
	return &discoveryv1.Service{
		Id:         s.ID,
		Name:       s.Name,
		Address:    s.Address,
		Port:       uint32(s.Port),
		Tags:       copySlice(s.Tags),
		Meta:       copyMap(s.Meta),
		Weights:    WeightsToProto(s.Weights),
		TtlSeconds: ttlToSeconds(s.TTL),
	}
}

// ServiceFromProto converts a wire Service definition to the domain form.
func ServiceFromProto(p *discoveryv1.Service) model.ServiceInstance {
	if p == nil {
		return model.ServiceInstance{}
	}
	return model.ServiceInstance{
		ID:      p.GetId(),
		Name:    p.GetName(),
		Address: p.GetAddress(),
		Port:    portFromProto(p.GetPort()),
		Tags:    copySlice(p.GetTags()),
		Meta:    copyMap(p.GetMeta()),
		Weights: WeightsFromProto(p.GetWeights()),
		TTL:     time.Duration(p.GetTtlSeconds()) * time.Second,
	}
}

// --- Registration ---

// RegistrationToProto converts a domain Registration to the wire form.
func RegistrationToProto(r model.Registration) *discoveryv1.Registration {
	out := &discoveryv1.Registration{
		Node:       NodeToProto(r.Node),
		Generation: r.Generation,
	}
	if len(r.Services) > 0 {
		out.Services = make([]*discoveryv1.Service, len(r.Services))
		for i := range r.Services {
			out.Services[i] = ServiceToProto(r.Services[i])
		}
	}
	return out
}

// RegistrationFromProto converts a wire Registration to the domain form.
func RegistrationFromProto(p *discoveryv1.Registration) model.Registration {
	if p == nil {
		return model.Registration{}
	}
	r := model.Registration{
		Node:       NodeFromProto(p.GetNode()),
		Generation: p.GetGeneration(),
	}
	if svcs := p.GetServices(); len(svcs) > 0 {
		r.Services = make([]model.ServiceInstance, len(svcs))
		for i, s := range svcs {
			r.Services[i] = ServiceFromProto(s)
		}
	}
	return r
}

// --- ServiceEntry ---

// EntryToProto converts a domain ServiceEntry to the wire form. The per-service
// merge fields (Generation, LastSeen) are taken from the embedded Service.
func EntryToProto(e model.ServiceEntry) *discoveryv1.ServiceEntry {
	return &discoveryv1.ServiceEntry{
		Node:             NodeToProto(e.Node),
		Service:          ServiceToProto(e.Service),
		Health:           HealthToProto(e.Health),
		Maintenance:      e.Maintenance,
		Generation:       e.Service.Generation,
		LastSeenUnixNano: toUnixNano(e.Service.LastSeen),
	}
}

// EntryFromProto converts a wire ServiceEntry to the domain form, stamping the
// merge fields back onto the embedded Service.
func EntryFromProto(p *discoveryv1.ServiceEntry) model.ServiceEntry {
	if p == nil {
		return model.ServiceEntry{}
	}
	svc := ServiceFromProto(p.GetService())
	svc.Generation = p.GetGeneration()
	svc.LastSeen = fromUnixNano(p.GetLastSeenUnixNano())
	return model.ServiceEntry{
		Node:        NodeFromProto(p.GetNode()),
		Service:     svc,
		Health:      HealthFromProto(p.GetHealth()),
		Maintenance: p.GetMaintenance(),
	}
}

// --- Query ---

// QueryToProto converts a domain Query to the wire form.
func QueryToProto(q model.Query) *discoveryv1.Query {
	return &discoveryv1.Query{
		Name:        q.Name,
		Datacenter:  q.Datacenter,
		Tags:        copySlice(q.Tags),
		OnlyHealthy: q.OnlyHealthy,
	}
}

// QueryFromProto converts a wire Query to the domain form.
func QueryFromProto(p *discoveryv1.Query) model.Query {
	if p == nil {
		return model.Query{}
	}
	return model.Query{
		Name:        p.GetName(),
		Datacenter:  p.GetDatacenter(),
		Tags:        copySlice(p.GetTags()),
		OnlyHealthy: p.GetOnlyHealthy(),
	}
}

// --- LookupResult ---

// LookupResultToProto converts a domain LookupResult to a LookupResponse.
func LookupResultToProto(r model.LookupResult) *discoveryv1.LookupResponse {
	out := &discoveryv1.LookupResponse{Index: r.Index}
	if len(r.Entries) > 0 {
		out.Entries = make([]*discoveryv1.ServiceEntry, len(r.Entries))
		for i := range r.Entries {
			out.Entries[i] = EntryToProto(r.Entries[i])
		}
	}
	return out
}

// LookupResultFromProto converts a LookupResponse to a domain LookupResult.
func LookupResultFromProto(p *discoveryv1.LookupResponse) model.LookupResult {
	if p == nil {
		return model.LookupResult{}
	}
	r := model.LookupResult{Index: p.GetIndex()}
	if entries := p.GetEntries(); len(entries) > 0 {
		r.Entries = make([]model.ServiceEntry, len(entries))
		for i, e := range entries {
			r.Entries[i] = EntryFromProto(e)
		}
	}
	return r
}

// --- ChangeEvent ---

// ChangeEventToProto converts a domain ChangeEvent to the wire form.
func ChangeEventToProto(e model.ChangeEvent) *discoveryv1.ChangeEvent {
	return &discoveryv1.ChangeEvent{
		Type:  ChangeTypeToProto(e.Type),
		Entry: EntryToProto(e.Entry),
	}
}

// ChangeEventFromProto converts a wire ChangeEvent to the domain form. The
// registry index is carried alongside the event on the wire (WatchResponse).
func ChangeEventFromProto(p *discoveryv1.ChangeEvent, index uint64) model.ChangeEvent {
	if p == nil {
		return model.ChangeEvent{Index: index}
	}
	return model.ChangeEvent{
		Type:  ChangeTypeFromProto(p.GetType()),
		Entry: EntryFromProto(p.GetEntry()),
		Index: index,
	}
}

// --- enums ---

// HealthToProto maps a domain HealthState to the wire enum.
func HealthToProto(h model.HealthState) discoveryv1.HealthState {
	switch h {
	case model.HealthPassing:
		return discoveryv1.HealthState_HEALTH_STATE_PASSING
	case model.HealthWarning:
		return discoveryv1.HealthState_HEALTH_STATE_WARNING
	case model.HealthCritical:
		return discoveryv1.HealthState_HEALTH_STATE_CRITICAL
	default:
		return discoveryv1.HealthState_HEALTH_STATE_UNSPECIFIED
	}
}

// HealthFromProto maps a wire health enum to the domain HealthState.
func HealthFromProto(h discoveryv1.HealthState) model.HealthState {
	switch h {
	case discoveryv1.HealthState_HEALTH_STATE_PASSING:
		return model.HealthPassing
	case discoveryv1.HealthState_HEALTH_STATE_WARNING:
		return model.HealthWarning
	case discoveryv1.HealthState_HEALTH_STATE_CRITICAL:
		return model.HealthCritical
	default:
		return model.HealthUnspecified
	}
}

// ChangeTypeToProto maps a domain ChangeType to the wire enum.
func ChangeTypeToProto(c model.ChangeType) discoveryv1.ChangeType {
	switch c {
	case model.ChangePut:
		return discoveryv1.ChangeType_CHANGE_TYPE_PUT
	case model.ChangeDelete:
		return discoveryv1.ChangeType_CHANGE_TYPE_DELETE
	default:
		return discoveryv1.ChangeType_CHANGE_TYPE_UNSPECIFIED
	}
}

// ChangeTypeFromProto maps a wire change-type enum to the domain ChangeType.
func ChangeTypeFromProto(c discoveryv1.ChangeType) model.ChangeType {
	switch c {
	case discoveryv1.ChangeType_CHANGE_TYPE_PUT:
		return model.ChangePut
	case discoveryv1.ChangeType_CHANGE_TYPE_DELETE:
		return model.ChangeDelete
	default:
		return model.ChangeUnspecified
	}
}

// --- helpers ---

// ttlToSeconds converts a TTL to whole seconds for the wire, clamping to the
// uint32 range (negative -> 0). Sub-second precision is intentionally dropped.
func ttlToSeconds(d time.Duration) uint32 {
	secs := d / time.Second
	switch {
	case secs < 0:
		return 0
	case secs > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(secs)
	}
}

// portFromProto narrows a wire port to uint16, clamping out-of-range values.
func portFromProto(p uint32) uint16 {
	if p > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(p)
}

func toUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromUnixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func copySlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func copyMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
