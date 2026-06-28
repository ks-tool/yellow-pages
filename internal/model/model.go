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

// Package model is the canonical domain model that every transport adapter
// (native gRPC, Consul HTTP, Consul DNS) projects. It depends only on the
// standard library — never on grpc/protobuf — so the contract and the domain
// can evolve independently. Conversions live at the boundary in the
// internal/protoconv package.
package model

import "time"

// Node identifies an agent and its node-scoped attributes.
type Node struct {
	// ID is the stable agent identity carried on every RPC.
	ID string
	// Name is the node name (Consul Node.Node).
	Name string
	// Address is the node's reachable address.
	Address string
	// Datacenter is required by the Consul surfaces (?dc, .dc.consul).
	Datacenter string
	// Meta is node metadata (Consul NodeMeta).
	Meta map[string]string
	// TaggedAddresses are alternate addresses (Consul TaggedAddresses).
	TaggedAddresses map[string]string
}

// ServiceInstance is one service instance: its stable definition plus the
// per-service lease state. Identity within an agent is (Node.ID, ID); Address
// and Port are mutable data, not identity.
type ServiceInstance struct {
	// ID is unique within an agent and defaults to Name.
	ID string
	// Name is the logical service name; lookups aggregate instances by Name.
	Name string
	// Address is the reachable address (mutable).
	Address string
	// Port is the reachable port (mutable).
	Port uint16
	// Tags is an ordered list of opaque raw strings — the source of truth.
	Tags []string
	// Meta is service metadata.
	Meta map[string]string
	// Weights influence load balancing.
	Weights Weights
	// TTL is the per-service lease window.
	TTL time.Duration
	// LastSeen is the server-stamped time of the last renew/register; zero when
	// supplied by a client (the server stamps it).
	LastSeen time.Time
	// Generation is the client-supplied data version (see Registration).
	Generation uint64
}

// Weights influence load balancing. The default is {Passing:1, Warning:1}.
type Weights struct {
	Passing uint32
	Warning uint32
}

// DefaultWeights returns the Consul-compatible default weights.
func DefaultWeights() Weights { return Weights{Passing: 1, Warning: 1} }

// IsZero reports whether both weights are zero (unset).
func (w Weights) IsZero() bool { return w.Passing == 0 && w.Warning == 0 }

// OrDefault returns w, or DefaultWeights when w is unset.
func (w Weights) OrDefault() Weights {
	if w.IsZero() {
		return DefaultWeights()
	}
	return w
}

// Registration is a single Register operation: a node and its services plus the
// client-supplied data version.
//
// Generation is identical on all seeds for one registration and is incremented
// only when an endpoint/tags/meta change; Renew does not change it. It is the
// primary LWW key (max(Generation), then max(LastSeen) as a coarse tiebreak).
type Registration struct {
	Node       Node
	Services   []ServiceInstance
	Generation uint64
}

// ServiceEntry is a merged lookup/watch result: a service instance on a node
// with its derived health.
type ServiceEntry struct {
	Node        Node
	Service     ServiceInstance
	Health      HealthState
	Maintenance bool
}

// LookupResult is the result of a Lookup: matching entries and the registry
// index at which they were observed.
type LookupResult struct {
	Entries []ServiceEntry
	Index   uint64
}

// Query selects service instances by exact name with optional filters.
type Query struct {
	// Name is matched exactly (not by prefix).
	Name string
	// Datacenter filters by datacenter when set.
	Datacenter string
	// Tags are matched against the raw tag strings (AND across multiple).
	Tags []string
	// OnlyHealthy excludes critical/maintenance instances (Consul ?passing).
	OnlyHealthy bool
}

// ChangeType classifies a ChangeEvent.
type ChangeType int

const (
	// ChangeUnspecified is the zero value.
	ChangeUnspecified ChangeType = iota
	// ChangePut is a register or update.
	ChangePut
	// ChangeDelete is a deregister or expiry.
	ChangeDelete
)

// String renders the change type.
func (c ChangeType) String() string {
	switch c {
	case ChangePut:
		return "put"
	case ChangeDelete:
		return "delete"
	default:
		return "unspecified"
	}
}

// ChangeEvent is one entry-level change emitted by Watch.
type ChangeEvent struct {
	Type  ChangeType
	Entry ServiceEntry
	Index uint64
}

// Principal is the authenticated identity of a caller (authz, M4). In trusted-L3
// mode callers are Anonymous; with mTLS the ID is derived from the cert subject.
type Principal struct {
	// ID is the principal identity (cert subject / token principal).
	ID string
	// Anonymous reports an unauthenticated caller (allowed in acl.mode=allow).
	Anonymous bool
	// Attributes carries additional verified claims.
	Attributes map[string]string
}
