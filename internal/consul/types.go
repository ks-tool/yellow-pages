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

// Package consul is the Consul-compatible HTTP adapter: a thin projection of the
// domain model onto Consul's exact wire shapes (strict PascalCase), served on
// stdlib net/http. The catalog uses the FLAT schema (ServiceID/ServiceName/...)
// and health uses the NESTED schema ({Node, Service, Checks}); the two are
// deliberately separate types so a field never bleeds across.
package consul

import "time"

// weights is Consul's Service.Weights / ServiceWeights. Always populated.
type weights struct {
	Passing uint32
	Warning uint32
}

// catalogService is the FLAT /v1/catalog/service/:service entry.
type catalogService struct {
	ID              string `json:",omitempty"` // node id
	Node            string
	Address         string // node address
	Datacenter      string
	NodeMeta        map[string]string `json:",omitempty"`
	TaggedAddresses map[string]string `json:",omitempty"`
	ServiceID       string
	ServiceName     string
	ServiceAddress  string
	ServicePort     int
	ServiceTags     []string
	ServiceMeta     map[string]string `json:",omitempty"`
	ServiceWeights  weights
	CreateIndex     uint64
	ModifyIndex     uint64
}

// node is the Consul Node object (catalog/nodes, and nested in health).
type node struct {
	ID              string `json:",omitempty"`
	Node            string
	Address         string
	Datacenter      string
	Meta            map[string]string `json:",omitempty"`
	TaggedAddresses map[string]string `json:",omitempty"`
}

// healthService is the un-prefixed Service inside a nested health entry.
type healthService struct {
	ID      string
	Service string
	Tags    []string
	Address string
	Meta    map[string]string `json:",omitempty"`
	Port    int
	Weights weights
}

// healthCheck is a single synthetic Consul health check.
type healthCheck struct {
	Node        string
	CheckID     string
	Name        string
	Status      string // passing | warning | critical
	ServiceID   string
	ServiceName string
}

// healthServiceEntry is the NESTED /v1/health/service/:service entry.
type healthServiceEntry struct {
	Node    node
	Service healthService
	Checks  []healthCheck
}

// agentService is /v1/agent/services map value.
type agentService struct {
	ID      string
	Service string
	Tags    []string
	Meta    map[string]string `json:",omitempty"`
	Port    int
	Address string
	Weights weights
}

// agentSelf is the subset of /v1/agent/self official clients need.
type agentSelf struct {
	Config agentSelfConfig
	Member agentMember
}

type agentSelfConfig struct {
	Datacenter string
	NodeName   string
	Version    string
}

type agentMember struct {
	Name string
	Addr string
	Port int
}

// --- lenient register input ---

// registerInput is the lenient PUT /v1/agent/service/register body. Unknown
// fields are ignored (clients send check/Connect fields we do not model).
type registerInput struct {
	ID      string            `json:"ID"`
	Name    string            `json:"Name"`
	Tags    []string          `json:"Tags"`
	Address string            `json:"Address"`
	Port    int               `json:"Port"`
	Meta    map[string]string `json:"Meta"`
	Weights *weights          `json:"Weights"`
	Check   *checkInput       `json:"Check"`
	Checks  []checkInput      `json:"Checks"`
}

// firstTTL returns the bridged TTL from Check or the first of Checks.
func (in registerInput) firstTTL() time.Duration {
	if d := ttlFromCheck(in.Check); d > 0 {
		return d
	}
	for i := range in.Checks {
		if d := ttlFromCheck(&in.Checks[i]); d > 0 {
			return d
		}
	}
	return 0
}

// checkInput captures only the TTL we bridge to the lease; other check kinds are
// accepted and ignored.
type checkInput struct {
	TTL string `json:"TTL"`
}
