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
	"errors"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/store"
)

// StoreRegistry adapts a seed's Store to the Consul Registry + Dumper surfaces.
// A seed serves the Consul API directly over its registry (no fan-out).
type StoreRegistry struct {
	st   store.Store
	node model.Node
}

// compile-time assertions.
var (
	_ Registry = (*StoreRegistry)(nil)
	_ Dumper   = (*StoreRegistry)(nil)
)

// NewStoreRegistry builds the adapter for the seed's node identity.
func NewStoreRegistry(st store.Store, node model.Node) *StoreRegistry {
	return &StoreRegistry{st: st, node: node}
}

// RegisterServices stamps the seed's node and registers.
func (s *StoreRegistry) RegisterServices(_ context.Context, reg model.Registration) error {
	reg.Node = s.node
	return s.st.Register(reg)
}

// RegisterExternal registers a node with the payload's identity (backfill).
func (s *StoreRegistry) RegisterExternal(_ context.Context, reg model.Registration) error {
	return s.st.Register(reg)
}

// RemoveService removes one of the seed-node's services.
func (s *StoreRegistry) RemoveService(_ context.Context, serviceID string) error {
	return s.st.DeregisterService(s.node.ID, serviceID)
}

// RemoveNode deregisters an external node, tolerating an unknown node.
func (s *StoreRegistry) RemoveNode(_ context.Context, nodeID string) error {
	if err := s.st.Deregister(nodeID); err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	return nil
}

// Resolve reads the registry (a seed has no cache; age is always 0).
func (s *StoreRegistry) Resolve(_ context.Context, q model.Query, _ model.Consistency) (model.LookupResult, time.Duration, error) {
	return s.st.Lookup(q), 0, nil
}

// RenewService refreshes a service's lease (check pass/warn/update).
func (s *StoreRegistry) RenewService(_ context.Context, serviceID string) error {
	return s.st.Renew(s.node.ID, []string{serviceID})
}

// FailService forces a service critical (check fail).
func (s *StoreRegistry) FailService(_ context.Context, serviceID string) error {
	return s.st.Fail(s.node.ID, serviceID)
}

// SetMaintenance toggles a service's maintenance flag.
func (s *StoreRegistry) SetMaintenance(_ context.Context, serviceID string, enabled bool) error {
	return s.st.SetMaintenance(s.node.ID, serviceID, enabled)
}

// Hosted reports no services: a seed serves the registry but hosts none itself.
func (s *StoreRegistry) Hosted() []model.ServiceInstance { return nil }

// Dump returns the seed's own registry view (no per-seed divergence on a seed).
func (s *StoreRegistry) Dump(_ context.Context, q model.Query) map[string][]model.ServiceEntry {
	return map[string][]model.ServiceEntry{"local": s.st.Lookup(q).Entries}
}
