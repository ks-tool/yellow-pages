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
	"encoding/json"
	"net/http"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// catalogRegisterInput is PUT /v1/catalog/register (external node registration,
// used by migration backfill). Lenient: unknown fields are ignored.
type catalogRegisterInput struct {
	Node       string               `json:"Node"`
	Address    string               `json:"Address"`
	Datacenter string               `json:"Datacenter"`
	NodeMeta   map[string]string    `json:"NodeMeta"`
	Service    *catalogServiceInput `json:"Service"`
}

// catalogServiceInput is the catalog/register Service object: unlike the agent
// register body, the service NAME lives in the "Service" field (flat schema).
type catalogServiceInput struct {
	ID      string            `json:"ID"`
	Service string            `json:"Service"`
	Address string            `json:"Address"`
	Port    int               `json:"Port"`
	Tags    []string          `json:"Tags"`
	Meta    map[string]string `json:"Meta"`
}

type catalogDeregisterInput struct {
	Node      string `json:"Node"`
	ServiceID string `json:"ServiceID"`
}

// catalogRegister registers an arbitrary node and (optionally) one service, with
// the node taken from the payload — the backfill path during a Consul migration.
func (h *Handler) catalogRegister(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	var in catalogRegisterInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid catalog/register body", http.StatusBadRequest)
		return
	}
	if in.Node == "" {
		http.Error(w, "Node is required", http.StatusBadRequest)
		return
	}
	dc := in.Datacenter
	if dc == "" {
		dc = h.info.Datacenter
	}
	reg := model.Registration{
		Node:       model.Node{ID: in.Node, Name: in.Node, Address: in.Address, Datacenter: dc, Meta: in.NodeMeta},
		Generation: 1,
	}
	if in.Service != nil && in.Service.Service != "" {
		id := in.Service.ID
		if id == "" {
			id = in.Service.Service
		}
		reg.Services = []model.ServiceInstance{{
			ID: id, Name: in.Service.Service, Address: in.Service.Address,
			Port: clampPort(in.Service.Port), Tags: in.Service.Tags, Meta: in.Service.Meta,
		}}
	}
	if err := h.reg.RegisterExternal(r.Context(), reg); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// catalogDeregister removes an external node (ServiceID, when present, is treated
// as node-level removal in this single-DC adapter).
func (h *Handler) catalogDeregister(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeWrite(w, r) {
		return
	}
	var in catalogDeregisterInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid catalog/deregister body", http.StatusBadRequest)
		return
	}
	if in.Node == "" {
		http.Error(w, "Node is required", http.StatusBadRequest)
		return
	}
	if err := h.reg.RemoveNode(r.Context(), in.Node); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
