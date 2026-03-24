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
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/ks-tool/yellow-pages/proto/gen"

	"github.com/uptrace/bunrouter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Gateway provides a Consul‑compatible HTTP API for service discovery.
type Gateway struct {
	client discovery.AgentServiceClient
	conn   *grpc.ClientConn
}

// NewGateway creates a new Gateway connected to a seed at the given address.
func NewGateway(seedAddr string) (*Gateway, error) {
	conn, err := grpc.NewClient(
		seedAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Gateway{
		client: discovery.NewAgentServiceClient(conn),
		conn:   conn,
	}, nil
}

// Close closes the underlying gRPC connection.
func (g *Gateway) Close() error {
	return g.conn.Close()
}

// Routes returns a bunrouter with Consul‑compatible endpoints.
func (g *Gateway) Routes(router *bunrouter.Router) {
	agentGroup := router.NewGroup("/agent")
	serviceGroup := router.NewGroup("/service")

	agentGroup.GET("/services", g.listServices)
	serviceGroup.GET("/:service_id", g.getService)
	serviceGroup.PUT("/register", g.registerService)
	serviceGroup.PUT("/deregister/:service_id", g.deregisterService)
	serviceGroup.PUT("/maintenance/:service_id", g.maintenance)
}

// listServices returns all registered services (Consul: GET /agent/services)
func (g *Gateway) listServices(w http.ResponseWriter, req bunrouter.Request) error {
	// This endpoint would ideally list all services known to the discovery system.
	// Since our gRPC API does not yet provide a "list all services" method,
	// we return an empty map as a placeholder. A full implementation would
	// either query all known seeds or maintain a local cache.
	w.Header().Set("Content-Type", "application/json")
	// TODO
	// https://developer.hashicorp.com/consul/api-docs/agent/service#filtering
	filter := req.URL.Query().Get("filter")
	_ = filter

	return bunrouter.JSON(w, map[string]any{})
}

// getService returns details for a specific service (Consul: GET /service/:service_id)
func (g *Gateway) getService(w http.ResponseWriter, req bunrouter.Request) error {
	serviceID := req.Param("service_id")
	if serviceID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": "service_id required"})
	}

	// TODO
	// https://developer.hashicorp.com/consul/api-docs/agent/service#get-service-configuration

	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()

	resp, err := g.client.GetServiceAgents(ctx, &discovery.GetServiceAgentsRequest{
		ServiceName: serviceID,
	})
	if err != nil {
		slog.Error("failed to get service agents", "service", serviceID, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return bunrouter.JSON(w, bunrouter.H{"error": "internal error"})
	}

	// Build response in Consul format: map of agent ID -> service info.
	result := make(map[string]any)
	for _, agentWithMeta := range resp.Agents {
		agent := agentWithMeta.Agent
		// In Consul, the key is the service ID. We use the agent ID as the service ID
		// for simplicity. The actual service might have its own ID, but we don't have
		// that separation in the current proto.
		serviceEntry := map[string]any{
			"ID":       agent.Id,
			"Service":  serviceID,
			"Address":  agent.Address,
			"Port":     agent.Port,
			"Tags":     []string{}, // could be extracted from service metadata
			"Meta":     agent.Metadata,
			"LastSeen": agentWithMeta.LastSeen,
		}
		result[agent.Id] = serviceEntry
	}

	return bunrouter.JSON(w, result)
}

// registerService handles service registration (Consul: PUT /service/register)
func (g *Gateway) registerService(w http.ResponseWriter, req bunrouter.Request) error {
	// TODO
	// https://developer.hashicorp.com/consul/api-docs/agent/service#json-request-body-schema
	replaceExistingChecks := req.URL.Query().Get("replace-existing-checks")
	_ = replaceExistingChecks

	var regReq struct {
		ID      string            `json:"ID"`
		Name    string            `json:"Name"`
		Address string            `json:"Address"`
		Port    int               `json:"Port"`
		Tags    []string          `json:"Tags"`
		Meta    map[string]string `json:"Meta"`
		// Additional Consul fields (e.g., Check) are omitted for brevity.
	}
	if err := json.NewDecoder(req.Body).Decode(&regReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": "invalid request body"})
	}

	agentID := regReq.ID
	if agentID == "" {
		agentID = regReq.Name
	}
	agent := &discovery.Agent{
		Id:         agentID,
		Address:    regReq.Address,
		Port:       int32(regReq.Port),
		Datacenter: "", // could be set from configuration
		Metadata:   regReq.Meta,
	}

	endpoints := []*discovery.Endpoint{
		{
			Name:     "default",
			Protocol: "tcp",
			Address:  regReq.Address,
			Port:     int32(regReq.Port),
			Path:     "",
			Metadata: regReq.Meta,
		},
	}
	service := &discovery.Service{
		Name:      regReq.Name,
		Endpoints: endpoints,
		Tags:      regReq.Tags,
		Metadata:  regReq.Meta,
	}

	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()

	registerReq := &discovery.RegisterAgentRequest{
		Agent:      agent,
		Services:   []*discovery.Service{service},
		TtlSeconds: 30, // default TTL
	}
	resp, err := g.client.RegisterAgent(ctx, registerReq)
	if err != nil {
		slog.Error("failed to register agent", "agent_id", agentID, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return bunrouter.JSON(w, bunrouter.H{"error": "registration failed"})
	}
	if !resp.Success {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": resp.Message})
	}

	return bunrouter.JSON(w, bunrouter.H{"success": true})
}

// deregisterService handles service deregistration (Consul: PUT /service/deregister/:service_id)
func (g *Gateway) deregisterService(w http.ResponseWriter, req bunrouter.Request) error {
	serviceID := req.Param("service_id")
	if serviceID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": "service_id required"})
	}

	// TODO
	// https://developer.hashicorp.com/consul/api-docs/agent/service#deregister-service

	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()

	deregReq := &discovery.DeregisterAgentRequest{
		AgentId: serviceID, // assumes agent ID equals service ID
	}
	resp, err := g.client.DeregisterAgent(ctx, deregReq)
	if err != nil {
		slog.Error("failed to deregister agent", "agent_id", serviceID, "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return bunrouter.JSON(w, bunrouter.H{"error": "deregistration failed"})
	}
	if !resp.Success {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": resp.Message})
	}

	return bunrouter.JSON(w, bunrouter.H{"success": true})
}

// maintenance handles setting maintenance mode (Consul: PUT /service/maintenance/:service_id)
func (g *Gateway) maintenance(w http.ResponseWriter, req bunrouter.Request) error {
	serviceID := req.Param("service_id")
	if serviceID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return bunrouter.JSON(w, bunrouter.H{"error": "service_id required"})
	}

	// TODO
	// https://developer.hashicorp.com/consul/api-docs/agent/service#enable-maintenance-mode

	// In Consul, this endpoint toggles a maintenance flag on the service.
	// Here we simply return success without modifying anything.
	// A real implementation would update the service's metadata (e.g., add a "maintenance" tag)
	// and possibly re‑register the service.
	return bunrouter.JSON(w, bunrouter.H{"success": true})
}
