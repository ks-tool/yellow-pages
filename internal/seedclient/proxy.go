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

package seedclient

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/protoconv"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// Proxy is the local-agent-proxy: the AgentService that local apps on 127.0.0.1
// talk to. Writes are stamped with the agent's node identity and fanned out to
// the seeds; reads fan out and merge. The agent tracks what it hosts so the
// renew loop can keep those leases alive and the drain can deregister them.
type Proxy struct {
	discoveryv1.UnimplementedAgentServiceServer
	client *SeedClient
	node   model.Node
	quorum int
	log    *slog.Logger

	mu     sync.Mutex
	hosted map[string]model.ServiceInstance // serviceID -> definition
}

// NewProxy builds the proxy for node, requiring quorum seeds for a write to
// succeed (k-of-N). A quorum < 1 is clamped to 1.
func NewProxy(client *SeedClient, node model.Node, quorum int, log *slog.Logger) *Proxy {
	if quorum < 1 {
		quorum = 1
	}
	if log == nil {
		log = slog.Default()
	}
	return &Proxy{client: client, node: node, quorum: quorum, log: log, hosted: make(map[string]model.ServiceInstance)}
}

// Register hosts the request's services on this agent and fans the registration
// out to the seeds. The agent's node identity always replaces the caller's.
func (p *Proxy) Register(ctx context.Context, req *discoveryv1.RegisterRequest) (*discoveryv1.RegisterResponse, error) {
	reg := protoconv.RegistrationFromProto(req.GetRegistration())
	if len(reg.Services) == 0 {
		return nil, status.Error(codes.InvalidArgument, "registration has no services")
	}
	reg.Node = p.node // the agent owns its hosted registrations
	for i := range reg.Services {
		if reg.Services[i].Name == "" {
			return nil, status.Error(codes.InvalidArgument, "service name is required")
		}
		if reg.Services[i].ID == "" {
			reg.Services[i].ID = reg.Services[i].Name
		}
	}

	res := p.client.Register(ctx, reg)
	if !res.OK(p.quorum) {
		return nil, status.Errorf(codes.Unavailable,
			"registered on %d/%d seeds (quorum %d)", res.Succeeded, res.Total, p.quorum)
	}

	p.mu.Lock()
	for _, s := range reg.Services {
		p.hosted[s.ID] = s
	}
	p.mu.Unlock()
	return &discoveryv1.RegisterResponse{}, nil
}

// Renew refreshes the agent's leases on the seeds (node-scoped; optionally
// narrowed to service ids).
func (p *Proxy) Renew(ctx context.Context, req *discoveryv1.RenewRequest) (*discoveryv1.RenewResponse, error) {
	res := p.client.Renew(ctx, p.node.ID, req.GetServiceIds())
	if !res.OK(1) {
		return nil, status.Errorf(codes.Unavailable, "renewed on %d/%d seeds", res.Succeeded, res.Total)
	}
	return &discoveryv1.RenewResponse{}, nil
}

// Deregister removes all of the agent's registrations from the seeds.
func (p *Proxy) Deregister(ctx context.Context, _ *discoveryv1.DeregisterRequest) (*discoveryv1.DeregisterResponse, error) {
	res := p.client.Deregister(ctx, p.node.ID)
	p.mu.Lock()
	p.hosted = make(map[string]model.ServiceInstance)
	p.mu.Unlock()
	if !res.OK(1) {
		return nil, status.Errorf(codes.Unavailable, "deregistered on %d/%d seeds", res.Succeeded, res.Total)
	}
	return &discoveryv1.DeregisterResponse{}, nil
}

// DeregisterService removes one hosted service from the seeds.
func (p *Proxy) DeregisterService(ctx context.Context, req *discoveryv1.DeregisterServiceRequest) (*discoveryv1.DeregisterServiceResponse, error) {
	if req.GetServiceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "service_id is required")
	}
	res := p.client.DeregisterService(ctx, p.node.ID, req.GetServiceId())
	p.mu.Lock()
	delete(p.hosted, req.GetServiceId())
	p.mu.Unlock()
	if !res.OK(1) {
		return nil, status.Errorf(codes.Unavailable, "deregistered on %d/%d seeds", res.Succeeded, res.Total)
	}
	return &discoveryv1.DeregisterServiceResponse{}, nil
}

// Lookup fans out the query to the seeds and returns the merged result.
func (p *Proxy) Lookup(ctx context.Context, req *discoveryv1.LookupRequest) (*discoveryv1.LookupResponse, error) {
	q := protoconv.QueryFromProto(req.GetQuery())
	if q.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "query.name is required")
	}
	lr, err := p.client.Lookup(ctx, q)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "lookup failed on all seeds")
	}
	return protoconv.LookupResultToProto(lr), nil
}

// hostedCount reports how many services the agent currently hosts.
func (p *Proxy) hostedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.hosted)
}

// renewAll refreshes every hosted lease on the seeds.
func (p *Proxy) renewAll(ctx context.Context) WriteResult {
	return p.client.Renew(ctx, p.node.ID, nil)
}
