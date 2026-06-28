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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/protoconv"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// propagationProbeTimeout bounds the async register/deregister SLI probes.
const propagationProbeTimeout = 30 * time.Second

// Proxy is the local-agent-proxy: the AgentService that local apps on 127.0.0.1
// talk to. Writes are stamped with the agent's node identity and fanned out to
// the seeds; reads go through the bounded-staleness cache. The agent tracks what
// it hosts so the renew loop can keep those leases alive and the drain can
// deregister them.
type Proxy struct {
	discoveryv1.UnimplementedAgentServiceServer
	client *SeedClient
	cache  *Cache
	node   model.Node
	quorum int
	prop   *observability.Propagation
	log    *slog.Logger

	mu     sync.Mutex
	hosted map[string]model.ServiceInstance // serviceID -> definition
}

// ProxyOptions configures a Proxy.
type ProxyOptions struct {
	// Client fans out to the seeds (required).
	Client *SeedClient
	// Node is the agent's node identity, stamped onto every registration.
	Node model.Node
	// Quorum is the minimum seeds a write must reach (k-of-N); < 1 clamps to 1.
	Quorum int
	// Cache serves reads with bounded staleness; nil reads fan out directly.
	Cache *Cache
	// Prop records register-to-visible / deregister-to-removed SLIs (optional).
	Prop *observability.Propagation
	// Log is the structured logger.
	Log *slog.Logger
}

// NewProxy builds the proxy from opts.
func NewProxy(opts ProxyOptions) *Proxy {
	if opts.Quorum < 1 {
		opts.Quorum = 1
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Proxy{
		client: opts.Client,
		cache:  opts.Cache,
		node:   opts.Node,
		quorum: opts.Quorum,
		prop:   opts.Prop,
		log:    opts.Log,
		hosted: make(map[string]model.ServiceInstance),
	}
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

	if p.prop != nil {
		for _, s := range reg.Services {
			// The probe must outlive this RPC (it measures visibility after the
			// handler returns), so it uses its own bounded context, not ctx.
			go p.measurePropagation(s.Name, s.ID, true) //nolint:gosec // G118: intentional detached probe
		}
	}
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
	name := p.hosted[req.GetServiceId()].Name
	delete(p.hosted, req.GetServiceId())
	p.mu.Unlock()
	if !res.OK(1) {
		return nil, status.Errorf(codes.Unavailable, "deregistered on %d/%d seeds", res.Succeeded, res.Total)
	}
	if p.prop != nil && name != "" {
		// Detached probe (must outlive this RPC); see Register.
		go p.measurePropagation(name, req.GetServiceId(), false) //nolint:gosec // G118: intentional detached probe
	}
	return &discoveryv1.DeregisterServiceResponse{}, nil
}

// Lookup serves the query from the cache (bounded staleness), or fans out
// directly when no cache is configured.
func (p *Proxy) Lookup(ctx context.Context, req *discoveryv1.LookupRequest) (*discoveryv1.LookupResponse, error) {
	q := protoconv.QueryFromProto(req.GetQuery())
	if q.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "query.name is required")
	}

	var (
		lr  model.LookupResult
		err error
	)
	if p.cache != nil {
		lr, err = p.cache.Lookup(ctx, q)
	} else {
		lr, err = p.directLookup(ctx, q)
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "lookup failed on all seeds")
	}
	return protoconv.LookupResultToProto(lr), nil
}

// directLookup fans out, merges and applies the health filter (the no-cache path).
func (p *Proxy) directLookup(ctx context.Context, q model.Query) (model.LookupResult, error) {
	raw := q
	raw.OnlyHealthy = false
	lr, err := p.client.Lookup(ctx, raw)
	if err != nil {
		return model.LookupResult{}, err
	}
	if q.OnlyHealthy {
		lr.Entries = health.Filter(lr.Entries, health.FilterOptions{OnlyPassing: true})
	}
	return lr, nil
}

// measurePropagation polls the cluster until the (node, serviceID) appears
// (register) or disappears (deregister), recording the latency. Bounded by a
// timeout; runs in its own goroutine and uses the wall clock (a real-time SLI).
func (p *Proxy) measurePropagation(name, serviceID string, appear bool) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), propagationProbeTimeout)
	defer cancel()

	for {
		lr, err := p.client.Lookup(ctx, model.Query{Name: name})
		if err == nil && contains(lr.Entries, p.node.ID, serviceID) == appear {
			d := time.Since(start)
			if appear {
				p.prop.ObserveRegisterToVisible(d)
			} else {
				p.prop.ObserveDeregisterToRemoved(d)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func contains(entries []model.ServiceEntry, nodeID, serviceID string) bool {
	for _, e := range entries {
		if e.Node.ID == nodeID && e.Service.ID == serviceID {
			return true
		}
	}
	return false
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
