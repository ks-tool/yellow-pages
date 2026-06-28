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
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// propagationProbeTimeout bounds the async register/deregister SLI probes.
const propagationProbeTimeout = 30 * time.Second

// watchMaxWait bounds one blocking iteration of the agent's Watch long-poll.
const watchMaxWait = 10 * time.Minute

// Proxy is the local-agent-proxy: the AgentService that local apps on 127.0.0.1
// talk to. Writes are stamped with the agent's node identity and fanned out to
// the seeds; reads go through the bounded-staleness cache. The agent tracks what
// it hosts so the renew loop can keep those leases alive and the drain can
// deregister them.
type Proxy struct {
	discoveryv1.UnimplementedAgentServiceServer
	client  *SeedClient
	cache   *Cache
	watcher *watch.Watcher
	node    model.Node
	quorum  int
	prop    *observability.Propagation
	log     *slog.Logger

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
	// Watcher backs the Watch RPC with the agent's synthesised monotonic index;
	// nil reports Watch as Unimplemented.
	Watcher *watch.Watcher
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
		client:  opts.Client,
		cache:   opts.Cache,
		watcher: opts.Watcher,
		node:    opts.Node,
		quorum:  opts.Quorum,
		prop:    opts.Prop,
		log:     opts.Log,
		hosted:  make(map[string]model.ServiceInstance),
	}
}

// Register hosts the request's services on this agent and fans the registration
// out to the seeds. The agent's node identity always replaces the caller's.
func (p *Proxy) Register(ctx context.Context, req *discoveryv1.RegisterRequest) (*discoveryv1.RegisterResponse, error) {
	if err := p.RegisterServices(ctx, protoconv.RegistrationFromProto(req.GetRegistration())); err != nil {
		return nil, err
	}
	return &discoveryv1.RegisterResponse{}, nil
}

// RegisterServices is the model-level register shared by the native gRPC and the
// Consul HTTP surfaces: it stamps the agent's node identity, fans out to the
// seeds (k-of-N) and tracks the hosted services.
func (p *Proxy) RegisterServices(ctx context.Context, reg model.Registration) error {
	if len(reg.Services) == 0 {
		return status.Error(codes.InvalidArgument, "registration has no services")
	}
	reg.Node = p.node // the agent owns its hosted registrations
	for i := range reg.Services {
		if reg.Services[i].Name == "" {
			return status.Error(codes.InvalidArgument, "service name is required")
		}
		if reg.Services[i].ID == "" {
			reg.Services[i].ID = reg.Services[i].Name
		}
	}

	res := p.client.Register(ctx, reg)
	if !res.OK(p.quorum) {
		return status.Errorf(codes.Unavailable,
			"registered on %d/%d seeds (quorum %d)", res.Succeeded, res.Total, p.quorum)
	}

	p.mu.Lock()
	for _, s := range reg.Services {
		p.hosted[s.ID] = s
	}
	p.mu.Unlock()

	if p.prop != nil {
		for _, s := range reg.Services {
			// The probe must outlive this call (it measures visibility after it
			// returns), so it uses its own bounded context, not ctx.
			go p.measurePropagation(s.Name, s.ID, true) //nolint:gosec // G118: intentional detached probe
		}
	}
	return nil
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
	if err := p.RemoveService(ctx, req.GetServiceId()); err != nil {
		return nil, err
	}
	return &discoveryv1.DeregisterServiceResponse{}, nil
}

// RemoveService is the model-level single-service deregister shared by the
// native gRPC and Consul HTTP surfaces.
func (p *Proxy) RemoveService(ctx context.Context, serviceID string) error {
	if serviceID == "" {
		return status.Error(codes.InvalidArgument, "service_id is required")
	}
	res := p.client.DeregisterService(ctx, p.node.ID, serviceID)
	p.mu.Lock()
	name := p.hosted[serviceID].Name
	delete(p.hosted, serviceID)
	p.mu.Unlock()
	if !res.OK(1) {
		return status.Errorf(codes.Unavailable, "deregistered on %d/%d seeds", res.Succeeded, res.Total)
	}
	if p.prop != nil && name != "" {
		// Detached probe (must outlive this call); see RegisterServices.
		go p.measurePropagation(name, serviceID, false) //nolint:gosec // G118: intentional detached probe
	}
	return nil
}

// Resolve returns the merged instance set for q under the given consistency
// mode, plus the age of the cache entry it came from (0 for a fresh fan-out).
// ConsistencyConsistent bypasses the cache; the others serve it.
func (p *Proxy) Resolve(ctx context.Context, q model.Query, mode model.Consistency) (model.LookupResult, time.Duration, error) {
	if mode == model.ConsistencyConsistent || p.cache == nil {
		lr, err := p.directLookup(ctx, q)
		return lr, 0, err
	}
	return p.cache.LookupWithAge(ctx, q)
}

// Hosted returns the services this agent currently hosts.
func (p *Proxy) Hosted() []model.ServiceInstance {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]model.ServiceInstance, 0, len(p.hosted))
	for _, s := range p.hosted {
		out = append(out, s)
	}
	return out
}

// Lookup serves the query from the cache (bounded staleness), or fans out
// directly when no cache is configured.
func (p *Proxy) Lookup(ctx context.Context, req *discoveryv1.LookupRequest) (*discoveryv1.LookupResponse, error) {
	q := protoconv.QueryFromProto(req.GetQuery())
	lr, err := p.lookup(ctx, q)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "lookup failed on all seeds")
	}
	return protoconv.LookupResultToProto(lr), nil
}

// Watch streams the merged set for the query: an initial snapshot (a put per
// current instance, ended by snapshot_done) followed by put/delete deltas
// whenever the agent's synthesised index advances. Deltas are computed by
// diffing successive merged snapshots, matching the seed's Watch contract so one
// SDK consumes both.
func (p *Proxy) Watch(req *discoveryv1.WatchRequest, stream discoveryv1.AgentService_WatchServer) error {
	if p.watcher == nil {
		return status.Error(codes.Unimplemented, "watch is not enabled on this node")
	}
	q := protoconv.QueryFromProto(req.GetQuery())
	if q.Name == "" {
		return status.Error(codes.InvalidArgument, "query.name is required")
	}
	ctx := stream.Context()

	last := make(map[string]model.ServiceEntry)
	var lastIdx uint64
	first := true
	for {
		if ctx.Err() != nil {
			return nil
		}
		idx := p.watcher.WaitForChange(ctx, q, lastIdx, watchMaxWait)
		if idx == lastIdx && !first {
			continue // long-poll timeout with no change
		}
		lastIdx = idx

		lr, err := p.lookup(ctx, q)
		if err != nil {
			continue // transient: keep the stream open and retry on the next change
		}
		cur := make(map[string]model.ServiceEntry, len(lr.Entries))
		for _, e := range lr.Entries {
			cur[entryKey(e)] = e
		}

		if first {
			for _, e := range lr.Entries {
				if err := sendEvent(stream, model.ChangePut, e, idx); err != nil {
					return err
				}
			}
			if err := stream.Send(&discoveryv1.WatchResponse{SnapshotDone: true, Index: idx}); err != nil {
				return err
			}
			first = false
		} else {
			for k, e := range cur {
				if prev, ok := last[k]; !ok || endpointChanged(prev, e) {
					if err := sendEvent(stream, model.ChangePut, e, idx); err != nil {
						return err
					}
				}
			}
			for k, e := range last {
				if _, ok := cur[k]; !ok {
					if err := sendEvent(stream, model.ChangeDelete, e, idx); err != nil {
						return err
					}
				}
			}
		}
		last = cur
	}
}

func sendEvent(stream discoveryv1.AgentService_WatchServer, t model.ChangeType, e model.ServiceEntry, idx uint64) error {
	return stream.Send(&discoveryv1.WatchResponse{
		Event: protoconv.ChangeEventToProto(model.ChangeEvent{Type: t, Entry: e}),
		Index: idx,
	})
}

func entryKey(e model.ServiceEntry) string { return e.Node.ID + "\x00" + e.Service.ID }

func endpointChanged(a, b model.ServiceEntry) bool {
	return a.Service.Address != b.Service.Address || a.Service.Port != b.Service.Port ||
		a.Service.Generation != b.Service.Generation || a.Health != b.Health
}

// lookup reads via the cache when configured, else fans out directly.
func (p *Proxy) lookup(ctx context.Context, q model.Query) (model.LookupResult, error) {
	if p.cache != nil {
		return p.cache.Lookup(ctx, q)
	}
	return p.directLookup(ctx, q)
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
