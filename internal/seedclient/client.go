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

// Package seedclient is the agent's seed-facing subsystem: a fan-out client that
// writes registrations to every seed (k-of-N, per-seed deadline) and reads by
// querying all seeds and merging by data version (MergeLWW); the local-agent-proxy
// AgentService that local apps talk to; and the renew, readiness and drain
// components that keep the agent's leases alive and shed traffic cleanly on stop.
package seedclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/protoconv"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

const defaultSeedTimeout = 3 * time.Second

// WriteResult is the outcome of a fan-out write across the seeds.
type WriteResult struct {
	// Total is the number of seeds the write was attempted on.
	Total int
	// Succeeded is the number of seeds that accepted the write.
	Succeeded int
	// Errors maps a seed address to its failure (only failing seeds appear).
	Errors map[string]error
}

// OK reports whether at least quorum seeds accepted the write (k-of-N).
func (w WriteResult) OK(quorum int) bool { return w.Succeeded >= quorum }

type seedConn struct {
	addr   string
	conn   *grpc.ClientConn
	agent  discoveryv1.AgentServiceClient
	health healthpb.HealthClient
}

// SeedClient fans out writes/reads to a fixed set of seeds. Connections are lazy
// (grpc.NewClient): a seed that is down fails only its own call.
type SeedClient struct {
	conns       []seedConn
	seedTimeout time.Duration
	prop        *observability.Propagation
	log         *slog.Logger
}

// SetPropagation attaches the propagation SLIs (clock-skew estimate). Optional.
func (c *SeedClient) SetPropagation(p *observability.Propagation) { c.prop = p }

// New dials every seed through the transport (its credentials are already baked
// in) and returns a SeedClient. seedTimeout bounds each per-seed RPC.
func New(seeds []string, t transport.Transport, seedTimeout time.Duration, log *slog.Logger) (*SeedClient, error) {
	if len(seeds) == 0 {
		return nil, errors.New("seedclient: no seeds")
	}
	if seedTimeout <= 0 {
		seedTimeout = defaultSeedTimeout
	}
	if log == nil {
		log = slog.Default()
	}

	conns := make([]seedConn, 0, len(seeds))
	for _, addr := range seeds {
		conn, err := t.Dial(context.Background(), addr)
		if err != nil {
			for _, c := range conns {
				_ = c.conn.Close()
			}
			return nil, fmt.Errorf("seedclient: dial %q: %w", addr, err)
		}
		conns = append(conns, seedConn{
			addr:   addr,
			conn:   conn,
			agent:  discoveryv1.NewAgentServiceClient(conn),
			health: healthpb.NewHealthClient(conn),
		})
	}
	return &SeedClient{conns: conns, seedTimeout: seedTimeout, log: log}, nil
}

// Seeds returns the configured seed addresses.
func (c *SeedClient) Seeds() []string {
	out := make([]string, len(c.conns))
	for i, sc := range c.conns {
		out[i] = sc.addr
	}
	return out
}

// Close closes every seed connection.
func (c *SeedClient) Close() error {
	var errs []error
	for _, sc := range c.conns {
		if err := sc.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// fanOut runs op against every seed concurrently, each under its own per-seed
// deadline, and aggregates a WriteResult. A hung seed is bounded by its deadline
// and never blocks the others. Results are collected under a mutex (no shared
// slice), so go test -race stays clean.
// forEachSeed runs fn against every seed concurrently, each with its own
// per-seed timeout context, and waits for all to finish. It is the one fan-out
// scaffold behind the write fan-out, Lookup, Dump and Reachable.
func (c *SeedClient) forEachSeed(ctx context.Context, fn func(cctx context.Context, sc seedConn)) {
	var wg sync.WaitGroup
	for _, sc := range c.conns {
		wg.Add(1)
		go func(sc seedConn) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, c.seedTimeout)
			defer cancel()
			fn(cctx, sc)
		}(sc)
	}
	wg.Wait()
}

func (c *SeedClient) fanOut(ctx context.Context, op func(context.Context, discoveryv1.AgentServiceClient) error) WriteResult {
	var mu sync.Mutex
	res := WriteResult{Total: len(c.conns), Errors: make(map[string]error)}

	c.forEachSeed(ctx, func(cctx context.Context, sc seedConn) {
		err := op(cctx, sc.agent)
		mu.Lock()
		if err != nil {
			res.Errors[sc.addr] = err
		} else {
			res.Succeeded++
		}
		mu.Unlock()
	})
	c.prop.ObserveFanout(res.Succeeded, res.Total)
	return res
}

// Register fans out a registration to every seed.
func (c *SeedClient) Register(ctx context.Context, reg model.Registration) WriteResult {
	req := &discoveryv1.RegisterRequest{Registration: protoconv.RegistrationToProto(reg)}
	return c.fanOut(ctx, func(cctx context.Context, cl discoveryv1.AgentServiceClient) error {
		_, err := cl.Register(cctx, req)
		return err
	})
}

// Renew fans out a node-scoped (optionally service-scoped) renew to every seed.
func (c *SeedClient) Renew(ctx context.Context, nodeID string, serviceIDs []string) WriteResult {
	req := &discoveryv1.RenewRequest{NodeId: nodeID, ServiceIds: serviceIDs}
	return c.fanOut(ctx, func(cctx context.Context, cl discoveryv1.AgentServiceClient) error {
		_, err := cl.Renew(cctx, req)
		return err
	})
}

// Deregister fans out a whole-node deregister to every seed.
func (c *SeedClient) Deregister(ctx context.Context, nodeID string) WriteResult {
	req := &discoveryv1.DeregisterRequest{NodeId: nodeID}
	return c.fanOut(ctx, func(cctx context.Context, cl discoveryv1.AgentServiceClient) error {
		_, err := cl.Deregister(cctx, req)
		return err
	})
}

// DeregisterService fans out a single-service deregister to every seed.
func (c *SeedClient) DeregisterService(ctx context.Context, nodeID, serviceID string) WriteResult {
	req := &discoveryv1.DeregisterServiceRequest{NodeId: nodeID, ServiceId: serviceID}
	return c.fanOut(ctx, func(cctx context.Context, cl discoveryv1.AgentServiceClient) error {
		_, err := cl.DeregisterService(cctx, req)
		return err
	})
}

// Lookup queries every seed and merges the results by data version (MergeLWW),
// tolerating partial failures. It errors only when no seed could be reached.
func (c *SeedClient) Lookup(ctx context.Context, q model.Query) (model.LookupResult, error) {
	req := &discoveryv1.LookupRequest{Query: protoconv.QueryToProto(q)}

	var (
		mu       sync.Mutex
		all      []model.ServiceEntry
		counts   []int
		maxIndex uint64
		okCount  int
		errs     []error
	)
	c.forEachSeed(ctx, func(cctx context.Context, sc seedConn) {
		resp, err := sc.agent.Lookup(cctx, req)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", sc.addr, err))
			return
		}
		okCount++
		lr := protoconv.LookupResultFromProto(resp)
		all = append(all, lr.Entries...)
		counts = append(counts, len(lr.Entries))
		if lr.Index > maxIndex {
			maxIndex = lr.Index
		}
	})

	c.prop.ObserveFanout(okCount, len(c.conns))
	if okCount == 0 {
		return model.LookupResult{}, fmt.Errorf("seedclient: lookup failed on all seeds: %w", errors.Join(errs...))
	}
	c.prop.SetClockSkew(estimateSkew(all))
	c.prop.SetDivergence(spread(counts))
	return model.LookupResult{Entries: health.MergeLWW(all), Index: maxIndex}, nil
}

// spread is max-min over the per-seed instance counts: 0 when seeds agree.
func spread(counts []int) int {
	if len(counts) == 0 {
		return 0
	}
	lo, hi := counts[0], counts[0]
	for _, c := range counts[1:] {
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	return hi - lo
}

// estimateSkew approximates the clock skew between seeds: for one registration
// (same node, service and generation) seen on several seeds, the spread of the
// server-stamped last_seen reflects clock skew (plus fan-out jitter). The max
// spread across registrations is reported. It is a coarse estimate by design —
// generation, not last_seen, decides the merge.
func estimateSkew(entries []model.ServiceEntry) time.Duration {
	type key struct {
		node, service string
		generation    uint64
	}
	type span struct{ min, max time.Time }
	spans := make(map[key]*span)
	for _, e := range entries {
		ts := e.Service.LastSeen
		if ts.IsZero() {
			continue
		}
		k := key{e.Node.ID, e.Service.ID, e.Service.Generation}
		s, ok := spans[k]
		if !ok {
			spans[k] = &span{min: ts, max: ts}
			continue
		}
		if ts.Before(s.min) {
			s.min = ts
		}
		if ts.After(s.max) {
			s.max = ts
		}
	}
	var maxSpread time.Duration
	for _, s := range spans {
		if d := s.max.Sub(s.min); d > maxSpread {
			maxSpread = d
		}
	}
	return maxSpread
}

// Dump returns each seed's raw (unmerged) instance set for q, keyed by seed
// address — the basis for the registry-dump divergence view.
func (c *SeedClient) Dump(ctx context.Context, q model.Query) map[string][]model.ServiceEntry {
	req := &discoveryv1.LookupRequest{Query: protoconv.QueryToProto(q)}
	var (
		mu  sync.Mutex
		out = make(map[string][]model.ServiceEntry, len(c.conns))
	)
	c.forEachSeed(ctx, func(cctx context.Context, sc seedConn) {
		resp, err := sc.agent.Lookup(cctx, req)
		mu.Lock()
		if err == nil {
			out[sc.addr] = protoconv.LookupResultFromProto(resp).Entries
		} else {
			out[sc.addr] = nil
		}
		mu.Unlock()
	})
	return out
}

// Reachable returns how many seeds currently report SERVING on grpc.health.v1.
func (c *SeedClient) Reachable(ctx context.Context) int {
	var (
		mu    sync.Mutex
		count int
	)
	c.forEachSeed(ctx, func(cctx context.Context, sc seedConn) {
		resp, err := sc.health.Check(cctx, &healthpb.HealthCheckRequest{})
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			mu.Lock()
			count++
			mu.Unlock()
		}
	})
	return count
}
