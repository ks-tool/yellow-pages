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

// Package federation provides cross-datacenter lookups (M17, v1.x, feature-flagged).
// A Pool fans a ?dc/.dc.consul query out to a remote cluster's seeds and merges
// the result with the same LWW logic as a local read. Remote seeds serve their
// own registry and do NOT re-federate, so a single hop cannot loop; the Pool
// only ever queries datacenters it is explicitly configured for (no storm on an
// unknown dc), and MaxHops bounds the configured depth.
package federation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/seedclient"
	"github.com/ks-tool/yellow-pages/internal/transport"
)

// Pool routes remote-datacenter reads to the matching cluster's seeds.
type Pool struct {
	local   string
	maxHops int
	clients map[string]*seedclient.SeedClient
	log     *slog.Logger
}

// NewPool dials one seed client per remote datacenter. The local datacenter is
// skipped (it is served by the normal local path).
func NewPool(local string, maxHops int, datacenters map[string][]string, t transport.Transport, timeout time.Duration, log *slog.Logger) (*Pool, error) {
	if log == nil {
		log = slog.Default()
	}
	if maxHops <= 0 {
		maxHops = 1
	}
	clients := make(map[string]*seedclient.SeedClient, len(datacenters))
	for dc, seeds := range datacenters {
		if dc == local || len(seeds) == 0 {
			continue
		}
		c, err := seedclient.New(seeds, t, timeout, log)
		if err != nil {
			return nil, fmt.Errorf("federation: dial dc %q: %w", dc, err)
		}
		clients[dc] = c
	}
	return &Pool{local: local, maxHops: maxHops, clients: clients, log: log}, nil
}

// IsRemote reports whether dc is a configured remote datacenter (so the read
// must be federated). The empty dc and the local dc are never remote.
func (p *Pool) IsRemote(dc string) bool {
	if dc == "" || dc == p.local {
		return false
	}
	_, ok := p.clients[dc]
	return ok
}

// Resolve federates the query to dc's seeds. An unknown dc returns an empty
// result (Consul-like) rather than fanning out — the loop/storm guard.
func (p *Pool) Resolve(ctx context.Context, dc string, q model.Query) (model.LookupResult, error) {
	c, ok := p.clients[dc]
	if !ok {
		return model.LookupResult{}, nil
	}
	rq := q
	rq.Datacenter = dc // provenance: results carry the source dc on each Node
	return c.Lookup(ctx, rq)
}

// Datacenters returns the configured remote datacenter names.
func (p *Pool) Datacenters() []string {
	out := make([]string, 0, len(p.clients))
	for dc := range p.clients {
		out = append(out, dc)
	}
	return out
}

// Close releases the remote seed connections.
func (p *Pool) Close() error {
	for _, c := range p.clients {
		_ = c.Close()
	}
	return nil
}

// Name identifies the component.
func (p *Pool) Name() string { return "federation" }

// Start is passive: it idles until shutdown (reads are driven by the read path).
func (p *Pool) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Stop releases the remote connections.
func (p *Pool) Stop(context.Context) error { return p.Close() }
