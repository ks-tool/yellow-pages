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
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/observability"
)

// RenewLoop is an app.Component that periodically renews the agent's hosted
// leases on the seeds, keeping them from expiring while the agent is alive.
type RenewLoop struct {
	proxy    *Proxy
	interval time.Duration
	clock    clock.Clock
	log      *slog.Logger
}

// NewRenewLoop builds the renew loop ticking every interval.
func NewRenewLoop(proxy *Proxy, interval time.Duration, clk clock.Clock, log *slog.Logger) *RenewLoop {
	if clk == nil {
		clk = clock.System()
	}
	if log == nil {
		log = slog.Default()
	}
	return &RenewLoop{proxy: proxy, interval: interval, clock: clk, log: log}
}

// Name identifies the component.
func (l *RenewLoop) Name() string { return "renew-loop" }

// Start renews on every tick until ctx is cancelled.
func (l *RenewLoop) Start(ctx context.Context) error {
	ticker := l.clock.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			l.renewOnce(ctx)
		}
	}
}

func (l *RenewLoop) renewOnce(ctx context.Context) {
	if l.proxy.hostedCount() == 0 {
		return
	}
	res := l.proxy.renewAll(ctx)
	if res.Total > 0 && res.Succeeded == 0 {
		l.log.Warn("renew reached no seeds", "total", res.Total, "errors", len(res.Errors))
	}
}

// Stop is a no-op; Start returns when its context is cancelled.
func (l *RenewLoop) Stop(context.Context) error { return nil }

// ReadinessProbe is an app.Component that drives the gRPC readiness gate from
// seed connectivity: the agent is READY only while at least minSeeds seeds are
// reachable (k-of-N).
type ReadinessProbe struct {
	client    *SeedClient
	readiness *observability.Readiness
	minSeeds  int
	interval  time.Duration
	clock     clock.Clock
	log       *slog.Logger
}

// NewReadinessProbe builds the probe. minSeeds < 1 is clamped to 1.
func NewReadinessProbe(client *SeedClient, readiness *observability.Readiness, minSeeds int, interval time.Duration, clk clock.Clock, log *slog.Logger) *ReadinessProbe {
	if minSeeds < 1 {
		minSeeds = 1
	}
	if clk == nil {
		clk = clock.System()
	}
	if log == nil {
		log = slog.Default()
	}
	return &ReadinessProbe{client: client, readiness: readiness, minSeeds: minSeeds, interval: interval, clock: clk, log: log}
}

// Name identifies the component.
func (p *ReadinessProbe) Name() string { return "readiness-probe" }

// Start probes immediately and then on every tick until ctx is cancelled.
func (p *ReadinessProbe) Start(ctx context.Context) error {
	p.probe(ctx)
	ticker := p.clock.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			p.probe(ctx)
		}
	}
}

func (p *ReadinessProbe) probe(ctx context.Context) {
	reachable := p.client.Reachable(ctx)
	if ctx.Err() != nil {
		return // shutting down: leave readiness to the server's drain
	}
	p.readiness.SetReady(reachable >= p.minSeeds)
}

// Stop is a no-op; the server component flips readiness off during drain.
func (p *ReadinessProbe) Stop(context.Context) error { return nil }

// Deregistrar is an app.Component whose Stop deregisters the agent from every
// seed and closes the connections. Ordered to stop last, it runs after the
// server has stopped accepting, completing the drain: deregister, then close.
type Deregistrar struct {
	client *SeedClient
	nodeID string
	log    *slog.Logger
}

// NewDeregistrar builds the shutdown deregistrar for nodeID.
func NewDeregistrar(client *SeedClient, nodeID string, log *slog.Logger) *Deregistrar {
	if log == nil {
		log = slog.Default()
	}
	return &Deregistrar{client: client, nodeID: nodeID, log: log}
}

// Name identifies the component.
func (d *Deregistrar) Name() string { return "deregistrar" }

// Start blocks until ctx is cancelled; the work happens in Stop.
func (d *Deregistrar) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Stop deregisters the agent's node from every seed (bounded by ctx) and closes
// the seed connections.
func (d *Deregistrar) Stop(ctx context.Context) error {
	res := d.client.Deregister(ctx, d.nodeID)
	d.log.Info("deregistered on shutdown", "succeeded", res.Succeeded, "total", res.Total)
	return d.client.Close()
}
