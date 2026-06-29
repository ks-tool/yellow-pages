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

// Package membership provides the seed tier's self-healing (M18, v1.x,
// feature-flagged): snapshot-on-join (a joining/recovered seed pulls peers'
// state before it reports ready) and pull-based anti-entropy (a periodic LWW
// merge that collapses divergence between seeds). It uses the existing Lookup
// RPC — no new wire contract — and the store's LWW Merge, so a concurrent live
// write is never lost.
package membership

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
)

// Puller fetches a peer snapshot (the merged registry of the other seeds). A
// seedclient.SeedClient over the peer seeds satisfies it.
type Puller interface {
	Lookup(ctx context.Context, q model.Query) (model.LookupResult, error)
	Reachable(ctx context.Context) int
	Seeds() []string
}

// Merger is the local registry the snapshot is merged into (store.Memory).
type Merger interface {
	Merge(entries []model.ServiceEntry) int
}

// Gate flips the seed's readiness once the snapshot completes (server Readiness).
type Gate interface {
	SetReady(bool)
}

// Options configures a Syncer.
type Options struct {
	Self     string // local seed address (for /v1/agent/members)
	Peers    Puller
	Store    Merger
	Interval time.Duration
	Clock    clock.Clock
	Gate     Gate                       // optional readiness gate (snapshot-on-join)
	Prop     *observability.Propagation // optional convergence-lag SLI
	Log      *slog.Logger
}

// Syncer is the membership/anti-entropy component.
type Syncer struct {
	self     string
	peers    Puller
	store    Merger
	interval time.Duration
	clock    clock.Clock
	gate     Gate
	prop     *observability.Propagation
	log      *slog.Logger

	readyOnce sync.Once
	readyCh   chan struct{}
}

// New builds the Syncer.
func New(opts Options) *Syncer {
	if opts.Interval <= 0 {
		opts.Interval = 30 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Syncer{
		self: opts.Self, peers: opts.Peers, store: opts.Store, interval: opts.Interval,
		clock: opts.Clock, gate: opts.Gate, prop: opts.Prop, log: opts.Log,
		readyCh: make(chan struct{}),
	}
}

// Name identifies the component.
func (s *Syncer) Name() string { return "membership" }

// Start performs the join snapshot, marks the seed ready, then runs anti-entropy
// on a ticker until ctx ends.
func (s *Syncer) Start(ctx context.Context) error {
	s.syncOnce(ctx) // snapshot-on-join: pull peer state before serving reads
	s.markReady()

	ticker := s.clock.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			s.syncOnce(ctx)
		}
	}
}

// Stop is a no-op (the peer client is owned by the caller).
func (s *Syncer) Stop(context.Context) error { return nil }

// Ready is closed once the join snapshot has been applied and the seed marked
// ready — the no-false-negative gate.
func (s *Syncer) Ready() <-chan struct{} { return s.readyCh }

func (s *Syncer) markReady() {
	s.readyOnce.Do(func() {
		if s.gate != nil {
			s.gate.SetReady(true)
		}
		close(s.readyCh)
	})
}

func (s *Syncer) syncOnce(ctx context.Context) {
	res, err := s.peers.Lookup(ctx, model.Query{})
	if err != nil {
		s.log.Warn("anti-entropy pull failed", "error", err)
		s.prop.SetConvergenceLag(0)
		return
	}
	applied := s.store.Merge(res.Entries)
	s.prop.SetConvergenceLag(applied)
	if applied > 0 {
		s.log.Info("anti-entropy reconciled divergence", "applied", applied)
	}
}

// Member is one seed's membership status.
type Member struct {
	Name   string `json:"Name"`
	Addr   string `json:"Addr"`
	Status string `json:"Status"` // "alive" | "failed"
}

// Members reports the live membership: self plus each peer, with liveness from
// the peer health probes (best-effort).
func (s *Syncer) Members(ctx context.Context) []Member {
	members := []Member{{Name: s.self, Addr: s.self, Status: "alive"}}
	peers := s.peers.Seeds()
	reachable := s.peers.Reachable(ctx)
	for i, addr := range peers {
		status := "failed"
		if i < reachable { // probes report a count, not per-peer; approximate
			status = "alive"
		}
		members = append(members, Member{Name: addr, Addr: addr, Status: status})
	}
	return members
}
