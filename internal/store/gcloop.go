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

package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

const defaultGCInterval = 5 * time.Second

// GCLoop is an app.Component that periodically reaps expired registrations and
// reconciles lease transitions on a Store. Seeds run it so per-service leases
// that stop being renewed eventually expire past their grace window.
type GCLoop struct {
	store    Store
	interval time.Duration
	clock    clock.Clock
	log      *slog.Logger
	onReap   func(removed, size int)
}

// NewGCLoop builds a GC loop over s ticking every interval (default 5s). A nil
// clock/log default to the system clock and slog.Default.
func NewGCLoop(s Store, interval time.Duration, clk clock.Clock, log *slog.Logger) *GCLoop {
	if interval <= 0 {
		interval = defaultGCInterval
	}
	if clk == nil {
		clk = clock.System()
	}
	if log == nil {
		log = slog.Default()
	}
	return &GCLoop{store: s, interval: interval, clock: clk, log: log}
}

// WithReapHook sets a callback invoked after every GC tick with the number
// reaped and the current registry size (for metrics). Returns the loop.
func (g *GCLoop) WithReapHook(fn func(removed, size int)) *GCLoop {
	g.onReap = fn
	return g
}

// Name identifies the component.
func (g *GCLoop) Name() string { return "store-gc" }

// Start runs the GC tick until ctx is cancelled.
func (g *GCLoop) Start(ctx context.Context) error {
	ticker := g.clock.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			n := g.store.GC()
			if n > 0 {
				g.log.Debug("reaped expired registrations", "count", n)
			}
			if g.onReap != nil {
				g.onReap(n, g.store.Size())
			}
		}
	}
}

// Stop is a no-op; Start returns when its context is cancelled.
func (g *GCLoop) Stop(context.Context) error { return nil }
