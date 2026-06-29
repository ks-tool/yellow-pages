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

package healthcheck

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

// Keeper refreshes a hosted service's lease while its checks pass. The agent's
// Proxy satisfies it; KeepAlive must be idempotent (re-create an expired service).
type Keeper interface {
	KeepAlive(ctx context.Context, serviceID string) error
}

// Options configures the Monitor.
type Options struct {
	Keeper       Keeper // required
	EnableScript bool   // allow exec/script checks (off by default)
	Clock        clock.Clock
	Log          *slog.Logger
}

// Monitor runs active checks for hosted services and gates their liveness. It is
// an app.Component; check loops are bound to its own lifetime so Set/Remove may
// be called any time (including before Start).
type Monitor struct {
	keeper       Keeper
	enableScript bool
	clock        clock.Clock
	log          *slog.Logger

	rootCtx    context.Context
	rootCancel context.CancelFunc

	mu       sync.Mutex
	services map[string]*svcState
}

type svcState struct {
	cancel  context.CancelFunc
	healthy bool
}

// New builds the Monitor.
func New(opts Options) *Monitor {
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is stored as rootCancel and called in Stop
	return &Monitor{
		keeper: opts.Keeper, enableScript: opts.EnableScript, clock: opts.Clock, log: opts.Log,
		rootCtx: ctx, rootCancel: cancel, services: make(map[string]*svcState),
	}
}

// Name identifies the component.
func (m *Monitor) Name() string { return "healthchecks" }

// Start blocks until the app context ends (checks run under the Monitor's own
// context, cancelled by Stop).
func (m *Monitor) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Stop cancels every running check.
func (m *Monitor) Stop(context.Context) error {
	m.rootCancel()
	m.mu.Lock()
	m.services = make(map[string]*svcState)
	m.mu.Unlock()
	return nil
}

// Set installs (or replaces) the active checks for a service. An empty defs
// removes it. Returns whether the service now has active checks.
func (m *Monitor) Set(serviceID string, defs []Definition) {
	m.Remove(serviceID)
	if len(defs) == 0 {
		return
	}
	ctx, cancel := context.WithCancel(m.rootCtx) //nolint:gosec // G118: cancel is stored on svcState and called by Remove/Stop
	m.mu.Lock()
	m.services[serviceID] = &svcState{cancel: cancel, healthy: true}
	m.mu.Unlock()
	go m.loop(ctx, serviceID, defs)
}

// Remove stops checking a service (e.g. on deregister).
func (m *Monitor) Remove(serviceID string) {
	m.mu.Lock()
	if s, ok := m.services[serviceID]; ok {
		s.cancel()
		delete(m.services, serviceID)
	}
	m.mu.Unlock()
}

// Active reports whether a service has active checks — such services are managed
// by the Monitor and excluded from the agent's blanket renew loop.
func (m *Monitor) Active(serviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.services[serviceID]
	return ok
}

func (m *Monitor) loop(ctx context.Context, serviceID string, defs []Definition) {
	ticker := m.clock.NewTicker(minInterval(defs))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			healthy := true
			for _, d := range defs {
				if err := probe(ctx, d, m.enableScript); err != nil {
					healthy = false
					m.log.Warn("service check failed", "service", serviceID, "kind", d.Kind, "error", err.Error())
					break
				}
			}
			if healthy {
				// Refresh the lease; while failing we do nothing so the lease
				// lapses and the registry shows the instance critical (then drops it).
				if err := m.keeper.KeepAlive(ctx, serviceID); err != nil {
					m.log.Warn("keep-alive after passing check failed", "service", serviceID, "error", err.Error())
				}
			}
			m.note(serviceID, healthy)
		}
	}
}

// note logs a passing<->critical transition once.
func (m *Monitor) note(serviceID string, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.services[serviceID]
	if !ok || s.healthy == healthy {
		return
	}
	s.healthy = healthy
	if healthy {
		m.log.Info("service checks recovering", "service", serviceID)
	} else {
		m.log.Warn("service checks failing — lease will lapse to critical", "service", serviceID)
	}
}

func minInterval(defs []Definition) time.Duration {
	lowest := defs[0].interval()
	for _, d := range defs[1:] {
		if iv := d.interval(); iv < lowest {
			lowest = iv
		}
	}
	return lowest
}
