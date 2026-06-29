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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// serviceDefFile is a Consul service-definition file: {"service": {...}} and/or
// {"services": [...]}, the same shape as the HTTP register body.
type serviceDefFile struct {
	Service  *registerInput  `json:"service"`
	Services []registerInput `json:"services"`
}

// node identity for service-def registrations is supplied by the Registry
// implementation (it stamps the agent's node).
type defRegistrar interface {
	RegisterServices(ctx context.Context, reg model.Registration) error
}

// LoadServiceDefs reads every *.json service-definition file in dir and registers
// the services it declares. Returns the number registered. A bad file is logged
// and skipped (best-effort, like Consul).
func LoadServiceDefs(ctx context.Context, dir string, reg defRegistrar, checks ChecksReporter, log *slog.Logger) (int, error) {
	if dir == "" {
		return 0, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		inputs, perr := parseServiceDef(path)
		if perr != nil {
			log.Warn("skipping invalid service-definition file", "path", path, "error", perr)
			continue
		}
		for _, in := range inputs {
			if in.Name == "" {
				continue
			}
			svc := inputToService(in)
			regn := model.Registration{Services: []model.ServiceInstance{svc}, Generation: 1}
			if rerr := reg.RegisterServices(ctx, regn); rerr != nil {
				log.Warn("service-definition register failed", "service", in.Name, "error", rerr)
				continue
			}
			if checks != nil {
				checks.Set(svc.ID, activeChecks(in))
			}
			count++
		}
	}
	return count, nil
}

func parseServiceDef(path string) ([]registerInput, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-provided config dir
	if err != nil {
		return nil, err
	}
	var f serviceDefFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	var inputs []registerInput
	if f.Service != nil {
		inputs = append(inputs, *f.Service)
	}
	inputs = append(inputs, f.Services...)
	return inputs, nil
}

// Loader is an app.Component that registers service-definition files at start and
// re-reads them on SIGHUP (hot-reload without a restart).
type Loader struct {
	dir    string
	reg    defRegistrar
	checks ChecksReporter
	log    *slog.Logger
}

// NewLoader builds the service-definition loader. checks (optional) starts any
// active health checks declared in the files.
func NewLoader(dir string, reg defRegistrar, checks ChecksReporter, log *slog.Logger) *Loader {
	if log == nil {
		log = slog.Default()
	}
	return &Loader{dir: dir, reg: reg, checks: checks, log: log}
}

// Name identifies the component.
func (l *Loader) Name() string { return "service-defs" }

// Start loads the definitions once, then reloads on every SIGHUP until ctx ends.
func (l *Loader) Start(ctx context.Context) error {
	if l.dir == "" {
		<-ctx.Done()
		return nil
	}
	l.reload(ctx)

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-hup:
			l.reload(ctx)
		}
	}
}

// Stop is a no-op.
func (l *Loader) Stop(context.Context) error { return nil }

func (l *Loader) reload(ctx context.Context) {
	n, err := LoadServiceDefs(ctx, l.dir, l.reg, l.checks, l.log)
	if err != nil {
		l.log.Warn("service-definition reload failed", "dir", l.dir, "error", err)
		return
	}
	l.log.Info("loaded service definitions", "dir", l.dir, "count", n)
}
