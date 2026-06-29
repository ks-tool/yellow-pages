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

// Package resolver bootstraps the set of seed addresses an agent writes to and
// reads from. A SeedProvider yields seeds; the Static provider returns a fixed
// list, the Plugin provider runs an external executable, and Merged combines
// providers with a NON-DESTRUCTIVE ordered merge: order is preserved, duplicates
// collapse, and a failing provider never wipes the seeds resolved by the others.
// Phase 2 (real membership / anti-entropy) lands behind this seam in M18.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// SeedProvider yields the seed addresses (host:port) for the cluster.
type SeedProvider interface {
	// Name identifies the provider in logs.
	Name() string
	// Seeds returns the resolved seed addresses, or an error if it could not.
	Seeds(ctx context.Context) ([]string, error)
}

// Static is a SeedProvider backed by a fixed configured list.
type Static struct {
	seeds []string
}

// NewStatic builds a Static provider over a copy of seeds.
func NewStatic(seeds []string) *Static {
	return &Static{seeds: append([]string(nil), seeds...)}
}

// Name identifies the provider.
func (s *Static) Name() string { return "static" }

// Seeds returns a copy of the configured seeds.
func (s *Static) Seeds(context.Context) ([]string, error) {
	return append([]string(nil), s.seeds...), nil
}

// Merged combines providers with a non-destructive ordered merge.
type Merged struct {
	providers []SeedProvider
	log       *slog.Logger
}

// NewMerged combines providers in order. A nil log defaults to slog.Default.
func NewMerged(log *slog.Logger, providers ...SeedProvider) *Merged {
	if log == nil {
		log = slog.Default()
	}
	return &Merged{providers: providers, log: log}
}

// Name identifies the provider.
func (m *Merged) Name() string { return "merged" }

// Seeds resolves every provider in order and returns the deduplicated union,
// preserving first-seen order. A provider that errors is logged and skipped —
// its failure does not drop seeds already gathered (non-destructive). An error
// is returned only when no seeds could be resolved at all.
func (m *Merged) Seeds(ctx context.Context) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	var errs []error

	for _, p := range m.providers {
		seeds, err := p.Seeds(ctx)
		if err != nil {
			m.log.Warn("seed provider failed; preserving seeds from other providers",
				"provider", p.Name(), "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", p.Name(), err))
			continue
		}
		for _, s := range seeds {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}

	if len(out) == 0 {
		if len(errs) > 0 {
			return nil, fmt.Errorf("resolver: no seeds resolved: %w", errors.Join(errs...))
		}
		return nil, errors.New("resolver: no seeds resolved")
	}
	return out, nil
}
