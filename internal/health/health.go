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

// Package health holds the two pure read-path functions shared by every
// transport surface (native gRPC, Consul HTTP, Consul DNS): the cross-seed
// MergeLWW reconciliation and the health/visibility Filter. Keeping them here,
// stateless and dependency-free (only internal/model), guarantees the three
// surfaces stay consistent — they all merge and filter through the same code.
package health

import (
	"sort"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// FilterOptions controls the visibility filter applied at the read path.
type FilterOptions struct {
	// OnlyPassing drops critical and maintenance entries (Consul ?passing /
	// DNS only_passing). Warning is treated as passing (Consul default).
	OnlyPassing bool
}

// Visible reports whether e is visible under opts. Without OnlyPassing every
// entry is visible (catalog/health without ?passing show critical and
// maintenance records too). With OnlyPassing, maintenance and critical entries
// are hidden while warning is treated as passing.
func Visible(e model.ServiceEntry, opts FilterOptions) bool {
	if !opts.OnlyPassing {
		return true
	}
	if e.Maintenance {
		return false
	}
	switch e.Health {
	case model.HealthPassing, model.HealthWarning:
		return true
	default:
		return false
	}
}

// Filter returns the entries from in that are Visible under opts, preserving
// their order. The input is never mutated and a fresh slice is always returned,
// so callers (native gRPC, Consul HTTP, Consul DNS) can share one input and
// each get an independent, consistent result.
func Filter(in []model.ServiceEntry, opts FilterOptions) []model.ServiceEntry {
	out := make([]model.ServiceEntry, 0, len(in))
	for _, e := range in {
		if Visible(e, opts) {
			out = append(out, e)
		}
	}
	return out
}

// MergeLWW deduplicates entries that describe the same service instance
// (Node.ID, Service.ID) across seeds and keeps a single winner per identity.
//
// The winner is chosen by the deterministic last-writer-wins key: PRIMARY by
// max(Generation) — the client-supplied data version, identical on every seed
// for one registration — and only as a coarse tiebreak, when generations are
// equal, by max(LastSeen) (server-stamped). Generation first is what makes a
// stale endpoint on a seed that merely collected more renews lose to the fresher
// data version. The output is sorted by (Node.ID, Service.ID), so the result is
// independent of the input order. The input slice is not mutated.
func MergeLWW(in []model.ServiceEntry) []model.ServiceEntry {
	if len(in) == 0 {
		return nil
	}

	type key struct{ node, service string }
	winners := make(map[key]model.ServiceEntry, len(in))
	for _, e := range in {
		k := key{e.Node.ID, e.Service.ID}
		if cur, ok := winners[k]; ok && !worseThan(cur, e) {
			continue
		}
		winners[k] = e
	}

	out := make([]model.ServiceEntry, 0, len(winners))
	for _, e := range winners {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node.ID != out[j].Node.ID {
			return out[i].Node.ID < out[j].Node.ID
		}
		return out[i].Service.ID < out[j].Service.ID
	})
	return out
}

// worseThan reports whether a should lose to b under the LWW key: lower
// Generation, or equal Generation with an earlier server-stamped LastSeen.
func worseThan(a, b model.ServiceEntry) bool {
	if a.Service.Generation != b.Service.Generation {
		return a.Service.Generation < b.Service.Generation
	}
	return a.Service.LastSeen.Before(b.Service.LastSeen)
}
