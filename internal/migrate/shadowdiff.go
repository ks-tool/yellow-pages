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

// Package migrate holds the Consul→yellow-pages migration helpers: a one-shot
// catalog import (backfill) and a NORMALIZED shadow-diff that compares two
// catalogs as a set keyed by (Node, ServiceID, Address, Port, Tags, status),
// ignoring volatile fields (X-Consul-Index, timestamps, LastContact, order).
package migrate

import (
	"sort"
	"strconv"
	"strings"
)

// ShadowEntry is the normalized identity of one service instance.
type ShadowEntry struct {
	Node      string
	ServiceID string
	Address   string
	Port      int
	Tags      []string
	Status    string // passing | warning | critical
}

// key is the order-independent canonical key of an entry.
func (e ShadowEntry) key() string {
	tags := append([]string(nil), e.Tags...)
	sort.Strings(tags)
	return strings.Join([]string{
		e.Node, e.ServiceID, e.Address, strconv.Itoa(e.Port), strings.Join(tags, ","), e.Status,
	}, "\x1f")
}

// Diff is the symmetric difference of two catalogs.
type Diff struct {
	OnlyLeft  []ShadowEntry // present in left, missing in right
	OnlyRight []ShadowEntry // present in right, missing in left
}

// Empty reports whether the two catalogs are equivalent.
func (d Diff) Empty() bool { return len(d.OnlyLeft) == 0 && len(d.OnlyRight) == 0 }

// ShadowDiff compares two catalogs as sets, ignoring order and duplicates.
func ShadowDiff(left, right []ShadowEntry) Diff {
	l := index(left)
	r := index(right)

	var d Diff
	for k, e := range l {
		if _, ok := r[k]; !ok {
			d.OnlyLeft = append(d.OnlyLeft, e)
		}
	}
	for k, e := range r {
		if _, ok := l[k]; !ok {
			d.OnlyRight = append(d.OnlyRight, e)
		}
	}
	return d
}

func index(entries []ShadowEntry) map[string]ShadowEntry {
	m := make(map[string]ShadowEntry, len(entries))
	for _, e := range entries {
		m[e.key()] = e
	}
	return m
}
