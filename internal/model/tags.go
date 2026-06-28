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

package model

import "strings"

// TagsMap returns a derived, convenience map-view of the raw Tags for native
// consumers. It is NOT the source of truth — Tags (ordered raw []string) is.
//
// A tag without '=' maps to key -> "" ("key"); a tag "key=value" maps to
// key -> "value", splitting on the FIRST '=' so values may themselves contain
// '=' (base64, query strings). Because a map cannot preserve order or duplicate
// keys, the first occurrence of a key wins; matching/round-trip must use Tags.
func (s ServiceInstance) TagsMap() map[string]string {
	if len(s.Tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(s.Tags))
	for _, t := range s.Tags {
		key, value, _ := strings.Cut(t, "=")
		if _, ok := m[key]; ok {
			continue // keep the first occurrence
		}
		m[key] = value
	}
	return m
}

// HasTag reports whether the raw tag is present (exact, case-sensitive match).
func (s ServiceInstance) HasTag(tag string) bool {
	for _, t := range s.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// MatchesTags reports whether every wanted raw tag is present (AND semantics).
func (s ServiceInstance) MatchesTags(wanted []string) bool {
	for _, w := range wanted {
		if !s.HasTag(w) {
			return false
		}
	}
	return true
}
