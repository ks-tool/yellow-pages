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

import (
	"reflect"
	"testing"
)

func TestTagsMap(t *testing.T) {
	t.Parallel()

	s := ServiceInstance{Tags: []string{
		"lone",
		"k=v",
		"a=b=c",    // split on first '='
		"k=second", // duplicate key: first wins
		"empty=",
	}}

	want := map[string]string{
		"lone":  "",
		"k":     "v",
		"a":     "b=c",
		"empty": "",
	}
	if got := s.TagsMap(); !reflect.DeepEqual(got, want) {
		t.Errorf("TagsMap() = %#v, want %#v", got, want)
	}

	if got := (ServiceInstance{}).TagsMap(); got != nil {
		t.Errorf("TagsMap() with no tags = %#v, want nil", got)
	}
}

func TestMatchesTags(t *testing.T) {
	t.Parallel()

	s := ServiceInstance{Tags: []string{"primary", "v2", "zone=a"}}

	cases := []struct {
		name   string
		wanted []string
		want   bool
	}{
		{"single present", []string{"v2"}, true},
		{"exact raw with =", []string{"zone=a"}, true},
		{"all present (AND)", []string{"primary", "v2"}, true},
		{"one missing", []string{"primary", "v3"}, false},
		{"case sensitive", []string{"V2"}, false},
		{"prefix is not a match", []string{"zone"}, false},
		{"empty matches", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := s.MatchesTags(tc.wanted); got != tc.want {
				t.Errorf("MatchesTags(%v) = %v, want %v", tc.wanted, got, tc.want)
			}
		})
	}
}
