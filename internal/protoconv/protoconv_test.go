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

package protoconv

import (
	"reflect"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/model"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// trickyTags exercises the cases a map<string,string> would corrupt: multiple
// '=', duplicate "prefix" before '=', empty key/value, and exact duplicates.
// The raw ordered list must survive a round-trip byte-for-byte.
var trickyTags = []string{
	"urlprefix-/api",
	"urlprefix-/api2",
	"traefik.http.routers.r.rule=Host(`x`)",
	"a=b=c",
	"lone",
	"k=",
	"=v",
	"dup",
	"dup",
}

func TestServiceDefinitionRoundTrip(t *testing.T) {
	t.Parallel()

	cases := map[string]model.ServiceInstance{
		"full": {
			ID:      "web-1",
			Name:    "web",
			Address: "10.0.0.5",
			Port:    8080,
			Tags:    trickyTags,
			Meta:    map[string]string{"version": "v2", "zone": "a"},
			Weights: model.Weights{Passing: 10, Warning: 1},
			TTL:     30 * time.Second,
		},
		"minimal": {
			ID:   "x",
			Name: "x",
		},
		"empty": {},
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := ServiceFromProto(ServiceToProto(in))
			if !reflect.DeepEqual(got, in) {
				t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
			}
		})
	}
}

func TestRegistrationRoundTrip(t *testing.T) {
	t.Parallel()

	in := model.Registration{
		Node: model.Node{
			ID:              "agent-1",
			Name:            "node-a",
			Address:         "10.0.0.5",
			Datacenter:      "dc1",
			Meta:            map[string]string{"rack": "r1"},
			TaggedAddresses: map[string]string{"lan": "10.0.0.5", "wan": "1.2.3.4"},
		},
		Services: []model.ServiceInstance{
			{ID: "web-1", Name: "web", Address: "10.0.0.5", Port: 8080, Tags: trickyTags, TTL: 15 * time.Second},
			{ID: "db-1", Name: "db", Address: "10.0.0.6", Port: 5432, Weights: model.Weights{Passing: 1, Warning: 1}},
		},
		Generation: 42,
	}

	got := RegistrationFromProto(RegistrationToProto(in))
	if !reflect.DeepEqual(got, in) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
	}
}

func TestServiceEntryRoundTrip(t *testing.T) {
	t.Parallel()

	in := model.ServiceEntry{
		Node: model.Node{ID: "agent-1", Name: "node-a", Address: "10.0.0.5", Datacenter: "dc1"},
		Service: model.ServiceInstance{
			ID:         "web-1",
			Name:       "web",
			Address:    "10.0.0.5",
			Port:       8080,
			Tags:       trickyTags,
			Meta:       map[string]string{"version": "v2"},
			Weights:    model.Weights{Passing: 5, Warning: 2},
			TTL:        30 * time.Second,
			LastSeen:   time.Unix(0, 1_700_000_000_123_456_789).UTC(),
			Generation: 7,
		},
		Health:      model.HealthCritical,
		Maintenance: true,
	}

	got := EntryFromProto(EntryToProto(in))
	if !reflect.DeepEqual(got, in) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
	}
}

func TestLookupResultRoundTrip(t *testing.T) {
	t.Parallel()

	in := model.LookupResult{
		Index: 99,
		Entries: []model.ServiceEntry{
			{
				Node:    model.Node{ID: "a1", Datacenter: "dc1"},
				Service: model.ServiceInstance{ID: "s1", Name: "s", Port: 1, Generation: 3, LastSeen: time.Unix(0, 5).UTC()},
				Health:  model.HealthPassing,
			},
		},
	}

	got := LookupResultFromProto(LookupResultToProto(in))
	if !reflect.DeepEqual(got, in) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
	}
}

func TestQueryRoundTrip(t *testing.T) {
	t.Parallel()

	in := model.Query{Name: "web", Datacenter: "dc2", Tags: []string{"a", "b=c"}, OnlyHealthy: true}
	got := QueryFromProto(QueryToProto(in))
	if !reflect.DeepEqual(got, in) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
	}
}

func TestChangeEventRoundTrip(t *testing.T) {
	t.Parallel()

	entry := model.ServiceEntry{
		Node:    model.Node{ID: "a1", Datacenter: "dc1"},
		Service: model.ServiceInstance{ID: "s1", Name: "s", Generation: 4, LastSeen: time.Unix(0, 7).UTC()},
		Health:  model.HealthWarning,
	}
	in := model.ChangeEvent{Type: model.ChangeDelete, Entry: entry, Index: 17}

	got := ChangeEventFromProto(ChangeEventToProto(in), in.Index)
	if !reflect.DeepEqual(got, in) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, in)
	}
}

func TestHealthEnumRoundTrip(t *testing.T) {
	t.Parallel()

	for _, h := range []model.HealthState{
		model.HealthUnspecified, model.HealthPassing, model.HealthWarning, model.HealthCritical,
	} {
		if got := HealthFromProto(HealthToProto(h)); got != h {
			t.Errorf("health round-trip: got %v, want %v", got, h)
		}
	}
}

func TestChangeTypeEnumRoundTrip(t *testing.T) {
	t.Parallel()

	for _, c := range []model.ChangeType{model.ChangeUnspecified, model.ChangePut, model.ChangeDelete} {
		if got := ChangeTypeFromProto(ChangeTypeToProto(c)); got != c {
			t.Errorf("change-type round-trip: got %v, want %v", got, c)
		}
	}
}

func TestNilProtoInputs(t *testing.T) {
	t.Parallel()

	// Converting nil wire pointers must not panic and must yield zero values.
	if got := NodeFromProto(nil); !reflect.DeepEqual(got, model.Node{}) {
		t.Errorf("NodeFromProto(nil) = %+v", got)
	}
	if got := ServiceFromProto(nil); !reflect.DeepEqual(got, model.ServiceInstance{}) {
		t.Errorf("ServiceFromProto(nil) = %+v", got)
	}
	if got := RegistrationFromProto(nil); !reflect.DeepEqual(got, model.Registration{}) {
		t.Errorf("RegistrationFromProto(nil) = %+v", got)
	}
	if got := EntryFromProto(nil); !reflect.DeepEqual(got, model.ServiceEntry{}) {
		t.Errorf("EntryFromProto(nil) = %+v", got)
	}
}

func TestTtlSecondsTruncation(t *testing.T) {
	t.Parallel()

	// Sub-second TTL truncates to whole seconds on the wire (documented).
	p := ServiceToProto(model.ServiceInstance{TTL: 1500 * time.Millisecond})
	if p.GetTtlSeconds() != 1 {
		t.Errorf("ttl_seconds = %d, want 1", p.GetTtlSeconds())
	}
	if got := ServiceFromProto(&discoveryv1.Service{TtlSeconds: 45}).TTL; got != 45*time.Second {
		t.Errorf("TTL = %s, want 45s", got)
	}
}
