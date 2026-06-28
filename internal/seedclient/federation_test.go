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

package seedclient

import (
	"context"
	"testing"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// stubRouter routes only "dc2" and records the dc it was asked for.
type stubRouter struct {
	calledDC string
	entries  []model.ServiceEntry
}

func (s *stubRouter) IsRemote(dc string) bool { return dc == "dc2" }
func (s *stubRouter) Resolve(_ context.Context, dc string, _ model.Query) (model.LookupResult, error) {
	s.calledDC = dc
	return model.LookupResult{Entries: s.entries}, nil
}

func TestProxyRoutesRemoteDC(t *testing.T) {
	t.Parallel()
	router := &stubRouter{entries: []model.ServiceEntry{
		{Node: model.Node{ID: "remote-1", Datacenter: "dc2"}, Service: model.ServiceInstance{ID: "web", Name: "web"}},
	}}
	// No client/cache: a local read would panic, proving remote reads never touch them.
	p := NewProxy(ProxyOptions{Node: model.Node{ID: "agent-1", Datacenter: "dc1"}, Federation: router})

	res, _, err := p.Resolve(context.Background(), model.Query{Name: "web", Datacenter: "dc2"}, model.ConsistencyDefault)
	if err != nil {
		t.Fatalf("Resolve dc2: %v", err)
	}
	if router.calledDC != "dc2" {
		t.Errorf("router asked for %q, want dc2", router.calledDC)
	}
	if len(res.Entries) != 1 || res.Entries[0].Node.Datacenter != "dc2" {
		t.Errorf("remote result = %+v, want one dc2 entry", res.Entries)
	}
}
