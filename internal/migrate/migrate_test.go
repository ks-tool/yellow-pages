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

package migrate_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/consul"
	"github.com/ks-tool/yellow-pages/internal/migrate"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/store"
)

func TestShadowDiff(t *testing.T) {
	t.Parallel()
	left := []migrate.ShadowEntry{
		{Node: "n1", ServiceID: "web", Address: "10.0.0.5", Port: 80, Tags: []string{"v1", "a"}, Status: "passing"},
		{Node: "n2", ServiceID: "api", Address: "10.0.0.6", Port: 81, Status: "passing"},
	}
	// Same set, different tag order and entry order -> equivalent.
	right := []migrate.ShadowEntry{
		{Node: "n2", ServiceID: "api", Address: "10.0.0.6", Port: 81, Status: "passing"},
		{Node: "n1", ServiceID: "web", Address: "10.0.0.5", Port: 80, Tags: []string{"a", "v1"}, Status: "passing"},
	}
	if d := migrate.ShadowDiff(left, right); !d.Empty() {
		t.Errorf("equivalent sets diverged: %+v", d)
	}

	// Drop one and change a port -> divergence reported on both sides.
	right = right[:1]
	right[0].Port = 999
	d := migrate.ShadowDiff(left, right)
	if len(d.OnlyLeft) != 2 || len(d.OnlyRight) != 1 {
		t.Errorf("diff = %+v, want 2 only-left, 1 only-right", d)
	}
}

func startConsul(t *testing.T) (*httptest.Server, *store.Memory) {
	t.Helper()
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
	node := model.Node{ID: "yp-node", Name: "yp-node", Address: "10.0.0.9", Datacenter: "dc1"}
	h := consul.NewHandler(consul.Options{
		Registry: consul.NewStoreRegistry(st, node),
		Info:     consul.NodeInfo{ID: "yp-node", Name: "yp-node", Datacenter: "dc1"},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, st
}

func TestImportBackfill(t *testing.T) {
	t.Parallel()
	source, srcStore := startConsul(t)
	target, dstStore := startConsul(t)

	// Seed the "Consul" source catalog with two instances on an external node.
	for _, svc := range []struct {
		name string
		port uint16
	}{{"web", 80}, {"api", 81}} {
		if err := srcStore.Register(model.Registration{
			Node:       model.Node{ID: "consul-1", Name: "consul-1", Address: "10.1.0.1", Datacenter: "dc1"},
			Services:   []model.ServiceInstance{{ID: svc.name, Name: svc.name, Address: "10.1.0.1", Port: svc.port, TTL: 30 * time.Second}},
			Generation: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := migrate.Import(context.Background(), http.DefaultClient, source.URL, target.URL)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n != 2 {
		t.Errorf("imported %d, want 2", n)
	}

	// The target now holds both instances under the external node identity.
	for _, name := range []string{"web", "api"} {
		entries := dstStore.Lookup(model.Query{Name: name}).Entries
		if len(entries) != 1 || entries[0].Node.ID != "consul-1" {
			t.Errorf("target missing backfilled %q under consul-1: %+v", name, entries)
		}
	}
}
