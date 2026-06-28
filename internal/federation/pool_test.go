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

package federation

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/transport"
)

func newPool(t *testing.T) *Pool {
	t.Helper()
	tr := transport.New(cred.Insecure())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	p, err := NewPool("dc1", 1, map[string][]string{
		"dc1": {"127.0.0.1:1"}, // local: skipped
		"dc2": {"127.0.0.1:19990"},
	}, tr, time.Second, log)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPoolIsRemote(t *testing.T) {
	t.Parallel()
	p := newPool(t)
	cases := map[string]bool{"dc2": true, "dc1": false, "": false, "dc9": false}
	for dc, want := range cases {
		if got := p.IsRemote(dc); got != want {
			t.Errorf("IsRemote(%q) = %v, want %v", dc, got, want)
		}
	}
	if dcs := p.Datacenters(); len(dcs) != 1 || dcs[0] != "dc2" {
		t.Errorf("Datacenters() = %v, want [dc2] (local dc1 skipped)", dcs)
	}
}

// TestPoolUnknownDCNoStorm: an unknown dc returns empty without fanning out —
// the loop/storm guard.
func TestPoolUnknownDCNoStorm(t *testing.T) {
	t.Parallel()
	p := newPool(t)
	res, err := p.Resolve(context.Background(), "dc9", model.Query{Name: "web"})
	if err != nil {
		t.Fatalf("Resolve unknown dc = %v, want nil", err)
	}
	if len(res.Entries) != 0 {
		t.Errorf("unknown dc returned %d entries, want 0", len(res.Entries))
	}
}
