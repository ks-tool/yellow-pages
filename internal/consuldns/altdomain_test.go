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

package consuldns

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/store"
)

func newDNSWithAlt(t *testing.T) (*Handler, *store.Memory) {
	t.Helper()
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
	h := NewHandler(storeResolver{st: st},
		Config{Domain: "consul.", AltDomain: "mycorp.", Datacenter: "dc1", Truncate: true},
		nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h, st
}

// TestDNSReplacedDomain: with only a custom Domain, the old ".consul" is gone.
func TestDNSReplacedDomain(t *testing.T) {
	t.Parallel()
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
	h := NewHandler(storeResolver{st: st},
		Config{Domain: "mycorp.", Datacenter: "dc1", Truncate: true},
		nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mustReg(t, st, reg("n1", "10.0.0.1", "web", "10.0.0.5", 8080))

	if got := ask(t, h, "web.service.mycorp", dns.TypeA); len(got.Answer) != 1 {
		t.Errorf("custom domain A answers = %d, want 1", len(got.Answer))
	}
	if got := ask(t, h, "web.service.consul", dns.TypeA); got.Rcode != dns.RcodeNameError {
		t.Errorf("replaced .consul rcode = %d, want NXDOMAIN", got.Rcode)
	}
}

// TestDNSAltDomain: both the primary and the alt zone resolve, and records are
// rendered under whichever zone the query arrived on.
func TestDNSAltDomain(t *testing.T) {
	t.Parallel()
	h, st := newDNSWithAlt(t)
	mustReg(t, st, reg("n1", "10.0.0.1", "web", "10.0.0.5", 8080))

	for _, dom := range []string{"consul", "mycorp"} {
		msg := ask(t, h, "web.service."+dom, dns.TypeA)
		if len(msg.Answer) != 1 {
			t.Fatalf("%s A answers = %d, want 1", dom, len(msg.Answer))
		}
		if a, ok := msg.Answer[0].(*dns.A); !ok || a.A.String() != "10.0.0.5" {
			t.Errorf("%s A = %v, want 10.0.0.5", dom, msg.Answer[0])
		}
	}

	// SRV target on the alt zone must be rendered under that zone (self-consistent).
	srvMsg := ask(t, h, "web.service.mycorp", dns.TypeSRV)
	if len(srvMsg.Answer) != 1 {
		t.Fatalf("alt SRV answers = %d, want 1", len(srvMsg.Answer))
	}
	srv := srvMsg.Answer[0].(*dns.SRV)
	if !strings.HasSuffix(srv.Target, ".mycorp.") {
		t.Errorf("alt SRV target = %q, want a *.mycorp. target", srv.Target)
	}
	// SOA authority on the alt zone names the alt zone.
	soaMsg := ask(t, h, "mycorp", dns.TypeSOA)
	if len(soaMsg.Answer) != 1 || soaMsg.Answer[0].(*dns.SOA).Hdr.Name != "mycorp." {
		t.Errorf("alt SOA = %+v, want name mycorp.", soaMsg.Answer)
	}

	// A name under neither zone is NXDOMAIN.
	if got := ask(t, h, "web.service.other", dns.TypeA); got.Rcode != dns.RcodeNameError {
		t.Errorf("unknown-zone rcode = %d, want NXDOMAIN", got.Rcode)
	}
}
