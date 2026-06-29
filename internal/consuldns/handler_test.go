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
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/store"
)

type storeResolver struct{ st *store.Memory }

func (r storeResolver) Resolve(_ context.Context, q model.Query, _ model.Consistency) (model.LookupResult, time.Duration, error) {
	return r.st.Lookup(q), 0, nil
}

// testRW captures the response message. RemoteAddr is UDP unless set otherwise.
type testRW struct {
	msg    *dns.Msg
	remote net.Addr
}

func (w *testRW) WriteMsg(m *dns.Msg) error { w.msg = m; return nil }
func (w *testRW) LocalAddr() net.Addr       { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8600} }
func (w *testRW) RemoteAddr() net.Addr {
	if w.remote != nil {
		return w.remote
	}
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5353}
}
func (w *testRW) Write([]byte) (int, error) { return 0, nil }
func (w *testRW) Close() error              { return nil }
func (w *testRW) TsigStatus() error         { return nil }
func (w *testRW) TsigTimersOnly(bool)       {}
func (w *testRW) Hijack()                   {}

func newDNS(t *testing.T) (*Handler, *store.Memory) {
	t.Helper()
	st := store.NewMemory(store.Options{Clock: clock.System(), DefaultTTL: 30 * time.Second})
	h := NewHandler(storeResolver{st: st}, Config{Domain: "consul.", Datacenter: "dc1", Truncate: true}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h, st
}

func reg(nodeID, nodeAddr, service, svcAddr string, port uint16) model.Registration {
	return model.Registration{
		Node:       model.Node{ID: nodeID, Name: nodeID, Address: nodeAddr, Datacenter: "dc1"},
		Services:   []model.ServiceInstance{{ID: service, Name: service, Address: svcAddr, Port: port, TTL: 30 * time.Second}},
		Generation: 1,
	}
}

func ask(t *testing.T, h *Handler, name string, qtype uint16) *dns.Msg {
	t.Helper()
	req := new(dns.Msg)
	req.SetQuestion(dns.Fqdn(name), qtype)
	rw := &testRW{}
	h.ServeDNS(rw, req)
	if rw.msg == nil {
		t.Fatal("no response written")
	}
	return rw.msg
}

func TestDNSAReturnsHealthyOnly(t *testing.T) {
	t.Parallel()
	h, st := newDNS(t)
	mustReg(t, st, reg("n1", "10.0.0.1", "web", "10.0.0.5", 8080))
	mustReg(t, st, reg("n2", "10.0.0.2", "web", "10.0.0.6", 8080))
	if err := st.SetMaintenance("n2", "web", true); err != nil {
		t.Fatal(err)
	}

	msg := ask(t, h, "web.service.consul", dns.TypeA)
	if msg.Rcode != dns.RcodeSuccess {
		t.Fatalf("rcode = %d", msg.Rcode)
	}
	var addrs []string
	for _, rr := range msg.Answer {
		a, ok := rr.(*dns.A)
		if !ok {
			t.Errorf("A query returned non-A record: %T", rr)
			continue
		}
		addrs = append(addrs, a.A.String())
	}
	if len(addrs) != 1 || addrs[0] != "10.0.0.5" {
		t.Errorf("A answers = %v, want only the healthy 10.0.0.5", addrs)
	}
}

func TestDNSSRVTargetResolvesToServiceAddress(t *testing.T) {
	t.Parallel()
	h, st := newDNS(t)
	// Service address differs from the node address -> synthetic <hexip>.addr target.
	mustReg(t, st, reg("n1", "10.0.0.1", "web", "10.0.0.5", 8080))

	msg := ask(t, h, "web.service.consul", dns.TypeSRV)
	if len(msg.Answer) != 1 {
		t.Fatalf("SRV answers = %d, want 1", len(msg.Answer))
	}
	srv, ok := msg.Answer[0].(*dns.SRV)
	if !ok {
		t.Fatalf("not an SRV: %T", msg.Answer[0])
	}
	if srv.Port != 8080 {
		t.Errorf("SRV port = %d, want 8080", srv.Port)
	}
	if !strings.HasSuffix(srv.Target, ".addr.dc1.consul.") {
		t.Errorf("SRV target = %q, want a <hexip>.addr.dc1.consul. form", srv.Target)
	}
	// The target's A record in Additional must point to the SERVICE address.
	var found bool
	for _, rr := range msg.Extra {
		if a, ok := rr.(*dns.A); ok && a.Hdr.Name == srv.Target && a.A.String() == "10.0.0.5" {
			found = true
		}
	}
	if !found {
		t.Errorf("SRV target does not resolve to the service address 10.0.0.5: %+v", msg.Extra)
	}
}

func TestDNSNXDOMAINvsNOERROR(t *testing.T) {
	t.Parallel()
	h, st := newDNS(t)

	// Unknown service -> NXDOMAIN with SOA in authority.
	unknown := ask(t, h, "nope.service.consul", dns.TypeA)
	if unknown.Rcode != dns.RcodeNameError {
		t.Errorf("unknown service rcode = %d, want NXDOMAIN", unknown.Rcode)
	}
	if len(unknown.Ns) == 0 {
		t.Error("NXDOMAIN missing SOA authority")
	}

	// Existing service with no healthy instances -> NOERROR, empty answer, SOA.
	mustReg(t, st, reg("n1", "10.0.0.1", "web", "10.0.0.5", 8080))
	if err := st.SetMaintenance("n1", "web", true); err != nil {
		t.Fatal(err)
	}
	empty := ask(t, h, "web.service.consul", dns.TypeA)
	if empty.Rcode != dns.RcodeSuccess {
		t.Errorf("no-healthy rcode = %d, want NOERROR", empty.Rcode)
	}
	if len(empty.Answer) != 0 {
		t.Errorf("no-healthy answer = %d, want empty", len(empty.Answer))
	}
	if len(empty.Ns) == 0 {
		t.Error("no-healthy NOERROR missing SOA authority")
	}

	// Mesh subdomain -> NXDOMAIN.
	if mesh := ask(t, h, "web.connect.consul", dns.TypeA); mesh.Rcode != dns.RcodeNameError {
		t.Errorf("mesh rcode = %d, want NXDOMAIN", mesh.Rcode)
	}
}

func TestDNSTagAndDatacenterForms(t *testing.T) {
	t.Parallel()
	h, st := newDNS(t)
	mustReg(t, st, regTags("n1", "10.0.0.1", "web", "10.0.0.5", 8080, "v1"))

	// Tag filter matches the raw tag.
	if msg := ask(t, h, "v1.web.service.consul", dns.TypeA); len(msg.Answer) != 1 {
		t.Errorf("tag v1 answers = %d, want 1", len(msg.Answer))
	}
	if msg := ask(t, h, "v2.web.service.consul", dns.TypeA); msg.Rcode != dns.RcodeNameError {
		t.Errorf("tag v2 rcode = %d, want NXDOMAIN", msg.Rcode)
	}
	// Canonical .dc and legacy short dc both resolve (dc1 alias = local dc).
	for _, name := range []string{"web.service.dc1.dc.consul", "web.service.dc1.consul"} {
		if msg := ask(t, h, name, dns.TypeA); len(msg.Answer) != 1 {
			t.Errorf("%s answers = %d, want 1", name, len(msg.Answer))
		}
	}
	// RFC2782 SRV with the proto label as the tag filter.
	if msg := ask(t, h, "_web._v1.service.consul", dns.TypeSRV); len(msg.Answer) != 1 {
		t.Errorf("RFC2782 SRV answers = %d, want 1", len(msg.Answer))
	}
}

func TestDNSNodeQuery(t *testing.T) {
	t.Parallel()
	h, st := newDNS(t)
	mustReg(t, st, reg("node-a", "10.0.0.1", "web", "10.0.0.5", 8080))

	msg := ask(t, h, "node-a.node.consul", dns.TypeA)
	if len(msg.Answer) != 1 {
		t.Fatalf("node answers = %d, want 1", len(msg.Answer))
	}
	if a, ok := msg.Answer[0].(*dns.A); !ok || a.A.String() != "10.0.0.1" {
		t.Errorf("node A = %v, want the node address 10.0.0.1", msg.Answer[0])
	}
}

func regTags(nodeID, nodeAddr, service, svcAddr string, port uint16, tags ...string) model.Registration {
	r := reg(nodeID, nodeAddr, service, svcAddr, port)
	r.Services[0].Tags = tags
	return r
}

func mustReg(t *testing.T, st *store.Memory, r model.Registration) {
	t.Helper()
	if err := st.Register(r); err != nil {
		t.Fatalf("register: %v", err)
	}
}
