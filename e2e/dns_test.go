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

package e2e

import (
	"fmt"
	"testing"

	"github.com/miekg/dns"
)

// TestDNSAgainstYP exercises yp's Consul-compatible DNS over the wire (the dig
// path) — no Docker needed; yp runs as a real process.
func TestDNSAgainstYP(t *testing.T) {
	yp := startYPSeed(t)
	ypc := mustClient(t, yp.consulHTTP)
	seedCatalog(t, ypc)

	server := fmt.Sprintf("127.0.0.1:%d", yp.dnsPort)

	t.Run("A", func(t *testing.T) {
		msg := dnsExchange(t, server, "web.service.consul.", dns.TypeA)
		if msg.Rcode != dns.RcodeSuccess {
			t.Fatalf("rcode = %d, want NOERROR", msg.Rcode)
		}
		var addrs []string
		for _, rr := range msg.Answer {
			if a, ok := rr.(*dns.A); ok {
				addrs = append(addrs, a.A.String())
			}
		}
		if len(addrs) != 2 { // web-1 (10.1.0.1) + web-2 (10.1.0.2)
			t.Errorf("A answers = %v, want 2 healthy web instances", addrs)
		}
	})

	t.Run("SRV", func(t *testing.T) {
		msg := dnsExchange(t, server, "web.service.consul.", dns.TypeSRV)
		var ports []uint16
		for _, rr := range msg.Answer {
			if srv, ok := rr.(*dns.SRV); ok {
				ports = append(ports, srv.Port)
			}
		}
		if len(ports) != 2 {
			t.Errorf("SRV answers = %v, want 2", ports)
		}
		// The SRV target must resolve to an address in Additional.
		if len(msg.Extra) == 0 {
			t.Error("SRV response carries no Additional address records")
		}
	})

	t.Run("NXDOMAIN", func(t *testing.T) {
		msg := dnsExchange(t, server, "absent.service.consul.", dns.TypeA)
		if msg.Rcode != dns.RcodeNameError {
			t.Errorf("rcode = %d, want NXDOMAIN", msg.Rcode)
		}
	})
}

func dnsExchange(t *testing.T, server, name string, qtype uint16) *dns.Msg {
	t.Helper()
	m := new(dns.Msg)
	m.SetQuestion(name, qtype)
	c := new(dns.Client)
	r, _, err := c.Exchange(m, server)
	if err != nil {
		t.Fatalf("dns exchange %s %d: %v", name, qtype, err)
	}
	return r
}
