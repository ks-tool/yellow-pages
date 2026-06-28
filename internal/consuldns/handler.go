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
	"encoding/hex"
	"log/slog"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
)

// Resolver is the merged read path the DNS interface projects (the agent Proxy
// or a seed's Store adapter satisfy it).
type Resolver interface {
	Resolve(ctx context.Context, q model.Query, mode model.Consistency) (model.LookupResult, time.Duration, error)
}

// Config configures the DNS handler.
type Config struct {
	Domain       string // served zone, trailing dot (e.g. "consul.")
	Datacenter   string // local datacenter (also the dc1 alias target)
	ServiceTTL   uint32
	NodeTTL      uint32
	OnlyPassing  bool
	ARecordLimit int
	Truncate     bool
	// RateLimit caps queries-per-second per client (0 = unlimited; RRL).
	RateLimit int
}

// Handler answers DNS queries for the served zone.
type Handler struct {
	resolver Resolver
	cfg      Config
	prop     *observability.Propagation
	rrl      *rateLimiter
	log      *slog.Logger
}

// NewHandler builds the DNS handler. prop is optional (surface metrics).
func NewHandler(resolver Resolver, cfg Config, prop *observability.Propagation, log *slog.Logger) *Handler {
	if log == nil {
		log = slog.Default()
	}
	if !strings.HasSuffix(cfg.Domain, ".") {
		cfg.Domain += "."
	}
	return &Handler{resolver: resolver, cfg: cfg, prop: prop, rrl: newRateLimiter(cfg.RateLimit), log: log}
}

// ServeDNS implements dns.Handler.
func (h *Handler) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	h.prop.CountRequest("dns")
	if !h.rrl.allow(clientIP(w)) {
		resp := new(dns.Msg)
		resp.SetRcode(req, dns.RcodeRefused)
		_ = w.WriteMsg(resp)
		return
	}
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Authoritative = true

	if len(req.Question) == 1 {
		h.answer(resp, req.Question[0])
	} else {
		resp.Rcode = dns.RcodeFormatError
	}

	if h.cfg.Truncate {
		resp.Truncate(udpSize(w, req))
	}
	_ = w.WriteMsg(resp)
}

func (h *Handler) answer(resp *dns.Msg, q dns.Question) {
	switch q.Qtype {
	case dns.TypeSOA:
		resp.Answer = append(resp.Answer, h.soa())
		return
	case dns.TypeNS:
		resp.Answer = append(resp.Answer, h.ns(q.Name))
		return
	}

	pq := parseName(q.Name, h.cfg.Domain)
	if pq.kind == kindUnknown {
		h.nxdomain(resp, q.Name)
		return
	}

	dc := h.effectiveDC(pq.datacenter)
	all, _, err := h.resolver.Resolve(context.Background(), model.Query{
		Name: pq.service, Tags: tagsOf(pq), Datacenter: dc,
	}, model.ConsistencyDefault)
	if err != nil {
		resp.Rcode = dns.RcodeServerFailure
		return
	}

	entries := all.Entries
	if pq.kind == kindNode {
		entries = filterNode(entries, pq.node)
	}
	if len(entries) == 0 {
		h.nxdomain(resp, q.Name) // name does not exist
		return
	}

	healthy := health.Filter(entries, health.FilterOptions{OnlyPassing: true})
	if len(healthy) == 0 {
		// Exists but nothing healthy: NOERROR with empty answer + SOA authority.
		resp.Ns = append(resp.Ns, h.soa())
		return
	}

	if pq.kind == kindNode {
		h.renderNode(resp, q, healthy[0])
		return
	}
	h.renderService(resp, q, healthy, dc)
}

func (h *Handler) renderService(resp *dns.Msg, q dns.Question, entries []model.ServiceEntry, dc string) {
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })

	switch q.Qtype {
	case dns.TypeSRV:
		for _, e := range entries {
			target, ip := h.srvTarget(e, dc)
			resp.Answer = append(resp.Answer, &dns.SRV{
				Hdr:      dns.RR_Header{Name: q.Name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: h.cfg.ServiceTTL},
				Priority: 1, Weight: weight16(e.Service.Weights.OrDefault().Passing), Port: e.Service.Port, Target: target,
			})
			if rr := h.addressRR(target, ip, h.cfg.ServiceTTL); rr != nil {
				resp.Extra = append(resp.Extra, rr)
			}
		}
	case dns.TypeAAAA:
		h.appendAddresses(resp, q, entries, true)
	default: // A and ANY answer with address records (no SRV mixed in)
		h.appendAddresses(resp, q, entries, false)
	}
	if len(resp.Answer) == 0 {
		resp.Ns = append(resp.Ns, h.soa())
	}
}

func (h *Handler) appendAddresses(resp *dns.Msg, q dns.Question, entries []model.ServiceEntry, wantV6 bool) {
	count := 0
	for _, e := range entries {
		ip := net.ParseIP(serviceAddr(e))
		if ip == nil {
			continue
		}
		isV6 := ip.To4() == nil
		if isV6 != wantV6 {
			continue
		}
		if rr := h.addressRR(q.Name, ip, h.cfg.ServiceTTL); rr != nil {
			resp.Answer = append(resp.Answer, rr)
			count++
			if h.cfg.ARecordLimit > 0 && count >= h.cfg.ARecordLimit {
				break
			}
		}
	}
}

func (h *Handler) renderNode(resp *dns.Msg, q dns.Question, e model.ServiceEntry) {
	ip := net.ParseIP(e.Node.Address)
	switch q.Qtype {
	case dns.TypeTXT:
		resp.Answer = append(resp.Answer, metaTXT(q.Name, e.Node.Meta, h.cfg.NodeTTL)...)
	default:
		if rr := h.addressRR(q.Name, ip, h.cfg.NodeTTL); rr != nil {
			resp.Answer = append(resp.Answer, rr)
		}
	}
	if len(resp.Answer) == 0 {
		resp.Ns = append(resp.Ns, h.soa())
	}
}

// srvTarget returns the SRV target and the IP it must resolve to (always the
// service address). It is <node>.node.<dc> when the instance inherits the node
// address, else a synthetic <hexip>.addr.<dc>.
func (h *Handler) srvTarget(e model.ServiceEntry, dc string) (string, net.IP) {
	addr := serviceAddr(e)
	ip := net.ParseIP(addr)
	if addr == e.Node.Address || ip == nil {
		return nodeName(e.Node) + ".node." + dc + "." + h.cfg.Domain, net.ParseIP(e.Node.Address)
	}
	return hexIP(ip) + ".addr." + dc + "." + h.cfg.Domain, ip
}

func (h *Handler) addressRR(name string, ip net.IP, ttl uint32) dns.RR {
	if ip == nil {
		return nil
	}
	hdr := dns.RR_Header{Name: name, Class: dns.ClassINET, Ttl: ttl}
	if v4 := ip.To4(); v4 != nil {
		hdr.Rrtype = dns.TypeA
		return &dns.A{Hdr: hdr, A: v4}
	}
	hdr.Rrtype = dns.TypeAAAA
	return &dns.AAAA{Hdr: hdr, AAAA: ip.To16()}
}

func (h *Handler) nxdomain(resp *dns.Msg, _ string) {
	resp.Rcode = dns.RcodeNameError
	resp.Ns = append(resp.Ns, h.soa())
}

func (h *Handler) soa() *dns.SOA {
	return &dns.SOA{
		Hdr:     dns.RR_Header{Name: h.cfg.Domain, Rrtype: dns.TypeSOA, Class: dns.ClassINET, Ttl: 0},
		Ns:      "ns." + h.cfg.Domain,
		Mbox:    "hostmaster." + h.cfg.Domain,
		Serial:  1,
		Refresh: 3600, Retry: 600, Expire: 86400, Minttl: 0,
	}
}

func (h *Handler) ns(name string) *dns.NS {
	return &dns.NS{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 0}, Ns: "ns." + h.cfg.Domain}
}

func (h *Handler) effectiveDC(dc string) string {
	if dc == "" || dc == "dc1" {
		return h.cfg.Datacenter
	}
	return dc
}

// --- helpers ---

func tagsOf(pq parsedQuery) []string {
	if pq.tag != "" {
		return []string{pq.tag}
	}
	return nil
}

func filterNode(entries []model.ServiceEntry, nodeName string) []model.ServiceEntry {
	out := entries[:0:0]
	for _, e := range entries {
		if strings.EqualFold(e.Node.Name, nodeName) || strings.EqualFold(e.Node.ID, nodeName) {
			out = append(out, e)
		}
	}
	return out
}

func serviceAddr(e model.ServiceEntry) string {
	if e.Service.Address != "" {
		return e.Service.Address
	}
	return e.Node.Address
}

func nodeName(n model.Node) string {
	if n.Name != "" {
		return n.Name
	}
	return n.ID
}

func weight16(w uint32) uint16 {
	if w > 0xffff {
		return 0xffff
	}
	return uint16(w) //nolint:gosec // clamped to [0, MaxUint16] above
}

func hexIP(ip net.IP) string {
	if v4 := ip.To4(); v4 != nil {
		return hex.EncodeToString(v4)
	}
	return hex.EncodeToString(ip.To16())
}

func metaTXT(name string, meta map[string]string, ttl uint32) []dns.RR {
	out := make([]dns.RR, 0, len(meta))
	for k, v := range meta {
		out = append(out, &dns.TXT{
			Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: ttl},
			Txt: []string{k + "=" + v},
		})
	}
	return out
}

func clientIP(w dns.ResponseWriter) string {
	addr := w.RemoteAddr()
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

func udpSize(w dns.ResponseWriter, req *dns.Msg) int {
	if _, ok := w.RemoteAddr().(*net.TCPAddr); ok {
		return dns.MaxMsgSize
	}
	if opt := req.IsEdns0(); opt != nil {
		return int(opt.UDPSize())
	}
	return dns.MinMsgSize
}
