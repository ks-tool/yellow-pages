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

// Package consuldns is the Consul-compatible DNS interface (github.com/miekg/dns):
// it answers *.service.<domain> and _svc._proto SRV queries on UDP+TCP over the
// same merge+health core, rendering strictly by qtype with Consul's SRV target
// semantics, and the precise NXDOMAIN-vs-NOERROR dichotomy.
package consuldns

import "strings"

type queryKind int

const (
	kindUnknown queryKind = iota
	kindService
	kindNode
)

// parsedQuery is the result of parsing a DNS name within the served domain.
type parsedQuery struct {
	kind       queryKind
	service    string
	tag        string
	node       string
	datacenter string
}

// meshLabels are Consul subdomains we do not serve (NXDOMAIN).
var meshLabels = map[string]struct{}{
	"connect": {}, "virtual": {}, "ingress": {}, "ns": {}, "ap": {}, "peer": {}, "query": {}, "addr": {},
}

// parseName parses a DNS query name against the served domain. Supported forms:
//
//	<service>.service[.<dc>.dc].<domain>           (canonical, optional dc)
//	<service>.service.<dc>.<domain>                (legacy short dc)
//	<tag>.<service>.service[.<dc>.dc].<domain>     (raw-tag filter)
//	_<service>._<proto>[.service][.<dc>.dc].<domain> (RFC2782; proto label = tag)
//	<node>.node[.<dc>.dc].<domain>                 (node lookup)
func parseName(name, domain string) parsedQuery {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	domain = strings.ToLower(strings.TrimSuffix(domain, "."))
	if name != domain && !strings.HasSuffix(name, "."+domain) {
		return parsedQuery{kind: kindUnknown}
	}
	prefix := strings.TrimSuffix(strings.TrimSuffix(name, domain), ".")
	if prefix == "" {
		return parsedQuery{kind: kindUnknown}
	}
	labels := strings.Split(prefix, ".")

	// RFC2782: _service._proto[.service][.<dc>.dc]
	if len(labels) >= 2 && strings.HasPrefix(labels[0], "_") && strings.HasPrefix(labels[1], "_") {
		rest := labels[2:]
		if len(rest) > 0 && rest[0] == "service" {
			rest = rest[1:]
		}
		return parsedQuery{
			kind: kindService, service: strings.TrimPrefix(labels[0], "_"),
			tag: strings.TrimPrefix(labels[1], "_"), datacenter: dcFromRest(rest),
		}
	}

	for i, l := range labels {
		switch l {
		case "service":
			return parseServicePrefix(labels[:i], dcFromRest(labels[i+1:]))
		case "node":
			if len(labels[:i]) == 0 {
				return parsedQuery{kind: kindUnknown}
			}
			return parsedQuery{kind: kindNode, node: strings.Join(labels[:i], "."), datacenter: dcFromRest(labels[i+1:])}
		}
		if _, mesh := meshLabels[l]; mesh {
			return parsedQuery{kind: kindUnknown}
		}
	}
	return parsedQuery{kind: kindUnknown}
}

func parseServicePrefix(before []string, dc string) parsedQuery {
	switch len(before) {
	case 1:
		return parsedQuery{kind: kindService, service: before[0], datacenter: dc}
	case 2:
		return parsedQuery{kind: kindService, tag: before[0], service: before[1], datacenter: dc}
	default:
		return parsedQuery{kind: kindUnknown}
	}
}

// dcFromRest reads the datacenter from the labels after the service/node keyword:
// [<dc> dc] (canonical) or [<dc>] (legacy), else empty.
func dcFromRest(rest []string) string {
	switch {
	case len(rest) == 2 && rest[1] == "dc":
		return rest[0]
	case len(rest) == 1:
		return rest[0]
	default:
		return ""
	}
}
