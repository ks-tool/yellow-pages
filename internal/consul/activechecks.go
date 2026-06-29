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

package consul

import (
	"time"

	"github.com/ks-tool/yellow-pages/internal/healthcheck"
)

// ChecksReporter receives a service's active (HTTP/TCP/UDP/exec) checks so the
// agent can run them. Optional — the seed-served surface leaves it nil (a seed
// hosts no services to probe). The healthcheck Monitor satisfies it.
type ChecksReporter interface {
	Set(serviceID string, defs []healthcheck.Definition)
	Remove(serviceID string)
}

// activeChecks extracts the runnable (non-TTL) checks from a register body.
func activeChecks(in registerInput) []healthcheck.Definition {
	var defs []healthcheck.Definition
	if d, ok := in.Check.toDefinition(); ok {
		defs = append(defs, d)
	}
	for i := range in.Checks {
		if d, ok := in.Checks[i].toDefinition(); ok {
			defs = append(defs, d)
		}
	}
	return defs
}

// toDefinition maps a check to an active-check definition, or false for a
// passive (TTL-only / empty) check.
func (c *checkInput) toDefinition() (healthcheck.Definition, bool) {
	if c == nil {
		return healthcheck.Definition{}, false
	}
	d := healthcheck.Definition{
		Method:        c.Method,
		Header:        c.Header,
		TLSSkipVerify: c.TLSSkipVerify,
		Interval:      parseDur(c.Interval),
		Timeout:       parseDur(c.Timeout),
	}
	switch {
	case c.HTTP != "":
		d.Kind, d.Target = healthcheck.KindHTTP, c.HTTP
	case c.TCP != "":
		d.Kind, d.Target = healthcheck.KindTCP, c.TCP
	case c.UDP != "":
		d.Kind, d.Target = healthcheck.KindUDP, c.UDP
	case len(c.Args) > 0:
		d.Kind, d.Args = healthcheck.KindScript, c.Args
	default:
		return healthcheck.Definition{}, false
	}
	return d, true
}

func parseDur(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	return 0
}
