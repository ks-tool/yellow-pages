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

package observability

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Readiness scaffolds the gRPC health readiness gate. The named services report
// NOT_SERVING until SetReady(true), and flip back to NOT_SERVING on SetReady(false)
// (graceful drain). In M3 the server flips it SERVING once it is listening; M6
// drives it from seed connectivity (k-of-N reachable).
type Readiness struct {
	srv      *health.Server
	services []string
}

// NewReadiness wraps a health server and gates the given service names. The empty
// string "" is the conventional overall-server health key. All listed services
// start NOT_SERVING.
func NewReadiness(srv *health.Server, services ...string) *Readiness {
	r := &Readiness{srv: srv, services: services}
	r.set(healthpb.HealthCheckResponse_NOT_SERVING)
	return r
}

// SetReady flips the gated services to SERVING (ready) or NOT_SERVING (not ready).
func (r *Readiness) SetReady(ready bool) {
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if ready {
		status = healthpb.HealthCheckResponse_SERVING
	}
	r.set(status)
}

func (r *Readiness) set(status healthpb.HealthCheckResponse_ServingStatus) {
	for _, s := range r.services {
		r.srv.SetServingStatus(s, status)
	}
}
