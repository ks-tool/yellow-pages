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

package model

// HealthState is the derived liveness of a service instance. Maintenance is a
// separate drain flag (ServiceEntry.Maintenance), not a health state.
type HealthState int

const (
	// HealthUnspecified is the zero value.
	HealthUnspecified HealthState = iota
	// HealthPassing — lease is fresh.
	HealthPassing
	// HealthWarning — degraded but still usable.
	HealthWarning
	// HealthCritical — expired or otherwise unhealthy.
	HealthCritical
)

// String renders the Consul-compatible state name.
func (h HealthState) String() string {
	switch h {
	case HealthPassing:
		return "passing"
	case HealthWarning:
		return "warning"
	case HealthCritical:
		return "critical"
	default:
		return "unspecified"
	}
}
