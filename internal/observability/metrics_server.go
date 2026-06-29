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
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ks-tool/yellow-pages/internal/httpcomp"
)

// NewMetricsServer builds the /metrics HTTP component for reg, listening on addr.
// It serves the Prometheus registry over the standard library net/http (no
// framework) through the shared httpcomp wrapper.
func NewMetricsServer(addr string, reg *prometheus.Registry, log *slog.Logger) *httpcomp.Component {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return httpcomp.New("metrics-http", addr, mux, orDefault(log))
}
