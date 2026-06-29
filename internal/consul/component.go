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
	"log/slog"
	"net/http"

	"github.com/ks-tool/yellow-pages/internal/httpcomp"
)

// NewComponent serves the Consul-compatible HTTP API (default 127.0.0.1:8500) on
// the standard library net/http — no framework. It satisfies app.Component.
func NewComponent(addr string, handler http.Handler, log *slog.Logger) *httpcomp.Component {
	return httpcomp.New("consul-http", addr, handler, log)
}
