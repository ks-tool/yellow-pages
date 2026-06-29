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

// Command yp is the yellow-pages binary. Its role (agent or seed) is selected
// by config; every node runs the same binary.
package main

import (
	"fmt"
	"os"
)

// version is the build version, injected at link time via:
//
//	-ldflags "-X main.version=<version>"
var version = "dev"

func main() {
	// The command silences cobra's own error printing so we render a single,
	// actionable line here (validation errors may be several joined messages).
	if err := newRootCmd().Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "yp:", err)
		os.Exit(1)
	}
}
