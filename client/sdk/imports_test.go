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

package sdk_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestPublicClientDoesNotImportInternal guards the public surface: client/sdk and
// client/grpcresolver production code must depend only on the generated proto and
// grpc, never on internal packages.
func TestPublicClientDoesNotImportInternal(t *testing.T) {
	t.Parallel()
	for _, dir := range []string{".", "../grpcresolver"} {
		pkg, err := build.ImportDir(dir, 0)
		if err != nil {
			t.Fatalf("import %s: %v", dir, err)
		}
		for _, imp := range pkg.Imports { // production imports only (excludes _test.go)
			if strings.Contains(imp, "yellow-pages/internal/") {
				t.Errorf("%s imports internal package %q (public client must not)", pkg.ImportPath, imp)
			}
		}
	}
}
