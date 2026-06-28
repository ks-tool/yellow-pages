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
	"context"
	"net/http"
	"testing"

	"github.com/ks-tool/yellow-pages/internal/migrate"
)

// TestImportFromRealConsul backfills a real Consul catalog into yp via the
// migrate Import tool and asserts the normalized shadow-diff is empty.
func TestImportFromRealConsul(t *testing.T) {
	consulAddr := startConsul(t)
	yp := startYPSeed(t)

	consul := mustClient(t, consulAddr)
	seedCatalog(t, consul)

	n, err := migrate.Import(context.Background(), http.DefaultClient, consulAddr, yp.consulHTTP)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if n < len(instances) {
		t.Errorf("imported %d instances, want >= %d", n, len(instances))
	}

	ypc := mustClient(t, yp.consulHTTP)
	for _, svc := range []string{"web", "api"} {
		cEntries, _, err := consul.Health().Service(svc, "", false, nil)
		if err != nil {
			t.Fatalf("consul health %s: %v", svc, err)
		}
		ypEntries, _, err := ypc.Health().Service(svc, "", false, nil)
		if err != nil {
			t.Fatalf("yp health %s: %v", svc, err)
		}
		if diff := migrate.ShadowDiff(shadowOf(cEntries), shadowOf(ypEntries)); !diff.Empty() {
			t.Errorf("after import, %s diverges:\n only-consul: %+v\n only-yp: %+v",
				svc, diff.OnlyLeft, diff.OnlyRight)
		}
	}
}
