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

package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/ks-tool/yellow-pages/internal/migrate"
)

// newImportCmd backfills a Consul catalog into yellow-pages before a cutover.
func newImportCmd() *cobra.Command {
	var (
		from    string
		to      string
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Backfill a Consul catalog into yellow-pages (one-shot, pre-cutover)",
		Long: "Read every service from a Consul-compatible HTTP API (--from) and register it\n" +
			"into a yellow-pages agent/seed (--to) via /v1/catalog/register. Idempotent.",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := &http.Client{Timeout: timeout}
			n, err := migrate.Import(cmd.Context(), client, from, to)
			if err != nil {
				return err
			}
			fmt.Printf("imported %d service instances from %s to %s\n", n, from, to)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&from, "from", "http://127.0.0.1:8500", "source Consul HTTP address")
	f.StringVar(&to, "to", "http://127.0.0.1:8500", "target yellow-pages Consul HTTP address")
	f.DurationVar(&timeout, "timeout", 30*time.Second, "per-request HTTP timeout")
	return cmd
}
