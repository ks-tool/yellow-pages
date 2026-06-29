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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/ks-tool/yellow-pages/internal/bootstrap"
	"github.com/ks-tool/yellow-pages/internal/config"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// newBootstrapCmd fetches a generated config from a seed's BootstrapService RPC.
func newBootstrapCmd() *cobra.Command {
	var (
		seed     string
		role     string
		token    string
		out      string
		useTLS   bool
		caFile   string
		certFile string
		keyFile  string
		insec    bool
		timeout  time.Duration
	)
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Fetch a generated agent/seed config from a seed (BootstrapService gRPC)",
		Long: "Contact a seed's gRPC BootstrapService with a token and write the generated,\n" +
			"sanitized config locally. TLS credentials and ACL tokens are NOT served —\n" +
			"provision them on the node separately. Re-run to pick up config changes.",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if token == "" {
				token = os.Getenv("YP_BOOTSTRAP_TOKEN")
			}
			if seed == "" || token == "" {
				return fmt.Errorf("--seed and --token (or YP_BOOTSTRAP_TOKEN) are required")
			}

			creds, err := bootstrapCreds(useTLS, caFile, certFile, keyFile, insec)
			if err != nil {
				return err
			}
			conn, err := grpc.NewClient(seed, grpc.WithTransportCredentials(creds))
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "bootstrap-token", token)

			resp, err := discoveryv1.NewBootstrapServiceClient(conn).GetConfig(ctx,
				&discoveryv1.GetConfigRequest{Role: role})
			if err != nil {
				return fmt.Errorf("bootstrap: %w", err)
			}

			if out == "" || out == "-" {
				_, err = os.Stdout.Write(resp.GetConfig())
				return err
			}
			if err := os.WriteFile(out, resp.GetConfig(), 0o600); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "wrote %d bytes of %s config to %s\n", len(resp.GetConfig()), role, out)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&seed, "seed", "", "seed gRPC address host:port (a seed whose signing key minted the token)")
	f.StringVar(&role, "role", "agent", "role to generate a config for (agent|seed)")
	f.StringVar(&token, "token", "", "bootstrap token (or env YP_BOOTSTRAP_TOKEN)")
	f.StringVarP(&out, "out", "o", "", "write config to this file (default: stdout)")
	f.BoolVar(&useTLS, "tls", false, "use TLS to reach the seed")
	f.StringVar(&caFile, "ca", "", "CA bundle to verify the seed (PEM)")
	f.StringVar(&certFile, "cert", "", "client certificate for mTLS (PEM)")
	f.StringVar(&keyFile, "key", "", "client private key for mTLS (PEM)")
	f.BoolVar(&insec, "insecure", false, "skip TLS verification (NOT recommended)")
	f.DurationVar(&timeout, "timeout", 10*time.Second, "request timeout")

	cmd.AddCommand(newCreateTokenCmd())
	return cmd
}

// newCreateTokenCmd mints a short-lived bootstrap token. Run it ON a seed node:
// it reads the signing key from the seed config and prints a signed token valid
// for the TTL (default 30s), which a joining node passes to `yp bootstrap`.
func newCreateTokenCmd() *cobra.Command {
	var (
		configPath string
		ttl        time.Duration
	)
	cmd := &cobra.Command{
		Use:           "create-token",
		Short:         "Mint a short-lived bootstrap token (run on a seed node)",
		Long:          "Read the bootstrap signing key from a seed config and print a signed token\nvalid for the TTL (default 30s). Hand it to a joining node's `yp bootstrap`.",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if !cfg.Bootstrap.Enabled {
				return fmt.Errorf("bootstrap is not enabled in %s", configPath)
			}
			key, err := bootstrap.ResolveSigningKey(cfg.Bootstrap.SigningKey, cfg.Bootstrap.SigningKeyFile)
			if err != nil {
				return err
			}
			if len(key) == 0 {
				return fmt.Errorf("bootstrap.signing_key or signing_key_file is required")
			}
			if ttl <= 0 {
				ttl = cfg.Bootstrap.TokenTTL
			}
			token, err := bootstrap.MintToken(key, ttl, time.Now())
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "bootstrap token (valid for %s):\n", ttl)
			fmt.Println(token)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVarP(&configPath, "config", "c", "", "seed config file (.yaml/.yml/.json)")
	_ = cmd.MarkFlagRequired("config")
	f.DurationVar(&ttl, "ttl", 0, "token lifetime (default: bootstrap.token_ttl, 30s)")
	return cmd
}

// bootstrapCreds builds gRPC transport credentials for the client: insecure when
// no TLS flag/material is given, else TLS (optionally mTLS).
func bootstrapCreds(useTLS bool, caFile, certFile, keyFile string, insec bool) (credentials.TransportCredentials, error) {
	// Any TLS-implying flag selects the TLS path; only a fully bare invocation is
	// plaintext (so --insecure/--key never silently downgrade to no-TLS).
	if !useTLS && !insec && caFile == "" && certFile == "" && keyFile == "" {
		return insecure.NewCredentials(), nil
	}
	conf := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insec} //nolint:gosec // insecure is an explicit opt-in flag
	if caFile != "" {
		pem, err := os.ReadFile(caFile) //nolint:gosec // operator-provided CA path
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca: no certificates parsed from %s", caFile)
		}
		conf.RootCAs = pool
	}
	if certFile != "" || keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		conf.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(conf), nil
}
