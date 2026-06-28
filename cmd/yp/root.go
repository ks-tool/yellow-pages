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
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	"github.com/ks-tool/yellow-pages/internal/app"
	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
)

func newRootCmd() *cobra.Command {
	var (
		configPath string
		roleFlag   string
		logLevel   string
		logFormat  string
	)

	cmd := &cobra.Command{
		Use:   "yp",
		Short: "yellow-pages — lightweight peer-to-peer service discovery",
		Long: "yellow-pages is a lightweight, peer-to-peer service discovery system for\n" +
			"on-premise environments. Every node runs this binary; its role (agent or\n" +
			"seed) is selected by config.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			if roleFlag != "" {
				cfg.Role = config.Role(roleFlag)
				if err := cfg.Validate(); err != nil {
					return err
				}
			}

			logger, err := newLogger(logLevel, logFormat)
			if err != nil {
				return err
			}

			sec, err := setupSecurity(cfg, logger)
			if err != nil {
				return err
			}

			logger = logger.With(
				"role", string(cfg.Role),
				"node", sec.nodeID,
				"dc", cfg.Datacenter,
			)
			logger.Info("yellow-pages starting",
				"version", version,
				"cluster", cfg.Cluster.Name,
				"tls", cfg.TLS.Enabled,
				"acl_mode", cfg.ACL.Mode,
				"grpc", cfg.Listeners.GRPC.Addr(),
				"consul_http", listenerState(cfg.Listeners.ConsulHTTP),
				"dns", listenerState(cfg.Listeners.DNS),
				"metrics", listenerState(cfg.Listeners.Metrics),
			)

			clk := clock.System()
			metrics := observability.NewPrometheus()

			components := buildComponents(cfg, metrics, clk, sec, logger)

			application := app.New(
				app.WithLogger(logger),
				app.WithClock(clk),
				app.WithShutdownTimeout(cfg.ShutdownTimeout.Duration()),
				app.WithComponents(components...),
			)
			return application.Run(cmd.Context())
		},
	}

	cmd.SetVersionTemplate("{{.Version}}\n")
	flags := cmd.Flags()
	flags.StringVarP(&configPath, "config", "c", "", "path to config file (.yaml/.yml/.json)")
	flags.StringVar(&roleFlag, "role", "", "override role from config (agent|seed)")
	flags.StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")
	flags.StringVar(&logFormat, "log-format", "json", "log format (json|text)")
	_ = cmd.MarkFlagRequired("config")

	return cmd
}

// security bundles the node identity, transport credentials and authorization
// resolved once at startup.
type security struct {
	nodeID   string
	creds    cred.Credentials
	identity cred.Identity
	authz    *cred.Authorizer
}

// setupSecurity resolves the node id, transport credentials (insecure or
// TLS/mTLS), the token→principal identity map and the acl.mode authorizer, and
// emits the migration warning for allow+deny.
func setupSecurity(cfg *config.Config, logger *slog.Logger) (security, error) {
	nodeID, err := cred.NodeID(cfg.NodeName, cfg.DataDir, logger)
	if err != nil {
		return security{}, err
	}

	creds := cred.Insecure()
	if cfg.TLS.Enabled {
		creds, err = cred.NewTLS(cred.TLSConfig{
			CertFile:  cfg.TLS.CertFile,
			KeyFile:   cfg.TLS.KeyFile,
			CAFile:    cfg.TLS.CAFile,
			MutualTLS: cfg.TLS.MutualTLS,
		})
		if err != nil {
			return security{}, err
		}
	}

	tokens, err := cred.LoadTokens(cfg.ACL.TokensFile)
	if err != nil {
		return security{}, err
	}
	mode, err := cred.ParseMode(cfg.ACL.Mode)
	if err != nil {
		return security{}, err
	}
	cred.WarnPolicyMismatch(mode, cfg.ACL.DefaultPolicy, logger)

	return security{
		nodeID:   nodeID,
		creds:    creds,
		identity: cred.NewIdentity(tokens),
		authz:    cred.NewAuthorizer(mode),
	}, nil
}

// buildComponents wires the serving components for the node's role. M3/M4 serve
// the native gRPC AgentService from a seed's local Store (single-seed path) plus
// the optional /metrics endpoint; the agent's local-agent-proxy serving path and
// the renew/GC loops arrive in M6.
func buildComponents(cfg *config.Config, metrics *observability.Prometheus, clk clock.Clock, sec security, logger *slog.Logger) []app.Component {
	var components []app.Component

	if cfg.Listeners.Metrics.Enabled {
		components = append(components,
			observability.NewMetricsServer(cfg.Listeners.Metrics.Addr(), metrics.Registry(), logger))
	}

	if cfg.Role == config.RoleSeed {
		st := store.NewMemory(store.Options{
			Clock:      clk,
			DefaultTTL: cfg.TTL.Duration(),
		})
		components = append(components, server.NewComponent(server.Options{
			Addr:      cfg.Listeners.GRPC.Addr(),
			Service:   server.New(st, logger),
			Transport: transport.New(sec.creds),
			Metrics:   metrics,
			Identity:  sec.identity,
			Authz:     sec.authz,
			Log:       logger,
		}))
	}

	return components
}

func listenerState(l config.Listener) string {
	if !l.Enabled {
		return "disabled"
	}
	return l.Addr()
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("invalid --log-format %q (want json|text)", format)
	}
	return slog.New(h), nil
}
