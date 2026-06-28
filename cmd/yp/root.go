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
	"github.com/ks-tool/yellow-pages/internal/config"
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
			logger = logger.With(
				"role", string(cfg.Role),
				"node", cfg.NodeName,
				"dc", cfg.Datacenter,
			)
			logger.Info("yellow-pages starting",
				"version", version,
				"cluster", cfg.Cluster.Name,
				"grpc", cfg.Listeners.GRPC.Addr(),
				"consul_http", listenerState(cfg.Listeners.ConsulHTTP),
				"dns", listenerState(cfg.Listeners.DNS),
			)

			// M0 has no serving components yet: the binary boots, idles, and
			// shuts down cleanly on SIGINT/SIGTERM. Components are wired in from
			// M3 (native gRPC) onward.
			application := app.New(
				app.WithLogger(logger),
				app.WithShutdownTimeout(cfg.ShutdownTimeout.Duration()),
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
