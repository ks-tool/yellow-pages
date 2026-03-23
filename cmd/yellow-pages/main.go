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
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/node"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "yellow-pages",
		Usage: "Service discovery agent/seed",
		Commands: []*cli.Command{
			{
				Name:  "get",
				Usage: "Get service",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "datacenter",
						Aliases: []string{"dc"},
						Usage:   "Request to specific Datacenter",
					},
					&cli.StringFlag{
						Name:    "tag",
						Aliases: []string{"t"},
						Usage:   "Request to specific tags",
					},
				},
				Action: func(c *cli.Context) error {
					dc := c.String("datacenter")
					tags := c.StringSlice("tag")
					_ = dc
					_ = tags

					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "config",
				Aliases:  []string{"c"},
				Usage:    "Path to configuration file (JSON or YAML)",
				Required: true,
			},
		},
		Action: func(ctx *cli.Context) error {
			configPath := ctx.String("config")
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}

			slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

			n, err := node.NewNode(cfg)
			if err != nil {
				return err
			}

			if err = n.Start(); err != nil {
				return err
			}

			slog.Info("Node started", "seed", cfg.Seed, "port", cfg.GRPCPort)

			// Wait for interrupt signal
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
			<-ch

			slog.Info("Shutting down...")
			n.Stop()
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}
