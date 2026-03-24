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

package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/node"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:               "yellow-pages",
	Short:             "Service discovery agent/seed",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath, _ := cmd.Flags().GetString("config")
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

func init() {
	rootCmd.SetHelpCommand(&cobra.Command{
		Use:    "no-help",
		Hidden: true,
	})

	rootCmd.Flags().StringP("config", "c", "", "Path to configuration file (JSON or YAML).")
	_ = rootCmd.MarkFlagRequired("config")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
