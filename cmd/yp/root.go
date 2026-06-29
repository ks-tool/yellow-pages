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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ks-tool/yellow-pages/internal/app"
	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/consul"
	"github.com/ks-tool/yellow-pages/internal/consuldns"
	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/federation"
	"github.com/ks-tool/yellow-pages/internal/membership"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/observability"
	"github.com/ks-tool/yellow-pages/internal/resolver"
	"github.com/ks-tool/yellow-pages/internal/seedclient"
	"github.com/ks-tool/yellow-pages/internal/server"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/transport"
	"github.com/ks-tool/yellow-pages/internal/watch"
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

			// Resolve the seed set (static + optional plugin). A resolution
			// failure is non-fatal here (the merge is non-destructive); an agent
			// with no usable seeds fails later in buildComponents.
			provider := buildSeedProvider(cfg, logger)
			seeds, rerr := provider.Seeds(cmd.Context())
			if rerr != nil {
				logger.Warn("seed resolution failed at startup", "error", rerr)
			} else {
				logger.Info("seeds resolved", "count", len(seeds), "seeds", seeds)
			}

			clk := clock.System()
			metrics := observability.NewPrometheus()

			components, err := buildComponents(cfg, metrics, clk, sec, seeds, logger)
			if err != nil {
				return err
			}

			application := app.New(
				app.WithLogger(logger),
				app.WithClock(clk),
				app.WithShutdownTimeout(cfg.ShutdownTimeout.Duration()),
				app.WithComponents(components...),
			)
			return application.Run(cmd.Context())
		},
	}

	cmd.AddCommand(newImportCmd())

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

// buildSeedProvider builds the SeedProvider from config: the static list plus,
// when configured, the discovery plugin, combined with a non-destructive merge.
func buildSeedProvider(cfg *config.Config, logger *slog.Logger) resolver.SeedProvider {
	var providers []resolver.SeedProvider
	if len(cfg.Cluster.Seeds) > 0 {
		providers = append(providers, resolver.NewStatic(cfg.Cluster.Seeds))
	}
	if d := cfg.Cluster.Discovery; d != nil {
		providers = append(providers, resolver.NewPlugin(resolver.PluginConfig{
			Path:    d.Name,
			Options: d.Options,
		}))
	}
	return resolver.NewMerged(logger, providers...)
}

// buildComponents wires the serving components for the node's role: a seed runs
// the registry gRPC server plus a GC loop; an agent runs the local-agent-proxy
// gRPC server backed by a SeedClient, a readiness probe, a renew loop and a
// shutdown deregistrar — ordered so Stop drains as
// readiness-off -> drain-window -> stop-accept -> deregister -> close.
func buildComponents(cfg *config.Config, metrics *observability.Prometheus, clk clock.Clock, sec security, seeds []string, logger *slog.Logger) ([]app.Component, error) {
	var components []app.Component

	if cfg.Listeners.Metrics.Enabled {
		components = append(components,
			observability.NewMetricsServer(cfg.Listeners.Metrics.Addr(), metrics.Registry(), logger))
	}

	prop := observability.NewPropagation(metrics.Registry())

	switch cfg.Role {
	case config.RoleSeed:
		watcher := watch.New(0, clk)
		st := store.NewMemory(store.Options{
			Clock:       clk,
			DefaultTTL:  cfg.TTL.Duration(),
			MaxServices: cfg.MaxServices,
			OnChange:    watcher.Notify,
		})
		seedNode := model.Node{ID: sec.nodeID, Name: cfg.NodeName, Address: cfg.Listeners.GRPC.Address, Datacenter: cfg.Datacenter}
		seedReg := consul.NewStoreRegistry(st, seedNode)

		// Membership (M18, v1.x): a seed with peers gates readiness on a join
		// snapshot, then runs pull-based anti-entropy.
		var membersFn func(ctx context.Context) any
		gateReadiness := cfg.Membership.Enabled && len(cfg.Membership.Peers) > 0

		seedServer := server.NewComponent(server.Options{
			Addr:          cfg.Listeners.GRPC.Addr(),
			Service:       server.New(st, logger).SetWatcher(watcher),
			Transport:     transport.New(sec.creds),
			Metrics:       metrics,
			Identity:      sec.identity,
			Authz:         sec.authz,
			StartNotReady: gateReadiness,
			Log:           logger,
		})
		components = append(components,
			seedServer,
			store.NewGCLoop(st, cfg.HeartbeatInterval.Duration(), clk, logger).
				WithReapHook(func(removed, size int) { prop.AddEvictions(removed); prop.SetRegistrySize(size) }),
		)
		if gateReadiness {
			peers, err := seedclient.New(cfg.Membership.Peers, transport.New(sec.creds), cfg.Agent.SeedTimeout.Duration(), logger)
			if err != nil {
				return nil, fmt.Errorf("membership: dial peers: %w", err)
			}
			syncer := membership.New(membership.Options{
				Self:     cfg.Listeners.GRPC.Addr(),
				Peers:    peers,
				Store:    st,
				Interval: cfg.Membership.SyncInterval.Duration(),
				Clock:    clk,
				Gate:     seedServer.Readiness(),
				Prop:     prop,
				Log:      logger,
			})
			membersFn = func(ctx context.Context) any { return syncer.Members(ctx) }
			components = append(components, syncer)
		}
		if c := consulComponent(cfg, seedReg, seedNode, seeds, watcher, sec, prop, seedReg, membersFn, logger); c != nil {
			components = append(components, c)
		}
		if c := dnsComponent(cfg, seedReg, prop, logger); c != nil {
			components = append(components, c)
		}
		if cfg.ConfigDir != "" {
			components = append(components, consul.NewLoader(cfg.ConfigDir, seedReg, logger))
		}

	case config.RoleAgent:
		if len(seeds) == 0 {
			return nil, fmt.Errorf("agent: no seeds resolved (configure cluster.seeds or cluster.discovery)")
		}
		client, err := seedclient.New(seeds, transport.New(sec.creds), cfg.Agent.SeedTimeout.Duration(), logger)
		if err != nil {
			return nil, err
		}
		client.SetPropagation(prop)
		node := model.Node{
			ID:         sec.nodeID,
			Name:       cfg.NodeName,
			Address:    cfg.Listeners.GRPC.Address,
			Datacenter: cfg.Datacenter,
		}
		agentWatcher := watch.New(watch.LoadBase(cfg.DataDir), clk)
		cache := seedclient.NewCache(client, seedclient.CacheOptions{
			MaxAge:   cfg.Agent.CacheMaxAge.Duration(),
			Clock:    clk,
			Prop:     prop,
			OnChange: func(name string) { agentWatcher.NotifyNames(name) },
			Log:      logger,
		})
		var fedRouter seedclient.Router
		if cfg.Federation.Enabled {
			pool, err := federation.NewPool(cfg.Datacenter, cfg.Federation.MaxHops, cfg.Federation.Datacenters,
				transport.New(sec.creds), cfg.Agent.SeedTimeout.Duration(), logger)
			if err != nil {
				return nil, err
			}
			fedRouter = pool
			components = append(components, pool)
		}
		proxy := seedclient.NewProxy(seedclient.ProxyOptions{
			Client:     client,
			Node:       node,
			Quorum:     cfg.Agent.WriteQuorum,
			Cache:      cache,
			Watcher:    agentWatcher,
			Federation: fedRouter,
			Prop:       prop,
			Log:        logger,
		})
		// The local-agent-proxy listens on a trusted loopback for local apps;
		// ownership authz is enforced by the seeds, not here, so the local server
		// runs with authz disabled (audit still records writes).
		grpcComp := server.NewComponent(server.Options{
			Addr:        cfg.Listeners.GRPC.Addr(),
			Service:     proxy,
			Transport:   transport.New(sec.creds),
			Metrics:     metrics,
			Identity:    sec.identity,
			DrainWindow: cfg.Agent.DrainWindow.Duration(),
			Clock:       clk,
			Log:         logger,
		})
		// Start order chosen so Stop (reverse) drains correctly: renew-loop,
		// readiness-probe, grpc-server (readiness-off + drain-window + GracefulStop),
		// then deregistrar (deregister + close).
		components = append(components,
			seedclient.NewDeregistrar(client, node.ID, logger),
			grpcComp,
			seedclient.NewReadinessProbe(client, grpcComp.Readiness(), cfg.Agent.ReadyMinSeeds, cfg.HeartbeatInterval.Duration(), clk, logger),
			seedclient.NewRenewLoop(proxy, cfg.HeartbeatInterval.Duration(), clk, logger),
			seedclient.NewRefreshLoop(cache, cfg.Agent.CacheMaxAge.Duration(), clk, logger),
			watch.NewFlusher(agentWatcher, cfg.DataDir, cfg.HeartbeatInterval.Duration(), clk, logger),
		)
		if c := consulComponent(cfg, proxy, node, seeds, agentWatcher, sec, prop, proxy, nil, logger); c != nil {
			components = append(components, c)
		}
		if c := dnsComponent(cfg, proxy, prop, logger); c != nil {
			components = append(components, c)
		}
		if cfg.ConfigDir != "" {
			components = append(components, consul.NewLoader(cfg.ConfigDir, proxy, logger))
		}
	}

	return components, nil
}

// dnsComponent builds the Consul DNS component when its listener is enabled.
func dnsComponent(cfg *config.Config, reg consuldns.Resolver, prop *observability.Propagation, logger *slog.Logger) app.Component {
	if !cfg.Listeners.DNS.Enabled {
		return nil
	}
	handler := consuldns.NewHandler(reg, consuldns.Config{
		Domain:       cfg.DNS.Domain,
		AltDomain:    cfg.DNS.AltDomain,
		Datacenter:   cfg.Datacenter,
		ServiceTTL:   ttlSeconds(cfg.DNS.ServiceTTL.Duration()),
		NodeTTL:      ttlSeconds(cfg.DNS.NodeTTL.Duration()),
		OnlyPassing:  cfg.DNS.OnlyPassing,
		ARecordLimit: cfg.DNS.ARecordLimit,
		Truncate:     cfg.DNS.EnableTruncate,
		RateLimit:    cfg.DNS.RateLimit,
	}, prop, logger)
	return consuldns.NewComponent(cfg.Listeners.DNS.Addr(), handler, logger)
}

func ttlSeconds(d time.Duration) uint32 {
	s := d / time.Second
	switch {
	case s < 0:
		return 0
	case s > 0xffffffff:
		return 0xffffffff
	default:
		return uint32(s) //nolint:gosec // clamped to [0, MaxUint32] above
	}
}

// consulComponent builds the Consul-compatible HTTP component when its listener
// is enabled, backed by reg (the seed's Store or the agent's Proxy) and watcher
// (blocking queries + X-Consul-Index).
func consulComponent(cfg *config.Config, reg consul.Registry, node model.Node, seeds []string, watcher *watch.Watcher, sec security, prop *observability.Propagation, dumper consul.Dumper, members func(ctx context.Context) any, logger *slog.Logger) app.Component {
	if !cfg.Listeners.ConsulHTTP.Enabled {
		return nil
	}
	handler := consul.NewHandler(consul.Options{
		Registry: reg,
		Info: consul.NodeInfo{
			ID:         node.ID,
			Name:       cfg.NodeName,
			Datacenter: cfg.Datacenter,
			Address:    node.Address,
			Version:    version,
			Seeds:      seeds,
			Federated:  federatedDCs(cfg),
		},
		Watcher:   watcher,
		Identity:  sec.identity,
		Authz:     sec.authz,
		RateLimit: cfg.ConsulRateLimit,
		Prop:      prop,
		Dumper:    dumper,
		Members:   members,
		Log:       logger,
	})
	return consul.NewComponent(cfg.Listeners.ConsulHTTP.Addr(), handler, logger)
}

// federatedDCs lists the configured remote datacenter names (M17), or nil when
// federation is disabled.
func federatedDCs(cfg *config.Config) []string {
	if !cfg.Federation.Enabled {
		return nil
	}
	dcs := make([]string, 0, len(cfg.Federation.Datacenters))
	for dc := range cfg.Federation.Datacenters {
		if dc != cfg.Datacenter {
			dcs = append(dcs, dc)
		}
	}
	return dcs
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
