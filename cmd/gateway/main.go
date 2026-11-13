// Command gateway runs the rate-limiting reverse proxy.
//
//	gateway -config configs/example.yaml
//
// Signals: SIGINT/SIGTERM shut down gracefully, SIGHUP reloads routes.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xungirl/rate-gate/internal/breaker"
	"github.com/xungirl/rate-gate/internal/config"
	"github.com/xungirl/rate-gate/internal/gateway"
	"github.com/xungirl/rate-gate/internal/metrics"
)

func main() {
	configPath := flag.String("config", "configs/example.yaml", "path to YAML config")
	listen := flag.String("listen", "", "override the config's listen address (handy for running several instances)")
	flag.Parse()

	if err := run(*configPath, *listen); err != nil {
		slog.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(configPath, listenOverride string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if listenOverride != "" {
		cfg.Listen = listenOverride
	}
	logger := newLogger(cfg.Log.Level)
	m := metrics.New()

	deps := gateway.Deps{Logger: logger, Metrics: m}
	if cfg.Backend.Type == "redis" {
		rc := cfg.Backend.Redis
		client := redis.NewClient(&redis.Options{
			Addr:     rc.Addr,
			Password: rc.Password,
			DB:       rc.DB,
			PoolSize: rc.PoolSize,
			// Keep socket timeouts near the per-call budget so a dead Redis
			// fails fast instead of hanging requests.
			DialTimeout:  200 * time.Millisecond,
			ReadTimeout:  rc.Timeout,
			WriteTimeout: rc.Timeout,
			MaxRetries:   -1, // no retries: the breaker + failure policy handle errors
		})
		defer client.Close()
		if err := client.Ping(context.Background()).Err(); err != nil {
			// Not fatal: the failure policy covers a missing Redis.
			logger.Warn("redis not reachable at startup", "addr", rc.Addr, "err", err)
		}
		deps.Redis = client
		deps.Breaker = breaker.New(cfg.Backend.Breaker.FailureThreshold, cfg.Backend.Breaker.OpenTimeout, nil,
			func(s breaker.State) {
				m.BreakerState.Set(float64(s))
				logger.Warn("redis circuit breaker changed state", "state", s.String())
			})
	}

	gw, err := gateway.New(cfg, deps)
	if err != nil {
		return err
	}
	defer gw.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.Handle("/healthz", gw.HealthHandler())
	mux.Handle("/", gw)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// SIGHUP => reload routes from the same file.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			newCfg, err := config.Load(configPath)
			if err == nil {
				if listenOverride != "" {
					newCfg.Listen = listenOverride
				}
				err = gw.Reload(newCfg)
			}
			if err != nil {
				logger.Error("config reload failed; keeping old config", "err", err)
				continue
			}
			logger.Info("config reloaded", "routes", len(newCfg.Routes))
		}
	}()

	// SIGINT/SIGTERM => ctx is cancelled => graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", "addr", cfg.Listen, "backend", cfg.Backend.Type, "routes", len(cfg.Routes))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh: // failed to start (e.g. port in use)
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	// Shutdown stops accepting new connections and waits for in-flight
	// requests to finish (or the timeout to expire).
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
