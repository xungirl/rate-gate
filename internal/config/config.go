// Package config loads and validates the gateway's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/xungirl/rate-gate/internal/limiter"
)

// Config is the whole YAML file. Struct tags (`yaml:"..."`) tell the YAML
// decoder which key maps to which field.
type Config struct {
	Listen          string        `yaml:"listen"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	// TrustForwardedFor uses the right-most X-Forwarded-For entry as the
	// client IP. Only enable it behind a proxy/load balancer you control,
	// otherwise clients can spoof their IP and dodge per-IP limits.
	TrustForwardedFor bool    `yaml:"trust_forwarded_for"`
	Log               Log     `yaml:"log"`
	Backend           Backend `yaml:"backend"`
	Routes            []Route `yaml:"routes"`
}

// Log controls structured logging.
type Log struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Access bool   `yaml:"access"` // one log line per request (costly at high RPS)
}

// Backend chooses where limiter state lives.
type Backend struct {
	Type            string        `yaml:"type"` // memory | redis
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
	Redis           Redis         `yaml:"redis"`
	// FailureMode: fail_open | fail_closed | local_fallback (redis only).
	FailureMode string  `yaml:"failure_mode"`
	Breaker     Breaker `yaml:"breaker"`
}

// Redis connection settings.
type Redis struct {
	Addr     string        `yaml:"addr"`
	Password string        `yaml:"password"`
	DB       int           `yaml:"db"`
	PoolSize int           `yaml:"pool_size"`
	Timeout  time.Duration `yaml:"timeout"` // per limiter call
}

// Breaker settings for the Redis circuit breaker.
type Breaker struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	OpenTimeout      time.Duration `yaml:"open_timeout"`
}

// Route maps a path prefix to an upstream, optionally rate limited.
type Route struct {
	Name        string     `yaml:"name"`
	PathPrefix  string     `yaml:"path_prefix"`
	Upstream    string     `yaml:"upstream"`
	StripPrefix bool       `yaml:"strip_prefix"`
	RateLimit   *RateLimit `yaml:"rate_limit"` // nil => not limited
}

// RateLimit is a route's limit. KeyBy picks what identifies a client:
//
//	ip             client IP
//	api_key        X-API-Key header
//	user_id        X-User-ID header
//	header:<Name>  any header
//
// If the header is missing the request is keyed by client IP instead, so
// anonymous traffic is still limited.
type RateLimit struct {
	Algorithm string        `yaml:"algorithm"`
	KeyBy     string        `yaml:"key_by"`
	Limit     int64         `yaml:"limit"`
	Window    time.Duration `yaml:"window"`
	Burst     int64         `yaml:"burst"`
	Overrides []Override    `yaml:"overrides"`
}

// Override gives one specific key (e.g. a premium API key) its own limit.
type Override struct {
	Key    string        `yaml:"key"`
	Limit  int64         `yaml:"limit"`
	Window time.Duration `yaml:"window"`
	Burst  int64         `yaml:"burst"`
}

// Rule converts the YAML fields into a limiter.Rule.
func (r RateLimit) Rule() limiter.Rule {
	return limiter.Rule{Limit: r.Limit, Window: r.Window, Burst: r.Burst}
}

// Rule converts an override; a zero window inherits the parent's window.
func (o Override) Rule(parent RateLimit) limiter.Rule {
	w := o.Window
	if w == 0 {
		w = parent.Window
	}
	return limiter.Rule{Limit: o.Limit, Window: w, Burst: o.Burst}
}

// Load reads, defaults and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse decodes YAML bytes, applies defaults and validates.
func Parse(raw []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // a typo like "limt:" is an error, not silently ignored
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	b := &c.Backend
	if b.Type == "" {
		b.Type = "memory"
	}
	if b.CleanupInterval == 0 {
		b.CleanupInterval = time.Minute
	}
	if b.FailureMode == "" {
		b.FailureMode = string(limiter.LocalFallback)
	}
	if b.Redis.Timeout == 0 {
		b.Redis.Timeout = 50 * time.Millisecond
	}
	if b.Redis.PoolSize == 0 {
		b.Redis.PoolSize = 256
	}
	if b.Breaker.FailureThreshold == 0 {
		b.Breaker.FailureThreshold = 5
	}
	if b.Breaker.OpenTimeout == 0 {
		b.Breaker.OpenTimeout = 2 * time.Second
	}
	for i := range c.Routes {
		if rl := c.Routes[i].RateLimit; rl != nil {
			if rl.Algorithm == "" {
				rl.Algorithm = string(limiter.TokenBucket)
			}
			if rl.KeyBy == "" {
				rl.KeyBy = "ip"
			}
		}
	}
}

// Validate checks the config and returns all problems found, joined.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	switch c.Backend.Type {
	case "memory":
	case "redis":
		if c.Backend.Redis.Addr == "" {
			add("backend.redis.addr is required for the redis backend")
		}
	default:
		add("backend.type must be memory or redis, got %q", c.Backend.Type)
	}
	if _, err := limiter.ParseFailureMode(c.Backend.FailureMode); err != nil {
		add("backend.failure_mode: %v", err)
	}
	if len(c.Routes) == 0 {
		add("at least one route is required")
	}

	names := map[string]bool{}
	for i, r := range c.Routes {
		where := fmt.Sprintf("routes[%d] (%s)", i, r.Name)
		if r.Name == "" {
			add("routes[%d]: name is required", i)
		} else if names[r.Name] {
			add("%s: duplicate route name", where)
		}
		names[r.Name] = true
		if !strings.HasPrefix(r.PathPrefix, "/") {
			add("%s: path_prefix must start with /", where)
		}
		if u, err := url.Parse(r.Upstream); err != nil || u.Scheme == "" || u.Host == "" {
			add("%s: upstream must be an absolute URL like http://host:port", where)
		}
		if rl := r.RateLimit; rl != nil {
			if _, err := limiter.ParseAlgorithm(rl.Algorithm); err != nil {
				add("%s: %v", where, err)
			}
			if !validKeyBy(rl.KeyBy) {
				add("%s: key_by must be ip, api_key, user_id or header:<Name>, got %q", where, rl.KeyBy)
			}
			if err := rl.Rule().Validate(); err != nil {
				add("%s: %v", where, err)
			}
			for j, o := range rl.Overrides {
				if o.Key == "" {
					add("%s: overrides[%d].key is required", where, j)
				}
				if err := o.Rule(*rl).Validate(); err != nil {
					add("%s: overrides[%d]: %v", where, j, err)
				}
			}
		}
	}
	return errors.Join(errs...)
}

func validKeyBy(k string) bool {
	switch k {
	case "ip", "api_key", "user_id":
		return true
	}
	return strings.HasPrefix(k, "header:") && len(k) > len("header:")
}
