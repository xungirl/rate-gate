package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `
listen: ":9000"
backend:
  type: redis
  redis: { addr: "127.0.0.1:6379" }
  failure_mode: fail_closed
routes:
  - name: api
    path_prefix: /api/
    upstream: http://127.0.0.1:9001
    rate_limit:
      algorithm: sliding_window_counter
      key_by: api_key
      limit: 100
      window: 1m
      overrides:
        - { key: premium, limit: 1000 }
  - name: static
    path_prefix: /
    upstream: http://127.0.0.1:9002
`

func TestParseValidAppliesDefaults(t *testing.T) {
	c, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":9000" || c.Backend.Redis.Timeout != 50*time.Millisecond || c.ShutdownTimeout != 10*time.Second {
		t.Errorf("defaults not applied: %+v", c)
	}
	rl := c.Routes[0].RateLimit
	if rl.Window != time.Minute {
		t.Errorf("window=%s want 1m", rl.Window)
	}
	if got := rl.Overrides[0].Rule(*rl); got.Window != time.Minute || got.Limit != 1000 {
		t.Errorf("override should inherit window: %+v", got)
	}
	if c.Routes[1].RateLimit != nil {
		t.Error("route without rate_limit should be unlimited")
	}
}

func TestParseRejectsBadConfig(t *testing.T) {
	cases := map[string]string{
		"unknown field":    strings.Replace(valid, "limit: 100", "limt: 100", 1),
		"bad algorithm":    strings.Replace(valid, "sliding_window_counter", "leaky_bucket", 1),
		"bad key_by":       strings.Replace(valid, "key_by: api_key", "key_by: cookie", 1),
		"no redis addr":    strings.Replace(valid, `redis: { addr: "127.0.0.1:6379" }`, "", 1),
		"bad upstream":     strings.Replace(valid, "http://127.0.0.1:9001", "not a url", 1),
		"zero limit":       strings.Replace(valid, "limit: 100", "limit: 0", 1),
		"bad failure mode": strings.Replace(valid, "fail_closed", "fail_sideways", 1),
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: accepted, want error", name)
		}
	}
}
