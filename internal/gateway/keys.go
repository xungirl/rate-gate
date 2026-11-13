package gateway

import (
	"net"
	"net/http"
	"strings"
)

// keyFunc extracts the rate-limit identity from a request. It returns:
//
//	key  the string the limiter counts against, prefixed by kind
//	     ("key:abc", "ip:10.0.0.1") so an API key can never collide with an IP
//	id   the raw identity, used to look up per-key overrides
type keyFunc func(r *http.Request) (key, id string)

// newKeyFunc builds the extractor for a route's key_by setting
// (already validated by the config package).
func newKeyFunc(keyBy string, trustForwardedFor bool) keyFunc {
	ipKey := func(r *http.Request) (string, string) {
		ip := clientIP(r, trustForwardedFor)
		return "ip:" + ip, ip
	}

	var header, kind string
	switch {
	case keyBy == "ip":
		return ipKey
	case keyBy == "api_key":
		header, kind = "X-API-Key", "key:"
	case keyBy == "user_id":
		header, kind = "X-User-ID", "user:"
	default: // "header:<Name>"
		header, kind = strings.TrimPrefix(keyBy, "header:"), "hdr:"
	}

	return func(r *http.Request) (string, string) {
		if v := r.Header.Get(header); v != "" {
			return kind + v, v
		}
		// No identity header: fall back to IP so anonymous callers are
		// still limited instead of sharing one unlimited bucket.
		return ipKey(r)
	}
}

// clientIP returns the caller's IP. With trustForwardedFor it uses the
// right-most X-Forwarded-For entry: the address our own load balancer saw.
// Left-most entries are written by the client and can be forged.
func clientIP(r *http.Request, trustForwardedFor bool) string {
	if trustForwardedFor {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if ip := strings.TrimSpace(parts[len(parts)-1]); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
