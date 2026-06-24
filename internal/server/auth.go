package server

import (
	"net/http"
	"strings"
)

// authMiddleware checks X-Agent-Key against the configured key. Constant-time
// compare avoids leaking length / prefix via timing.
func authMiddleware(expected string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			got := r.Header.Get("X-Agent-Key")
			if got == "" || !constantTimeEq(got, expected) {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "X-Agent-Key required or invalid"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func constantTimeEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	// Compare-by-byte is enough; we already checked length above.
	return v == 0 && !strings.ContainsRune(a, 0)
}
