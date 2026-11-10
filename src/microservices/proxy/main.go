package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func main() {
	gradual, err := strconv.ParseBool(env("GRADUAL_MIGRATION", "true"))
	if err != nil {
		log.Fatal("GRADUAL_MIGRATION must be a boolean")
	}
	percent, err := strconv.Atoi(env("MOVIES_MIGRATION_PERCENT", "50"))
	if err != nil || percent < 0 || percent > 100 {
		log.Fatal("MOVIES_MIGRATION_PERCENT must be an integer between 0 and 100")
	}

	monolith := newProxy(env("MONOLITH_URL", "http://monolith:8080"))
	movies := newProxy(env("MOVIES_SERVICE_URL", "http://movies-service:8081"))
	events := newProxy(env("EVENTS_SERVICE_URL", "http://events-service:8082"))

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Strangler Fig Proxy is healthy")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		proxy, upstream := monolith, "monolith"
		switch {
		case r.URL.Path == "/api/events" || strings.HasPrefix(r.URL.Path, "/api/events/"):
			proxy, upstream = events, "events"
		case r.URL.Path == "/api/movies" || strings.HasPrefix(r.URL.Path, "/api/movies/"):
			// Disabling gradual migration completes the move to the movies service.
			if !gradual || rand.IntN(100) < percent {
				proxy, upstream = movies, "movies"
			}
		}
		log.Printf("Routing %s %s to %s", r.Method, r.URL.RequestURI(), upstream)
		proxy.ServeHTTP(w, r)
	})

	port := env("PORT", "8000")
	log.Printf("Starting proxy on port %s: gradual_migration=%t movies_migration_percent=%d", port, gradual, percent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func newProxy(address string) *httputil.ReverseProxy {
	target, err := url.Parse(address)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		log.Fatalf("Invalid upstream URL: %s", address)
	}
	return httputil.NewSingleHostReverseProxy(target)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
