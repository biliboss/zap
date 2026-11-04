package core

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the one file a person edits to choose where allowed messages go.
// zap reads it and never rewrites it: a file that a program rewrites loses the
// comments its owner wrote. What zap itself changes (who is followed) lives in
// the allowlist, a separate file.
type Config struct {
	Backend BackendConfig `toml:"backend"`
}

// BackendConfig picks ONE backend by Type and carries the settings of each.
type BackendConfig struct {
	Type   string       `toml:"type"` // ndjson | nats
	NDJSON NDJSONConfig `toml:"ndjson"`
	NATS   NATSConfig   `toml:"nats"`
}

// NDJSONConfig is what exists today: `zap tail` over the unix socket, which is
// always on, plus an optional copy on the stdout of `zap serve`.
type NDJSONConfig struct {
	Stdout bool `toml:"stdout"`
}

// NATSConfig publishes to a JetStream stream. No default points at a real
// server of anyone: the URL is the local one, and anything else is the
// owner's config.
type NATSConfig struct {
	URL          string   `toml:"url"`
	Stream       string   `toml:"stream"`
	Subjects     []string `toml:"subjects"` // what the stream captures; default <prefix>.>
	Prefix       string   `toml:"prefix"`   // the subject is <prefix>.<entity>.<event>
	CreateStream bool     `toml:"create_stream"`
	Storage      string   `toml:"storage"` // file | memory
	MaxAge       string   `toml:"max_age"` // a Go duration, "0" keeps forever
	CredsFile    string   `toml:"creds_file"`
	// TokenEnv is the NAME of the environment variable holding the token. The
	// token itself never goes into a file that can be committed.
	TokenEnv string `toml:"token_env"`
}

// DefaultConfig is what zap runs with when there is no config file: ndjson,
// nothing on stdout, the NATS block filled in so an owner who only flips the
// type gets a working local setup.
func DefaultConfig() Config {
	return Config{Backend: BackendConfig{
		Type: "ndjson",
		NATS: NATSConfig{
			URL:          "nats://127.0.0.1:4222",
			Stream:       "ZAP",
			Prefix:       "zap",
			CreateStream: true,
			Storage:      "file",
			MaxAge:       "720h",
		},
	}}
}

// LoadConfig reads path. A missing file is not an error — it is the default —
// but an unknown key is: a typo in a privacy setting must not be silently
// ignored.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	meta, err := toml.Decode(string(raw), &cfg)
	if err != nil {
		return cfg, fmt.Errorf("config %s: %w", path, err)
	}
	if un := meta.Undecoded(); len(un) > 0 {
		keys := make([]string, len(un))
		for i, k := range un {
			keys[i] = k.String()
		}
		sort.Strings(keys)
		return cfg, fmt.Errorf("config %s: unknown key(s): %s", path, strings.Join(keys, ", "))
	}
	return cfg, cfg.Validate()
}

// Validate refuses a config that cannot work, before anything connects.
func (c Config) Validate() error {
	switch c.Backend.Type {
	case "ndjson":
		return nil
	case "nats":
		n := c.Backend.NATS
		if n.URL == "" || n.Stream == "" {
			return errors.New("config: backend.nats needs url and stream")
		}
		if err := ValidPrefix(n.Prefix); err != nil {
			return err
		}
		if n.Storage != "file" && n.Storage != "memory" {
			return fmt.Errorf("config: backend.nats.storage must be file or memory, got %q", n.Storage)
		}
		if n.MaxAge != "" {
			if _, err := time.ParseDuration(n.MaxAge); err != nil {
				return fmt.Errorf("config: backend.nats.max_age: %w", err)
			}
		}
		return nil
	default:
		return fmt.Errorf("config: unknown backend.type %q (want ndjson or nats)", c.Backend.Type)
	}
}

// ValidPrefix keeps a subject prefix to plain NATS tokens: no wildcards, no
// spaces, no leading or trailing dot.
func ValidPrefix(p string) error {
	if p == "" {
		return errors.New("config: backend.nats.prefix is empty")
	}
	for _, tok := range strings.Split(p, ".") {
		if tok == "" || strings.ContainsAny(tok, " *>\t\r\n") {
			return fmt.Errorf("config: backend.nats.prefix %q is not a plain subject", p)
		}
	}
	return nil
}
