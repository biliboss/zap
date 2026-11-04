package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// natsSink publishes every allowed event to a JetStream stream, one subject
// per kind of event: <prefix>.message.received, <prefix>.chat.followed ...
// and waits for the stream's ack, so "published" means "stored".
type natsSink struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	prefix string
}

func newNATSSink(ctx context.Context, cfg NATSConfig) (*natsSink, error) {
	opts := []nats.Option{
		nats.Name("zap"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
	}
	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(expandHome(cfg.CredsFile)))
	}
	if cfg.TokenEnv != "" {
		tok := os.Getenv(cfg.TokenEnv)
		if tok == "" {
			return nil, fmt.Errorf("nats: the token variable %s is empty", cfg.TokenEnv)
		}
		opts = append(opts, nats.Token(tok))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}
	s := &natsSink{nc: nc, js: js, prefix: cfg.Prefix}
	if err := s.ensureStream(ctx, cfg); err != nil {
		nc.Close()
		return nil, err
	}
	return s, nil
}

// ensureStream finds the stream (or creates it) and checks that it captures
// the subjects zap publishes. A stream that does not would accept the publish
// call with a "no responders" error at the first message; failing at startup
// says it at the right time.
func (s *natsSink) ensureStream(ctx context.Context, cfg NATSConfig) error {
	subjects := cfg.Subjects
	if len(subjects) == 0 {
		subjects = []string{cfg.Prefix + ".>"}
	}
	stream, err := s.js.Stream(ctx, cfg.Stream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if !cfg.CreateStream {
			return fmt.Errorf("nats: stream %q does not exist and create_stream is false", cfg.Stream)
		}
		sc := jetstream.StreamConfig{
			Name:     cfg.Stream,
			Subjects: subjects,
			Storage:  jetstream.FileStorage,
			// a window that makes a replayed backfill harmless
			Duplicates: 24 * time.Hour,
		}
		if cfg.Storage == "memory" {
			sc.Storage = jetstream.MemoryStorage
		}
		if cfg.MaxAge != "" {
			sc.MaxAge, _ = time.ParseDuration(cfg.MaxAge)
		}
		if stream, err = s.js.CreateStream(ctx, sc); err != nil {
			return fmt.Errorf("nats: create stream %q: %w", cfg.Stream, err)
		}
	} else if err != nil {
		return fmt.Errorf("nats: stream %q: %w", cfg.Stream, err)
	}
	have := stream.CachedInfo().Config.Subjects
	for _, probe := range []string{Subject(cfg.Prefix, Events.Message.Received), Subject(cfg.Prefix, Events.Chat.Followed)} {
		if !subjectCovered(have, probe) {
			return fmt.Errorf("nats: stream %q captures %v but zap publishes %q: set backend.nats.subjects or prefix", cfg.Stream, have, probe)
		}
	}
	return nil
}

func (s *natsSink) Publish(ctx context.Context, ev Event) error {
	subject := Subject(s.prefix, ev.Type)
	if subject == "" {
		return nil // not an event backends receive
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = data
	msg.Header.Set(nats.MsgIdHdr, MsgID(ev))
	_, err = s.js.PublishMsg(ctx, msg)
	return err
}

func (s *natsSink) Close() error {
	if err := s.nc.Drain(); err != nil {
		s.nc.Close()
	}
	return nil
}

// subjectCovered reports whether any NATS subject pattern matches subject.
func subjectCovered(patterns []string, subject string) bool {
	for _, p := range patterns {
		if subjectMatches(p, subject) {
			return true
		}
	}
	return false
}

func subjectMatches(pattern, subject string) bool {
	pt, st := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, p := range pt {
		switch {
		case p == ">":
			return len(st) > i
		case i >= len(st):
			return false
		case p == "*":
		case p != st[i]:
			return false
		}
	}
	return len(pt) == len(st)
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + p[1:]
		}
	}
	return p
}
