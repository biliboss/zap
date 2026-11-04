package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Sink is where an ALLOWED event goes to be kept or forwarded. It is the one
// seam a new backend plugs into: a type that satisfies it and one line in
// NewSink. The sink never sees a chat the allowlist did not let through —
// the Policy in front of it has already decided.
type Sink interface {
	Publish(ctx context.Context, ev Event) error
	Close() error
}

// NewSink builds the backend a config chooses. out is where the ndjson backend
// writes when stdout is on (os.Stdout in the daemon, a buffer in a test).
func NewSink(ctx context.Context, cfg Config, out io.Writer) (Sink, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch cfg.Backend.Type {
	case "ndjson":
		return &ndjsonSink{on: cfg.Backend.NDJSON.Stdout, out: out}, nil
	case "nats":
		return newNATSSink(ctx, cfg.Backend.NATS)
	}
	return nil, fmt.Errorf("unknown backend %q", cfg.Backend.Type)
}

// ndjsonSink is what zap did before backends existed: the event is on `zap
// tail` (the bus, which is always there) and, if asked, one line per event on
// the stdout of the process.
type ndjsonSink struct {
	on  bool
	mu  sync.Mutex
	out io.Writer
}

func (s *ndjsonSink) Publish(_ context.Context, ev Event) error {
	if !s.on || s.out == nil {
		return nil
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.out.Write(append(line, '\n'))
	return err
}

func (s *ndjsonSink) Close() error { return nil }

// SubjectEvent names the entity and the event a zap event becomes on a bus
// that uses `<prefix>.<entity>.<event>`. Only messages and the follow
// lifecycle are published: a group rename or a call is for `zap tail`.
func SubjectEvent(t EventType) (entity, event string, ok bool) {
	switch t {
	case Events.Message.Received, Events.Media.Received:
		return "message", "received", true
	case Events.Message.Sent:
		return "message", "sent", true
	case Events.Message.Edited:
		return "message", "edited", true
	case Events.Message.Revoked:
		return "message", "revoked", true
	case Events.Message.Reacted:
		return "message", "reacted", true
	case Events.Audio.Transcribed:
		return "message", "transcribed", true
	case Events.Chat.Followed:
		return "chat", "followed", true
	case Events.Chat.Unfollowed:
		return "chat", "unfollowed", true
	case Events.Chat.Backfilled:
		return "chat", "backfilled", true
	}
	return "", "", false
}

// Subject is the full subject of an event, or "" when the event is not one a
// backend receives.
func Subject(prefix string, t EventType) string {
	entity, event, ok := SubjectEvent(t)
	if !ok {
		return ""
	}
	return prefix + "." + entity + "." + event
}

// MsgID is the identity a backend can dedupe on, so replaying a backfill is
// harmless.
func MsgID(ev Event) string {
	switch ev.Type {
	case Events.Chat.Followed, Events.Chat.Unfollowed, Events.Chat.Backfilled:
		return fmt.Sprintf("%s/%s/%s/%d", ev.Account, ev.Chat, ev.Type, ev.Timestamp.UnixNano())
	case Events.Message.Received, Events.Media.Received, Events.Message.Sent:
		// one message, whatever the type it arrived as
		return fmt.Sprintf("%s/%s/%s", ev.Account, ev.Chat, ev.ID)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", ev.Account, ev.Chat, ev.Type, ev.ID, ev.Target)
}

// Outbox decouples the event handler from the backend: whatsmeow calls the
// handler on its own goroutine, and a slow or dead broker must not stall it.
// A full queue waits a moment and then drops, counting what was lost — it
// never grows without limit and never writes the message into a log.
type Outbox struct {
	sink    Sink
	ch      chan Event
	mu      sync.RWMutex
	closed  bool
	wg      sync.WaitGroup
	dropped atomic.Int64
	failed  atomic.Int64
	warn    func(string)
}

// NewOutbox starts the worker. depth is the queue size.
func NewOutbox(sink Sink, depth int) *Outbox {
	if depth <= 0 {
		depth = 1024
	}
	o := &Outbox{
		sink: sink,
		ch:   make(chan Event, depth),
		warn: func(s string) { fmt.Fprintln(os.Stderr, s) },
	}
	o.wg.Add(1)
	go o.run()
	return o
}

func (o *Outbox) run() {
	defer o.wg.Done()
	for ev := range o.ch {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := o.sink.Publish(ctx, ev)
		cancel()
		if err != nil {
			// counts and the error class only: never the event, whose text is
			// somebody else's words
			if n := o.failed.Add(1); n == 1 || n%100 == 0 {
				o.warn(fmt.Sprintf("zap: backend publish failed (%d so far): %v", n, err))
			}
		}
	}
}

// Put queues one event.
func (o *Outbox) Put(ev Event) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return // shutting down: a late event is dropped, never sent on a closed channel
	}
	select {
	case o.ch <- ev:
		return
	default:
	}
	select {
	case o.ch <- ev:
	case <-time.After(2 * time.Second):
		if n := o.dropped.Add(1); n == 1 || n%100 == 0 {
			o.warn(fmt.Sprintf("zap: backend queue full, dropped %d event(s)", n))
		}
	}
}

// Stats reports what was lost, for a test and for `zap`'s exit.
func (o *Outbox) Stats() (dropped, failed int64) { return o.dropped.Load(), o.failed.Load() }

// Close drains the queue, then closes the backend.
func (o *Outbox) Close() error {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil
	}
	o.closed = true
	close(o.ch)
	o.mu.Unlock()
	o.wg.Wait()
	return o.sink.Close()
}
