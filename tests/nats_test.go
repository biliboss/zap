package test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"zap/core"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// jetServer starts a real nats-server with JetStream, in this process, on a
// random port: no docker, no network, nothing shared with the machine.
func jetServer(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not come up")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

func natsConfig(url, stream, prefix string, subjects ...string) core.Config {
	cfg := core.DefaultConfig()
	cfg.Backend.Type = "nats"
	cfg.Backend.NATS.URL = url
	cfg.Backend.NATS.Stream = stream
	cfg.Backend.NATS.Prefix = prefix
	cfg.Backend.NATS.Subjects = subjects
	cfg.Backend.NATS.Storage = "memory"
	return cfg
}

func streamOf(t *testing.T, url, name string) jetstream.Stream {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := js.Stream(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func msgsIn(t *testing.T, st jetstream.Stream) uint64 {
	t.Helper()
	info, err := st.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// The prefix is the owner's: "mesa.zap" gives the house grammar
// mesa.<domain>.<entity>.<event>, "zap" gives the generic one.
func TestNATSSinkPublishesOnTheConfiguredSubjects(t *testing.T) {
	url := jetServer(t)
	ctx := context.Background()
	sink, err := core.NewSink(ctx, natsConfig(url, "ZAPT", "mesa.zap"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	msg := core.Event{Account: "a", Type: core.Events.Message.Received, Chat: "c@s.whatsapp.net", ID: "M1", Text: "olá", Timestamp: time.Now()}
	follow := core.Event{Account: "a", Type: core.Events.Chat.Followed, Chat: "c@s.whatsapp.net", Text: "Ana", Timestamp: time.Now()}
	for _, ev := range []core.Event{msg, msg, follow} { // the message twice: a replay
		if err := sink.Publish(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	// not an event a backend receives: published to nothing, no error
	if err := sink.Publish(ctx, core.Event{Account: "a", Type: core.Events.Group.Renamed, Chat: "g@g.us"}); err != nil {
		t.Fatal(err)
	}

	st := streamOf(t, url, "ZAPT")
	if n := msgsIn(t, st); n != 2 {
		t.Fatalf("want 2 (the replayed message deduped, the group event not published), got %d", n)
	}
	raw, err := st.GetLastMsgForSubject(ctx, "mesa.zap.message.received")
	if err != nil {
		t.Fatal(err)
	}
	var got core.Event
	if err := json.Unmarshal(raw.Data, &got); err != nil || got.Text != "olá" || got.ID != "M1" {
		t.Fatalf("got %s %v", raw.Data, err)
	}
	if _, err := st.GetLastMsgForSubject(ctx, "mesa.zap.chat.followed"); err != nil {
		t.Fatalf("no mesa.zap.chat.followed: %v", err)
	}
}

// The stream and its subjects are configurable, and a stream that would not
// capture what zap publishes is refused at startup, not at the first message.
func TestNATSSinkChecksTheStreamCapturesTheSubjects(t *testing.T) {
	url := jetServer(t)
	ctx := context.Background()

	ok, err := core.NewSink(ctx, natsConfig(url, "OWN", "chat.zap", "chat.>"), nil)
	if err != nil {
		t.Fatalf("a stream with matching subjects: %v", err)
	}
	ok.Close()

	_, err = core.NewSink(ctx, natsConfig(url, "WRONG", "chat.zap", "other.>"), nil)
	if err == nil || !strings.Contains(err.Error(), "captures") {
		t.Fatalf("want a refusal that names the mismatch, got %v", err)
	}

	cfg := natsConfig(url, "NOPE", "chat.zap")
	cfg.Backend.NATS.CreateStream = false
	if _, err := core.NewSink(ctx, cfg, nil); err == nil {
		t.Fatal("create_stream = false must not create the stream")
	}
}

func TestNATSTokenComesFromTheEnvironment(t *testing.T) {
	cfg := natsConfig("nats://127.0.0.1:1", "S", "zap")
	cfg.Backend.NATS.TokenEnv = "ZAP_TEST_TOKEN_THAT_IS_NOT_SET"
	os.Unsetenv("ZAP_TEST_TOKEN_THAT_IS_NOT_SET")
	_, err := core.NewSink(context.Background(), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "ZAP_TEST_TOKEN_THAT_IS_NOT_SET") {
		t.Fatalf("want an error naming the variable, got %v", err)
	}
}

// The whole thing, end to end: a fake daemon, a real JetStream. A followed chat
// reaches the stream, past and present; a chat nobody allowed leaves the
// stream empty.
func TestFollowEndToEndIntoJetStream(t *testing.T) {
	url := jetServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sink, err := core.NewSink(ctx, natsConfig(url, "ZAPE", "mesa.zap"), nil)
	if err != nil {
		t.Fatal(err)
	}
	out := core.NewOutbox(sink, 256)
	allow, err := core.OpenAllowlist(filepath.Join(t.TempDir(), "allow.toml"))
	if err != nil {
		t.Fatal(err)
	}
	back := core.NewFake(me)
	back.SetPolicy(&core.Policy{Allow: allow, Out: out})

	dir, err := os.MkdirTemp("", "zp")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "z.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)

	post := func(path string, body any) {
		t.Helper()
		if err := cli.JSON(ctx, "POST", path, body, nil); err != nil {
			t.Fatal(err)
		}
	}
	st := streamOf(t, url, "ZAPE")

	// nobody is followed: the stranger reaches nothing
	post("/fake", core.FakeRequest{Action: "inbound", From: me, Number: stranger, Text: "ignored"})
	post("/fake", core.FakeRequest{Action: "history", From: me, Number: ana, Count: 5, Text: "past"})
	post("/fake", core.FakeRequest{Action: "inbound", From: me, Number: ana, Text: "anchor"})
	time.Sleep(300 * time.Millisecond)
	if n := msgsIn(t, st); n != 0 {
		t.Fatalf("a stream that nobody was allowed to write to has %d message(s)", n)
	}

	// follow Ana: chat.followed, her 5 past messages, then the live ones
	post("/follow", core.FollowRequest{Chat: ana, Label: "Ana"})
	until(t, "backfill in the stream", func() bool { return msgsIn(t, st) >= 7 }) // followed + 5 + backfilled
	post("/fake", core.FakeRequest{Action: "inbound", From: me, Number: ana, Text: "live one"})
	post("/fake", core.FakeRequest{Action: "inbound", From: me, Number: stranger, Text: "still ignored"})
	until(t, "live message in the stream", func() bool {
		raw, err := st.GetLastMsgForSubject(ctx, "mesa.zap.message.received")
		return err == nil && strings.Contains(string(raw.Data), "live one")
	})

	// every message in the stream is Ana's; the stranger is nowhere
	nc, _ := nats.Connect(url)
	defer nc.Close()
	js, _ := jetstream.New(nc)
	cons, err := js.OrderedConsumer(ctx, "ZAPE", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	total := msgsIn(t, st)
	backfilled := 0
	for i := uint64(0); i < total; i++ {
		m, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(m.Data()), "5550100002") || strings.Contains(string(m.Data()), "ignored") {
			t.Fatalf("the stranger reached the stream: %s", m.Data())
		}
		var ev core.Event
		json.Unmarshal(m.Data(), &ev)
		if ev.Backfill {
			backfilled++
		}
	}
	if backfilled != 5 {
		t.Fatalf("want 5 backfilled messages in the stream, got %d", backfilled)
	}
	out.Close()
}
