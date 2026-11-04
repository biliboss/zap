package test

import (
	"context"
	"os"
	"strings"

	"path/filepath"
	"testing"
	"time"
	"zap/core"
)

// TestSocketRoundTrip exercises the wire protocol against core.NewFake — the
// code that ships, not a stub — so the shapes tested here are the shapes a
// consumer actually gets.
func TestSocketRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	back := core.NewFake("+5511999999999")
	socket := filepath.Join(t.TempDir(), "zap.sock")
	d := &core.Daemon{Backend: back, Socket: socket}
	go d.Serve(ctx)

	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)

	t.Run("accounts", func(t *testing.T) {
		var accs []core.Account
		if err := cli.JSON(ctx, "GET", "/accounts", nil, &accs); err != nil {
			t.Fatal(err)
		}
		if len(accs) != 1 || !accs[0].Connected {
			t.Fatalf("got %+v", accs)
		}
	})

	t.Run("send", func(t *testing.T) {
		var res core.SendResult
		follow(t, ctx, cli, "+55 11 98888-7777")
		req := core.SendRequest{To: "+55 11 98888-7777", Text: "oi"}
		if err := cli.JSON(ctx, "POST", "/send", req, &res); err != nil {
			t.Fatal(err)
		}
		if res.ID == "" {
			t.Fatalf("got %+v", res)
		}
		var msgs []core.Event
		if err := cli.JSON(ctx, "GET", "/messages", nil, &msgs); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, m := range msgs {
			if m.ID == res.ID {
				found = true
			}
		}
		if !found {
			t.Fatalf("sent message %s not reported by /messages: %+v", res.ID, msgs)
		}
	})

	t.Run("pair streams qr then jid", func(t *testing.T) {
		var seen []core.PairEvent
		err := cli.Stream(ctx, "POST", "/pair", nil, func(line []byte) error {
			var ev core.PairEvent
			if err := core.Decode(line, &ev); err != nil {
				return err
			}
			seen = append(seen, ev)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(seen) != 2 || seen[0].QR == "" || seen[1].JID == "" {
			t.Fatalf("got %+v", seen)
		}
	})

	t.Run("groups", func(t *testing.T) {
		var g core.Group
		create := core.GroupRequest{From: "+5511999999999", Action: "create", Name: "obra", Who: []string{"+5511911111111", "+5511922222222"}}
		if err := cli.JSON(ctx, "POST", "/groups", create, &g); err != nil {
			t.Fatal(err)
		}
		if len(g.Participants) != 2 {
			t.Fatalf("got %+v", g)
		}

		var added core.Group
		add := core.GroupRequest{From: "+5511999999999", Action: "add", Group: g.JID, Who: []string{"+5511933333333"}}
		if err := cli.JSON(ctx, "POST", "/groups", add, &added); err != nil {
			t.Fatal(err)
		}
		if len(added.Participants) != 3 {
			t.Fatalf("got %+v", added)
		}

		var groups []core.Group
		if err := cli.JSON(ctx, "GET", "/groups?from=+5511999999999", nil, &groups); err != nil {
			t.Fatal(err)
		}
		if len(groups) != 1 {
			t.Fatalf("got %+v", groups)
		}
	})

	// The request stream is the integration surface: anything that speaks ndjson
	// can watch the daemon work without being built into it.
	t.Run("log streams every request", func(t *testing.T) {
		streamCtx, stop := context.WithCancel(ctx)
		defer stop()

		got := make(chan core.Call, 1)
		go cli.Stream(streamCtx, "GET", "/log", nil, func(line []byte) error {
			var c core.Call
			if err := core.Decode(line, &c); err != nil {
				return err
			}
			got <- c
			stop()
			return nil
		})
		time.Sleep(200 * time.Millisecond) // let the reader subscribe

		var accs []core.Account
		if err := cli.JSON(ctx, "GET", "/accounts", nil, &accs); err != nil {
			t.Fatal(err)
		}
		select {
		case c := <-got:
			if c.Path != "/accounts" || c.Status != 200 {
				t.Fatalf("got %+v", c)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no call on /log")
		}
	})

	t.Run("tail carries the account", func(t *testing.T) {
		follow(t, ctx, cli, "+5511988888888")
		streamCtx, stop := context.WithCancel(ctx)
		defer stop()

		got := make(chan core.Event, 1)
		go cli.Stream(streamCtx, "GET", "/tail", nil, func(line []byte) error {
			var ev core.Event
			if err := core.Decode(line, &ev); err != nil {
				return err
			}
			got <- ev
			stop()
			return nil
		})
		time.Sleep(200 * time.Millisecond) // let the reader subscribe

		var res core.SendResult
		req := core.SendRequest{From: "+5511999999999", To: "+5511988888888", Text: "eae"}
		if err := cli.JSON(ctx, "POST", "/send", req, &res); err != nil {
			t.Fatal(err)
		}
		select {
		case ev := <-got:
			if ev.Account == "" || ev.Text != "eae" {
				t.Fatalf("got %+v", ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no event on /tail")
		}
	})
}

// TestSecondDaemonRefused guards the rule that a live socket is never stolen.
func TestSecondDaemonRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	back := core.NewFake("+5511999999999")
	socket := filepath.Join(t.TempDir(), "zap.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	waitUp(t, ctx, core.NewClient(socket))

	second := &core.Daemon{Backend: back, Socket: socket}
	if err := second.Serve(ctx); err == nil {
		t.Fatal("second daemon took over a live socket")
	}
}

// TestFakeInbound proves an injected message reaches both the live stream and
// the store, decorated from a contact created first.
func TestFakeInbound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	back := core.NewFake("+5511999999999")
	socket := filepath.Join(t.TempDir(), "zap.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)

	if err := cli.JSON(ctx, "POST", "/fake", core.FakeRequest{Action: "contact", Number: "+5511900000000", Name: "Ana"}, nil); err != nil {
		t.Fatal(err)
	}

	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	got := make(chan core.Event, 1)
	go cli.Stream(streamCtx, "GET", "/tail", nil, func(line []byte) error {
		var ev core.Event
		if err := core.Decode(line, &ev); err != nil {
			return err
		}
		if ev.Type == core.Events.Message.Received {
			got <- ev
			stop()
		}
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	follow(t, ctx, cli, "+5511900000000")
	inbound := core.FakeRequest{Action: "inbound", From: "+5511999999999", Number: "+5511900000000", Text: "oi"}
	if err := cli.JSON(ctx, "POST", "/fake", inbound, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-got:
		if ev.SenderName != "Ana" || ev.SenderNumber == "" {
			t.Fatalf("got %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message_received on /tail")
	}

	var msgs []core.Event
	if err := cli.JSON(ctx, "GET", "/messages", nil, &msgs); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range msgs {
		if m.Text == "oi" {
			found = true
		}
	}
	if !found {
		t.Fatalf("inbound message not in /messages: %+v", msgs)
	}
}

// TestFakeSendAndSessions checks both directions land in the store with the
// right from_me, and that /sessions cuts them into one session.
func TestFakeSendAndSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	back := core.NewFake("+5511999999999")
	socket := filepath.Join(t.TempDir(), "zap.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)

	follow(t, ctx, cli, "+5511900000000")
	inbound := core.FakeRequest{Action: "inbound", From: "+5511999999999", Number: "+5511900000000", Text: "oi"}
	if err := cli.JSON(ctx, "POST", "/fake", inbound, nil); err != nil {
		t.Fatal(err)
	}
	var res core.SendResult
	if err := cli.JSON(ctx, "POST", "/send", core.SendRequest{To: "+5511900000000", Text: "opa"}, &res); err != nil {
		t.Fatal(err)
	}

	var msgs []core.Event
	if err := cli.JSON(ctx, "GET", "/messages", nil, &msgs); err != nil {
		t.Fatal(err)
	}
	var in, out int
	for _, m := range msgs {
		if m.FromMe {
			out++
		} else {
			in++
		}
	}
	if in != 1 || out != 1 {
		t.Fatalf("want 1 in / 1 out, got %d/%d: %+v", in, out, msgs)
	}

	var sessions []core.Session
	if err := cli.JSON(ctx, "GET", "/sessions?gap=24h", nil, &sessions); err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Messages != 2 {
		t.Fatalf("want 1 session of 2 messages, got %+v", sessions)
	}
}

// TestFakeAudio proves the whole audio_queued → audio_transcribed → text in
// store chain works with --text, without mlx-whisper installed.
func TestFakeAudio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	back := core.NewFake("+5511999999999")
	socket := filepath.Join(t.TempDir(), "zap.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)

	path := filepath.Join(t.TempDir(), "voice.ogg")
	if err := os.WriteFile(path, []byte("fake audio bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	seenQueued, seenTranscribed := make(chan struct{}, 1), make(chan struct{}, 1)
	go cli.Stream(streamCtx, "GET", "/tail", nil, func(line []byte) error {
		var ev core.Event
		if err := core.Decode(line, &ev); err != nil {
			return err
		}
		switch ev.Type {
		case core.Events.Audio.Queued:
			seenQueued <- struct{}{}
		case core.Events.Audio.Transcribed:
			seenTranscribed <- struct{}{}
			stop()
		}
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	follow(t, ctx, cli, "+5511900000000")
	audio := core.FakeRequest{Action: "audio", From: "+5511999999999", Number: "+5511900000000", Path: path, Text: "transcrição"}
	if err := cli.JSON(ctx, "POST", "/fake", audio, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case <-seenQueued:
	case <-time.After(5 * time.Second):
		t.Fatal("never saw audio_queued")
	}
	select {
	case <-seenTranscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("never saw audio_transcribed")
	}

	var msgs []core.Event
	if err := cli.JSON(ctx, "GET", "/messages", nil, &msgs); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range msgs {
		if m.Text == "transcrição" {
			found = true
		}
	}
	if !found {
		t.Fatalf("transcript not in /messages: %+v", msgs)
	}
}

// TestFakeVerbsAgainstRealDaemon proves the type assertion fails before
// anything is dereferenced: a real daemon answers /fake with the sentence,
// not a bare 404 and not a panic from a nil *WhatsApp.
func TestFakeVerbsAgainstRealDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// a short dir, not t.TempDir(): this test's own name is long enough that
	// nesting the socket under it blows the ~100 byte kernel limit.
	dir, err := os.MkdirTemp("", "zapfake")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "zap.sock")
	d := &core.Daemon{Backend: (*core.WhatsApp)(nil), Socket: socket}
	go d.Serve(ctx)

	cli := core.NewClient(socket)
	for i := 0; i < 100; i++ {
		err = cli.JSON(ctx, "POST", "/fake", core.FakeRequest{Action: "contact"}, nil)
		if err != nil && strings.Contains(err.Error(), "this daemon is real") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("want the fake-verbs-on-real-daemon sentence, got %v", err)
}

func waitUp(t *testing.T, ctx context.Context, cli *core.Client) {
	t.Helper()
	for i := 0; i < 100; i++ {
		var accs []core.Account
		if err := cli.JSON(ctx, "GET", "/accounts", nil, &accs); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("daemon never came up")
}

// TestSplitSessions is the only real logic in the session view: a chat is cut
// wherever it stayed quiet longer than the gap.
func TestSplitSessions(t *testing.T) {
	base := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	at := func(h int) time.Time { return base.Add(time.Duration(h) * time.Hour) }

	// newest first, the order Messages returns
	msgs := []core.Event{
		{Chat: "a", Timestamp: at(50), Text: "terceira"},
		{Chat: "a", Timestamp: at(20), FromMe: true},
		{Chat: "a", Timestamp: at(19)},
		{Chat: "a", Timestamp: at(0)},
		{Chat: "b", Timestamp: at(1)},
	}
	got := core.SplitSessions(msgs, 24*time.Hour)
	if len(got) != 3 {
		t.Fatalf("want 3 sessions, got %d: %+v", len(got), got)
	}
	if got[0].Last != "terceira" || got[0].Messages != 1 {
		t.Fatalf("newest session wrong: %+v", got[0])
	}
	mid := got[1]
	if mid.Chat != "a" || mid.Messages != 3 || mid.Outbound != 1 || mid.Inbound != 2 {
		t.Fatalf("middle session wrong: %+v", mid)
	}
	if !mid.Started.Equal(at(0)) || !mid.Ended.Equal(at(20)) {
		t.Fatalf("session should span 0h→20h, got %s→%s", mid.Started, mid.Ended)
	}
	if wide := core.SplitSessions(msgs, 72*time.Hour); len(wide) != 2 {
		t.Fatalf("a 72h gap should merge chat a into one session, got %d", len(wide))
	}
}

// TestSchemaIsCurrent fails when a model changed and schema.json did not.
// Regenerate with: zap schema > schema.json
func TestSchemaIsCurrent(t *testing.T) {
	want, err := core.SchemaJSON()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("../zap/schema.json")
	if err != nil {
		t.Fatalf("%v — run: zap schema > schema.json", err)
	}
	if string(got) != string(want) {
		t.Fatal("schema.json is stale — run: zap schema > schema.json")
	}
}

// TestHookRun proves the event reaches the command's stdin as one ndjson
// line, without going through a shell: the command is /usr/bin/tee writing
// to a file we can read back.
func TestHookRun(t *testing.T) {
	if _, err := os.Stat("/usr/bin/tee"); err != nil {
		t.Skip("/usr/bin/tee not present on this machine")
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "event.ndjson")

	hook := &core.Hook{
		Match:   []core.EventType{core.Events.Message.Received},
		Command: []string{"/usr/bin/tee", out},
	}
	ev := core.Event{
		Account: "5511999999999@s.whatsapp.net",
		Type:    core.Events.Message.Received,
		Chat:    "5511988888888@s.whatsapp.net",
		Sender:  "5511988888888@s.whatsapp.net",
		Text:    "oi",
	}
	if !hook.Matches(ev) {
		t.Fatal("hook should match message_received")
	}
	if err := hook.Run(context.Background(), ev); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading tee output: %v", err)
	}
	var decoded core.Event
	if err := core.Decode(got, &decoded); err != nil {
		t.Fatalf("decoding what reached the command's stdin: %v", err)
	}
	if decoded.Type != ev.Type || decoded.Text != ev.Text {
		t.Fatalf("event did not round-trip through the command's stdin: got %+v", decoded)
	}
}

// follow opens the door for one chat: zap listens to nothing until it is
// followed, so every test that wants to see a message starts here.
func follow(t *testing.T, ctx context.Context, cli *core.Client, chat string) {
	t.Helper()
	// the first account: a test that paired more numbers must still be unambiguous
	var accs []core.Account
	if err := cli.JSON(ctx, "GET", "/accounts", nil, &accs); err != nil || len(accs) == 0 {
		t.Fatalf("accounts: %v", err)
	}
	var res core.FollowResult
	req := core.FollowRequest{From: accs[0].Number, Chat: chat, NoBackfill: true}
	if err := cli.JSON(ctx, "POST", "/follow", req, &res); err != nil {
		t.Fatalf("follow %s: %v", chat, err)
	}
}
