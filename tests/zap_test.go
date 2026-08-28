package test

import (
	"context"
	"os"

	"path/filepath"
	"testing"
	"time"
	"zap/core"
)

// stubBackend stands in for whatsmeow so the wire protocol is checkable without
// a paired phone.
type stubBackend struct {
	sent   chan core.SendRequest
	events chan core.Event
}

func (s *stubBackend) Accounts() []core.Account {
	return []core.Account{{JID: "5511999999999@s.whatsapp.net", PushName: "bili", Connected: true}}
}

func (s *stubBackend) Send(_ context.Context, req core.SendRequest) (core.SendResult, error) {
	s.sent <- req
	return core.SendResult{ID: "3EB0", From: "5511999999999@s.whatsapp.net", Timestamp: time.Unix(0, 0).UTC()}, nil
}

func (s *stubBackend) Pair(_ context.Context, emit func(core.PairEvent) error) error {
	if err := emit(core.PairEvent{QR: "2@abc"}); err != nil {
		return err
	}
	return emit(core.PairEvent{JID: "5511999999999@s.whatsapp.net"})
}

func (s *stubBackend) Logout(context.Context, string) error             { return nil }
func (s *stubBackend) Typing(context.Context, core.TypingRequest) error { return nil }
func (s *stubBackend) Presence(context.Context, string, bool) error     { return nil }
func (s *stubBackend) Status(_ context.Context, r core.SendRequest) (core.SendResult, error) {
	return core.SendResult{ID: "STATUS", Timestamp: time.Unix(0, 0).UTC()}, nil
}

func (s *stubBackend) Subscribe(from string) (<-chan core.Event, func()) {
	return s.events, func() {}
}

func (s *stubBackend) Messages(context.Context, string, string, int) ([]core.Event, error) {
	return []core.Event{{Account: "5511999999999@s.whatsapp.net", Type: "message", Chat: "x@s.whatsapp.net", ID: "A", Text: "guardada"}}, nil
}

func (s *stubBackend) SyncChat(_ context.Context, req core.SyncRequest) (core.SyncResult, error) {
	return core.SyncResult{Chat: req.Chat, Requested: req.Count, Anchor: "A"}, nil
}

func (s *stubBackend) Sessions(context.Context, core.SessionQuery) ([]core.Session, error) {
	return []core.Session{{Chat: "x@s.whatsapp.net", Messages: 3}}, nil
}

func (s *stubBackend) Contacts(context.Context, string, string) ([]core.Contact, error) {
	return []core.Contact{{Number: "+5511999999999", JID: "5511999999999@s.whatsapp.net", Name: "Fulano"}}, nil
}

func (s *stubBackend) QueueStats(context.Context) ([]core.QueueStat, error) {
	return []core.QueueStat{{State: "pending", Count: 2}}, nil
}

func (s *stubBackend) Retry(context.Context) (int, error) { return 2, nil }

func (s *stubBackend) Groups(context.Context, string) ([]core.Group, error) {
	return []core.Group{{JID: "120363@g.us", Name: "obra", Participants: []core.GroupMember{{Number: "+5511999999999", Admin: true}}}}, nil
}

func (s *stubBackend) GroupAct(_ context.Context, req core.GroupRequest) (core.Group, error) {
	return core.Group{JID: "120363@g.us", Name: req.Action}, nil
}

func TestSocketRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	back := &stubBackend{sent: make(chan core.SendRequest, 1), events: make(chan core.Event, 1)}
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
		req := core.SendRequest{To: "+55 11 98888-7777", Text: "oi"}
		if err := cli.JSON(ctx, "POST", "/send", req, &res); err != nil {
			t.Fatal(err)
		}
		if res.ID != "3EB0" {
			t.Fatalf("got %+v", res)
		}
		if got := <-back.sent; got.To != req.To || got.Text != "oi" {
			t.Fatalf("backend saw %+v", got)
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
		var groups []core.Group
		if err := cli.JSON(ctx, "GET", "/groups", nil, &groups); err != nil {
			t.Fatal(err)
		}
		if len(groups) != 1 || !groups[0].Participants[0].Admin {
			t.Fatalf("got %+v", groups)
		}
		var g core.Group
		if err := cli.JSON(ctx, "POST", "/groups", core.GroupRequest{Action: "add", Group: "obra"}, &g); err != nil {
			t.Fatal(err)
		}
		if g.Name != "add" {
			t.Fatalf("backend saw %+v", g)
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
		back.events <- core.Event{Account: "5511999999999@s.whatsapp.net", Type: "message", Text: "eae"}
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

	back := &stubBackend{sent: make(chan core.SendRequest, 1), events: make(chan core.Event, 1)}
	socket := filepath.Join(t.TempDir(), "zap.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	waitUp(t, ctx, core.NewClient(socket))

	second := &core.Daemon{Backend: back, Socket: socket}
	if err := second.Serve(ctx); err == nil {
		t.Fatal("second daemon took over a live socket")
	}
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
