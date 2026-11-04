package test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"zap/core"
)

// Every number in this file is from the reserved fictional range 555-01xx.
const (
	me       = "+15550100000"
	ana      = "+15550100001"
	stranger = "+15550100002"
)

// memSink is a Sink that keeps what it is handed, so a test can say exactly
// what reached a backend.
type memSink struct {
	mu  sync.Mutex
	got []core.Event
}

func (m *memSink) Publish(_ context.Context, ev core.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.got = append(m.got, ev)
	return nil
}
func (m *memSink) Close() error { return nil }

func (m *memSink) events() []core.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]core.Event(nil), m.got...)
}

func (m *memSink) count(t core.EventType) (n int) {
	for _, ev := range m.events() {
		if ev.Type == t {
			n++
		}
	}
	return n
}

// rig is a fake daemon behind the real policy: an allowlist file and a backend.
type rig struct {
	ctx   context.Context
	cli   *core.Client
	back  *core.Fake
	allow *core.Allowlist
	path  string // the allowlist file
	sink  *memSink
	out   *core.Outbox
}

func newRig(t *testing.T) *rig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "allow.toml")
	allow, err := core.OpenAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	out := core.NewOutbox(sink, 256)
	t.Cleanup(func() { out.Close() })

	back := core.NewFake(me)
	back.SetPolicy(&core.Policy{Allow: allow, Out: out})
	// a short dir, not t.TempDir(): the socket path has a ~100 byte kernel limit
	dir, err := os.MkdirTemp("", "zp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "z.sock")
	go (&core.Daemon{Backend: back, Socket: socket}).Serve(ctx)
	cli := core.NewClient(socket)
	waitUp(t, ctx, cli)
	return &rig{ctx: ctx, cli: cli, back: back, allow: allow, path: path, sink: sink, out: out}
}

func (r *rig) inbound(t *testing.T, from, text string) {
	t.Helper()
	req := core.FakeRequest{Action: "inbound", From: me, Number: from, Text: text}
	if err := r.cli.JSON(r.ctx, "POST", "/fake", req, nil); err != nil {
		t.Fatal(err)
	}
}

func (r *rig) history(t *testing.T, chat string, n int) {
	t.Helper()
	req := core.FakeRequest{Action: "history", From: me, Number: chat, Count: n, Text: "past"}
	if err := r.cli.JSON(r.ctx, "POST", "/fake", req, nil); err != nil {
		t.Fatal(err)
	}
}

func (r *rig) messages(t *testing.T, chat string) []core.Event {
	t.Helper()
	var msgs []core.Event
	path := "/messages?limit=1000"
	if chat != "" {
		path += "&chat=" + chat
	}
	if err := r.cli.JSON(r.ctx, "GET", path, nil, &msgs); err != nil {
		t.Fatal(err)
	}
	return msgs
}

func (r *rig) follow(t *testing.T, req core.FollowRequest) core.FollowResult {
	t.Helper()
	var res core.FollowResult
	if err := r.cli.JSON(r.ctx, "POST", "/follow", req, &res); err != nil {
		t.Fatal(err)
	}
	return res
}

func (r *rig) follows(t *testing.T) []core.Follow {
	t.Helper()
	var list []core.Follow
	if err := r.cli.JSON(r.ctx, "GET", "/follows", nil, &list); err != nil {
		t.Fatal(err)
	}
	return list
}

// until polls cond for a few seconds: the phone answers asynchronously.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (r *rig) backfillState(t *testing.T) string {
	if l := r.follows(t); len(l) > 0 {
		return l[0].Backfill
	}
	return ""
}

// ---------------------------------------------------------------- default deny

// A chat that is not on the allowlist leaves no trace: not in the store, not
// on the tail, not at the backend. This is the rule the whole design hangs on.
func TestDefaultDenyListensToNothing(t *testing.T) {
	r := newRig(t)

	tailed := make(chan core.Event, 8)
	go r.cli.Stream(r.ctx, "GET", "/tail", nil, func(line []byte) error {
		var ev core.Event
		if err := core.Decode(line, &ev); err != nil {
			return err
		}
		tailed <- ev
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	r.inbound(t, stranger, "this must go nowhere")
	var res core.SendResult
	if err := r.cli.JSON(r.ctx, "POST", "/send", core.SendRequest{To: stranger, Text: "nor this"}, &res); err != nil {
		t.Fatal(err)
	}
	if res.ID == "" {
		t.Fatal("sending must keep working: the owner's own act is not a listen")
	}

	time.Sleep(300 * time.Millisecond)
	if msgs := r.messages(t, ""); len(msgs) != 0 {
		t.Fatalf("store holds a chat nobody allowed: %+v", msgs)
	}
	select {
	case ev := <-tailed:
		t.Fatalf("tail carried a chat nobody allowed: %+v", ev)
	default:
	}
	if evs := r.sink.events(); len(evs) != 0 {
		t.Fatalf("backend received a chat nobody allowed: %+v", evs)
	}
}

// What is on the list works; the stranger next to it still does not.
func TestAllowedChatPassesAndOthersDoNot(t *testing.T) {
	r := newRig(t)
	r.follow(t, core.FollowRequest{Chat: ana, NoBackfill: true})

	r.inbound(t, ana, "hello")
	r.inbound(t, stranger, "psst")

	msgs := r.messages(t, "")
	if len(msgs) != 1 || msgs[0].Text != "hello" {
		t.Fatalf("want only Ana's message, got %+v", msgs)
	}
	until(t, "message at the backend", func() bool { return r.sink.count(core.Events.Message.Received) == 1 })
	for _, ev := range r.sink.events() {
		if strings.Contains(ev.Chat, "5550100002") {
			t.Fatalf("stranger reached the backend: %+v", ev)
		}
		if ev.Type == core.Events.Message.Received && ev.Backfill {
			t.Fatalf("a live message is not backfill: %+v", ev)
		}
	}
}

// The request log is about requests, not about what people said.
func TestLogDoesNotCarryMessageText(t *testing.T) {
	r := newRig(t)
	calls := make(chan core.Call, 8)
	go r.cli.Stream(r.ctx, "GET", "/log", nil, func(line []byte) error {
		var c core.Call
		if err := core.Decode(line, &c); err != nil {
			return err
		}
		calls <- c
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	if err := r.cli.JSON(r.ctx, "POST", "/send", core.SendRequest{To: stranger, Text: "secret words"}, nil); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case c := <-calls:
			if c.Path != "/send" {
				continue
			}
			raw, _ := json.Marshal(c)
			if strings.Contains(string(raw), "secret words") {
				t.Fatalf("the log carried the text: %s", raw)
			}
			return
		case <-time.After(5 * time.Second):
			t.Fatal("no /send on /log")
		}
	}
}

// ----------------------------------------------------------------------- follow

// Follow = allow + the past + sync from then on; unfollow undoes all of it.
func TestFollowBackfillSyncAndUnfollow(t *testing.T) {
	r := newRig(t)

	// Ana wrote once before anybody followed her: only an anchor was noted, in
	// memory. The phone holds 120 older messages, one per hour.
	r.history(t, ana, 120)
	r.inbound(t, ana, "the anchor")
	if msgs := r.messages(t, ""); len(msgs) != 0 {
		t.Fatalf("stored before the follow: %+v", msgs)
	}

	res := r.follow(t, core.FollowRequest{Chat: ana, Label: "Ana"})
	if !res.New || res.Label != "Ana" {
		t.Fatalf("got %+v", res)
	}
	until(t, "backfill done", func() bool { return r.backfillState(t) == core.BackfillDone })

	// the past: 120 messages, in three pages of 50, 50 and 20
	msgs := r.messages(t, ana)
	if len(msgs) != 120 {
		t.Fatalf("want the 120 past messages, got %d", len(msgs))
	}
	for _, m := range msgs {
		if m.Text == "the anchor" {
			t.Fatal("the anchor message was never stored, and must not appear")
		}
	}
	if l := r.follows(t); l[0].Backfilled != 120 || l[0].Label != "Ana" {
		t.Fatalf("got %+v", l)
	}
	until(t, "backfill at the backend", func() bool {
		return r.sink.count(core.Events.Message.Received)+r.sink.count(core.Events.Message.Sent) == 120
	})
	for _, ev := range r.sink.events() {
		if (ev.Type == core.Events.Message.Received || ev.Type == core.Events.Message.Sent) && !ev.Backfill {
			t.Fatalf("a past message must say so: %+v", ev)
		}
	}
	if r.sink.count(core.Events.Chat.Followed) != 1 || r.sink.count(core.Events.Chat.Backfilled) != 1 {
		t.Fatalf("want one chat_followed and one chat_backfilled, got %+v", r.sink.events()[:2])
	}

	// from then on, new messages sync by themselves
	r.inbound(t, ana, "new one")
	until(t, "live message at the backend", func() bool {
		for _, ev := range r.sink.events() {
			if ev.Text == "new one" && !ev.Backfill {
				return true
			}
		}
		return false
	})

	// unfollow: off the list, the local copy gone, and silence again
	var un core.UnfollowResult
	if err := r.cli.JSON(r.ctx, "POST", "/unfollow", core.UnfollowRequest{Chat: "Ana"}, &un); err != nil {
		t.Fatal(err)
	}
	if !un.Removed || un.Dropped != 121 {
		t.Fatalf("got %+v", un)
	}
	if len(r.follows(t)) != 0 || len(r.messages(t, "")) != 0 {
		t.Fatal("unfollow left something behind")
	}
	before := len(r.sink.events())
	r.inbound(t, ana, "after unfollow")
	time.Sleep(300 * time.Millisecond)
	if len(r.messages(t, "")) != 0 || len(r.sink.events()) != before {
		t.Fatal("a chat that was unfollowed is still listened to")
	}
	until(t, "chat_unfollowed at the backend", func() bool { return r.sink.count(core.Events.Chat.Unfollowed) == 1 })
}

func TestBackfillStopsAtMaxAndAtDays(t *testing.T) {
	t.Run("max", func(t *testing.T) {
		r := newRig(t)
		r.history(t, ana, 120)
		r.inbound(t, ana, "anchor")
		r.follow(t, core.FollowRequest{Chat: ana, Max: 60})
		until(t, "done", func() bool { return r.backfillState(t) == core.BackfillDone })
		if n := len(r.messages(t, ana)); n != 60 {
			t.Fatalf("want exactly 60, got %d", n)
		}
	})
	t.Run("days", func(t *testing.T) {
		r := newRig(t)
		r.history(t, ana, 120) // one per hour, five days
		r.inbound(t, ana, "anchor")
		r.follow(t, core.FollowRequest{Chat: ana, Days: 1})
		until(t, "done", func() bool { return r.backfillState(t) == core.BackfillDone })
		n := len(r.messages(t, ana))
		if n < 22 || n > 24 {
			t.Fatalf("a day of hourly messages is about 23, got %d", n)
		}
	})
	t.Run("no backfill", func(t *testing.T) {
		r := newRig(t)
		r.history(t, ana, 20)
		r.inbound(t, ana, "anchor")
		res := r.follow(t, core.FollowRequest{Chat: ana, NoBackfill: true})
		if res.Backfill != core.BackfillOff {
			t.Fatalf("got %+v", res)
		}
		time.Sleep(200 * time.Millisecond)
		if n := len(r.messages(t, ana)); n != 0 {
			t.Fatalf("asked for the past nobody wanted: %d", n)
		}
	})
}

// A chat followed before zap saw a single message of it has no anchor: it waits
// as pending, and the first live message starts the backfill.
func TestFollowWithoutAnchorWaitsForTheFirstMessage(t *testing.T) {
	r := newRig(t)
	r.history(t, ana, 30)

	res := r.follow(t, core.FollowRequest{Chat: ana})
	if res.Backfill != core.BackfillPending {
		t.Fatalf("want pending, got %+v", res)
	}
	r.inbound(t, ana, "first live message")
	until(t, "done", func() bool { return r.backfillState(t) == core.BackfillDone })
	if n := len(r.messages(t, ana)); n != 31 {
		t.Fatalf("want 30 past + the live one, got %d", n)
	}
}

func TestFollowAGroupByNameAndUnfollowByLabel(t *testing.T) {
	r := newRig(t)
	var g core.Group
	create := core.GroupRequest{From: me, Action: "create", Name: "Book club", Who: []string{ana}}
	if err := r.cli.JSON(r.ctx, "POST", "/groups", create, &g); err != nil {
		t.Fatal(err)
	}
	res := r.follow(t, core.FollowRequest{Chat: "Book club", NoBackfill: true})
	if res.Chat != g.JID || res.Label != "Book club" {
		t.Fatalf("got %+v", res)
	}
	var un core.UnfollowResult
	if err := r.cli.JSON(r.ctx, "POST", "/unfollow", core.UnfollowRequest{Chat: "book club"}, &un); err != nil || !un.Removed {
		t.Fatalf("got %+v %v", un, err)
	}
	if err := r.cli.JSON(r.ctx, "POST", "/unfollow", core.UnfollowRequest{Chat: "book club"}, &un); err == nil {
		t.Fatal("unfollowing what is not followed must say so")
	}
}

// zap chats names and ids, never a message, and marks who is followed.
func TestChatsListsWhoIsFollowed(t *testing.T) {
	r := newRig(t)
	if err := r.cli.JSON(r.ctx, "POST", "/fake", core.FakeRequest{Action: "contact", Number: ana, Name: "Ana"}, nil); err != nil {
		t.Fatal(err)
	}
	r.inbound(t, stranger, "hi") // seen, not stored
	r.follow(t, core.FollowRequest{Chat: ana, NoBackfill: true})

	var chats []core.ChatInfo
	if err := r.cli.JSON(r.ctx, "GET", "/chats", nil, &chats); err != nil {
		t.Fatal(err)
	}
	if len(chats) != 2 || !chats[0].Followed || chats[0].Name != "Ana" || chats[1].Followed {
		t.Fatalf("got %+v", chats)
	}
	raw, _ := json.Marshal(chats)
	if strings.Contains(string(raw), "hi\"") {
		t.Fatalf("chats carried a message: %s", raw)
	}
}

// purge deletes what an older zap stored before the allowlist existed.
func TestPurgeDeletesWhatIsNotFollowed(t *testing.T) {
	r := newRig(t)
	r.follow(t, core.FollowRequest{Chat: ana, NoBackfill: true})
	r.inbound(t, ana, "keep me")

	old := core.Event{Account: "15550100000@s.whatsapp.net", Type: core.Events.Message.Received,
		Chat: "15550100002@s.whatsapp.net", ID: "OLD1", Timestamp: time.Now(), Text: "left over"}
	if !r.back.Keep(old) {
		t.Fatal("seeding the old row failed")
	}
	var res core.PurgeResult
	if err := r.cli.JSON(r.ctx, "POST", "/purge", nil, &res); err != nil {
		t.Fatal(err)
	}
	if res.Chats != 1 || res.Messages != 1 {
		t.Fatalf("got %+v", res)
	}
	msgs := r.messages(t, "")
	if len(msgs) != 1 || msgs[0].Text != "keep me" {
		t.Fatalf("got %+v", msgs)
	}
}

// sync, which asks the phone for one chat, refuses a chat that is not followed.
func TestSyncRefusesAChatThatIsNotFollowed(t *testing.T) {
	r := newRig(t)
	err := r.cli.JSON(r.ctx, "POST", "/sync", core.SyncRequest{Chat: stranger}, nil)
	if err == nil || !strings.Contains(err.Error(), "not followed") {
		t.Fatalf("want a refusal, got %v", err)
	}
}

// ------------------------------------------------------------------- allowlist

func TestAllowlistFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "allow.toml")
	a, err := core.OpenAllowlist(path)
	if err != nil {
		t.Fatal(err)
	}
	if a.Allowed("15550100001@s.whatsapp.net") {
		t.Fatal("an empty list must allow nothing")
	}
	e, added, err := a.Add(core.AllowEntry{JID: "15550100001@s.whatsapp.net", Alt: []string{"9988776655@lid"}, Label: "Ana"})
	if err != nil || !added || e.Backfill != core.BackfillPending {
		t.Fatalf("got %+v %v %v", e, added, err)
	}

	// the same person, in every spelling WhatsApp uses
	for _, jid := range []string{"15550100001@s.whatsapp.net", "15550100001:7@s.whatsapp.net", "+1 555 010 0001", "9988776655@lid", "9988776655:3@lid"} {
		if !a.Allowed(jid) {
			t.Errorf("%q should be allowed", jid)
		}
	}
	for _, jid := range []string{"", "15550100002@s.whatsapp.net", "status@broadcast", "15550100001@g.us"} {
		if a.Allowed(jid) {
			t.Errorf("%q must not be allowed", jid)
		}
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("the allowlist names the people followed: want 0600, got %v", st.Mode().Perm())
	}

	// it survives a restart
	b, err := core.OpenAllowlist(path)
	if err != nil || !b.Allowed("9988776655@lid") {
		t.Fatalf("reload: %v", err)
	}

	// and a hand edit while the daemon runs
	raw, _ := os.ReadFile(path)
	edited := string(raw) + "\n[[chat]]\njid = \"15550100003@s.whatsapp.net\"\nsince = 2026-01-01T00:00:00Z\nbackfill = \"off\"\n"
	if err := os.WriteFile(path, []byte(edited), 0o600); err != nil {
		t.Fatal(err)
	}
	if !a.Allowed("15550100003@s.whatsapp.net") {
		t.Fatal("a hand edit was not picked up")
	}

	// a broken edit keeps the list that works: a typo must not open the door
	if err := os.WriteFile(path, []byte("[[chat]\nnot toml"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !a.Allowed("15550100001@s.whatsapp.net") || a.Allowed("15550100002@s.whatsapp.net") {
		t.Fatal("a broken file changed who is allowed")
	}
}

// ---------------------------------------------------------------------- config

func TestConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "config.toml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	t.Run("missing file is the default", func(t *testing.T) {
		cfg, err := core.LoadConfig(filepath.Join(dir, "absent.toml"))
		if err != nil || cfg.Backend.Type != "ndjson" {
			t.Fatalf("got %+v %v", cfg, err)
		}
	})
	t.Run("nats", func(t *testing.T) {
		cfg, err := core.LoadConfig(write(`
[backend]
type = "nats"
[backend.nats]
url = "nats://127.0.0.1:4222"
stream = "CHAT"
subjects = ["chat.>"]
prefix = "chat"
storage = "memory"
max_age = "24h"
token_env = "MY_TOKEN_VAR"
`))
		if err != nil {
			t.Fatal(err)
		}
		n := cfg.Backend.NATS
		if n.Stream != "CHAT" || n.Prefix != "chat" || n.Storage != "memory" || n.TokenEnv != "MY_TOKEN_VAR" || !n.CreateStream {
			t.Fatalf("got %+v", n) // create_stream keeps its default
		}
	})
	t.Run("a typo is an error", func(t *testing.T) {
		_, err := core.LoadConfig(write("[backend]\ntype = \"ndjson\"\nstdoutt = true\n"))
		if err == nil || !strings.Contains(err.Error(), "unknown key") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a backend that does not exist", func(t *testing.T) {
		if _, err := core.LoadConfig(write("[backend]\ntype = \"kafka\"\n")); err == nil {
			t.Fatal("want an error")
		}
	})
	t.Run("a prefix that is not a subject", func(t *testing.T) {
		for _, bad := range []string{"", "a..b", "a.>", "a b", ".a"} {
			if _, err := core.LoadConfig(write("[backend]\ntype = \"nats\"\n[backend.nats]\nprefix = \"" + bad + "\"\n")); err == nil {
				t.Errorf("prefix %q should be refused", bad)
			}
		}
	})
}

// ----------------------------------------------------------------------- sinks

func TestSubjectGrammar(t *testing.T) {
	want := map[core.EventType]string{
		core.Events.Message.Received:      "mesa.zap.message.received",
		core.Events.Media.Received:        "mesa.zap.message.received",
		core.Events.Message.Sent:          "mesa.zap.message.sent",
		core.Events.Message.Edited:        "mesa.zap.message.edited",
		core.Events.Message.Revoked:       "mesa.zap.message.revoked",
		core.Events.Message.Reacted:       "mesa.zap.message.reacted",
		core.Events.Audio.Transcribed:     "mesa.zap.message.transcribed",
		core.Events.Chat.Followed:         "mesa.zap.chat.followed",
		core.Events.Chat.Unfollowed:       "mesa.zap.chat.unfollowed",
		core.Events.Chat.Backfilled:       "mesa.zap.chat.backfilled",
		core.Events.Group.Renamed:         "",
		core.Events.Call.Offered:          "",
		core.Events.Chat.Typing:           "",
		core.Events.Message.Delivered:     "",
		core.Events.Account.Connected:     "",
		core.Events.Contact.Blocked:       "",
		core.Events.History.Synced:        "",
		core.Events.Message.Undecryptable: "",
	}
	for typ, subject := range want {
		if got := core.Subject("mesa.zap", typ); got != subject {
			t.Errorf("%s: want %q, got %q", typ, subject, got)
		}
	}
}

func TestNDJSONSink(t *testing.T) {
	var buf bytes.Buffer
	cfg := core.DefaultConfig()
	cfg.Backend.NDJSON.Stdout = true
	sink, err := core.NewSink(context.Background(), cfg, &buf)
	if err != nil {
		t.Fatal(err)
	}
	ev := core.Event{Account: "a", Type: core.Events.Message.Received, Chat: "c", ID: "1", Text: "hi", Timestamp: time.Now()}
	if err := sink.Publish(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	var back core.Event
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &back); err != nil || back.Text != "hi" || back.Type != ev.Type {
		t.Fatalf("got %q %v", buf.String(), err)
	}

	buf.Reset()
	cfg.Backend.NDJSON.Stdout = false
	quiet, _ := core.NewSink(context.Background(), cfg, &buf)
	quiet.Publish(context.Background(), ev)
	if buf.Len() != 0 {
		t.Fatalf("stdout is off, got %q", buf.String())
	}
}

type failSink struct{}

func (failSink) Publish(context.Context, core.Event) error { return os.ErrClosed }
func (failSink) Close() error                              { return nil }

// A backend that is down is counted, not logged with the message, and never
// blocks the caller.
func TestOutboxCountsFailuresWithoutLeakingText(t *testing.T) {
	out := core.NewOutbox(failSink{}, 8)
	for i := 0; i < 3; i++ {
		out.Put(core.Event{Type: core.Events.Message.Received, Chat: "c", Text: "never in a log"})
	}
	out.Close()
	if _, failed := out.Stats(); failed != 3 {
		t.Fatalf("want 3 failures counted, got %d", failed)
	}
}
