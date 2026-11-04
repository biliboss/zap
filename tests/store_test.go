package test

import (
	"context"
	"path/filepath"
	"testing"
	"time"
	"zap/core"
)

// The real WhatsApp backend, on a real sqlite file, with no number paired and
// so nothing to connect to: enough to prove that the message store itself
// refuses a chat nobody allowed, and that unfollow and purge delete from it.
func TestStoreBehindTheDoor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dir := t.TempDir()
	allow, err := core.OpenAllowlist(filepath.Join(dir, "allow.toml"))
	if err != nil {
		t.Fatal(err)
	}
	wa, err := core.OpenWhatsApp(ctx, filepath.Join(dir, "store.db"), &core.Policy{Allow: allow})
	if err != nil {
		t.Fatal(err)
	}
	defer wa.Close()

	if _, err := core.OpenWhatsApp(ctx, filepath.Join(dir, "other.db"), nil); err == nil {
		t.Fatal("a daemon without a policy must not open: it would listen to everybody")
	}

	const account = "15550100000@s.whatsapp.net"
	anaJID, strangerJID := "15550100001@s.whatsapp.net", "15550100002@s.whatsapp.net"
	msg := func(chat, id string, at time.Time) core.Event {
		return core.Event{Account: account, Type: core.Events.Message.Received, Chat: chat, ID: id, Sender: chat, Timestamp: at, Text: "hi"}
	}
	now := time.Now().Truncate(time.Second)

	// nothing is allowed: the store takes nothing
	if wa.Keep(msg(anaJID, "A1", now)) {
		t.Fatal("the store kept a chat that is not on the allowlist")
	}
	if got := wa.Stored(); len(got) != 0 {
		t.Fatalf("got %v", got)
	}

	// allow Ana (and her @lid): both spellings are kept, the stranger is not
	if _, _, err := allow.Add(core.AllowEntry{JID: anaJID, Alt: []string{"998877@lid"}}); err != nil {
		t.Fatal(err)
	}
	if !wa.Keep(msg(anaJID, "A1", now.Add(-2*time.Hour))) || !wa.Keep(msg("998877@lid", "A2", now.Add(-time.Hour))) {
		t.Fatal("an allowed chat must be stored, under either spelling")
	}
	if wa.Keep(msg(anaJID, "A1", now)) {
		t.Fatal("the same message twice is not new")
	}
	if wa.Keep(msg(strangerJID, "S1", now)) {
		t.Fatal("a stranger was stored")
	}

	// the oldest stored message is the anchor the phone is asked from
	if at, ok := wa.Oldest(account, anaJID); !ok || at.ID != "A1" || at.At.Unix() != now.Add(-2*time.Hour).Unix() {
		t.Fatalf("got %+v %v", at, ok)
	}

	// what an older zap left behind: a row for a chat that is no longer allowed
	if _, _, err := allow.Add(core.AllowEntry{JID: strangerJID}); err != nil {
		t.Fatal(err)
	}
	wa.Keep(msg(strangerJID, "S1", now))
	if _, _, err := allow.Remove(strangerJID); err != nil {
		t.Fatal(err)
	}
	if n := wa.Stray(); n != 1 {
		t.Fatalf("want 1 stray chat, got %d", n)
	}
	res, err := wa.Purge(ctx)
	if err != nil || res.Chats != 1 || res.Messages != 1 {
		t.Fatalf("got %+v %v", res, err)
	}
	if n := wa.Stray(); n != 0 {
		t.Fatalf("purge left %d", n)
	}

	// unfollow by the label drops both spellings of the chat
	if _, _, err := allow.Add(core.AllowEntry{JID: anaJID, Label: "Ana"}); err != nil {
		t.Fatal(err)
	}
	un, err := wa.Unfollow(ctx, core.UnfollowRequest{Chat: "ana"})
	if err != nil || un.Dropped != 2 {
		t.Fatalf("want 2 dropped (phone jid and lid), got %+v %v", un, err)
	}
	if got := wa.Stored(); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}
