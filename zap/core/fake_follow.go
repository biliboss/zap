package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SetPolicy swaps the door the fake listens through, so `zap serve --fake` can
// use the same allowlist file and backend a real daemon would. The follower is
// rebuilt around it.
func (f *Fake) SetPolicy(p *Policy) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policy = p
	f.fol = &Follower{Policy: p, Phone: f, Archive: f, Bus: f.bus.Publish}
}

// ------------------------------------------------------------ Backend verbs

func (f *Fake) Follow(ctx context.Context, req FollowRequest) (FollowResult, error) {
	return f.fol.Follow(ctx, req)
}

func (f *Fake) Unfollow(ctx context.Context, req UnfollowRequest) (UnfollowResult, error) {
	return f.fol.Unfollow(ctx, req)
}

func (f *Fake) Follows(ctx context.Context) ([]Follow, error) { return f.fol.Follows(), nil }

func (f *Fake) Purge(ctx context.Context) (PurgeResult, error) { return f.fol.Purge() }

func (f *Fake) Chats(ctx context.Context, from string) ([]ChatInfo, error) {
	f.mu.RLock()
	if _, err := f.account(from); err != nil {
		f.mu.RUnlock()
		return nil, err
	}
	byJID := map[string]*ChatInfo{}
	for jid, c := range f.contacts {
		byJID[NormJID(jid)] = &ChatInfo{JID: jid, Number: c.Number, Name: firstNonEmpty(c.Name, c.PushName), Kind: "person"}
	}
	for jid, g := range f.groups {
		byJID[NormJID(jid)] = &ChatInfo{JID: jid, Name: g.Name, Kind: "group"}
	}
	f.mu.RUnlock()
	for jid, at := range f.fol.Seen() {
		c, ok := byJID[jid]
		if !ok {
			kind := "person"
			if strings.HasSuffix(jid, "@g.us") {
				kind = "group"
			}
			c = &ChatInfo{JID: jid, Number: e164(jid), Kind: kind}
			byJID[jid] = c
		}
		if at.After(c.Last) {
			c.Last = at
		}
	}
	return finishChats(byJID, f.policy.Allow), nil
}

// ------------------------------------------------------------------- Phone

// Resolve mirrors WhatsApp.Resolve: a number, a jid or a group name.
func (f *Fake) Resolve(ctx context.Context, from, ref string) (Resolved, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	account, err := f.account(from)
	if err != nil {
		return Resolved{}, err
	}
	if strings.HasSuffix(ref, "@g.us") || (!strings.Contains(ref, "@") && digits(ref) == "") {
		g, jid, err := f.resolveGroup(ref)
		if err != nil {
			return Resolved{}, err
		}
		return Resolved{Account: account, JID: jid, Label: g.Name}, nil
	}
	jid := NormJID(ref)
	out := Resolved{Account: account, JID: jid}
	if c, ok := f.contacts[jid]; ok {
		out.Label = firstNonEmpty(c.Name, c.PushName)
	}
	return out, nil
}

// Ask plays the phone: it answers asynchronously, like the real one, with up to
// count of the messages it still holds that are older than the anchor.
func (f *Fake) Ask(ctx context.Context, account, chat string, at Anchor, count int) error {
	f.mu.RLock()
	var older []Event
	for _, m := range f.phone[NormJID(chat)] {
		if m.Timestamp.Before(at.At) {
			older = append(older, m)
		}
	}
	f.mu.RUnlock()
	sort.Slice(older, func(i, j int) bool { return older[i].Timestamp.Before(older[j].Timestamp) })
	more := false
	if len(older) > count {
		older, more = older[len(older)-count:], true
	}
	page := make([]Event, len(older))
	for i, m := range older {
		m.Account = account
		page[i] = m
	}
	go f.fol.OnDemand(account, chat, page, more)
	return nil
}

// ----------------------------------------------------------------- Archive

func (f *Fake) Keep(ev Event) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.msgs {
		if m.Account == ev.Account && m.Chat == ev.Chat && m.ID == ev.ID {
			return false
		}
	}
	f.decorate(&ev)
	f.msgs = append(f.msgs, ev)
	return true
}

func (f *Fake) Oldest(account, chat string) (Anchor, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var best *Event
	for i := range f.msgs {
		m := &f.msgs[i]
		if m.Account != account || NormJID(m.Chat) != NormJID(chat) {
			continue
		}
		if best == nil || m.Timestamp.Before(best.Timestamp) {
			best = m
		}
	}
	if best == nil {
		return Anchor{}, false
	}
	return Anchor{ID: best.ID, FromMe: best.FromMe, At: best.Timestamp}, true
}

func (f *Fake) Drop(chats []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	want := map[string]bool{}
	for _, c := range chats {
		want[NormJID(c)] = true
	}
	kept, dropped := f.msgs[:0], 0
	for _, m := range f.msgs {
		if want[NormJID(m.Chat)] {
			dropped++
			continue
		}
		kept = append(kept, m)
	}
	f.msgs = kept
	audio := f.audio[:0]
	for _, a := range f.audio {
		if !want[NormJID(a.chat)] {
			audio = append(audio, a)
		}
	}
	f.audio = audio
	return dropped, nil
}

func (f *Fake) Stored() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for _, m := range f.msgs {
		if !seen[m.Chat] {
			seen[m.Chat] = true
			out = append(out, m.Chat)
		}
	}
	return out
}

// ------------------------------------------------------------------ history

// actHistory gives the fake phone a past for a chat: Count messages, one per
// hour going back from now, every third one sent by us. It does NOT put them in
// the store; only a follow asks the phone for them.
func (f *Fake) actHistory(req FakeRequest) (Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	account, err := f.account(req.From)
	if err != nil {
		return Event{}, err
	}
	chat := req.Chat
	if chat == "" {
		chat = digits(req.Number) + "@s.whatsapp.net"
	}
	chat = NormJID(chat)
	n := req.Count
	if n <= 0 {
		n = 10
	}
	prefix := req.Text
	if prefix == "" {
		prefix = "past"
	}
	now := time.Now()
	for i := n; i >= 1; i-- {
		fromMe := i%3 == 0
		m := Event{
			Account: account, Type: Events.Message.Received, Chat: chat,
			Sender: chat, ID: fmt.Sprintf("H%s%04d", digits(chat), i),
			Timestamp: now.Add(-time.Duration(i) * time.Hour).Truncate(time.Second),
			Text:      fmt.Sprintf("%s %d", prefix, n-i+1),
		}
		if fromMe {
			m.Type, m.FromMe, m.Sender = Events.Message.Sent, true, account
		}
		f.phone[chat] = append(f.phone[chat], m)
	}
	return Event{Account: account, Type: Events.History.Synced, Chat: "", Count: n, Timestamp: now}, nil
}
