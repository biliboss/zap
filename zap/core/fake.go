package core

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Fake is a second Backend, in memory only, for a consumer that needs a real
// unix socket with real wire shapes and no phone. It never opens sqlite —
// there is nothing on disk, so there is nothing to clobber.
type Fake struct {
	mu       sync.RWMutex
	accounts []Account          // fake numbers, in creation order
	contacts map[string]Contact // jid -> contact
	groups   map[string]*Group  // jid -> group
	msgs     []Event            // oldest first; Messages reverses
	audio    []fakeAudio
	seq      int // message ids and group jids
	bus      Bus[Event]
	whisper  *Transcriber // nil unless --transcribe; only run() is used

	// the same door the real daemon has: deny by default, so a fake daemon
	// teaches the same lesson the real one enforces
	policy *Policy
	fol    *Follower
	phone  map[string][]Event // chat -> the past the fake "phone" still holds, oldest first

	SessionGap time.Duration
}

type fakeAudio struct{ account, chat, id, path, state, err string }

// NewFake seeds one or more fake numbers, "+5511999999999" if none are given.
func NewFake(numbers ...string) *Fake {
	if len(numbers) == 0 {
		numbers = []string{"+5511999999999"}
	}
	f := &Fake{contacts: map[string]Contact{}, groups: map[string]*Group{}, phone: map[string][]Event{}}
	f.SetPolicy(&Policy{Allow: NewMemoryAllowlist()})
	for _, n := range numbers {
		f.addAccount(n)
	}
	return f
}

func (f *Fake) addAccount(number string) Account {
	jid := digits(number) + "@s.whatsapp.net"
	acc := Account{Number: e164(jid), JID: jid, PushName: "fake", Connected: true}
	f.accounts = append(f.accounts, acc)
	return acc
}

func (f *Fake) Close() {}

// StartTranscriber runs a poller only when transcription is on, exactly like
// WhatsApp.StartTranscriber: without --transcribe, queued audio stays pending.
func (f *Fake) StartTranscriber(ctx context.Context, t *Transcriber) error {
	f.mu.Lock()
	f.whisper = t
	f.mu.Unlock()
	if t == nil || !t.Enabled {
		return nil
	}
	poll := t.Poll
	if poll <= 0 {
		poll = 2 * time.Second
	}
	go f.pollAudio(ctx, t, poll)
	return nil
}

func (f *Fake) pollAudio(ctx context.Context, t *Transcriber, poll time.Duration) {
	tick := time.NewTicker(poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for f.transcribeNext(ctx, t) {
			}
		}
	}
}

func (f *Fake) transcribeNext(ctx context.Context, t *Transcriber) bool {
	f.mu.Lock()
	idx := -1
	for i, a := range f.audio {
		if a.state == "pending" {
			idx = i
			break
		}
	}
	if idx == -1 {
		f.mu.Unlock()
		return false
	}
	f.audio[idx].state = "running"
	job := f.audio[idx]
	f.mu.Unlock()

	text, err := t.run(ctx, job.path)

	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.audio {
		if f.audio[i] == job {
			if err != nil {
				f.audio[i].state, f.audio[i].err = "failed", err.Error()
				return true
			}
			f.audio[i].state = "done"
		}
	}
	if err != nil {
		return true
	}
	f.setText(job.account, job.chat, job.id, text)
	f.publish(Event{
		Account: job.account, Type: Events.Audio.Transcribed,
		Chat: job.chat, ID: job.id, Text: text, Timestamp: time.Now(),
	})
	return true
}

func (f *Fake) setText(account, chat, id, text string) {
	for i := range f.msgs {
		if f.msgs[i].Account == account && f.msgs[i].Chat == chat && f.msgs[i].ID == id {
			f.msgs[i].Text = text
		}
	}
}

// account repeats pick's rule and its two error strings verbatim: a consumer
// asserts on them. Callers hold f.mu (read or write) before calling it.
func (f *Fake) account(from string) (string, error) {
	if len(f.accounts) == 0 {
		return "", fmt.Errorf("no accounts paired — run `zap pair`")
	}
	if from == "" {
		if len(f.accounts) > 1 {
			return "", fmt.Errorf("%d accounts paired, pass --from", len(f.accounts))
		}
		return f.accounts[0].JID, nil
	}
	want := digits(from)
	for _, a := range f.accounts {
		if a.JID == from || digits(a.JID) == want {
			return a.JID, nil
		}
	}
	return "", fmt.Errorf("no account matching %q", from)
}

func (f *Fake) nextID() string {
	f.seq++
	return fmt.Sprintf("FAKE%04d", f.seq)
}

// record stores and publishes ev, and opens a session when this chat was
// quiet longer than SessionGap. Callers hold f.mu (write).
func (f *Fake) record(ev Event) bool {
	// same door as the real daemon: a trace for a later follow, then the gate
	f.fol.Observe(ev.Chat, Anchor{ID: ev.ID, FromMe: ev.FromMe, At: ev.Timestamp})
	if !f.policy.Admit(ev) {
		return false
	}
	gap := f.SessionGap
	if gap <= 0 {
		gap = DefaultSessionGap
	}
	var prev time.Time
	for i := len(f.msgs) - 1; i >= 0; i-- {
		if f.msgs[i].Chat == ev.Chat && f.msgs[i].Account == ev.Account {
			prev = f.msgs[i].Timestamp
			break
		}
	}
	f.msgs = append(f.msgs, ev)
	f.bus.Publish(ev)
	if prev.IsZero() || ev.Timestamp.Sub(prev) > gap {
		f.bus.Publish(Event{
			Account: ev.Account, Type: Events.Session.Opened,
			Chat: ev.Chat, ChatName: ev.ChatName, Timestamp: ev.Timestamp,
		})
	}
	f.policy.Emit(ev)
	f.fol.Resume(ev)
	return true
}

// publish is the fake's only way out for events that do not go through record.
func (f *Fake) publish(ev Event) {
	if !f.policy.Admit(ev) {
		return
	}
	f.bus.Publish(ev)
	f.policy.Emit(ev)
}

// decorate fills ChatName/SenderName/SenderNumber from the contact and group
// maps, the same job WhatsApp_Directory does against the phone.
func (f *Fake) decorate(ev *Event) {
	if strings.HasSuffix(ev.Chat, "@g.us") {
		if g, ok := f.groups[ev.Chat]; ok {
			ev.ChatName = g.Name
		}
	} else if c, ok := f.contacts[ev.Chat]; ok {
		ev.ChatName = firstNonEmpty(c.Name, c.PushName)
	}
	if ev.FromMe {
		ev.SenderNumber, ev.SenderName = e164(ev.Account), "me"
		return
	}
	if c, ok := f.contacts[ev.Sender]; ok {
		ev.SenderNumber, ev.SenderName = c.Number, firstNonEmpty(c.Name, c.PushName)
	} else if ev.Sender != "" {
		ev.SenderNumber = e164(ev.Sender)
	}
}

// ------------------------------------------------------------------ Backend

func (f *Fake) Accounts() []Account {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Account, len(f.accounts))
	copy(out, f.accounts)
	return out
}

func (f *Fake) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	from, err := f.account(req.From)
	if err != nil {
		return SendResult{}, err
	}
	target := req.To
	if req.Myself {
		target = from
	}
	to, err := parseJID(target)
	if err != nil {
		return SendResult{}, err
	}
	id := req.ID
	if id == "" {
		id = f.nextID()
	}
	now := time.Now()
	ev := Event{
		Account: from, Type: Events.Message.Sent, Chat: to.String(),
		ID: id, Sender: from, FromMe: true, Timestamp: now, Text: req.Text,
	}
	f.decorate(&ev)
	f.record(ev)
	return SendResult{ID: id, From: from, Timestamp: now}, nil
}

// Pair mints a fresh fake number, the sandbox equivalent of scanning a QR.
func (f *Fake) Pair(ctx context.Context, emit func(PairEvent) error) error {
	if err := emit(PairEvent{QR: "2@fake"}); err != nil {
		return err
	}
	f.mu.Lock()
	acc := f.addAccount(fmt.Sprintf("+55119%08d", len(f.accounts)+1))
	f.mu.Unlock()
	return emit(PairEvent{JID: acc.JID})
}

func (f *Fake) Logout(ctx context.Context, from string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	jid, err := f.account(from)
	if err != nil {
		return err
	}
	for i, a := range f.accounts {
		if a.JID == jid {
			f.accounts = append(f.accounts[:i], f.accounts[i+1:]...)
			break
		}
	}
	f.bus.Publish(Event{Account: jid, Type: Events.Account.LoggedOut, Timestamp: time.Now()})
	return nil
}

// Typing and Presence are no-ops: a consumer asserts them through /log, not
// through any visible state change here.
func (f *Fake) Typing(ctx context.Context, req TypingRequest) error      { return nil }
func (f *Fake) Presence(ctx context.Context, from string, on bool) error { return nil }

func (f *Fake) Status(ctx context.Context, req SendRequest) (SendResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	from, err := f.account(req.From)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{ID: f.nextID(), From: from, Timestamp: time.Now()}, nil
}

func (f *Fake) Subscribe(from string) (<-chan Event, func()) {
	want := digits(from)
	if want == "" {
		return f.bus.Subscribe(nil)
	}
	return f.bus.Subscribe(func(ev Event) bool { return digits(ev.Account) == want })
}

func (f *Fake) Messages(ctx context.Context, q MessagesQuery) ([]Event, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	account, err := f.account(q.From)
	if err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		q.Limit = 50
	}
	var filtered []Event
	for _, m := range f.msgs {
		if m.Account != account {
			continue
		}
		if q.Chat != "" && !strings.Contains(digits(m.Chat), digits(q.Chat)) {
			continue
		}
		filtered = append(filtered, m)
	}

	var out []Event
	if !q.Since.IsZero() {
		// a cursor reads FORWARD, oldest first — same flip as WhatsApp.Messages
		for _, m := range filtered {
			if m.Timestamp.Before(q.Since) {
				continue
			}
			out = append(out, m)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	} else {
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].Timestamp.After(filtered[j].Timestamp) })
		out = filtered
	}
	if len(out) > q.Limit {
		out = out[:q.Limit]
	}

	for i := range out {
		// mirror WhatsApp.Messages's quirk: a stored row always comes back
		// tagged Events.Message.Received, even when from_me is set — the
		// store is not re-classifying direction, from_me already does that.
		out[i].Type = Events.Message.Received
		f.decorate(&out[i])
	}
	return out, nil
}

func (f *Fake) Sessions(ctx context.Context, req SessionQuery) ([]Session, error) {
	if req.Gap <= 0 {
		req.Gap = DefaultSessionGap
	}
	msgs, err := f.Messages(ctx, MessagesQuery{From: req.From, Chat: req.Chat, Limit: 1 << 20})
	if err != nil {
		return nil, err
	}
	sessions := SplitSessions(msgs, req.Gap)
	if req.Mine {
		kept := sessions[:0]
		for _, s := range sessions {
			if s.Outbound > 0 {
				kept = append(kept, s)
			}
		}
		sessions = kept
	}
	if req.Transcript {
		for i := range sessions {
			sessions[i].Transcript = between(msgs, sessions[i])
		}
	}
	f.mu.RLock()
	for i := range sessions {
		if strings.HasSuffix(sessions[i].Chat, "@g.us") {
			if g, ok := f.groups[sessions[i].Chat]; ok {
				sessions[i].Name = g.Name
			}
		} else if c, ok := f.contacts[sessions[i].Chat]; ok {
			sessions[i].Name = firstNonEmpty(c.Name, c.PushName)
		}
	}
	f.mu.RUnlock()
	if req.Limit > 0 && len(sessions) > req.Limit {
		sessions = sessions[:req.Limit]
	}
	return sessions, nil
}

// SyncChat is a receipt only, same as WhatsApp.SyncChat: nothing is delivered
// because the fake daemon has no history bundle to answer with.
func (f *Fake) SyncChat(ctx context.Context, req SyncRequest) (SyncResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	account, err := f.account(req.From)
	if err != nil {
		return SyncResult{}, err
	}
	if !f.policy.Allow.Allowed(req.Chat) {
		return SyncResult{}, fmt.Errorf("%s is not followed: run `zap follow` first, zap asks the phone for no chat it may not listen to", digits(req.Chat))
	}
	var oldest *Event
	for i := range f.msgs {
		m := &f.msgs[i]
		if m.Account != account || digits(m.Chat) != digits(req.Chat) {
			continue
		}
		if oldest == nil || m.Timestamp.Before(oldest.Timestamp) {
			oldest = m
		}
	}
	if oldest == nil {
		return SyncResult{}, fmt.Errorf("no message stored for %s yet — the phone needs one to anchor the request", req.Chat)
	}
	count := req.Count
	if count <= 0 {
		count = 50
	}
	return SyncResult{Chat: oldest.Chat, Requested: count, Anchor: oldest.ID, AnchorAt: oldest.Timestamp}, nil
}

func (f *Fake) Contacts(ctx context.Context, from, query string) ([]Contact, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if _, err := f.account(from); err != nil {
		return nil, err
	}
	want := strings.ToLower(query)
	out := make([]Contact, 0, len(f.contacts))
	for _, c := range f.contacts {
		if want != "" && !strings.Contains(strings.ToLower(c.Name+c.PushName+c.Number), want) {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		return firstNonEmpty(out[i].Name, out[i].PushName, out[i].Number) < firstNonEmpty(out[j].Name, out[j].PushName, out[j].Number)
	})
	return out, nil
}

func (f *Fake) QueueStats(ctx context.Context) ([]QueueStat, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	counts := map[string]int{}
	for _, a := range f.audio {
		counts[a.state]++
	}
	states := make([]string, 0, len(counts))
	for s := range counts {
		states = append(states, s)
	}
	sort.Strings(states)
	out := make([]QueueStat, 0, len(states))
	for _, s := range states {
		out = append(out, QueueStat{State: s, Count: counts[s]})
	}
	return out, nil
}

func (f *Fake) Retry(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for i := range f.audio {
		if f.audio[i].state == "failed" || f.audio[i].state == "running" {
			f.audio[i].state, f.audio[i].err = "pending", ""
			n++
		}
	}
	return n, nil
}

func (f *Fake) Groups(ctx context.Context, from string) ([]Group, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if _, err := f.account(from); err != nil {
		return nil, err
	}
	out := make([]Group, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *Fake) GroupAct(ctx context.Context, req GroupRequest) (Group, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	from, err := f.account(req.From)
	if err != nil {
		return Group{}, err
	}
	if req.Action == "create" {
		if req.Name == "" {
			return Group{}, fmt.Errorf("create needs a name")
		}
		jid := f.nextID() + "@g.us"
		g := &Group{JID: jid, Name: req.Name, Owner: e164(from), Created: time.Now()}
		for _, who := range req.Who {
			g.Participants = append(g.Participants, f.member(who))
		}
		f.groups[jid] = g
		f.bus.Publish(Event{Account: from, Type: Events.Group.Joined, Chat: jid, Text: g.Name, Timestamp: time.Now()})
		return *g, nil
	}

	g, jid, err := f.resolveGroup(req.Group)
	if err != nil {
		return Group{}, err
	}
	switch req.Action {
	case "add", "remove", "promote", "demote":
		if len(req.Who) == 0 {
			return Group{}, fmt.Errorf("%s needs --who", req.Action)
		}
		f.applyMembership(g, req.Action, req.Who)
		var evType EventType
		switch req.Action {
		case "add":
			evType = Events.Group.MembersAdded
		case "remove":
			evType = Events.Group.MembersRemoved
		case "promote":
			evType = Events.Group.AdminsPromoted
		case "demote":
			evType = Events.Group.AdminsDemoted
		}
		f.bus.Publish(Event{Account: from, Type: evType, Chat: jid, Members: req.Who, Timestamp: time.Now()})
	case "name":
		g.Name = req.Name
		f.bus.Publish(Event{Account: from, Type: Events.Group.Renamed, Chat: jid, Text: req.Name, Timestamp: time.Now()})
	case "topic":
		g.Topic = req.Text
		f.bus.Publish(Event{Account: from, Type: Events.Group.TopicChanged, Chat: jid, Text: req.Text, Timestamp: time.Now()})
	case "leave":
		delete(f.groups, jid)
		return Group{JID: jid}, nil
	case "link":
		g.Link = "https://chat.whatsapp.com/FAKE" + fmt.Sprintf("%08d", f.seq)
		return *g, nil
	case "info":
		// nothing to change, fall through to returning the current state
	default:
		return Group{}, fmt.Errorf("unknown group action %q", req.Action)
	}
	return *g, nil
}

// applyMembership mutates g's participant list for add/remove/promote/demote.
func (f *Fake) applyMembership(g *Group, action string, who []string) {
	switch action {
	case "add":
		for _, w := range who {
			g.Participants = append(g.Participants, f.member(w))
		}
	case "remove":
		drop := digitsSet(who)
		kept := g.Participants[:0]
		for _, p := range g.Participants {
			if !drop[digits(p.Number)] {
				kept = append(kept, p)
			}
		}
		g.Participants = kept
	case "promote", "demote":
		want := digitsSet(who)
		for i, p := range g.Participants {
			if want[digits(p.Number)] {
				g.Participants[i].Admin = action == "promote"
			}
		}
	}
}

// resolveGroup accepts a jid or the group's name, same as WhatsApp.resolveGroup.
func (f *Fake) resolveGroup(ref string) (*Group, string, error) {
	if ref == "" {
		return nil, "", fmt.Errorf("no group given")
	}
	if g, ok := f.groups[ref]; ok {
		return g, ref, nil
	}
	want := strings.ToLower(ref)
	for jid, g := range f.groups {
		if strings.ToLower(g.Name) == want {
			return g, jid, nil
		}
	}
	return nil, "", fmt.Errorf("no group matching %q", ref)
}

func (f *Fake) member(number string) GroupMember {
	jid := digits(number) + "@s.whatsapp.net"
	name := ""
	if c, ok := f.contacts[jid]; ok {
		name = firstNonEmpty(c.Name, c.PushName)
	}
	return GroupMember{Number: e164(jid), JID: jid, Name: name}
}

func digitsSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, v := range values {
		out[digits(v)] = true
	}
	return out
}

// ---------------------------------------------------------------------- Act

// FakeRequest is every fake mutation in one shape, for the same reason
// GroupRequest is: the CLI is a passthrough.
type FakeRequest struct {
	Action  string   `json:"action"`           // contact group inbound audio history
	From    string   `json:"from,omitempty"`   // the fake ACCOUNT that receives
	Number  string   `json:"number,omitempty"` // contact number, or inbound sender
	Name    string   `json:"name,omitempty"`
	Chat    string   `json:"chat,omitempty"` // inbound into a group jid
	Text    string   `json:"text,omitempty"`
	Who     []string `json:"who,omitempty"`
	Path    string   `json:"path,omitempty"` // audio file on disk
	Seconds int      `json:"seconds,omitempty"`
	Count   int      `json:"count,omitempty"` // history: how many past messages
}

// Act returns the record the action produced — Contact, Group, Event, Event —
// so no new response model exists.
func (f *Fake) Act(ctx context.Context, req FakeRequest) (any, error) {
	switch req.Action {
	case "contact":
		return f.actContact(req)
	case "group":
		return f.GroupAct(ctx, GroupRequest{From: req.From, Action: "create", Name: req.Name, Who: req.Who})
	case "inbound":
		return f.actInbound(req)
	case "audio":
		return f.actAudio(req)
	case "history":
		return f.actHistory(req)
	default:
		return nil, fmt.Errorf("unknown fake action %q", req.Action)
	}
}

func (f *Fake) actContact(req FakeRequest) (Contact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	jid := digits(req.Number) + "@s.whatsapp.net"
	c := Contact{Number: e164(jid), JID: jid, Name: req.Name}
	f.contacts[jid] = c
	return c, nil
}

func (f *Fake) actInbound(req FakeRequest) (Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	account, err := f.account(req.From)
	if err != nil {
		return Event{}, err
	}
	sender := digits(req.Number) + "@s.whatsapp.net"
	chat := req.Chat
	if chat == "" {
		chat = sender
	}
	ev := Event{
		Account: account, Type: Events.Message.Received, Chat: chat,
		Sender: sender, ID: f.nextID(), Timestamp: time.Now(), Text: req.Text,
	}
	f.decorate(&ev)
	f.record(ev)
	return ev, nil
}

// actAudio downloads nothing — the file is already local — but otherwise
// walks the same states as the real queue: pending, then transcribed. --text
// short-circuits whisper so a consumer can assert the whole chain without
// mlx-whisper installed.
func (f *Fake) actAudio(req FakeRequest) (Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	account, err := f.account(req.From)
	if err != nil {
		return Event{}, err
	}
	if _, err := os.Stat(req.Path); err != nil {
		return Event{}, fmt.Errorf("audio file: %w", err)
	}
	sender := digits(req.Number) + "@s.whatsapp.net"
	chat := req.Chat
	if chat == "" {
		chat = sender
	}
	id := f.nextID()
	ev := Event{
		Account: account, Type: Events.Message.Received, Chat: chat,
		Sender: sender, ID: id, Timestamp: time.Now(), Media: "audio",
	}
	f.decorate(&ev)
	if !f.record(ev) {
		return ev, nil // a chat nobody allowed: no queue, no file kept
	}

	f.audio = append(f.audio, fakeAudio{account: account, chat: chat, id: id, path: req.Path, state: "pending"})
	f.publish(Event{Account: account, Type: Events.Audio.Queued, Chat: chat, ID: id, Timestamp: time.Now()})

	if req.Text != "" {
		f.audio[len(f.audio)-1].state = "done"
		f.setText(account, chat, id, req.Text)
		f.publish(Event{Account: account, Type: Events.Audio.Transcribed, Chat: chat, ID: id, Text: req.Text, Timestamp: time.Now()})
	}
	return ev, nil
}
