package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Policy is the one door in front of everything zap keeps or sends: a chat
// that is not on the allowlist is stored nowhere, shown on no tail, handed to
// no hook and published to no backend. There is no second path around it —
// the store, the bus and the backend all sit behind Admit.
type Policy struct {
	Allow *Allowlist
	Out   *Outbox // nil: nothing is forwarded to a backend
}

// Admit says whether an event may leave the process or be written down.
// Account-level events (connected, logged out, a history bundle arriving) are
// not about anyone else and pass; everything that names a chat or a contact
// needs that chat or contact on the list.
func (p *Policy) Admit(ev Event) bool {
	switch {
	case ev.Chat != "":
		return p.Allow.Allowed(ev.Chat)
	case isContactEvent(ev.Type):
		return len(ev.Members) == 0 && p.Allow.Allowed(ev.Sender)
	case len(ev.Members) > 0:
		return false
	}
	return true
}

func isContactEvent(t EventType) bool {
	switch t {
	case Events.Contact.NameChanged, Events.Contact.PictureChanged, Events.Contact.Blocked, Events.Contact.Unblocked:
		return true
	}
	return false
}

// Emit hands an already admitted event to the backend.
func (p *Policy) Emit(ev Event) {
	if p.Out == nil {
		return
	}
	if _, _, ok := SubjectEvent(ev.Type); ok {
		p.Out.Put(ev)
	}
}

// ------------------------------------------------------------------ follow

// Anchor names one message of a chat: the phone answers a history request with
// "the messages right before this one". It holds ids and a time, never text.
type Anchor struct {
	ID     string
	FromMe bool
	At     time.Time
}

// Resolved is a chat as the phone knows it.
type Resolved struct {
	Account string
	JID     string
	Alt     []string
	Label   string
}

// Phone is what following needs from the outside world.
type Phone interface {
	// Resolve turns a number, a jid or a group name into the chat.
	Resolve(ctx context.Context, from, ref string) (Resolved, error)
	// Ask requests up to count messages older than the anchor. The answer
	// comes later, through Follower.OnDemand.
	Ask(ctx context.Context, account, chat string, at Anchor, count int) error
}

// Archive is the local message store, as far as following needs it.
type Archive interface {
	// Keep stores ev and reports whether it was new.
	Keep(ev Event) bool
	// Oldest is the oldest stored message of a chat.
	Oldest(account, chat string) (Anchor, bool)
	// Drop deletes every stored message of the given chats.
	Drop(chats []string) (int, error)
	// Stored lists the distinct chats that have stored messages.
	Stored() []string
}

// Defaults of a backfill. The phone is asked for 50 at a time, which is what
// the library recommends for one request.
const (
	DefaultBackfillDays = 30
	DefaultBackfillMax  = 500
	BackfillPage        = 50
)

// Follower turns "follow this person" into its three steps — allow, fetch the
// past, then keep syncing — and undoes them.
//
// It also keeps the ANCHOR CACHE: for each chat, the newest message seen, as
// an id, a direction and a time. It lives only in memory and is never written
// anywhere, because a history request needs a real message to start from and
// zap stores nothing of a chat nobody allowed.
type Follower struct {
	Policy  *Policy
	Phone   Phone
	Archive Archive
	// Bus publishes an event to `zap tail`.
	Bus func(Event)
	// Stall is how long to wait for the phone before calling a backfill stalled.
	Stall time.Duration

	mu      sync.Mutex
	anchors map[string]Anchor
	runs    map[string]*backfillRun
}

type backfillRun struct {
	account string
	chat    string
	max     int
	cutoff  time.Time
	count   int
	timer   *time.Timer
}

const maxAnchors = 20000

func (f *Follower) key(chat string) string { return NormJID(chat) }

// Observe remembers the newest message of a chat as a possible anchor.
func (f *Follower) Observe(chat string, a Anchor) {
	if chat == "" || a.ID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.anchors == nil {
		f.anchors = map[string]Anchor{}
	}
	k := f.key(chat)
	if cur, ok := f.anchors[k]; ok && !a.At.After(cur.At) {
		return
	}
	if _, ok := f.anchors[k]; !ok && len(f.anchors) >= maxAnchors {
		return
	}
	f.anchors[k] = a
}

// Seen lists the chats the anchor cache knows, with the time of the newest
// message. Nothing else about them.
func (f *Follower) Seen() map[string]time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]time.Time, len(f.anchors))
	for k, a := range f.anchors {
		out[k] = a.At
	}
	return out
}

func (f *Follower) anchorFor(account string, e AllowEntry) (Anchor, bool) {
	for _, jid := range append([]string{e.JID}, e.Alt...) {
		if a, ok := f.Archive.Oldest(account, jid); ok {
			return a, true
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var best Anchor
	var found bool
	for _, jid := range append([]string{e.JID}, e.Alt...) {
		if a, ok := f.anchors[f.key(jid)]; ok && (!found || a.At.After(best.At)) {
			best, found = a, true
		}
	}
	return best, found
}

// Follow allows a chat, announces it, and asks the phone for the past.
func (f *Follower) Follow(ctx context.Context, req FollowRequest) (FollowResult, error) {
	if strings.TrimSpace(req.Chat) == "" {
		return FollowResult{}, errors.New("follow needs a chat: a number, a group name or a jid")
	}
	res, err := f.Phone.Resolve(ctx, req.From, req.Chat)
	if err != nil {
		return FollowResult{}, err
	}
	entry := AllowEntry{JID: res.JID, Alt: res.Alt, Label: firstNonEmpty(req.Label, res.Label)}
	if req.NoBackfill {
		entry.Backfill = BackfillOff
	}
	entry, added, err := f.Policy.Allow.Add(entry)
	if err != nil {
		return FollowResult{}, err
	}
	out := FollowResult{Chat: entry.JID, Label: entry.Label, New: added, Backfill: entry.Backfill}
	if added {
		f.announce(Event{Account: res.Account, Type: Events.Chat.Followed, Chat: entry.JID, Text: entry.Label, Timestamp: time.Now()})
	}
	if req.NoBackfill || (entry.Backfill == BackfillDone && !req.Again) {
		return out, nil
	}
	days, max := req.Days, req.Max
	if days <= 0 {
		days = DefaultBackfillDays
	}
	if max <= 0 {
		max = DefaultBackfillMax
	}
	run := &backfillRun{account: res.Account, chat: entry.JID, max: max, cutoff: time.Now().AddDate(0, 0, -days)}
	f.mu.Lock()
	if f.runs == nil {
		f.runs = map[string]*backfillRun{}
	}
	if old := f.runs[f.key(entry.JID)]; old != nil && old.timer != nil {
		old.timer.Stop()
	}
	f.runs[f.key(entry.JID)] = run
	f.mu.Unlock()

	state, err := f.start(ctx, run, entry)
	out.Backfill = state
	if err != nil {
		out.Error = err.Error()
	}
	return out, nil
}

// start asks for the first page, or leaves the chat pending when there is no
// message to ask from yet.
func (f *Follower) start(ctx context.Context, run *backfillRun, entry AllowEntry) (string, error) {
	at, ok := f.anchorFor(run.account, entry)
	if !ok {
		f.setState(entry.JID, BackfillPending)
		return BackfillPending, nil
	}
	return f.ask(ctx, run, at)
}

func (f *Follower) ask(ctx context.Context, run *backfillRun, at Anchor) (string, error) {
	f.setState(run.chat, BackfillRunning)
	if err := f.Phone.Ask(ctx, run.account, run.chat, at, BackfillPage); err != nil {
		f.setState(run.chat, BackfillStalled)
		return BackfillStalled, err
	}
	f.arm(run)
	return BackfillRunning, nil
}

func (f *Follower) arm(run *backfillRun) {
	stall := f.Stall
	if stall <= 0 {
		stall = 3 * time.Minute
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if run.timer != nil {
		run.timer.Stop()
	}
	run.timer = time.AfterFunc(stall, func() {
		f.mu.Lock()
		cur := f.runs[f.key(run.chat)]
		f.mu.Unlock()
		if cur == run {
			f.setState(run.chat, BackfillStalled)
		}
	})
}

func (f *Follower) setState(chat, state string) {
	_ = f.Policy.Allow.Update(chat, func(e *AllowEntry) { e.Backfill = state })
}

// Resume is called for every admitted live message: a chat that was followed
// before zap had seen a single message of it starts its backfill from the first
// one that arrives.
func (f *Follower) Resume(ev Event) {
	e, ok := f.Policy.Allow.Entry(ev.Chat)
	if !ok || e.Backfill != BackfillPending {
		return
	}
	f.mu.Lock()
	run := f.runs[f.key(e.JID)]
	if run == nil {
		run = &backfillRun{account: ev.Account, chat: e.JID, max: DefaultBackfillMax, cutoff: time.Now().AddDate(0, 0, -DefaultBackfillDays)}
		if f.runs == nil {
			f.runs = map[string]*backfillRun{}
		}
		f.runs[f.key(e.JID)] = run
	}
	f.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = f.ask(ctx, run, Anchor{ID: ev.ID, FromMe: ev.FromMe, At: ev.Timestamp})
	}()
}

// Ingest stores and forwards messages of ALLOWED chats that arrived as
// history. Messages of any other chat are dropped on the floor, here. It
// returns how many were new.
func (f *Follower) Ingest(msgs []Event) int {
	n := 0
	for _, m := range msgs {
		if !f.Policy.Allow.Allowed(m.Chat) {
			continue
		}
		if f.Archive.Keep(m) {
			n++
			m.Backfill = true
			f.Policy.Emit(m)
		}
	}
	return n
}

// OnDemand takes one page of the past the phone sent for a followed chat and
// decides whether to ask for the next one. more is the phone's own word on
// whether it has older messages.
func (f *Follower) OnDemand(account, chat string, msgs []Event, more bool) {
	if !f.Policy.Allow.Allowed(chat) {
		return
	}
	f.mu.Lock()
	run := f.runs[f.key(chat)]
	f.mu.Unlock()

	var recent []Event
	var oldest *Event
	for i := range msgs {
		m := msgs[i]
		if oldest == nil || m.Timestamp.Before(oldest.Timestamp) {
			oldest = &msgs[i]
		}
		if run == nil || !m.Timestamp.Before(run.cutoff) {
			recent = append(recent, m)
		}
	}
	if run != nil {
		// never keep more than max: of a page that overshoots, the newest win
		if room := run.max - run.count; room < len(recent) {
			sort.Slice(recent, func(i, j int) bool { return recent[i].Timestamp.Before(recent[j].Timestamp) })
			if room < 0 {
				room = 0
			}
			recent = recent[len(recent)-room:]
		}
	}
	got := f.Ingest(recent)
	if run == nil {
		return
	}
	run.count += got

	stop := oldest == nil || run.count >= run.max || !more || len(recent) < len(msgs)
	if stop {
		f.finish(run, more && run.count >= run.max)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = f.ask(ctx, run, Anchor{ID: oldest.ID, FromMe: oldest.FromMe, At: oldest.Timestamp})
}

func (f *Follower) finish(run *backfillRun, more bool) {
	f.mu.Lock()
	if run.timer != nil {
		run.timer.Stop()
	}
	delete(f.runs, f.key(run.chat))
	f.mu.Unlock()
	_ = f.Policy.Allow.Update(run.chat, func(e *AllowEntry) {
		e.Backfill = BackfillDone
		e.Backfilled += run.count
	})
	f.announce(Event{Account: run.account, Type: Events.Chat.Backfilled, Chat: run.chat, Count: run.count, More: more, Timestamp: time.Now()})
}

// Unfollow undoes a follow: the chat leaves the list, its backfill stops and
// its stored messages are deleted. Nothing about a chat that is not followed
// stays on this machine. What a backend already received is the backend's own
// to expire.
func (f *Follower) Unfollow(ctx context.Context, req UnfollowRequest) (UnfollowResult, error) {
	e, ok := f.Policy.Allow.find(req.Chat)
	if !ok {
		return UnfollowResult{Chat: req.Chat}, fmt.Errorf("%q is not followed", req.Chat)
	}
	f.mu.Lock()
	if run := f.runs[f.key(e.JID)]; run != nil {
		if run.timer != nil {
			run.timer.Stop()
		}
		delete(f.runs, f.key(e.JID))
	}
	f.mu.Unlock()

	if _, _, err := f.Policy.Allow.Remove(e.JID); err != nil {
		return UnfollowResult{}, err
	}
	out := UnfollowResult{Chat: e.JID, Removed: true}
	n, err := f.Archive.Drop(append([]string{e.JID}, e.Alt...))
	if err != nil {
		return out, err
	}
	out.Dropped = n
	f.announce(Event{Account: req.From, Type: Events.Chat.Unfollowed, Chat: e.JID, Timestamp: time.Now()})
	return out, nil
}

// Purge deletes every stored message whose chat is not on the allowlist: what
// an older zap kept before the allowlist existed.
func (f *Follower) Purge() (PurgeResult, error) {
	keep := f.Policy.Allow.keySet()
	var drop []string
	for _, chat := range f.Archive.Stored() {
		if !keep[NormJID(chat)] {
			drop = append(drop, chat)
		}
	}
	if len(drop) == 0 {
		return PurgeResult{}, nil
	}
	n, err := f.Archive.Drop(drop)
	return PurgeResult{Chats: len(drop), Messages: n}, err
}

// Follows lists what is followed and how each backfill stands.
func (f *Follower) Follows() []Follow {
	list := f.Policy.Allow.List()
	out := make([]Follow, len(list))
	for i, e := range list {
		out[i] = Follow{Chat: e.JID, Label: e.Label, Since: e.Since, Backfill: e.Backfill, Backfilled: e.Backfilled}
	}
	return out
}

// announce puts a follow-lifecycle event on the tail and on the backend.
func (f *Follower) announce(ev Event) {
	if f.Bus != nil {
		f.Bus(ev)
	}
	f.Policy.Emit(ev)
}

// find resolves what a person typed (a number, a jid, or the label they gave)
// to an entry on the list, without asking the phone.
func (a *Allowlist) find(ref string) (AllowEntry, bool) {
	if e, ok := a.Entry(ref); ok {
		return e, true
	}
	want := strings.ToLower(strings.TrimSpace(ref))
	for _, e := range a.List() {
		if e.Label != "" && strings.ToLower(e.Label) == want {
			return e, true
		}
	}
	return AllowEntry{}, false
}
