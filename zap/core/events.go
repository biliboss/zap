// The events of zap, in the order the system produces them.
//
//	zap pair ──▶ account_connected ──▶ history_synced ──▶ [store]
//	                     │                                   ▲
//	     the phone ──────┤                                   │
//	                     ├──▶ message_received ──────────────┤
//	                     │         └── audio? ──▶ audio_queued ──▶ audio_transcribed ──▶ [store]
//	     zap send ───────┼──▶ message_sent ───────────────────┘
//	                     ├──▶ message_delivered ──▶ message_read
//	                     ├──▶ account_disconnected  (whatsmeow reconnects)
//	                     └──▶ account_logged_out    (needs zap pair again)
//
// Every one of them reaches a consumer through `zap tail`; the requests that
// caused them reach it through `zap log`. Nothing else leaves the daemon.
package core

import (
	"strings"
	"sync"
	"time"
)

// EventType names a fact that already happened. EventStorming spelling —
// subject plus a past-tense verb — because "connected" is a state you can argue
// about and "account_connected" is a thing that occurred at a time.
type EventType string

// AccountEvents is the life of one linked number.
type AccountEvents struct {
	Connected    EventType // the socket came up and the account is usable
	Disconnected EventType // the link dropped; whatsmeow retries by itself
	LoggedOut    EventType // the phone unlinked us; only zap pair fixes it
}

// MessageEvents is the traffic, once a session exists.
type MessageEvents struct {
	Received      EventType // someone wrote to us
	Sent          EventType // we wrote to someone
	Delivered     EventType // the other side's phone got it
	Read          EventType // the other side opened it
	Reacted       EventType // an emoji landed on a message
	Edited        EventType // the author rewrote it
	Revoked       EventType // the author deleted it for everyone
	Undecryptable EventType // it arrived and we could not open it
}

// MediaEvents is anything with bytes attached.
type MediaEvents struct {
	Received EventType // an image, video, document or sticker arrived
}

// GroupEvents is what happens to a group we are in.
type GroupEvents struct {
	Joined          EventType // we entered a group
	MembersAdded    EventType // someone else entered
	MembersRemoved  EventType // someone else left or was removed
	AdminsPromoted  EventType // members became admins
	AdminsDemoted   EventType // admins became members
	Renamed         EventType // the subject changed
	TopicChanged    EventType // the description changed
	PictureChanged  EventType // the photo changed
	SettingsChanged EventType // locked, announce or disappearing changed
}

// ChatEvents is the state of a conversation, not its content.
type ChatEvents struct {
	Archived   EventType
	Pinned     EventType
	Muted      EventType
	MarkedRead EventType
	Deleted    EventType
	Typing     EventType // the other side is composing
}

// ContactEvents is the address book moving under us.
type ContactEvents struct {
	NameChanged    EventType // push name or business name changed
	PictureChanged EventType
	Blocked        EventType
	Unblocked      EventType
}

// CallEvents is the phone ringing.
type CallEvents struct {
	Offered    EventType
	Accepted   EventType
	Rejected   EventType
	Terminated EventType
}

// HistoryEvents is the past arriving.
type HistoryEvents struct {
	Synced EventType // a bundle of old messages landed and went into the store
}

// AudioEvents is the voice-note pipeline.
type AudioEvents struct {
	Queued      EventType // a voice note was downloaded and parked
	Transcribed EventType // whisper turned it into text, in place of "[media]"
}

// SessionEvents is OUR invention, not whatsmeow's. A session is a stretch of
// conversation cut by silence, so it opens on the first message after the gap
// and closes when the gap elapses with nobody speaking. Everything above this
// is a fact the phone told us; this one is a fact our own layer decided.
type SessionEvents struct {
	Opened EventType // a chat spoke again after the gap — a new conversation
	Closed EventType // the gap elapsed with silence — the conversation ended
}

// Catalogue is the whole vocabulary, grouped by the context that owns it. One
// symbol per context keeps the outline readable and keeps a new event from
// being born anywhere else.
type Catalogue struct {
	Account AccountEvents
	Message MessageEvents
	Media   MediaEvents
	Audio   AudioEvents
	History HistoryEvents
	Group   GroupEvents
	Chat    ChatEvents
	Contact ContactEvents
	Call    CallEvents

	// ours, derived from the ones above
	Session SessionEvents
}

// Raw names an event whatsmeow has that zap has not curated. Everything the
// library emits reaches `zap tail` one way or another: what is not in the
// catalogue arrives as wa_<TypeName>, so nothing is invisible while we decide
// whether it deserves a name of its own.
func Raw(whatsmeowType string) EventType {
	return EventType("wa_" + whatsmeowType)
}

// All is the catalogue in the order the system produces it.
func (c Catalogue) All() []EventType {
	return []EventType{
		c.Account.Connected, c.History.Synced,
		c.Message.Received, c.Message.Sent, c.Message.Delivered, c.Message.Read,
		c.Message.Reacted, c.Message.Edited, c.Message.Revoked, c.Message.Undecryptable,
		c.Media.Received, c.Audio.Queued, c.Audio.Transcribed,
		c.Group.Joined, c.Group.MembersAdded, c.Group.MembersRemoved,
		c.Group.AdminsPromoted, c.Group.AdminsDemoted,
		c.Group.Renamed, c.Group.TopicChanged, c.Group.PictureChanged, c.Group.SettingsChanged,
		c.Chat.Archived, c.Chat.Pinned, c.Chat.Muted, c.Chat.MarkedRead, c.Chat.Deleted, c.Chat.Typing,
		c.Contact.NameChanged, c.Contact.PictureChanged, c.Contact.Blocked, c.Contact.Unblocked,
		c.Call.Offered, c.Call.Accepted, c.Call.Rejected, c.Call.Terminated,
		c.Account.Disconnected, c.Account.LoggedOut,
		c.Session.Opened, c.Session.Closed,
	}
}

// Events is the vocabulary. Anything a consumer can watch is named here and
// nowhere else — a type spelled inline in two files is two vocabularies.
var Events = Catalogue{
	Account: AccountEvents{
		Connected:    "account_connected",
		Disconnected: "account_disconnected",
		LoggedOut:    "account_logged_out",
	},
	Message: MessageEvents{
		Received:      "message_received",
		Sent:          "message_sent",
		Delivered:     "message_delivered",
		Read:          "message_read",
		Reacted:       "message_reacted",
		Edited:        "message_edited",
		Revoked:       "message_revoked",
		Undecryptable: "message_undecryptable",
	},
	Media: MediaEvents{Received: "media_received"},
	Audio: AudioEvents{
		Queued:      "audio_queued",
		Transcribed: "audio_transcribed",
	},
	History: HistoryEvents{Synced: "history_synced"},
	Group: GroupEvents{
		Joined:          "group_joined",
		MembersAdded:    "group_members_added",
		MembersRemoved:  "group_members_removed",
		AdminsPromoted:  "group_admins_promoted",
		AdminsDemoted:   "group_admins_demoted",
		Renamed:         "group_renamed",
		TopicChanged:    "group_topic_changed",
		PictureChanged:  "group_picture_changed",
		SettingsChanged: "group_settings_changed",
	},
	Chat: ChatEvents{
		Archived:   "chat_archived",
		Pinned:     "chat_pinned",
		Muted:      "chat_muted",
		MarkedRead: "chat_marked_read",
		Deleted:    "chat_deleted",
		Typing:     "chat_typing",
	},
	Contact: ContactEvents{
		NameChanged:    "contact_name_changed",
		PictureChanged: "contact_picture_changed",
		Blocked:        "contact_blocked",
		Unblocked:      "contact_unblocked",
	},
	Call: CallEvents{
		Offered:    "call_offered",
		Accepted:   "call_accepted",
		Rejected:   "call_rejected",
		Terminated: "call_terminated",
	},

	Session: SessionEvents{
		Opened: "session_opened",
		Closed: "session_closed",
	},
}

// Emits says where each fact comes from and what it means, so `zap events`
// answers "what can show up on tail" from the binary, not from the source.
func (c Catalogue) Emits(t EventType) (source, meaning string) {
	switch t {
	case c.Account.Connected:
		return "socket", "the account came online"
	case c.Account.Disconnected:
		return "socket", "the link dropped, whatsmeow retries by itself"
	case c.Account.LoggedOut:
		return "phone", "the device was unlinked, run zap pair"
	case c.History.Synced:
		return "phone", "old messages arrived and went into the store"
	case c.Message.Received:
		return "phone", "someone wrote to us"
	case c.Message.Sent:
		return "zap", "we wrote to someone"
	case c.Message.Delivered:
		return "phone", "the other side's phone got it"
	case c.Message.Read:
		return "phone", "the other side opened it"
	case c.Audio.Queued:
		return "queue", "a voice note was downloaded and parked"
	case c.Audio.Transcribed:
		return "queue", "a voice note became text in the store"
	case c.Message.Reacted:
		return "phone", "an emoji landed on a message"
	case c.Message.Edited:
		return "phone", "the author rewrote a message"
	case c.Message.Revoked:
		return "phone", "the author deleted it for everyone"
	case c.Message.Undecryptable:
		return "phone", "a message arrived that we could not open"
	case c.Media.Received:
		return "phone", "an image, video, document or sticker arrived"
	case c.Group.Joined:
		return "phone", "we entered a group"
	case c.Group.MembersAdded, c.Group.MembersRemoved:
		return "group", "the member list changed"
	case c.Group.AdminsPromoted, c.Group.AdminsDemoted:
		return "group", "who can administer changed"
	case c.Group.Renamed, c.Group.TopicChanged, c.Group.PictureChanged, c.Group.SettingsChanged:
		return "group", "the group itself changed"
	case c.Chat.Archived, c.Chat.Pinned, c.Chat.Muted, c.Chat.MarkedRead, c.Chat.Deleted:
		return "phone", "you changed the state of a chat somewhere else"
	case c.Chat.Typing:
		return "phone", "the other side is composing"
	case c.Contact.NameChanged, c.Contact.PictureChanged:
		return "phone", "the address book moved"
	case c.Contact.Blocked, c.Contact.Unblocked:
		return "phone", "the block list changed"
	case c.Call.Offered, c.Call.Accepted, c.Call.Rejected, c.Call.Terminated:
		return "phone", "a call happened"
	case c.Session.Opened:
		return "zap", "a chat spoke again after the gap — a new conversation"
	case c.Session.Closed:
		return "zap", "the gap elapsed in silence — the conversation ended"
	}
	if strings.HasPrefix(string(t), "wa_") {
		return "whatsmeow", "uncurated event passed through as-is"
	}
	return "", ""
}

// Event is one thing that happened on an account. It always carries the account
// it belongs to — without that field two inboxes silently become one.
type Event struct {
	Account   string    `json:"account"`
	Type      EventType `json:"type"`
	Chat      string    `json:"chat,omitempty"`
	Sender    string    `json:"sender,omitempty"`
	FromMe    bool      `json:"from_me,omitempty"`
	ID        string    `json:"id,omitempty"`
	Text      string    `json:"text,omitempty"`
	Timestamp time.Time `json:"timestamp"`

	Members []string `json:"members,omitempty"` // group joins, leaves, promotions
	Emoji   string   `json:"emoji,omitempty"`   // the reaction itself
	Media   string   `json:"media,omitempty"`   // image, video, audio, document, sticker
	Target  string   `json:"target,omitempty"`  // the message a reaction or edit refers to

	ChatName     string `json:"chat_name,omitempty"`
	SenderNumber string `json:"sender_number,omitempty"`
	SenderName   string `json:"sender_name,omitempty"`
}

// Call is one request the daemon handled, published on /log so anything that
// speaks ndjson can watch the system work: `zap log | jq`.
type Call struct {
	Time    time.Time `json:"time"`
	Method  string    `json:"method"`
	Path    string    `json:"path"`
	Status  int       `json:"status"`
	Millis  int64     `json:"ms"`
	Request any       `json:"request,omitempty"`
}

// Bus fans one record out to every live reader and DROPS for anyone too slow.
// A stalled `zap tail` must never stall the client receiving messages, and a
// stalled `zap log` must never stall the daemon it is watching.
type Bus[T any] struct {
	mu   sync.RWMutex
	next int
	// anonymous on purpose: only this field ever names the shape, and a
	// top-level busReader would be a symbol claiming to stand on its own.
	readers map[int]struct {
		ch    chan T
		match func(T) bool
	}
}

// Subscribe returns a channel and the function that closes it. match may be nil
// to receive everything.
func (b *Bus[T]) Subscribe(match func(T) bool) (<-chan T, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.readers == nil {
		b.readers = map[int]struct {
			ch    chan T
			match func(T) bool
		}{}
	}
	id := b.next
	b.next++
	ch := make(chan T, 64)
	b.readers[id] = struct {
		ch    chan T
		match func(T) bool
	}{ch: ch, match: match}

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if cur, ok := b.readers[id]; ok {
			delete(b.readers, id)
			close(cur.ch)
		}
	}
}

func (b *Bus[T]) Publish(v T) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, r := range b.readers {
		if r.match != nil && !r.match(v) {
			continue
		}
		select {
		case r.ch <- v:
		default:
		}
	}
}
