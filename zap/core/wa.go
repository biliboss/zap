package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// WhatsApp holds every paired number in this one process. Adding a number costs
// ~87 KB and two goroutines here; it costs a whole process otherwise — see
// bench/ and the root CONTEXT.md.
type WhatsApp struct {
	container *sqlstore.Container
	db        *sql.DB

	mu       sync.RWMutex
	accounts map[string]*WhatsApp_Account
	bus      Bus[Event]

	audio      *Transcriber
	SessionGap time.Duration
	closed     map[string]int64 // chat -> the last ts we already closed on

	policy *Policy   // the door: nothing of a chat off the allowlist passes
	fol    *Follower // follow, backfill, anchors
}

// Account pairs a client with the connection state we track ourselves. Asking
// the client via IsConnected() would block behind the socket lock that Connect
// holds for the whole handshake.
type WhatsApp_Account struct {
	cli       *whatsmeow.Client
	connected atomic.Bool
}

// OpenWhatsApp opens the session store and connects every paired number. The
// policy is required: a daemon without an allowlist listens to nothing, and a
// nil one is a bug, not a default.
func OpenWhatsApp(ctx context.Context, dbPath string, policy *Policy) (*WhatsApp, error) {
	if policy == nil || policy.Allow == nil {
		return nil, fmt.Errorf("open whatsapp: a policy with an allowlist is required")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Dir(dbPath), err)
	}
	dsn := fmt.Sprintf("file:%s?_foreign_keys=on&_journal_mode=WAL", dbPath)
	container, err := sqlstore.New(ctx, "sqlite3", dsn, waLog.Noop)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open message store: %w", err)
	}
	if _, err := db.Exec(messagesSchema); err != nil {
		return nil, fmt.Errorf("create message store: %w", err)
	}
	w := &WhatsApp{
		container: container,
		db:        db,
		accounts:  map[string]*WhatsApp_Account{},
		policy:    policy,
	}
	w.fol = &Follower{Policy: policy, Phone: w, Archive: rawArchive{w}, Bus: w.bus.Publish}
	devices, err := container.GetAllDevices(ctx)
	if err != nil {
		return nil, fmt.Errorf("read devices: %w", err)
	}
	for _, device := range devices {
		acc := w.attach(device)
		go acc.cli.Connect()
	}
	return w, nil
}

// attach wires one device into a client whose event handler stamps every event
// with the account it came from.
func (w *WhatsApp) attach(device *store.Device) *WhatsApp_Account {
	cli := whatsmeow.NewClient(device, waLog.Noop)
	acc := &WhatsApp_Account{cli: cli}
	cli.AddEventHandler(func(raw any) { w.onEvent(acc, raw) })

	w.mu.Lock()
	defer w.mu.Unlock()
	w.accounts[accountKey(cli)] = acc
	return acc
}

func (w *WhatsApp) Accounts() []Account {
	w.mu.RLock()
	defer w.mu.RUnlock()

	out := make([]Account, 0, len(w.accounts))
	for jid, acc := range w.accounts {
		out = append(out, Account{
			Number:    e164(jid),
			JID:       jid,
			PushName:  acc.cli.Store.PushName,
			Connected: acc.connected.Load(),
		})
	}
	return out
}

func (w *WhatsApp) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	acc, from, err := w.pick(req.From)
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
	msg := &waE2E.Message{Conversation: proto.String(req.Text)}
	var extra []whatsmeow.SendRequestExtra
	if req.ID != "" {
		extra = append(extra, whatsmeow.SendRequestExtra{ID: req.ID})
	}
	resp, err := acc.cli.SendMessage(ctx, to, msg, extra...)
	if err != nil {
		return SendResult{}, err
	}
	// Own sends never come back as an event on the device that sent them, so
	// without this line the store would hold half a conversation.
	sent := Event{
		Account:   from,
		Type:      Events.Message.Sent,
		Chat:      to.String(),
		ID:        resp.ID,
		Sender:    from,
		FromMe:    true,
		Timestamp: resp.Timestamp,
		Text:      req.Text,
	}
	// Sending is the owner's act and works for any chat; keeping it is a
	// different matter: a chat that is not followed is not written down.
	// (no anchor either: a chat that is not followed leaves no trace)
	if w.policy.Allow.Allowed(sent.Chat) {
		w.fol.Observe(sent.Chat, Anchor{ID: sent.ID, FromMe: true, At: sent.Timestamp})
		w.remember(sent)
		w.publish(sent)
	}
	return SendResult{ID: resp.ID, From: from, Timestamp: resp.Timestamp}, nil
}

// pick resolves --from. With a single account it is optional; with several it is
// mandatory, because guessing which number sent a message is not recoverable.
func (w *WhatsApp) pick(from string) (*WhatsApp_Account, string, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.accounts) == 0 {
		return nil, "", fmt.Errorf("no accounts paired — run `zap pair`")
	}
	if from == "" {
		if len(w.accounts) > 1 {
			return nil, "", fmt.Errorf("%d accounts paired, pass --from", len(w.accounts))
		}
		for jid, acc := range w.accounts {
			return acc, jid, nil
		}
	}
	want := digits(from)
	for jid, acc := range w.accounts {
		if jid == from || digits(jid) == want {
			return acc, jid, nil
		}
	}
	return nil, "", fmt.Errorf("no account matching %q", from)
}

func (w *WhatsApp) Pair(ctx context.Context, emit func(PairEvent) error) error {
	acc := w.attach(w.container.NewDevice())
	qr, err := acc.cli.GetQRChannel(ctx)
	if err != nil {
		return err
	}
	if err := acc.cli.Connect(); err != nil {
		return err
	}
	for item := range qr {
		switch item.Event {
		case "code":
			if err := emit(PairEvent{QR: item.Code}); err != nil {
				return err
			}
		case "success":
			w.rekey(acc)
			return emit(PairEvent{JID: accountKey(acc.cli)})
		default:
			if item.Error != nil {
				return item.Error
			}
			return fmt.Errorf("pairing %s", item.Event)
		}
	}
	return nil
}

// rekey moves an account from its pre-pairing placeholder key to its real JID.
func (w *WhatsApp) rekey(acc *WhatsApp_Account) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, existing := range w.accounts {
		if existing == acc {
			delete(w.accounts, key)
		}
	}
	w.accounts[accountKey(acc.cli)] = acc
}

// Typing turns the "composing…" bubble on and off on the other side.
func (w *WhatsApp) Typing(ctx context.Context, req TypingRequest) error {
	acc, _, err := w.pick(req.From)
	if err != nil {
		return err
	}
	chat, err := w.resolveChat(ctx, acc, req.Chat)
	if err != nil {
		return err
	}
	state := types.ChatPresenceComposing
	if !req.On {
		state = types.ChatPresencePaused
	}
	media := types.ChatPresenceMediaText
	if req.Audio {
		media = types.ChatPresenceMediaAudio
	}
	return acc.cli.SendChatPresence(ctx, chat, state, media)
}

// Presence is the online/offline dot. Going unavailable is what makes the phone
// send push notifications again, so it is not cosmetic.
func (w *WhatsApp) Presence(ctx context.Context, from string, available bool) error {
	acc, _, err := w.pick(from)
	if err != nil {
		return err
	}
	state := types.PresenceAvailable
	if !available {
		state = types.PresenceUnavailable
	}
	return acc.cli.SendPresence(ctx, state)
}

// Status posts to status@broadcast — the 24h story everyone in your contacts
// can see. It is a normal send to a special JID.
func (w *WhatsApp) Status(ctx context.Context, req SendRequest) (SendResult, error) {
	acc, from, err := w.pick(req.From)
	if err != nil {
		return SendResult{}, err
	}
	msg := &waE2E.Message{Conversation: proto.String(req.Text)}
	resp, err := acc.cli.SendMessage(ctx, types.StatusBroadcastJID, msg)
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{ID: resp.ID, From: from, Timestamp: resp.Timestamp}, nil
}

// Logout unlinks the device on the phone and drops it from the store. The
// stored messages stay: they are ours, not the session's.
func (w *WhatsApp) Logout(ctx context.Context, from string) error {
	acc, key, err := w.pick(from)
	if err != nil {
		return err
	}
	if err := acc.cli.Logout(ctx); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.accounts, key)
	return nil
}

func (w *WhatsApp) Subscribe(from string) (<-chan Event, func()) {
	want := digits(from)
	if want == "" {
		return w.bus.Subscribe(nil)
	}
	return w.bus.Subscribe(func(ev Event) bool { return digits(ev.Account) == want })
}

// ----------------------------------------------------------------- messages

// whatsmeow keeps sessions, never message bodies: whatever the history sync
// hands over is gone unless we write it down here.
const messagesSchema = `
CREATE TABLE IF NOT EXISTS messages (
	account TEXT NOT NULL,
	chat    TEXT NOT NULL,
	id      TEXT NOT NULL,
	sender  TEXT,
	from_me INTEGER NOT NULL DEFAULT 0,
	ts      INTEGER NOT NULL,
	text    TEXT,
	PRIMARY KEY (account, chat, id)
);
CREATE INDEX IF NOT EXISTS messages_recent ON messages (account, chat, ts DESC);`

// remember writes one message down, but only if its chat is on the allowlist:
// this is the last line of the rule, so no caller can forget it.
func (w *WhatsApp) remember(ev Event) bool {
	if ev.ID == "" || ev.Chat == "" || !w.policy.Allow.Allowed(ev.Chat) {
		return false
	}
	res, err := w.db.Exec(
		`INSERT OR IGNORE INTO messages (account, chat, id, sender, from_me, ts, text) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		ev.Account, ev.Chat, ev.ID, ev.Sender, ev.FromMe, ev.Timestamp.Unix(), ev.Text)
	if err != nil {
		// the error, never the row: the text is somebody else's
		fmt.Fprintf(os.Stderr, "zap: store message: %v\n", err)
		return false
	}
	n, _ := res.RowsAffected()
	return n > 0
}

// Keep, Oldest, Drop and Stored make the message table an Archive for the
// follower.
func (w *WhatsApp) Keep(ev Event) bool { return w.remember(ev) }

func (w *WhatsApp) Oldest(account, chat string) (Anchor, bool) {
	var a Anchor
	var ts int64
	var from int
	err := w.db.QueryRow(
		`SELECT id, from_me, ts FROM messages WHERE account = ? AND chat = ? ORDER BY ts ASC LIMIT 1`,
		account, chat).Scan(&a.ID, &from, &ts)
	if err != nil {
		return Anchor{}, false
	}
	a.FromMe, a.At = from != 0, time.Unix(ts, 0)
	return a, true
}

func (w *WhatsApp) Drop(chats []string) (int, error) {
	total := 0
	// match by the normalised id: the store may hold the same chat under a
	// spelling that is not the one on the allowlist
	want := map[string]bool{}
	for _, chat := range chats {
		want[NormJID(chat)] = true
	}
	for _, chat := range w.storedAll() {
		if !want[NormJID(chat)] {
			continue
		}
		// the voice notes of the chat go with it: the files are somebody
		// else's words too
		if rows, err := w.db.Query(`SELECT path FROM audio_queue WHERE chat = ?`, chat); err == nil {
			for rows.Next() {
				var p string
				if rows.Scan(&p) == nil && p != "" {
					os.Remove(p)
				}
			}
			rows.Close()
			w.db.Exec(`DELETE FROM audio_queue WHERE chat = ?`, chat)
		}
		res, err := w.db.Exec(`DELETE FROM messages WHERE chat = ?`, chat)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += int(n)
	}
	return total, nil
}

// rawArchive is the Archive the follower sees: its Stored lists every chat in
// the table, allowed or not, because purge exists to find the ones that are not.
type rawArchive struct{ *WhatsApp }

func (r rawArchive) Stored() []string { return r.WhatsApp.storedAll() }

// Stored lists the allowed chats that have stored messages: a stray row, left
// by an older zap or by an allowlist edit, is not reported (purge finds it).
func (w *WhatsApp) Stored() []string {
	var out []string
	for _, chat := range w.storedAll() {
		if w.policy.Allow.Allowed(chat) {
			out = append(out, chat)
		}
	}
	return out
}

func (w *WhatsApp) storedAll() []string {
	rows, err := w.db.Query(`SELECT DISTINCT chat FROM messages`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var chat string
		if rows.Scan(&chat) == nil {
			out = append(out, chat)
		}
	}
	return out
}

func (w *WhatsApp) Messages(ctx context.Context, q MessagesQuery) ([]Event, error) {
	acc, account, err := w.pick(q.From)
	if err != nil {
		return nil, err
	}
	if q.Limit <= 0 {
		q.Limit = 50
	}
	query := `SELECT chat, id, sender, from_me, ts, text FROM messages WHERE account = ?`
	args := []any{account}
	if q.Chat != "" {
		query += ` AND chat LIKE ?`
		args = append(args, "%"+digits(q.Chat)+"%")
	}
	// A cursor reads FORWARD: oldest first, so the consumer can advance through
	// a backlog page by page. Without Since the reader wants the newest, so the
	// order flips.
	if !q.Since.IsZero() {
		query += ` AND ts >= ? ORDER BY ts ASC, id ASC LIMIT ?`
		args = append(args, q.Since.Unix(), q.Limit)
	} else {
		query += ` ORDER BY ts DESC LIMIT ?`
		args = append(args, q.Limit)
	}

	rows, err := w.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		ev := Event{Account: account, Type: Events.Message.Received}
		var ts int64
		if err := rows.Scan(&ev.Chat, &ev.ID, &ev.Sender, &ev.FromMe, &ts, &ev.Text); err != nil {
			return nil, err
		}
		if !w.policy.Allow.Allowed(ev.Chat) {
			continue // a stray row is not served
		}
		ev.Timestamp = time.Unix(ts, 0)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	dir := w.directory(ctx, acc)
	for i := range out {
		_, out[i].ChatName = dir.chatName(ctx, out[i].Chat)
		if out[i].Sender == "" && !out[i].FromMe {
			out[i].Sender = out[i].Chat
		}
		out[i].SenderNumber, out[i].SenderName = dir.look(ctx, out[i].Sender)
		if out[i].FromMe {
			out[i].SenderNumber, out[i].SenderName = e164(account), "me"
		}
	}
	return out, nil
}

// absorb writes a history sync bundle into the message store. It runs off the
// event handler because a full sync carries tens of thousands of messages.
//
// Only conversations on the allowlist are kept: the bundle the phone sends at
// pairing carries every chat it has, and all but the allowed ones are dropped
// here, unread. For the rest only an anchor (an id and a time, in memory) is
// noted, so a later follow knows where to ask the phone from.
func (w *WhatsApp) absorb(account string, data *waHistorySync.HistorySync) {
	onDemand := data.GetSyncType() == waHistorySync.HistorySync_ON_DEMAND
	for _, conv := range data.GetConversations() {
		chat := conv.GetID()
		msgs := conv.GetMessages()
		if len(msgs) > 0 {
			// the newest message is the last one the bundle lists, or the one
			// with the largest timestamp; ids and times only
			var newest *waHistorySync.HistorySyncMsg
			for _, hsm := range msgs {
				if newest == nil || hsm.GetMessage().GetMessageTimestamp() > newest.GetMessage().GetMessageTimestamp() {
					newest = hsm
				}
			}
			info := newest.GetMessage()
			w.fol.Observe(chat, Anchor{
				ID: info.GetKey().GetID(), FromMe: info.GetKey().GetFromMe(),
				At: time.Unix(int64(info.GetMessageTimestamp()), 0),
			})
		}
		// the phone names a person by phone jid or by lid; any spelling on the
		// list lets the conversation in, and it is kept under that spelling
		allowedAs := ""
		for _, id := range []string{chat, conv.GetPnJID(), conv.GetLidJID()} {
			if id != "" && w.policy.Allow.Allowed(id) {
				allowedAs = id
				break
			}
		}
		if allowedAs == "" {
			continue
		}
		chat = allowedAs
		page := make([]Event, 0, len(msgs))
		for _, hsm := range msgs {
			info := hsm.GetMessage()
			key := info.GetKey()
			ev := Event{
				Account:   account,
				Type:      Events.Message.Received,
				Chat:      chat,
				ID:        key.GetID(),
				Sender:    key.GetParticipant(),
				FromMe:    key.GetFromMe(),
				Timestamp: time.Unix(int64(info.GetMessageTimestamp()), 0),
				Text:      messageText(info.GetMessage()),
				Media:     mediaKind(info.GetMessage()),
			}
			if ev.FromMe {
				ev.Type = Events.Message.Sent
			}
			if ev.Sender == "" {
				ev.Sender = account
				if !ev.FromMe {
					ev.Sender = chat
				}
			}
			page = append(page, ev)
		}
		if onDemand {
			more := conv.GetEndOfHistoryTransferType() == waHistorySync.Conversation_COMPLETE_ON_DEMAND_SYNC_BUT_MORE_MSG_REMAIN_ON_PRIMARY
			w.fol.OnDemand(account, chat, page, more)
		} else {
			w.fol.Ingest(page)
		}
	}
}

// StartTranscriber wires the audio queue in. Without it voice notes stay as
// "[media]" forever.
func (w *WhatsApp) StartTranscriber(ctx context.Context, t *Transcriber) error {
	w.audio = t
	return t.start(ctx, w)
}

// SetHistoryWindow widens what the phone ships on link. It only affects numbers
// paired AFTER the call: the limits travel inside the pairing payload.
func SetHistoryWindow(days, maxPerChat int) {
	cfg := store.DeviceProps.GetHistorySyncConfig()
	if cfg == nil {
		return
	}
	if days > 0 {
		cfg.FullSyncDaysLimit = proto.Uint32(uint32(days))
		cfg.RecentSyncDaysLimit = proto.Uint32(uint32(days))
	}
	if maxPerChat > 0 {
		cfg.InitialSyncMaxMessagesPerChat = proto.Uint32(uint32(maxPerChat))
	}
}

// ---------------------------------------------------------------- directory

// Contact is a person as the phone knows them: a real number and the name you
// gave them, not the @lid the group protocol hands out.
type WhatsApp_Directory struct {
	acc      *WhatsApp_Account
	contacts map[types.JID]types.ContactInfo
	lids     map[string]types.JID
	groups   map[string]string
}

func (w *WhatsApp) directory(ctx context.Context, acc *WhatsApp_Account) *WhatsApp_Directory {
	contacts, err := acc.cli.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		contacts = map[types.JID]types.ContactInfo{}
	}
	return &WhatsApp_Directory{acc: acc, contacts: contacts, lids: map[string]types.JID{}}
}

// chatName names any chat: a group by its subject, a person by their contact
// name. Group subjects are fetched once and only when a group is actually asked
// about — GetJoinedGroups is a network round trip.
func (d *WhatsApp_Directory) chatName(ctx context.Context, chat string) (number, name string) {
	if !strings.HasSuffix(chat, "@g.us") {
		return d.look(ctx, chat)
	}
	if d.groups == nil {
		d.groups = map[string]string{}
		if infos, err := d.acc.cli.GetJoinedGroups(ctx); err == nil {
			for _, info := range infos {
				d.groups[info.JID.String()] = info.Name
			}
		}
	}
	return "", d.groups[chat]
}

// look resolves any JID to the number and name a human recognises. A @lid is an
// internal id: it has to go through the LID map before it means anything.
func (d *WhatsApp_Directory) look(ctx context.Context, raw string) (number, name string) {
	if raw == "" {
		return "", ""
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return "", ""
	}
	if jid.Server == types.HiddenUserServer {
		if pn, ok := d.lids[jid.User]; ok {
			jid = pn
		} else if pn, err := d.acc.cli.Store.LIDs.GetPNForLID(ctx, jid); err == nil && !pn.IsEmpty() {
			d.lids[jid.User] = pn
			jid = pn
		}
	}
	info := d.contacts[jid.ToNonAD()]
	name = firstNonEmpty(info.FullName, info.FirstName, info.BusinessName, info.PushName)
	number = e164(jid.ToNonAD().String())
	if number == "" && info.RedactedPhone != "" {
		number = info.RedactedPhone
	}
	return number, name
}

func (w *WhatsApp) Contacts(ctx context.Context, from, query string) ([]Contact, error) {
	acc, _, err := w.pick(from)
	if err != nil {
		return nil, err
	}
	dir := w.directory(ctx, acc)
	want := strings.ToLower(query)

	out := make([]Contact, 0, len(dir.contacts))
	for jid, info := range dir.contacts {
		c := Contact{
			Number:   e164(jid.ToNonAD().String()),
			JID:      jid.ToNonAD().String(),
			Name:     firstNonEmpty(info.FullName, info.FirstName, info.BusinessName),
			PushName: info.PushName,
		}
		if c.Number == "" {
			c.Number = info.RedactedPhone
		}
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

// ----------------------------------------------------------------- sessions

// DefaultSessionGap is how long a chat must stay quiet before the next message
// counts as a new conversation.
const DefaultSessionGap = 24 * time.Hour

func (w *WhatsApp) Sessions(ctx context.Context, req SessionQuery) ([]Session, error) {
	if req.Gap <= 0 {
		req.Gap = DefaultSessionGap
	}
	msgs, err := w.Messages(ctx, MessagesQuery{From: req.From, Chat: req.Chat, Limit: 1 << 20})
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
	if acc, _, err := w.pick(req.From); err == nil {
		dir := w.directory(ctx, acc)
		for i := range sessions {
			if number, name := dir.chatName(ctx, sessions[i].Chat); name != "" || number != "" {
				sessions[i].Number, sessions[i].Name = firstNonEmpty(number, sessions[i].Number), name
			}
			for j := range sessions[i].Transcript {
				m := &sessions[i].Transcript[j]
				if m.Sender == "" && !m.FromMe {
					m.Sender = m.Chat
				}
				m.SenderNumber, m.SenderName = dir.look(ctx, m.Sender)
			}
		}
	}
	if req.Limit > 0 && len(sessions) > req.Limit {
		sessions = sessions[:req.Limit]
	}
	return sessions, nil
}

// between picks the messages of one session out of the pile already read.
func between(msgs []Event, s Session) []Event {
	var out []Event
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Chat != s.Chat || m.Timestamp.Before(s.Started) || m.Timestamp.After(s.Ended) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// splitSessions cuts each chat's messages wherever the silence exceeded gap.
// Input comes newest first; output is newest session first.
func SplitSessions(msgs []Event, gap time.Duration) []Session {
	byChat := map[string][]Event{}
	order := []string{}
	for _, m := range msgs {
		if _, seen := byChat[m.Chat]; !seen {
			order = append(order, m.Chat)
		}
		byChat[m.Chat] = append(byChat[m.Chat], m)
	}

	var out []Session
	for _, chat := range order {
		msgs := byChat[chat]
		var cur *Session
		// walk oldest to newest so a session grows forwards in time
		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			if cur == nil || m.Timestamp.Sub(cur.Ended) > gap {
				if cur != nil {
					out = append(out, *cur)
				}
				cur = &Session{Chat: chat, Number: e164(chat), Started: m.Timestamp}
			}
			cur.Ended = m.Timestamp
			cur.Messages++
			if m.FromMe {
				cur.Outbound++
			} else {
				cur.Inbound++
			}
			cur.Last = m.Text
		}
		if cur != nil {
			out = append(out, *cur)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ended.After(out[j].Ended) })
	return out
}

// SyncChat asks the phone for older messages of one chat. WhatsApp answers with
// an ON_DEMAND history sync minutes later, so the result here is only the
// receipt of the request, never the messages.
func (w *WhatsApp) SyncChat(ctx context.Context, req SyncRequest) (SyncResult, error) {
	acc, account, err := w.pick(req.From)
	if err != nil {
		return SyncResult{}, err
	}
	chat, err := w.resolveChat(ctx, acc, req.Chat)
	if err != nil {
		return SyncResult{}, err
	}
	if !w.policy.Allow.Allowed(chat.String()) {
		return SyncResult{}, fmt.Errorf("%s is not followed: run `zap follow` first, zap asks the phone for no chat it may not listen to", chat.User)
	}
	anchor, err := w.oldestMessage(ctx, account, chat.String())
	if err != nil {
		return SyncResult{}, err
	}
	count := req.Count
	if count <= 0 {
		count = 50
	}
	sender, err := parseJID(firstNonEmpty(anchor.Sender, account))
	if err != nil {
		return SyncResult{}, err
	}
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: anchor.FromMe},
		ID:            anchor.ID,
		Timestamp:     anchor.Timestamp,
	}
	if _, err := acc.cli.SendPeerMessage(ctx, acc.cli.BuildHistorySyncRequest(info, count)); err != nil {
		return SyncResult{}, err
	}
	return SyncResult{Chat: chat.String(), Requested: count, Anchor: anchor.ID, AnchorAt: anchor.Timestamp}, nil
}

// oldestMessage is the anchor the phone needs: "older than this one".
func (w *WhatsApp) oldestMessage(ctx context.Context, account, chat string) (Event, error) {
	row := w.db.QueryRowContext(ctx,
		`SELECT id, sender, from_me, ts FROM messages WHERE account = ? AND chat = ? ORDER BY ts ASC LIMIT 1`,
		account, chat)
	ev := Event{Account: account, Chat: chat}
	var ts int64
	if err := row.Scan(&ev.ID, &ev.Sender, &ev.FromMe, &ts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Event{}, fmt.Errorf("no message stored for %s yet — the phone needs one to anchor the request", chat)
		}
		return Event{}, err
	}
	ev.Timestamp = time.Unix(ts, 0)
	return ev, nil
}

// resolveChat takes a number, a group name or a jid.
func (w *WhatsApp) resolveChat(ctx context.Context, acc *WhatsApp_Account, ref string) (types.JID, error) {
	if strings.HasSuffix(ref, "@g.us") || (!strings.Contains(ref, "@") && digits(ref) == "") {
		return w.resolveGroup(ctx, acc, ref)
	}
	return parseJID(ref)
}

// ------------------------------------------------------------------- groups

func (w *WhatsApp) Groups(ctx context.Context, from string) ([]Group, error) {
	acc, _, err := w.pick(from)
	if err != nil {
		return nil, err
	}
	infos, err := acc.cli.GetJoinedGroups(ctx)
	if err != nil {
		return nil, err
	}
	dir := w.directory(ctx, acc)
	out := make([]Group, 0, len(infos))
	for _, info := range infos {
		out = append(out, w.groupNamed(ctx, dir, info))
	}
	return out, nil
}

func (w *WhatsApp) GroupAct(ctx context.Context, req GroupRequest) (Group, error) {
	acc, _, err := w.pick(req.From)
	if err != nil {
		return Group{}, err
	}
	if req.Action == "create" {
		return w.groupCreate(ctx, acc, req)
	}
	jid, err := w.resolveGroup(ctx, acc, req.Group)
	if err != nil {
		return Group{}, err
	}
	switch req.Action {
	case "add", "remove", "promote", "demote":
		who, err := parseJIDs(req.Who)
		if err != nil {
			return Group{}, err
		}
		if len(who) == 0 {
			return Group{}, fmt.Errorf("%s needs --who", req.Action)
		}
		if _, err := acc.cli.UpdateGroupParticipants(ctx, jid, who, whatsmeow.ParticipantChange(req.Action)); err != nil {
			return Group{}, err
		}
	case "name":
		if err := acc.cli.SetGroupName(ctx, jid, req.Name); err != nil {
			return Group{}, err
		}
	case "topic":
		if err := acc.cli.SetGroupTopic(ctx, jid, "", "", req.Text); err != nil {
			return Group{}, err
		}
	case "leave":
		return Group{JID: jid.String()}, acc.cli.LeaveGroup(ctx, jid)
	case "link":
		link, err := acc.cli.GetGroupInviteLink(ctx, jid, req.Reset)
		if err != nil {
			return Group{}, err
		}
		return Group{JID: jid.String(), Link: link}, nil
	case "info":
	default:
		return Group{}, fmt.Errorf("unknown group action %q", req.Action)
	}
	info, err := acc.cli.GetGroupInfo(ctx, jid)
	if err != nil {
		return Group{}, err
	}
	return w.groupNamed(ctx, w.directory(ctx, acc), info), nil
}

func (w *WhatsApp) groupCreate(ctx context.Context, acc *WhatsApp_Account, req GroupRequest) (Group, error) {
	if req.Name == "" {
		return Group{}, fmt.Errorf("create needs a name")
	}
	who, err := parseJIDs(req.Who)
	if err != nil {
		return Group{}, err
	}
	info, err := acc.cli.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: req.Name, Participants: who})
	if err != nil {
		return Group{}, err
	}
	return w.groupNamed(ctx, w.directory(ctx, acc), info), nil
}

// resolveGroup accepts a JID or a group name, so you can type what you see in
// `zap groups` instead of copying an 18-digit id.
func (w *WhatsApp) resolveGroup(ctx context.Context, acc *WhatsApp_Account, ref string) (types.JID, error) {
	if ref == "" {
		return types.JID{}, fmt.Errorf("no group given")
	}
	if strings.Contains(ref, "@") {
		return types.ParseJID(ref)
	}
	infos, err := acc.cli.GetJoinedGroups(ctx)
	if err != nil {
		return types.JID{}, err
	}
	want := strings.ToLower(ref)
	var hits []*types.GroupInfo
	for _, info := range infos {
		name := strings.ToLower(info.Name)
		if name == want {
			return info.JID, nil
		}
		if strings.Contains(name, want) {
			hits = append(hits, info)
		}
	}
	switch len(hits) {
	case 0:
		if digits(ref) == ref {
			return types.NewJID(ref, types.GroupServer), nil
		}
		return types.JID{}, fmt.Errorf("no group matching %q", ref)
	case 1:
		return hits[0].JID, nil
	default:
		return types.JID{}, fmt.Errorf("%d groups match %q — use the jid", len(hits), ref)
	}
}

// groupNamed is groupOf with the participants run through the directory, so a
// member reads as "+5511… Fulano" instead of a @lid nobody can dial.
func (w *WhatsApp) groupNamed(ctx context.Context, dir *WhatsApp_Directory, info *types.GroupInfo) Group {
	g := groupOf(info)
	for i, p := range g.Participants {
		number, name := dir.look(ctx, p.JID)
		if number != "" {
			g.Participants[i].Number = number
		}
		g.Participants[i].Name = name
	}
	if number, name := dir.look(ctx, info.OwnerPN.String()); number != "" {
		g.Owner, g.OwnerName = number, name
	}
	return g
}

func groupOf(info *types.GroupInfo) Group {
	g := Group{
		JID:     info.JID.String(),
		Name:    info.Name,
		Topic:   info.Topic,
		Owner:   e164(firstNonEmpty(info.OwnerPN.String(), info.OwnerJID.String())),
		Created: info.GroupCreated,
	}
	for _, p := range info.Participants {
		jid := p.PhoneNumber
		if jid.IsEmpty() {
			jid = p.JID
		}
		g.Participants = append(g.Participants, GroupMember{
			Number: e164(jid.String()),
			JID:    p.JID.String(),
			Admin:  p.IsAdmin || p.IsSuperAdmin,
		})
	}
	return g
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" && v != "@s.whatsapp.net" {
			return v
		}
	}
	return ""
}

func parseJIDs(raw []string) ([]types.JID, error) {
	out := make([]types.JID, 0, len(raw))
	for _, r := range raw {
		if strings.TrimSpace(r) == "" {
			continue
		}
		jid, err := parseJID(r)
		if err != nil {
			return nil, err
		}
		out = append(out, jid)
	}
	return out, nil
}

func (w *WhatsApp) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, acc := range w.accounts {
		acc.cli.Disconnect()
	}
}

// ------------------------------------------------------------------- events

func (w *WhatsApp) onEvent(acc *WhatsApp_Account, raw any) {
	account := accountKey(acc.cli)
	ev := Event{Account: account, Timestamp: time.Now()}

	switch e := raw.(type) {
	case *events.Connected:
		acc.connected.Store(true)
		ev.Type = Events.Account.Connected
	case *events.Disconnected:
		acc.connected.Store(false)
		ev.Type = Events.Account.Disconnected
	case *events.LoggedOut:
		acc.connected.Store(false)
		ev.Type = Events.Account.LoggedOut

	case *events.Message:
		ev = w.onMessage(acc, e, ev)
		if ev.Type == "" {
			return
		}

	case *events.UndecryptableMessage:
		ev.Type = Events.Message.Undecryptable
		ev.Chat, ev.Sender, ev.ID = e.Info.Chat.String(), e.Info.Sender.String(), e.Info.ID

	case *events.Receipt:
		switch e.Type {
		case types.ReceiptTypeDelivered:
			ev.Type = Events.Message.Delivered
		case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
			ev.Type = Events.Message.Read
		default:
			return
		}
		ev.Chat, ev.Sender, ev.Timestamp = e.Chat.String(), e.Sender.String(), e.Timestamp
		if len(e.MessageIDs) > 0 {
			ev.Target = e.MessageIDs[0]
		}

	case *events.HistorySync:
		go w.absorb(account, e.Data)
		ev.Type = Events.History.Synced
		ev.Text = e.Data.GetSyncType().String()

	case *events.JoinedGroup:
		ev.Type, ev.Chat, ev.Text = Events.Group.Joined, e.JID.String(), e.Reason
	case *events.GroupInfo:
		ev = w.onGroupInfo(e, ev)
		if ev.Type == "" {
			return
		}

	case *events.Picture:
		ev.Type, ev.Chat, ev.Sender = Events.Contact.PictureChanged, e.JID.String(), e.Author.String()
		if e.JID.Server == types.GroupServer {
			ev.Type = Events.Group.PictureChanged
		}

	case *events.ChatPresence:
		if e.State != types.ChatPresenceComposing {
			return
		}
		ev.Type, ev.Chat, ev.Sender = Events.Chat.Typing, e.Chat.String(), e.Sender.String()

	case *events.Archive:
		ev.Type, ev.Chat = Events.Chat.Archived, e.JID.String()
	case *events.Pin:
		ev.Type, ev.Chat = Events.Chat.Pinned, e.JID.String()
	case *events.Mute:
		ev.Type, ev.Chat = Events.Chat.Muted, e.JID.String()
	case *events.MarkChatAsRead:
		ev.Type, ev.Chat = Events.Chat.MarkedRead, e.JID.String()
	case *events.DeleteChat:
		ev.Type, ev.Chat = Events.Chat.Deleted, e.JID.String()

	case *events.PushName:
		ev.Type, ev.Sender, ev.Text = Events.Contact.NameChanged, e.JID.String(), e.NewPushName
	case *events.BusinessName:
		ev.Type, ev.Sender, ev.Text = Events.Contact.NameChanged, e.JID.String(), e.NewBusinessName
	case *events.Blocklist:
		ev.Type = Events.Contact.Blocked
		for _, change := range e.Changes {
			ev.Members = append(ev.Members, change.JID.String())
		}

	case *events.CallOffer:
		ev.Type, ev.Chat, ev.Sender, ev.ID = Events.Call.Offered, e.From.String(), e.CallCreator.String(), e.CallID
	case *events.CallAccept:
		ev.Type, ev.Chat, ev.ID = Events.Call.Accepted, e.From.String(), e.CallID
	case *events.CallReject:
		ev.Type, ev.Chat, ev.ID = Events.Call.Rejected, e.From.String(), e.CallID
	case *events.CallTerminate:
		ev.Type, ev.Chat, ev.ID, ev.Text = Events.Call.Terminated, e.From.String(), e.CallID, e.Reason

	default:
		// nothing is invisible: what zap has not curated still reaches tail
		ev.Type = Raw(strings.TrimPrefix(fmt.Sprintf("%T", raw), "*events."))
	}
	w.publish(ev)
}

// publish is the only way an event leaves the process: through the policy, to
// the tail and to the backend. A chat that is not on the allowlist stops here.
func (w *WhatsApp) publish(ev Event) {
	if !w.policy.Admit(ev) {
		return
	}
	w.bus.Publish(ev)
	w.policy.Emit(ev)
}

// onMessage splits the one whatsmeow message event into the facts a consumer
// actually composes on: a reaction is not a message, and a deletion is not one
// either.
func (w *WhatsApp) onMessage(acc *WhatsApp_Account, e *events.Message, ev Event) Event {
	ev.Chat = e.Info.Chat.String()
	ev.Sender = e.Info.Sender.String()
	ev.FromMe = e.Info.IsFromMe
	ev.ID = e.Info.ID
	ev.Timestamp = e.Info.Timestamp

	// Before the door: a chat nobody allowed still leaves one trace, in memory
	// only, so a later follow can ask the phone for its past from here.
	w.fol.Observe(ev.Chat, Anchor{ID: ev.ID, FromMe: ev.FromMe, At: ev.Timestamp})
	if !w.policy.Admit(ev) {
		return Event{}
	}

	if r := e.Message.GetReactionMessage(); r != nil {
		ev.Type, ev.Emoji, ev.Target = Events.Message.Reacted, r.GetText(), r.GetKey().GetID()
		return ev
	}
	if p := e.Message.GetProtocolMessage(); p != nil && p.GetType() == waE2E.ProtocolMessage_REVOKE {
		ev.Type, ev.Target = Events.Message.Revoked, p.GetKey().GetID()
		return ev
	}
	if e.IsEdit {
		ev.Type, ev.Target, ev.Text = Events.Message.Edited, e.Info.ID, messageText(e.Message)
		return ev
	}

	ev.Text = messageText(e.Message)
	ev.Media = mediaKind(e.Message)
	ev.Type = Events.Message.Received
	if e.Info.IsFromMe {
		ev.Type = Events.Message.Sent
	}
	if audio := e.Message.GetAudioMessage(); audio != nil && w.audio != nil {
		go w.audio.enqueue(context.Background(), acc, e, audio)
	} else if ev.Media != "" {
		ev.Type = Events.Media.Received
	}
	w.remember(ev)
	w.noteSession(ev)
	w.fol.Resume(ev)
	return ev
}

// noteSession publishes session_opened when a chat speaks again after the gap.
// It runs only for live traffic: a history bundle would open thousands of
// sessions that all ended months ago.
func (w *WhatsApp) noteSession(ev Event) {
	gap := w.gap()
	var prev sql.NullInt64
	err := w.db.QueryRow(
		`SELECT max(ts) FROM messages WHERE account = ? AND chat = ? AND id != ?`,
		ev.Account, ev.Chat, ev.ID).Scan(&prev)
	if err != nil {
		return
	}
	if prev.Valid && ev.Timestamp.Sub(time.Unix(prev.Int64, 0)) <= gap {
		return
	}
	w.publish(Event{
		Account: ev.Account, Type: Events.Session.Opened,
		Chat: ev.Chat, ChatName: ev.ChatName, Timestamp: ev.Timestamp,
	})
}

// WatchSessions closes sessions that went quiet. Chats already silent when zap
// starts are primed as closed, so a restart does not replay months of endings.
func (w *WhatsApp) WatchSessions(ctx context.Context) {
	w.mu.Lock()
	w.closed = map[string]int64{}
	w.mu.Unlock()
	w.sweepSessions(ctx, false)

	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.sweepSessions(ctx, true)
		}
	}
}

func (w *WhatsApp) sweepSessions(ctx context.Context, publish bool) {
	cutoff := time.Now().Add(-w.gap()).Unix()
	rows, err := w.db.QueryContext(ctx,
		`SELECT account, chat, max(ts) FROM messages GROUP BY account, chat HAVING max(ts) < ?`, cutoff)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var account, chat string
		var last int64
		if rows.Scan(&account, &chat, &last) != nil {
			continue
		}
		w.mu.Lock()
		already := w.closed[chat] == last
		w.closed[chat] = last
		w.mu.Unlock()
		if already || !publish {
			continue
		}
		w.publish(Event{
			Account: account, Type: Events.Session.Closed,
			Chat: chat, Timestamp: time.Unix(last, 0),
		})
	}
}

func (w *WhatsApp) gap() time.Duration {
	if w.SessionGap > 0 {
		return w.SessionGap
	}
	return DefaultSessionGap
}

// onGroupInfo turns one "something changed" into the specific fact.
func (w *WhatsApp) onGroupInfo(e *events.GroupInfo, ev Event) Event {
	ev.Chat, ev.Timestamp = e.JID.String(), e.Timestamp
	if e.Sender != nil {
		ev.Sender = e.Sender.String()
	}
	names := func(jids []types.JID) []string {
		out := make([]string, 0, len(jids))
		for _, j := range jids {
			out = append(out, j.String())
		}
		return out
	}
	switch {
	case len(e.Join) > 0:
		ev.Type, ev.Members = Events.Group.MembersAdded, names(e.Join)
	case len(e.Leave) > 0:
		ev.Type, ev.Members = Events.Group.MembersRemoved, names(e.Leave)
	case len(e.Promote) > 0:
		ev.Type, ev.Members = Events.Group.AdminsPromoted, names(e.Promote)
	case len(e.Demote) > 0:
		ev.Type, ev.Members = Events.Group.AdminsDemoted, names(e.Demote)
	case e.Name != nil:
		ev.Type, ev.Text = Events.Group.Renamed, e.Name.Name
	case e.Topic != nil:
		ev.Type, ev.Text = Events.Group.TopicChanged, e.Topic.Topic
	case e.Locked != nil, e.Announce != nil, e.Ephemeral != nil:
		ev.Type = Events.Group.SettingsChanged
	}
	return ev
}

// mediaKind names what is attached, empty when it is plain text.
func mediaKind(m *waE2E.Message) string {
	switch {
	case m.GetImageMessage() != nil:
		return "image"
	case m.GetVideoMessage() != nil:
		return "video"
	case m.GetAudioMessage() != nil:
		return "audio"
	case m.GetDocumentMessage() != nil:
		return "document"
	case m.GetStickerMessage() != nil:
		return "sticker"
	}
	return ""
}

func messageText(m *waE2E.Message) string {
	if m == nil {
		return ""
	}
	if t := m.GetConversation(); t != "" {
		return t
	}
	if t := m.GetExtendedTextMessage().GetText(); t != "" {
		return t
	}
	return m.GetImageMessage().GetCaption() + m.GetVideoMessage().GetCaption()
}

// ------------------------------------------------------------------ helpers

// e164 is the number a human recognises. A @lid JID is an internal id, not a
// phone number, and printing it with a + in front invents a number nobody has.
func e164(jid string) string {
	// Only a real user JID is a phone number. A group, a broadcast list and a
	// @lid are ids, and printing them with a + invents a number nobody has.
	if !strings.Contains(jid, "@s.whatsapp.net") && strings.Contains(jid, "@") {
		return ""
	}
	if num := digits(jid); num != "" {
		return "+" + num
	}
	return ""
}

func accountKey(cli *whatsmeow.Client) string {
	if id := cli.Store.GetJID(); !id.IsEmpty() {
		return id.ToNonAD().String()
	}
	return fmt.Sprintf("unpaired-%p", cli)
}

var notDigits = regexp.MustCompile(`\D`)

func digits(s string) string {
	if at := strings.IndexByte(s, '@'); at >= 0 {
		s = s[:at]
	}
	return notDigits.ReplaceAllString(s, "")
}

func parseJID(raw string) (types.JID, error) {
	if strings.IndexByte(raw, '@') >= 0 {
		return types.ParseJID(raw)
	}
	num := digits(raw)
	if num == "" {
		return types.JID{}, fmt.Errorf("bad recipient %q", raw)
	}
	return types.NewJID(num, types.DefaultUserServer), nil
}
