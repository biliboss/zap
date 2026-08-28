package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Backend is what the daemon exposes over the socket. WhatsApp implements it;
// the tests implement it with a stub so the wire protocol is checkable without
// a paired phone.
type Backend interface {
	Accounts() []Account
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	Pair(ctx context.Context, emit func(PairEvent) error) error
	Logout(ctx context.Context, from string) error
	Typing(ctx context.Context, req TypingRequest) error
	Presence(ctx context.Context, from string, available bool) error
	Status(ctx context.Context, req SendRequest) (SendResult, error)
	Subscribe(from string) (<-chan Event, func())
	Messages(ctx context.Context, from, chat string, limit int) ([]Event, error)
	Sessions(ctx context.Context, req SessionQuery) ([]Session, error)
	SyncChat(ctx context.Context, req SyncRequest) (SyncResult, error)
	Contacts(ctx context.Context, from, query string) ([]Contact, error)
	QueueStats(ctx context.Context) ([]QueueStat, error)
	Retry(ctx context.Context) (int, error)
	Groups(ctx context.Context, from string) ([]Group, error)
	GroupAct(ctx context.Context, req GroupRequest) (Group, error)
}

// Account is one linked WhatsApp number held by the daemon.
type Account struct {
	Number    string `json:"number"`
	JID       string `json:"jid"`
	PushName  string `json:"push_name,omitempty"`
	Connected bool   `json:"connected"`
}

type SendRequest struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	Text string `json:"text"`
	// Myself sends to the sending account's own chat — the note-to-self one.
	Myself bool `json:"myself,omitempty"`
}

type SendResult struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	Timestamp time.Time `json:"timestamp"`
}

// PairEvent is one line of the pairing stream: a QR to scan, the resulting JID,
// or the reason it failed.
type PairEvent struct {
	QR    string `json:"qr,omitempty"`
	JID   string `json:"jid,omitempty"`
	Error string `json:"error,omitempty"`
}

// TypingRequest drives the "composing…" bubble on the other side.
type TypingRequest struct {
	From  string `json:"from,omitempty"`
	Chat  string `json:"chat"`
	On    bool   `json:"on"`
	Audio bool   `json:"audio,omitempty"` // "recording audio…" instead of typing
}

// Contact is a person the phone knows, with the number you can actually dial.
type Contact struct {
	Number   string `json:"number,omitempty"`
	JID      string `json:"jid"`
	Name     string `json:"name,omitempty"`
	PushName string `json:"push_name,omitempty"`
}

// Session is a stretch of conversation with one contact, cut wherever the two
// sides went quiet for longer than the gap. It is derived from the stored
// messages every time it is asked for — nothing to migrate when the gap changes.
type Session struct {
	Chat     string    `json:"chat"`
	Number   string    `json:"number,omitempty"`
	Name     string    `json:"name,omitempty"`
	Started  time.Time `json:"started"`
	Ended    time.Time `json:"ended"`
	Messages int       `json:"messages"`
	Inbound  int       `json:"inbound"`
	Outbound int       `json:"outbound"`
	Last     string    `json:"last,omitempty"`
	// Transcript is filled only when asked for: a session view is cheap, a
	// transcript of every session is the whole store.
	Transcript []Event `json:"transcript,omitempty"`
}

type SessionQuery struct {
	From  string
	Chat  string
	Gap   time.Duration
	Limit int
	// Mine keeps only the sessions you actually spoke in.
	Mine       bool
	Transcript bool
}

// SyncRequest asks the phone for messages older than the oldest one already
// stored for that chat. The answer does not come back here: it arrives minutes
// later as a history sync event and lands in the store on its own.
type SyncRequest struct {
	From  string `json:"from,omitempty"`
	Chat  string `json:"chat"`
	Count int    `json:"count,omitempty"`
}

type SyncResult struct {
	Chat      string    `json:"chat"`
	Requested int       `json:"requested"`
	Anchor    string    `json:"anchor"`
	AnchorAt  time.Time `json:"anchor_at"`
}

// Group is a WhatsApp group as the wire shows it.
type Group struct {
	JID          string        `json:"jid"`
	Name         string        `json:"name"`
	Topic        string        `json:"topic,omitempty"`
	Owner        string        `json:"owner,omitempty"`
	OwnerName    string        `json:"owner_name,omitempty"`
	Link         string        `json:"link,omitempty"`
	Participants []GroupMember `json:"participants,omitempty"`
	Created      time.Time     `json:"created,omitempty"`
}

type GroupMember struct {
	Number string `json:"number"`
	Name   string `json:"name,omitempty"`
	JID    string `json:"jid"`
	Admin  bool   `json:"admin,omitempty"`
}

// GroupRequest is every group mutation in one shape, because the CLI is a thin
// passthrough and a route per verb would be nine handlers saying the same thing.
type GroupRequest struct {
	From   string   `json:"from,omitempty"`
	Action string   `json:"action"` // create add remove promote demote name topic link leave info
	Group  string   `json:"group,omitempty"`
	Name   string   `json:"name,omitempty"`
	Text   string   `json:"text,omitempty"`
	Who    []string `json:"who,omitempty"`
	Reset  bool     `json:"reset,omitempty"`
}

// Daemon serves the Backend as JSON over HTTP on a unix socket, so any language
// with an HTTP client — and plain curl --unix-socket — can drive it.
type Daemon struct {
	Backend Backend
	Socket  string

	calls Bus[Call]
}

func (d *Daemon) Serve(ctx context.Context) error {
	ln, err := d.listen()
	if err != nil {
		return err
	}
	defer os.Remove(d.Socket)

	srv := &http.Server{Handler: d.routes()}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// listen removes a stale socket file, but never a live one: a second daemon
// silently stealing the path is how you end up with two processes holding the
// same number.
func (d *Daemon) listen() (net.Listener, error) {
	// The kernel caps sun_path at 104 bytes on darwin and 108 on linux, and
	// reports the overflow as a bare "invalid argument".
	if len(d.Socket) > 100 {
		return nil, fmt.Errorf("socket path is %d bytes, over the ~100 the kernel allows: %s", len(d.Socket), d.Socket)
	}
	if err := os.MkdirAll(filepath.Dir(d.Socket), 0o700); err != nil {
		return nil, err
	}
	if _, err := os.Stat(d.Socket); err == nil {
		if c, err := net.Dial("unix", d.Socket); err == nil {
			c.Close()
			return nil, fmt.Errorf("another zap is already serving %s", d.Socket)
		}
		os.Remove(d.Socket)
	}
	ln, err := net.Listen("unix", d.Socket)
	if err != nil {
		return nil, err
	}
	return ln, os.Chmod(d.Socket, 0o600)
}

func (d *Daemon) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /accounts", d.handleAccounts)
	mux.HandleFunc("POST /send", d.handleSend)
	mux.HandleFunc("POST /pair", d.handlePair)
	mux.HandleFunc("POST /logout", d.handleLogout)
	mux.HandleFunc("POST /typing", d.handleTyping)
	mux.HandleFunc("POST /presence", d.handlePresence)
	mux.HandleFunc("POST /status", d.handleStatus)
	mux.HandleFunc("GET /tail", d.handleTail)
	mux.HandleFunc("GET /messages", d.handleMessages)
	mux.HandleFunc("GET /sessions", d.handleSessions)
	mux.HandleFunc("POST /sync", d.handleSync)
	mux.HandleFunc("GET /queue", d.handleQueue)
	mux.HandleFunc("POST /queue/retry", d.handleQueueRetry)
	mux.HandleFunc("GET /contacts", d.handleContacts)
	mux.HandleFunc("GET /groups", d.handleGroups)
	mux.HandleFunc("POST /groups", d.handleGroupAct)
	mux.HandleFunc("GET /log", d.handleLog)
	return d.logging(mux)
}

// logging publishes every handled request. /log and /tail are excluded: a stream
// that logged its own reader would never fall silent.
func (d *Daemon) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/log" || r.URL.Path == "/tail" {
			next.ServeHTTP(w, r)
			return
		}
		var body any
		if r.Body != nil && r.Method != http.MethodGet {
			raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			r.Body = io.NopCloser(bytes.NewReader(raw))
			json.Unmarshal(raw, &body)
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		d.calls.Publish(Call{
			Time:    start,
			Method:  r.Method,
			Path:    r.URL.RequestURI(),
			Status:  rec.status,
			Millis:  time.Since(start).Milliseconds(),
			Request: body,
		})
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (d *Daemon) handleAccounts(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, d.Backend.Accounts())
}

func (d *Daemon) handleSend(w http.ResponseWriter, r *http.Request) {
	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := d.Backend.Send(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (d *Daemon) handlePair(w http.ResponseWriter, r *http.Request) {
	stream := newStream(w)
	err := d.Backend.Pair(r.Context(), func(ev PairEvent) error { return stream.send(ev) })
	if err != nil {
		stream.send(PairEvent{Error: err.Error()})
	}
}

func (d *Daemon) handleLogout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From string `json:"from,omitempty"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if err := d.Backend.Logout(r.Context(), req.From); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"logged_out": req.From})
}

func (d *Daemon) handleTyping(w http.ResponseWriter, r *http.Request) {
	var req TypingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := d.Backend.Typing(r.Context(), req); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"typing": req.On})
}

func (d *Daemon) handlePresence(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From      string `json:"from,omitempty"`
		Available bool   `json:"available"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if err := d.Backend.Presence(r.Context(), req.From, req.Available); err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"available": req.Available})
}

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := d.Backend.Status(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (d *Daemon) handleTail(w http.ResponseWriter, r *http.Request) {
	events, cancel := d.Backend.Subscribe(r.URL.Query().Get("from"))
	defer cancel()

	stream := newStream(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			if stream.send(ev) != nil {
				return
			}
		}
	}
}

func (d *Daemon) handleMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	msgs, err := d.Backend.Messages(r.Context(), q.Get("from"), q.Get("chat"), limit)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, msgs)
}

func (d *Daemon) handleSessions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	gap, err := time.ParseDuration(orDefault(q.Get("gap"), "24h"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	sessions, err := d.Backend.Sessions(r.Context(), SessionQuery{
		From: q.Get("from"), Chat: q.Get("chat"), Gap: gap, Limit: limit,
		Mine: q.Get("mine") == "1", Transcript: q.Get("transcript") == "1",
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func orDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func (d *Daemon) handleSync(w http.ResponseWriter, r *http.Request) {
	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := d.Backend.SyncChat(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (d *Daemon) handleQueue(w http.ResponseWriter, r *http.Request) {
	stats, err := d.Backend.QueueStats(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (d *Daemon) handleQueueRetry(w http.ResponseWriter, r *http.Request) {
	n, err := d.Backend.Retry(r.Context())
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"requeued": n})
}

func (d *Daemon) handleContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	contacts, err := d.Backend.Contacts(r.Context(), q.Get("from"), q.Get("q"))
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, contacts)
}

func (d *Daemon) handleGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := d.Backend.Groups(r.Context(), r.URL.Query().Get("from"))
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, groups)
}

func (d *Daemon) handleGroupAct(w http.ResponseWriter, r *http.Request) {
	var req GroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	group, err := d.Backend.GroupAct(r.Context(), req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, group)
}

func (d *Daemon) handleLog(w http.ResponseWriter, r *http.Request) {
	calls, cancel := d.calls.Subscribe(nil)
	defer cancel()

	stream := newStream(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case call := <-calls:
			if stream.send(call) != nil {
				return
			}
		}
	}
}

// ------------------------------------------------------------------- writing

// stream writes ndjson and flushes every line, so a `zap tail` pipe stays live
// instead of filling a buffer nobody drains.
type stream struct {
	w   http.ResponseWriter
	enc *json.Encoder
}

func newStream(w http.ResponseWriter) *stream {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return &stream{w: w, enc: json.NewEncoder(w)}
}

func (s *stream) send(v any) error {
	if err := s.enc.Encode(v); err != nil {
		return err
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
