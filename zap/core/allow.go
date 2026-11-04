package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"go.mau.fi/whatsmeow/types"
)

// Backfill states of one followed chat.
const (
	BackfillPending = "pending" // allowed, waiting for an anchor message to ask the phone from
	BackfillRunning = "running" // the phone was asked, pages are arriving
	BackfillDone    = "done"
	BackfillStalled = "stalled" // the phone did not answer; follow again to retry
	BackfillOff     = "off"     // followed without the past
)

// AllowEntry is one chat the owner let zap listen to.
type AllowEntry struct {
	JID string `toml:"jid"`
	// Alt holds the other ids the same chat arrives as: WhatsApp delivers one
	// person as a phone JID or as a @lid, and the allowlist must catch both.
	Alt        []string  `toml:"alt,omitempty"`
	Label      string    `toml:"label,omitempty"`
	Since      time.Time `toml:"since"`
	Backfill   string    `toml:"backfill"`
	Backfilled int       `toml:"backfilled,omitempty"` // messages brought in from the past
}

// Allowlist is the whole privacy model in one file: a chat that is not in it
// is never stored, never published and never reaches a hook. An empty or
// missing file therefore means zap listens to nothing.
//
// The file is re-read when it changes on disk, so an owner can edit it by hand
// while the daemon runs.
type Allowlist struct {
	path string

	mu    sync.Mutex
	chats []AllowEntry
	mtime time.Time
	size  int64
	keys  map[string]int // normalised jid -> index into chats
}

type allowFile struct {
	Chat []AllowEntry `toml:"chat"`
}

// OpenAllowlist loads path; a missing file is an empty list.
func OpenAllowlist(path string) (*Allowlist, error) {
	a := &Allowlist{path: path}
	if err := a.load(); err != nil {
		return nil, err
	}
	return a, nil
}

// NewMemoryAllowlist is an allowlist with no file, for a process that has
// nowhere to keep one.
func NewMemoryAllowlist() *Allowlist { return &Allowlist{keys: map[string]int{}} }

func (a *Allowlist) load() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.loadLocked()
}

func (a *Allowlist) loadLocked() error {
	if a.path == "" {
		return nil
	}
	st, err := os.Stat(a.path)
	if errors.Is(err, os.ErrNotExist) {
		a.chats, a.keys, a.mtime, a.size = nil, map[string]int{}, time.Time{}, 0
		return nil
	}
	if err != nil {
		return fmt.Errorf("allowlist: %w", err)
	}
	var f allowFile
	meta, err := toml.DecodeFile(a.path, &f)
	if err != nil {
		return fmt.Errorf("allowlist %s: %w", a.path, err)
	}
	if un := meta.Undecoded(); len(un) > 0 {
		return fmt.Errorf("allowlist %s: unknown key %s", a.path, un[0])
	}
	a.chats, a.mtime, a.size = f.Chat, st.ModTime(), st.Size()
	a.reindex()
	return nil
}

func (a *Allowlist) reindex() {
	a.keys = map[string]int{}
	for i, e := range a.chats {
		a.keys[NormJID(e.JID)] = i
		for _, alt := range e.Alt {
			a.keys[NormJID(alt)] = i
		}
	}
}

// refresh re-reads the file if somebody edited it. A bad edit keeps the list
// that was already loaded: a typo must never open or close the door by
// accident.
func (a *Allowlist) refresh() {
	if a.path == "" {
		return
	}
	st, err := os.Stat(a.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if len(a.chats) > 0 {
			a.chats, a.keys, a.mtime, a.size = nil, map[string]int{}, time.Time{}, 0
		}
	case err == nil && (!st.ModTime().Equal(a.mtime) || st.Size() != a.size):
		_ = a.loadLocked()
	}
}

// Allowed is the question every event asks before it goes anywhere.
func (a *Allowlist) Allowed(jid string) bool {
	if jid == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	_, ok := a.keys[NormJID(jid)]
	return ok
}

// Entry returns the entry a jid belongs to.
func (a *Allowlist) Entry(jid string) (AllowEntry, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	i, ok := a.keys[NormJID(jid)]
	if !ok {
		return AllowEntry{}, false
	}
	return a.chats[i], true
}

// List is a copy of the entries, oldest first.
func (a *Allowlist) List() []AllowEntry {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	out := make([]AllowEntry, len(a.chats))
	copy(out, a.chats)
	return out
}

// Add allows a chat. It reports whether the chat was new; an existing chat
// keeps its entry and only gains the alternative ids it did not have.
func (a *Allowlist) Add(e AllowEntry) (AllowEntry, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	if e.JID == "" {
		return e, false, errors.New("allowlist: empty jid")
	}
	if i, ok := a.keys[NormJID(e.JID)]; ok {
		cur := &a.chats[i]
		for _, alt := range e.Alt {
			if _, known := a.keys[NormJID(alt)]; !known {
				cur.Alt = append(cur.Alt, alt)
			}
		}
		if cur.Label == "" {
			cur.Label = e.Label
		}
		a.reindex()
		return *cur, false, a.saveLocked()
	}
	if e.Since.IsZero() {
		e.Since = time.Now().UTC().Truncate(time.Second)
	}
	if e.Backfill == "" {
		e.Backfill = BackfillPending
	}
	a.chats = append(a.chats, e)
	a.reindex()
	return e, true, a.saveLocked()
}

// Remove takes a chat out and returns what was there.
func (a *Allowlist) Remove(jid string) (AllowEntry, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	i, ok := a.keys[NormJID(jid)]
	if !ok {
		return AllowEntry{}, false, nil
	}
	gone := a.chats[i]
	a.chats = append(a.chats[:i], a.chats[i+1:]...)
	a.reindex()
	return gone, true, a.saveLocked()
}

// Update changes one entry in place and saves it.
func (a *Allowlist) Update(jid string, fn func(*AllowEntry)) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	i, ok := a.keys[NormJID(jid)]
	if !ok {
		return nil
	}
	fn(&a.chats[i])
	a.reindex()
	return a.saveLocked()
}

// saveLocked writes through a temp file and a rename, 0600: the file names the
// people the owner follows, and it must never be half-written or world-readable.
func (a *Allowlist) saveLocked() error {
	if a.path == "" {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteString("# The chats zap may listen to. zap follow / unfollow edit this file;\n" +
		"# a chat that is not here is never stored, never published.\n\n")
	if err := toml.NewEncoder(&buf).Encode(allowFile{Chat: a.chats}); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(a.path), ".allow-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), a.path); err != nil {
		return err
	}
	if st, err := os.Stat(a.path); err == nil {
		a.mtime, a.size = st.ModTime(), st.Size()
	}
	return nil
}

// NormJID makes the several spellings of one chat comparable: the device and
// agent suffix of a user id (`user:3@server`) is dropped, a group id is kept
// as it is, a bare number becomes a phone JID.
func NormJID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "@") {
		if d := digits(raw); d != "" {
			return d + "@" + types.DefaultUserServer
		}
		return raw
	}
	if j, err := types.ParseJID(raw); err == nil {
		return j.ToNonAD().String()
	}
	return raw
}

// Chats lists the normalised jids of everything allowed, for a purge.
func (a *Allowlist) keySet() map[string]bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refresh()
	out := make(map[string]bool, len(a.keys))
	for k := range a.keys {
		out[k] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
