package core

import "time"

// FollowRequest allows one chat, fetches its past and keeps it synced.
type FollowRequest struct {
	From  string `json:"from,omitempty"`
	Chat  string `json:"chat"`
	Label string `json:"label,omitempty"`
	// Days and Max bound the backfill: stop at whichever comes first.
	Days int `json:"days,omitempty"`
	Max  int `json:"max,omitempty"`
	// NoBackfill follows from now on, without asking the phone for the past.
	NoBackfill bool `json:"no_backfill,omitempty"`
	// Again asks for the past a second time on a chat that already finished.
	Again bool `json:"again,omitempty"`
}

type FollowResult struct {
	Chat     string `json:"chat"`
	Label    string `json:"label,omitempty"`
	New      bool   `json:"new"`
	Backfill string `json:"backfill"`
	Error    string `json:"error,omitempty"`
}

// UnfollowRequest undoes a follow.
type UnfollowRequest struct {
	From string `json:"from,omitempty"`
	Chat string `json:"chat"`
}

type UnfollowResult struct {
	Chat    string `json:"chat"`
	Removed bool   `json:"removed"`
	Dropped int    `json:"dropped"`
}

// Follow is one followed chat and how its backfill stands.
type Follow struct {
	Chat       string    `json:"chat"`
	Label      string    `json:"label,omitempty"`
	Since      time.Time `json:"since"`
	Backfill   string    `json:"backfill"`
	Backfilled int       `json:"backfilled"`
}

// ChatInfo is a conversation the phone knows, with whether zap listens to it.
// It carries names and ids, never a message.
type ChatInfo struct {
	JID      string    `json:"jid"`
	Number   string    `json:"number,omitempty"`
	Name     string    `json:"name,omitempty"`
	Kind     string    `json:"kind"` // person | group
	Followed bool      `json:"followed"`
	Last     time.Time `json:"last,omitempty"`
}

type PurgeResult struct {
	Chats    int `json:"chats"`
	Messages int `json:"messages"`
}
