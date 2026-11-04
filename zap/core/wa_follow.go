package core

import (
	"context"
	"fmt"
	"sort"

	"go.mau.fi/whatsmeow/types"
)

// This file is the WhatsApp side of following: how a reference becomes a chat,
// and how the phone is asked for the past. The rules themselves (the door, the
// backfill loop) live in follow.go and are shared with the fake.

func (w *WhatsApp) Follow(ctx context.Context, req FollowRequest) (FollowResult, error) {
	return w.fol.Follow(ctx, req)
}

func (w *WhatsApp) Unfollow(ctx context.Context, req UnfollowRequest) (UnfollowResult, error) {
	return w.fol.Unfollow(ctx, req)
}

func (w *WhatsApp) Follows(ctx context.Context) ([]Follow, error) {
	return w.fol.Follows(), nil
}

// Stray counts the stored chats that are not on the allowlist: what an older
// zap kept before the allowlist existed. `zap purge` deletes them.
func (w *WhatsApp) Stray() int {
	keep := w.policy.Allow.keySet()
	n := 0
	for _, chat := range w.Stored() {
		if !keep[NormJID(chat)] {
			n++
		}
	}
	return n
}

func (w *WhatsApp) Purge(ctx context.Context) (PurgeResult, error) {
	return w.fol.Purge()
}

// Resolve turns a number, a jid or a group name into the chat, with the other
// id the same person arrives as.
func (w *WhatsApp) Resolve(ctx context.Context, from, ref string) (Resolved, error) {
	acc, account, err := w.pick(from)
	if err != nil {
		return Resolved{}, err
	}
	chat, err := w.resolveChat(ctx, acc, ref)
	if err != nil {
		return Resolved{}, err
	}
	chat = chat.ToNonAD()
	out := Resolved{Account: account, JID: chat.String()}
	_, out.Label = w.directory(ctx, acc).chatName(ctx, out.JID)

	switch chat.Server {
	case types.DefaultUserServer:
		if lid, err := acc.cli.Store.LIDs.GetLIDForPN(ctx, chat); err == nil && !lid.IsEmpty() {
			out.Alt = append(out.Alt, lid.ToNonAD().String())
		}
	case types.HiddenUserServer:
		if pn, err := acc.cli.Store.LIDs.GetPNForLID(ctx, chat); err == nil && !pn.IsEmpty() {
			out.Alt = append(out.Alt, pn.ToNonAD().String())
		}
	}
	return out, nil
}

// Ask sends the on-demand history request: up to count messages before the
// anchor, from the primary device. The answer arrives later as a history sync
// of type ON_DEMAND (see absorb).
func (w *WhatsApp) Ask(ctx context.Context, account, chat string, at Anchor, count int) error {
	acc, _, err := w.pick(account)
	if err != nil {
		return err
	}
	jid, err := types.ParseJID(chat)
	if err != nil {
		return fmt.Errorf("bad chat id: %w", err)
	}
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: jid, IsFromMe: at.FromMe},
		ID:            at.ID,
		Timestamp:     at.At,
	}
	_, err = acc.cli.SendPeerMessage(ctx, acc.cli.BuildHistorySyncRequest(info, count))
	return err
}

// Chats lists the conversations zap knows by name, and the ones it has seen
// speak in this run. Names, ids and a time: never a message.
func (w *WhatsApp) Chats(ctx context.Context, from string) ([]ChatInfo, error) {
	byJID := map[string]*ChatInfo{}
	add := func(jid, number, name, kind string) *ChatInfo {
		k := NormJID(jid)
		c, ok := byJID[k]
		if !ok {
			c = &ChatInfo{JID: jid, Kind: kind}
			byJID[k] = c
		}
		if c.Number == "" {
			c.Number = number
		}
		if c.Name == "" {
			c.Name = name
		}
		return c
	}
	contacts, err := w.Contacts(ctx, from, "")
	if err != nil {
		return nil, err
	}
	for _, c := range contacts {
		add(c.JID, c.Number, firstNonEmpty(c.Name, c.PushName), "person")
	}
	groups, err := w.Groups(ctx, from)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		add(g.JID, "", g.Name, "group")
	}
	for jid, at := range w.fol.Seen() {
		kind := "person"
		if j, err := types.ParseJID(jid); err == nil && j.Server == types.GroupServer {
			kind = "group"
		}
		c := add(jid, e164(jid), "", kind)
		if at.After(c.Last) {
			c.Last = at
		}
	}
	return finishChats(byJID, w.policy.Allow), nil
}

// finishChats marks who is followed and sorts: followed first, then by recency,
// then by name.
func finishChats(byJID map[string]*ChatInfo, allow *Allowlist) []ChatInfo {
	out := make([]ChatInfo, 0, len(byJID))
	for _, c := range byJID {
		c.Followed = allow.Allowed(c.JID)
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Followed != out[j].Followed {
			return out[i].Followed
		}
		if !out[i].Last.Equal(out[j].Last) {
			return out[i].Last.After(out[j].Last)
		}
		return firstNonEmpty(out[i].Name, out[i].JID) < firstNonEmpty(out[j].Name, out[j].JID)
	})
	return out
}
