package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"zap/core"
)

// cmdFollow allows one chat, asks the phone for its past and keeps it synced.
// It is the one verb that opens the door: until it runs, zap listens to nothing.
func cmdFollow(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("follow", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account that follows")
	label := fs.String("label", "", "a name for the chat, to find it again")
	days := fs.Int("days", core.DefaultBackfillDays, "how far back to ask the phone for the past")
	max := fs.Int("max", core.DefaultBackfillMax, "at most this many past messages")
	none := fs.Bool("no-backfill", false, "follow from now on, without the past")
	again := fs.Bool("again", false, "ask for the past again on a chat that already has it")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) == 0 {
		return fmt.Errorf("usage: zap follow <number|group|jid> [--label X] [--days N] [--max N] [--no-backfill]")
	}
	req := core.FollowRequest{
		From: pickFrom(*from), Chat: strings.Join(positional, " "), Label: *label,
		Days: *days, Max: *max, NoBackfill: *none, Again: *again,
	}
	var res core.FollowResult
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/follow", req, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	verb := "already following"
	if res.New {
		verb = "now following"
	}
	fmt.Printf("%s %s — backfill %s\n", verb, firstNonEmptyStr(res.Label, res.Chat), res.Backfill)
	if res.Error != "" {
		fmt.Fprintf(os.Stderr, "backfill could not start: %s\n", res.Error)
	}
	return nil
}

// cmdUnfollow undoes follow: off the list, backfill stopped, stored messages
// deleted.
func cmdUnfollow(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("unfollow", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account that follows")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(positional) == 0 {
		return fmt.Errorf("usage: zap unfollow <number|label|jid>")
	}
	var res core.UnfollowResult
	req := core.UnfollowRequest{From: pickFrom(*from), Chat: strings.Join(positional, " ")}
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/unfollow", req, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	fmt.Printf("unfollowed %s — %d stored message(s) deleted\n", res.Chat, res.Dropped)
	return nil
}

// cmdFollows lists who is followed and how each backfill stands.
func cmdFollows(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("follows", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	fs.Parse(args)

	var list []core.Follow
	if err := core.NewClient(*sock).JSON(ctx, "GET", "/follows", nil, &list); err != nil {
		return err
	}
	if piped() {
		for _, f := range list {
			if err := emitJSON(f); err != nil {
				return err
			}
		}
		return nil
	}
	if len(list) == 0 {
		fmt.Fprintln(os.Stderr, "nobody is followed: zap listens to nothing")
		return nil
	}
	for _, f := range list {
		fmt.Printf("%-10s %-24s %-30s %d past, since %s\n",
			f.Backfill, firstNonEmptyStr(f.Label, "-"), f.Chat, f.Backfilled, f.Since.Format("2006-01-02"))
	}
	return nil
}

// cmdChats lists the conversations the phone knows by name, with who is
// followed. Names and ids only, never a message.
func cmdChats(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("chats", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to read")
	positional, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	path := "/chats"
	if v := pickFrom(*from); v != "" {
		path += "?from=" + url.QueryEscape(v)
	}
	var list []core.ChatInfo
	if err := core.NewClient(*sock).JSON(ctx, "GET", path, nil, &list); err != nil {
		return err
	}
	want := strings.ToLower(strings.Join(positional, " "))
	var shown []core.ChatInfo
	for _, c := range list {
		if want == "" || strings.Contains(strings.ToLower(c.Name+" "+c.Number+" "+c.JID), want) {
			shown = append(shown, c)
		}
	}
	if piped() {
		for _, c := range shown {
			if err := emitJSON(c); err != nil {
				return err
			}
		}
		return nil
	}
	for _, c := range shown {
		mark := " "
		if c.Followed {
			mark = "*"
		}
		last := ""
		if !c.Last.IsZero() {
			last = c.Last.Format(time.DateTime)
		}
		fmt.Printf("%s %-7s %-18s %-28s %s\n", mark, c.Kind, c.Number, firstNonEmptyStr(c.Name, "-"), last)
	}
	return nil
}

// cmdPurge deletes what an older zap stored for chats nobody allowed.
func cmdPurge(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	fs.Parse(args)

	var res core.PurgeResult
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/purge", nil, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	fmt.Printf("deleted %d stored message(s) of %d chat(s) that are not followed\n", res.Messages, res.Chats)
	return nil
}
