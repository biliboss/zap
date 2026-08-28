// Command zap drives one or more WhatsApp numbers through whatsmeow.
//
// One binary, two roles: `zap serve` holds every paired number alive in a
// single process and listens on a unix socket; every other verb is a thin
// client of that socket.
//
//	zap serve
//	zap pair
//	zap accounts
//	zap send --to +5511999999999 [--from +55…] "texto"
//	zap tail [--from +55…]
package main

import (
	"zap/core"

	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(ctx, os.Args[2:])
	case "pair":
		err = cmdPair(ctx, os.Args[2:])
	case "typing":
		err = cmdTyping(ctx, os.Args[2:])
	case "presence":
		err = cmdPresence(ctx, os.Args[2:])
	case "status":
		err = cmdStatus(ctx, os.Args[2:])
	case "logout":
		err = cmdLogout(ctx, os.Args[2:])
	case "accounts":
		err = cmdAccounts(ctx, os.Args[2:])
	case "send":
		err = cmdSend(ctx, os.Args[2:])
	case "tail":
		err = cmdTail(ctx, os.Args[2:])
	case "groups":
		err = cmdGroups(ctx, os.Args[2:])
	case "group":
		err = cmdGroup(ctx, os.Args[2:])
	case "log":
		err = cmdLog(ctx, os.Args[2:])
	case "use":
		err = cmdUse(ctx, os.Args[2:])
	case "queue":
		err = cmdQueue(ctx, os.Args[2:])
	case "events":
		err = cmdEvents()
	case "schema":
		err = cmdSchema()
	case "contacts":
		err = cmdContacts(ctx, os.Args[2:])
	case "messages":
		err = cmdMessages(ctx, os.Args[2:])
	case "sessions", "session":
		err = cmdSessions(ctx, os.Args[2:])
	case "sync":
		err = cmdSync(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "zap: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "zap: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `zap — WhatsApp from the terminal, many numbers in one process

  zap serve [--history-days N] [--transcribe]
                                  hold every paired number alive, listen on the socket
  zap queue [--retry]             voice notes waiting to be transcribed
  zap pair                        link a new number (QR in the terminal, redrawn in place)
  zap logout [--from <num>]       unlink a number, keeping its stored messages
  zap accounts                    list the numbers the daemon holds
  zap contacts [text]             the phone's address book: real number and name
  zap use [<num>]                 set (or print) the account --from defaults to
  zap schema                      json schema of every record zap emits
  zap events                      the event catalogue, in workflow order
  zap send --to <num>|--myself [--from <num>] [text]
  zap typing --to <num> [--off] [--audio]
                                  drive the "composing…" bubble
  zap presence [--away]           the online dot; --away lets the phone push again
  zap status <text>               post to status@broadcast, the 24h story
  zap tail [--from <num>]         stream incoming messages and receipts, ndjson
  zap messages [--chat <num>] [--limit N] [--plain-text]
                                  read what was stored; --plain-text is 'name: message'
  zap sessions [--number <num>] [--gap 24h] [--mine] [--count] [-n N]
               [--plain [--detail|-d]]
                                  conversations, cut where the chat went quiet
                                  --plain summarises each one; --detail prints it whole
  zap sync --chat <num> [--count N]
                                  ask the phone for messages older than the oldest stored
  zap log                         stream every request the daemon handles, ndjson

  zap groups                      list the groups you are in
  zap group info   <group>        who is in it, who is admin
  zap group create <name> --who +55…,+55…
  zap group add|remove|promote|demote <group> --who +55…
  zap group name   <group> <new name>
  zap group topic  <group> <text>
  zap group link   <group> [--reset]
  zap group leave  <group>

A <group> is its name — the one 'zap groups' prints — or its …@g.us jid.

examples
  zap send --to +5513988032053 "oi"
  zap send --myself "lembrete pra mim"
  cat aviso.txt | zap send --to +5513988032053
  zap accounts | jq -r .number
  zap tail | jq -r 'select(.type == "message_received") | "\(.sender_name): \(.text)"'
  zap log  | jq -r '"\(.status) \(.ms)ms \(.method) \(.path)"'
  zap groups | jq -r 'select(.participants | length > 50) | .name'
  zap sessions --number +5513988032053 --count
  zap sessions --mine --count
  zap messages --chat +5513991420971 --limit 40 --plain-text
  zap sessions --mine --plain -n 20
  zap sessions --gap 4h | jq -r 'select(.inbound == 0) | .number'

With one number paired, --from is optional. With several, either pass --from or
pick a default once with 'zap use +55…'; zap never guesses which number sends.

Numbers are given as +55…, or as a full JID when you have one. On a terminal zap
prints for a human; through a pipe it prints ndjson on stdout and nothing else,
so every verb composes. Exit 0 ok, 1 failed, 2 wrong usage.

Voice notes are downloaded the moment they arrive (the media url expires) and
queued. --transcribe runs mlx-whisper over that queue, one file at a time, and
the transcript replaces "[media]" in the message itself, so every verb reads it.
Needs mlx_whisper or uvx on PATH; --whisper-model picks the model.

History arrives in a bundle when a number is linked, and zap writes it into
store.db as it comes. How far back the phone goes is decided AT PAIRING TIME, so
'zap serve --history-days 365' only widens numbers paired after it.

Paths come from $ZAP_HOME (default ~/.zap): store.db and zap.sock.
`)
}

// ------------------------------------------------------------------ commands

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	db := fs.String("db", defaultDB(), "sqlite store path")
	histDays := fs.Int("history-days", 0, "days of history to ask the phone for, on numbers paired from now on")
	histPerChat := fs.Int("history-per-chat", 0, "max messages per chat in the initial sync")
	transcribe := fs.Bool("transcribe", false, "transcribe incoming voice notes with mlx-whisper")
	model := fs.String("whisper-model", core.DefaultWhisperModel, "mlx model to transcribe with")
	sessionGap := fs.Duration("session-gap", core.DefaultSessionGap, "silence that closes a session")
	mediaDir := fs.String("media", filepath.Join(zapHome(), "media"), "where voice notes are kept")
	fs.Parse(args)

	core.SetHistoryWindow(*histDays, *histPerChat)

	wa, err := core.OpenWhatsApp(ctx, *db)
	if err != nil {
		return err
	}
	defer wa.Close()

	wa.SessionGap = *sessionGap
	go wa.WatchSessions(ctx)

	if err := wa.StartTranscriber(ctx, &core.Transcriber{Enabled: *transcribe, Model: *model, Dir: *mediaDir}); err != nil {
		return err
	}

	d := &core.Daemon{Backend: wa, Socket: *sock}
	fmt.Fprintf(os.Stderr, "zap: %d account(s) on %s\n", len(wa.Accounts()), *sock)
	return d.Serve(ctx)
}

func cmdPair(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pair", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	fs.Parse(args)

	qr := core.NewQRPainter()
	return core.NewClient(*sock).Stream(ctx, "POST", "/pair", nil, func(line []byte) error {
		var ev core.PairEvent
		if err := core.Decode(line, &ev); err != nil {
			return err
		}
		switch {
		case ev.QR != "":
			qr.Paint(ev.QR)
		case ev.JID != "":
			fmt.Println(ev.JID)
		case ev.Error != "":
			return fmt.Errorf("%s", ev.Error)
		}
		return nil
	})
}

// cmdLogout unlinks a number. Re-pairing is the only way to make the phone ship
// its history again, so this is step one of widening the history window.
func cmdTyping(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("typing", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "sending account")
	to := fs.String("to", "", "chat to appear typing in")
	off := fs.Bool("off", false, "stop typing")
	audio := fs.Bool("audio", false, "show 'recording audio' instead")
	fs.Parse(args)

	if *to == "" {
		return fmt.Errorf("usage: zap typing --to <number> [--off] [--audio]")
	}
	req := core.TypingRequest{From: pickFrom(*from), Chat: *to, On: !*off, Audio: *audio}
	return core.NewClient(*sock).JSON(ctx, "POST", "/typing", req, nil)
}

func cmdPresence(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("presence", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account")
	away := fs.Bool("away", false, "appear offline (and let the phone push again)")
	fs.Parse(args)

	body := map[string]any{"from": pickFrom(*from), "available": !*away}
	return core.NewClient(*sock).JSON(ctx, "POST", "/presence", body, nil)
}

func cmdStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account")
	fs.Parse(args)

	text, err := textArg(fs.Args())
	if err != nil {
		return err
	}
	if text == "" {
		return fmt.Errorf("usage: zap status <text>   (or pipe it in)")
	}
	var res core.SendResult
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/status", core.SendRequest{From: pickFrom(*from), Text: text}, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	fmt.Fprintf(os.Stderr, "posted %s\n", res.ID)
	return nil
}

func cmdLogout(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("logout", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to unlink")
	fs.Parse(args)

	body := map[string]string{"from": pickFrom(*from)}
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/logout", body, nil); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "unlinked — stored messages kept, run `zap pair` to link again")
	return nil
}

func cmdAccounts(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("accounts", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	fs.Parse(args)

	var accs []core.Account
	if err := core.NewClient(*sock).JSON(ctx, "GET", "/accounts", nil, &accs); err != nil {
		return err
	}
	if piped() {
		for _, a := range accs {
			if err := emitJSON(a); err != nil {
				return err
			}
		}
		return nil
	}
	if len(accs) == 0 {
		fmt.Fprintln(os.Stderr, "no accounts — run `zap pair`")
		return nil
	}
	for _, a := range accs {
		state := "offline"
		if a.Connected {
			state = "online"
		}
		fmt.Printf("%-16s %-8s %s\n", a.Number, state, a.PushName)
	}
	return nil
}

func cmdSend(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	to := fs.String("to", "", "recipient number or JID")
	myself := fs.Bool("myself", false, "send to your own chat")
	from := fs.String("from", "", "sending account, required when several are paired")
	fs.Parse(args)

	text, err := textArg(fs.Args())
	if err != nil {
		return err
	}
	if (*to == "" && !*myself) || text == "" {
		return fmt.Errorf("usage: zap send --to <number>|--myself [--from <number>] <text>   (or pipe the text in)")
	}
	var res core.SendResult
	req := core.SendRequest{From: pickFrom(*from), To: *to, Text: text, Myself: *myself}
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/send", req, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	fmt.Fprintf(os.Stderr, "sent %s at %s\n", res.ID, res.Timestamp.Format(time.RFC3339))
	return nil
}

func cmdTail(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tail", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "only events of this account")
	fs.Parse(args)

	path := "/tail"
	if v := pickFrom(*from); v != "" {
		path += "?from=" + v
	}
	return core.NewClient(*sock).Stream(ctx, "GET", path, nil, func(line []byte) error {
		fmt.Println(string(line))
		return nil
	})
}

func cmdMessages(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("messages", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to read")
	chat := fs.String("chat", "", "only this chat (number or group jid)")
	limit := fs.Int("limit", 50, "how many, newest first")
	fs.IntVar(limit, "n", 50, "alias for --limit")
	plain := fs.Bool("plain-text", false, "transcript form: 'name: message', oldest first")
	fs.Parse(args)

	path := fmt.Sprintf("/messages?limit=%d", *limit)
	if v := pickFrom(*from); v != "" {
		path += "&from=" + v
	}
	if *chat != "" {
		path += "&chat=" + *chat
	}
	start := time.Now()
	var msgs []core.Event
	if err := core.NewClient(*sock).JSON(ctx, "GET", path, nil, &msgs); err != nil {
		return err
	}
	where := "all chats"
	if *chat != "" {
		where = *chat
	}
	caption(fmt.Sprintf("messages · %s · %d shown", where, len(msgs)), took(start))
	// --plain-text wins over the pipe rule: it was asked for explicitly, and it
	// is the shape you feed to a human or to a model.
	if *plain {
		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			who := firstNonEmptyStr(m.SenderName, m.SenderNumber, m.Sender)
			if m.FromMe {
				who = "me"
			}
			fmt.Printf("%s: %s\n", who, oneLine(m.Text))
		}
		return nil
	}
	if piped() {
		for _, m := range msgs {
			if err := emitJSON(m); err != nil {
				return err
			}
		}
		return nil
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		who := firstNonEmptyStr(m.SenderName, m.SenderNumber, m.Sender)
		if m.FromMe {
			who = "me"
		}
		where := ""
		if m.ChatName != "" {
			where = "[" + truncate(m.ChatName, 16) + "] "
		}
		fmt.Printf("%s  %s%-18s %s\n", m.Timestamp.Format("2006-01-02 15:04"), where, truncate(who, 18), m.Text)
	}
	return nil
}

// cmdSchema writes the JSON Schema of every record zap emits. `zap schema >
// schema.json` is how the file in the repo is kept honest.
// cmdEvents prints the catalogue: the shapes zap tail can hand you, in the
// order the system produces them.
func cmdEvents() error {
	if piped() {
		for _, t := range core.Events.All() {
			source, meaning := core.Events.Emits(t)
			if err := emitJSON(map[string]string{"type": string(t), "source": source, "meaning": meaning}); err != nil {
				return err
			}
		}
		return nil
	}
	caption("events · what zap tail can hand you")
	for _, t := range core.Events.All() {
		source, meaning := core.Events.Emits(t)
		fmt.Printf("%-14s %-8s %s\n", t, source, meaning)
	}
	return nil
}

func cmdQueue(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("queue", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	retry := fs.Bool("retry", false, "put failed and stuck transcriptions back in line")
	fs.Parse(args)

	cli := core.NewClient(*sock)
	if *retry {
		var res map[string]int
		if err := cli.JSON(ctx, "POST", "/queue/retry", nil, &res); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "requeued %d\n", res["requeued"])
		return nil
	}
	var stats []core.QueueStat
	if err := cli.JSON(ctx, "GET", "/queue", nil, &stats); err != nil {
		return err
	}
	if piped() {
		for _, s := range stats {
			if err := emitJSON(s); err != nil {
				return err
			}
		}
		return nil
	}
	if len(stats) == 0 {
		fmt.Fprintln(os.Stderr, "queue empty")
		return nil
	}
	for _, s := range stats {
		fmt.Printf("%-10s %d\n", s.State, s.Count)
	}
	return nil
}

func cmdSchema() error {
	raw, err := core.SchemaJSON()
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(raw)
	return err
}

func cmdContacts(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("contacts", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to read")
	fs.Parse(args)

	path := "/contacts"
	sep := "?"
	if v := pickFrom(*from); v != "" {
		path += sep + "from=" + v
		sep = "&"
	}
	if q := strings.Join(fs.Args(), " "); q != "" {
		path += sep + "q=" + url.QueryEscape(q)
	}
	var contacts []core.Contact
	if err := core.NewClient(*sock).JSON(ctx, "GET", path, nil, &contacts); err != nil {
		return err
	}
	if piped() {
		for _, c := range contacts {
			if err := emitJSON(c); err != nil {
				return err
			}
		}
		return nil
	}
	for _, c := range contacts {
		fmt.Printf("%-18s %s\n", c.Number, firstNonEmptyStr(c.Name, c.PushName))
	}
	return nil
}

// oneLine keeps a transcript grep-able: one message, one line.
func oneLine(text string) string {
	if text == "" {
		return "[media]"
	}
	return strings.Join(strings.Fields(text), " ")
}

// caption is the dim header a human reads before the data: what was asked and
// how long it took. It goes to stderr so a pipe still gets only records.
func caption(parts ...string) {
	line := strings.Join(parts, " · ")
	if core.IsTTY(os.Stderr) {
		fmt.Fprintf(os.Stderr, "\033[2m%s\033[0m\n", line)
		return
	}
	fmt.Fprintln(os.Stderr, line)
}

// shortDuration prints 24h instead of 24h0m0s.
func shortDuration(d time.Duration) string {
	s := strings.TrimSuffix(d.String(), "0s")
	return strings.TrimSuffix(s, "0m")
}

func took(start time.Time) string {
	d := time.Since(start)
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// kind marks a session at a glance: a group reads differently from a person,
// and the jid suffix is the only thing that says which.
func kind(chat string) string {
	if strings.HasSuffix(chat, "@g.us") {
		return "👥"
	}
	if strings.HasSuffix(chat, "@broadcast") {
		return "📢"
	}
	return "👤"
}

func firstNonEmptyStr(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func cmdSync(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to ask with")
	chat := fs.String("chat", "", "the chat to pull older messages for")
	count := fs.Int("count", 50, "how many older messages to ask for")
	fs.Parse(args)

	if *chat == "" {
		return fmt.Errorf("usage: zap sync --chat <number|group> [--count N]")
	}
	var res core.SyncResult
	req := core.SyncRequest{From: pickFrom(*from), Chat: *chat, Count: *count}
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/sync", req, &res); err != nil {
		return err
	}
	if piped() {
		return emitJSON(res)
	}
	fmt.Fprintf(os.Stderr, "asked for %d messages older than %s (%s)\nthey land in the store on their own — watch: zap tail | jq 'select(.type == \"history_sync\")'\n",
		res.Requested, res.AnchorAt.Format("2006-01-02 15:04"), res.Chat)
	return nil
}

func cmdSessions(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sessions", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to read")
	number := fs.String("number", "", "only this contact or group")
	gap := fs.Duration("gap", 24*time.Hour, "silence that starts a new session")
	limit := fs.Int("limit", 0, "how many, newest first")
	fs.IntVar(limit, "n", 0, "alias for --limit")
	count := fs.Bool("count", false, "print how many sessions, nothing else")
	plain := fs.Bool("plain", false, "print each session as text, separated by ----")
	detail := fs.Bool("detail", false, "with --plain, print the whole transcript instead of a summary")
	fs.BoolVar(detail, "d", false, "alias for --detail")
	mine := fs.Bool("mine", false, "only sessions you spoke in")
	fs.Parse(args)

	path := fmt.Sprintf("/sessions?gap=%s&limit=%d", *gap, *limit)
	if *mine {
		path += "&mine=1"
	}
	// the transcript is only worth fetching when it will be printed
	if *plain && *detail {
		path += "&transcript=1"
	}
	if v := pickFrom(*from); v != "" {
		path += "&from=" + v
	}
	if *number != "" {
		path += "&chat=" + *number
	}
	start := time.Now()
	var sessions []core.Session
	if err := core.NewClient(*sock).JSON(ctx, "GET", path, nil, &sessions); err != nil {
		return err
	}
	elapsed := took(start)
	if *count {
		fmt.Println(len(sessions))
		return nil
	}
	if *plain {
		caption(describeSessions(*mine, *number, *gap, len(sessions)), elapsed)
		for _, s := range sessions {
			fmt.Printf("# %s %s · %s → %s · %d msg (%d in / %d out)\n",
				kind(s.Chat), firstNonEmptyStr(s.Name, s.Number, s.Chat),
				s.Started.Format("2006-01-02 15:04"), s.Ended.Format("2006-01-02 15:04"),
				s.Messages, s.Inbound, s.Outbound)
			if !*detail {
				if s.Last != "" {
					fmt.Printf("last: %s\n", oneLine(s.Last))
				}
				fmt.Println("----")
				continue
			}
			for _, m := range s.Transcript {
				who := firstNonEmptyStr(m.SenderName, m.SenderNumber, m.Sender)
				if m.FromMe {
					who = "me"
				}
				fmt.Printf("%s: %s\n", who, oneLine(m.Text))
			}
			fmt.Println("----")
		}
		return nil
	}
	if piped() {
		for _, s := range sessions {
			if err := emitJSON(s); err != nil {
				return err
			}
		}
		return nil
	}
	caption(describeSessions(*mine, *number, *gap, len(sessions)), elapsed)
	var msgs, in, out int
	for _, s := range sessions {
		who := firstNonEmptyStr(s.Name, s.Number, s.Chat)
		fmt.Printf("%s → %s  %s %-24s %4d msg (%d in / %d out)\n",
			s.Started.Format("2006-01-02 15:04"), s.Ended.Format("01-02 15:04"),
			kind(s.Chat), truncate(who, 24), s.Messages, s.Inbound, s.Outbound)
		msgs, in, out = msgs+s.Messages, in+s.Inbound, out+s.Outbound
	}
	// the total goes to stderr so the listing above stays pipeable as data
	fmt.Fprintf(os.Stderr, "%d sessions · %d messages (%d in / %d out)\n", len(sessions), msgs, in, out)
	return nil
}

func describeSessions(mine bool, number string, gap time.Duration, n int) string {
	parts := []string{"sessions"}
	if mine {
		parts = append(parts, "mine")
	}
	if number != "" {
		parts = append(parts, number)
	}
	return strings.Join(append(parts, fmt.Sprintf("gap %s", shortDuration(gap)), fmt.Sprintf("%d shown", n)), " · ")
}

func cmdGroups(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("groups", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "account to ask")
	fs.Parse(args)

	path := "/groups"
	if v := pickFrom(*from); v != "" {
		path += "?from=" + v
	}
	var groups []core.Group
	if err := core.NewClient(*sock).JSON(ctx, "GET", path, nil, &groups); err != nil {
		return err
	}
	if piped() {
		for _, g := range groups {
			if err := emitJSON(g); err != nil {
				return err
			}
		}
		return nil
	}
	if len(groups) == 0 {
		fmt.Fprintln(os.Stderr, "no groups")
		return nil
	}
	for _, g := range groups {
		fmt.Printf("%-32s %4d  %s\n", truncate(g.Name, 32), len(g.Participants), g.JID)
	}
	return nil
}

func cmdGroup(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("group", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	from := fs.String("from", "", "sending account")
	who := fs.String("who", "", "comma separated numbers")
	reset := fs.Bool("reset", false, "link: revoke the old link")
	fs.Parse(args)

	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: zap group <create|info|add|remove|promote|demote|name|topic|link|leave> …")
	}
	req := core.GroupRequest{From: pickFrom(*from), Action: rest[0], Reset: *reset}
	if *who != "" {
		req.Who = strings.Split(*who, ",")
	}
	switch req.Action {
	case "create":
		req.Name = strings.Join(rest[1:], " ")
	case "name":
		if len(rest) < 3 {
			return fmt.Errorf("usage: zap group name <group> <new name>")
		}
		req.Group, req.Name = rest[1], strings.Join(rest[2:], " ")
	case "topic":
		if len(rest) < 2 {
			return fmt.Errorf("usage: zap group topic <group> <text>")
		}
		req.Group = rest[1]
		text, err := textArg(rest[2:])
		if err != nil {
			return err
		}
		req.Text = text
	default:
		if len(rest) < 2 {
			return fmt.Errorf("usage: zap group %s <group>", req.Action)
		}
		req.Group = rest[1]
	}

	var g core.Group
	if err := core.NewClient(*sock).JSON(ctx, "POST", "/groups", req, &g); err != nil {
		return err
	}
	if piped() {
		return emitJSON(g)
	}
	printGroup(g)
	return nil
}

func printGroup(g core.Group) {
	if g.Link != "" {
		fmt.Println(g.Link)
		return
	}
	fmt.Printf("%s  %s\n", g.Name, g.JID)
	if g.Topic != "" {
		fmt.Printf("  topic: %s\n", g.Topic)
	}
	for _, p := range g.Participants {
		mark := " "
		if p.Admin {
			mark = "*"
		}
		fmt.Printf("  %s %-16s %s\n", mark, p.Number, p.Name)
	}
}

// cmdUse writes the default account. It lives in a file next to the store
// instead of in the daemon: which number YOU send from is a property of your
// shell, not of the process holding every number.
func cmdUse(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("use", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	clear := fs.Bool("clear", false, "forget the default account")
	fs.Parse(args)

	switch {
	case *clear:
		return os.Remove(defaultAccountFile())
	case len(fs.Args()) == 0:
		if cur := defaultAccount(); cur != "" {
			fmt.Println(cur)
			return nil
		}
		fmt.Fprintln(os.Stderr, "no default account — zap use <number>")
		return nil
	}

	want := fs.Arg(0)
	var accs []core.Account
	if err := core.NewClient(*sock).JSON(ctx, "GET", "/accounts", nil, &accs); err != nil {
		return err
	}
	for _, a := range accs {
		if a.Number == want || a.JID == want || digitsOnly(a.Number) == digitsOnly(want) {
			if err := os.WriteFile(defaultAccountFile(), []byte(a.Number+"\n"), 0o600); err != nil {
				return err
			}
			fmt.Println(a.Number)
			return nil
		}
	}
	return fmt.Errorf("no paired account matching %q", want)
}

func cmdLog(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("log", flag.ExitOnError)
	sock := fs.String("socket", defaultSocket(), "unix socket path")
	fs.Parse(args)

	return core.NewClient(*sock).Stream(ctx, "GET", "/log", nil, func(line []byte) error {
		fmt.Println(string(line))
		return nil
	})
}

// pickFrom resolves which account acts: the flag wins, then `zap use`, then the
// daemon decides — and it only decides when exactly one number is paired.
func pickFrom(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	return defaultAccount()
}

func defaultAccountFile() string { return filepath.Join(zapHome(), "default") }

func defaultAccount() string {
	raw, err := os.ReadFile(defaultAccountFile())
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func digitsOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// ------------------------------------------------------------- unix manners

// piped reports whether stdout is a pipe or a file. On a terminal zap talks to a
// human; anywhere else it emits ndjson and nothing else, so `zap accounts | jq`
// never has to parse a table.
func piped() bool {
	info, err := os.Stdout.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice == 0
}

func emitJSON(record any) error {
	return json.NewEncoder(os.Stdout).Encode(record)
}

// textArg takes the message from the arguments, or from stdin when there are
// none, so `cat aviso.txt | zap send --to …` works like every other filter.
func textArg(args []string) (string, error) {
	if len(args) > 0 {
		return strings.Join(args, " "), nil
	}
	info, err := os.Stdin.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice != 0 {
		return "", nil
	}
	body, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\n"), nil
}

// --------------------------------------------------------------------- paths

func zapHome() string {
	if v := os.Getenv("ZAP_HOME"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".zap"
	}
	return filepath.Join(home, ".zap")
}

func defaultSocket() string { return filepath.Join(zapHome(), "zap.sock") }
func defaultDB() string     { return filepath.Join(zapHome(), "store.db") }
