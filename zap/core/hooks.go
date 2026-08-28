package core

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
)

// Hook runs one command for every event it matches. The caller (zap on) is
// what serialises calls to Run — one event at a time, in the order they
// arrive off /tail — so this type does not need its own queue.
//
// ponytail: the caller's queue, if it has one, should be an unbounded slice;
// a command that hangs forever grows it without limit. Add a max depth (drop
// oldest, log it) if a hook ever backs up in practice.
type Hook struct {
	Match   []EventType
	Command []string
}

// Matches reports whether ev should run this hook. "*" in Match means every
// event.
func (h *Hook) Matches(ev Event) bool {
	for _, m := range h.Match {
		if m == "*" || m == ev.Type {
			return true
		}
	}
	return false
}

// Run executes the command once for ev: the event as one ndjson line on
// stdin, and ZAP_EVENT/ZAP_CHAT/ZAP_SENDER/ZAP_TEXT/ZAP_ACCOUNT in the
// environment. It never goes through a shell. A non-zero exit is reported to
// stderr and returned as an error — the caller decides whether that stops
// the loop (it should not, for `zap on`).
func (h *Hook) Run(ctx context.Context, ev Event) error {
	line, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	cmd := exec.CommandContext(ctx, h.Command[0], h.Command[1:]...)
	cmd.Stdin = bytes.NewReader(line)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"ZAP_EVENT="+string(ev.Type),
		"ZAP_CHAT="+ev.Chat,
		"ZAP_SENDER="+ev.Sender,
		"ZAP_TEXT="+ev.Text,
		"ZAP_ACCOUNT="+ev.Account,
	)
	return cmd.Run()
}
