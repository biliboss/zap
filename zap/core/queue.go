package core

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// Transcriber turns voice notes into text with mlx-whisper on Apple Silicon.
// zap execs the python entry point directly — no shell in the middle, so a
// filename with a space or a quote cannot become a command.
type Transcriber struct {
	Enabled bool
	Model   string
	Dir     string // where the voice notes land
	Poll    time.Duration

	bin  string
	args []string
	wa   *WhatsApp
}

// DefaultWhisperModel is multilingual on purpose: a Portuguese voice note
// through an English-only model comes back as confident nonsense, not an error.
const DefaultWhisperModel = "mlx-community/whisper-large-v3-turbo"

const transcriberSchema = `
CREATE TABLE IF NOT EXISTS audio_queue (
	account TEXT NOT NULL,
	chat    TEXT NOT NULL,
	id      TEXT NOT NULL,
	path    TEXT NOT NULL,
	seconds INTEGER,
	state   TEXT NOT NULL DEFAULT 'pending',
	error   TEXT,
	queued  INTEGER NOT NULL,
	PRIMARY KEY (account, chat, id)
);
CREATE INDEX IF NOT EXISTS audio_pending ON audio_queue (state, queued);`

// QueueStat is the one-glance answer to "is the queue keeping up".
type QueueStat struct {
	State string `json:"state"`
	Count int    `json:"count"`
}

func (t *Transcriber) start(ctx context.Context, wa *WhatsApp) error {
	t.wa = wa
	if _, err := wa.db.Exec(transcriberSchema); err != nil {
		return fmt.Errorf("create audio queue: %w", err)
	}
	if err := os.MkdirAll(t.Dir, 0o700); err != nil {
		return err
	}
	if !t.Enabled {
		return nil // audio is still downloaded and queued, just not transcribed
	}
	if t.Model == "" {
		t.Model = DefaultWhisperModel
	}
	if t.Poll <= 0 {
		t.Poll = 5 * time.Second
	}
	if err := t.findWhisper(); err != nil {
		return err
	}
	go t.work(ctx)
	return nil
}

// findWhisper prefers an installed mlx_whisper and falls back to uvx, which
// fetches it on first use. Resolving it at startup means a missing runtime is
// an error you read now, not a queue that fails one file at a time in silence.
func (t *Transcriber) findWhisper() error {
	if path, err := exec.LookPath("mlx_whisper"); err == nil {
		t.bin, t.args = path, nil
		return nil
	}
	path, err := exec.LookPath("uvx")
	if err != nil {
		return fmt.Errorf("neither mlx_whisper nor uvx found — install one to transcribe")
	}
	t.bin, t.args = path, []string{"--from", "mlx-whisper", "mlx_whisper"}
	return nil
}

// enqueue downloads a voice note and parks it. Download happens here, on the
// event path, because the media URL expires — the transcription can wait, the
// bytes cannot.
func (t *Transcriber) enqueue(ctx context.Context, acc *WhatsApp_Account, evt *events.Message, audio *waE2E.AudioMessage) {
	if t == nil || t.wa == nil {
		return
	}
	data, err := acc.cli.Download(ctx, audio)
	if err != nil {
		fmt.Fprintf(os.Stderr, "zap: download audio %s: %v\n", evt.Info.ID, err)
		return
	}
	path := filepath.Join(t.Dir, evt.Info.ID+extensionFor(audio.GetMimetype()))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "zap: write audio: %v\n", err)
		return
	}
	_, err = t.wa.db.Exec(
		`INSERT OR IGNORE INTO audio_queue (account, chat, id, path, seconds, queued) VALUES (?, ?, ?, ?, ?, ?)`,
		accountKey(acc.cli), evt.Info.Chat.String(), evt.Info.ID, path, audio.GetSeconds(), time.Now().Unix())
	if err != nil {
		fmt.Fprintf(os.Stderr, "zap: queue audio: %v\n", err)
		return
	}
	t.wa.publish(Event{
		Account: accountKey(acc.cli), Type: Events.Audio.Queued,
		Chat: evt.Info.Chat.String(), ID: evt.Info.ID, Timestamp: time.Now(),
	})
}

// work runs one transcription at a time.
// ponytail: single worker, parallelise only if the queue starts backing up —
// four whisper processes on a 16 GB laptop is how you swap the machine.
func (t *Transcriber) work(ctx context.Context) {
	tick := time.NewTicker(t.Poll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			for t.next(ctx) {
			}
		}
	}
}

func (t *Transcriber) next(ctx context.Context) bool {
	var account, chat, id, path string
	row := t.wa.db.QueryRowContext(ctx,
		`SELECT account, chat, id, path FROM audio_queue WHERE state = 'pending' ORDER BY queued ASC LIMIT 1`)
	if err := row.Scan(&account, &chat, &id, &path); err != nil {
		return false
	}
	t.wa.db.Exec(`UPDATE audio_queue SET state = 'running' WHERE account = ? AND chat = ? AND id = ?`, account, chat, id)

	text, err := t.run(ctx, path)
	if err != nil {
		t.wa.db.Exec(`UPDATE audio_queue SET state = 'failed', error = ? WHERE account = ? AND chat = ? AND id = ?`,
			err.Error(), account, chat, id)
		return true
	}
	t.wa.db.Exec(`UPDATE audio_queue SET state = 'done', error = NULL WHERE account = ? AND chat = ? AND id = ?`, account, chat, id)
	t.wa.db.Exec(`UPDATE messages SET text = ? WHERE account = ? AND chat = ? AND id = ?`, text, account, chat, id)
	t.wa.publish(Event{
		Account: account, Type: Events.Audio.Transcribed, Chat: chat, ID: id,
		Text: text, Timestamp: time.Now(),
	})
	return true
}

// run transcribes one file. mlx_whisper writes its result into a directory
// instead of stdout, so a temp dir is the channel.
func (t *Transcriber) run(ctx context.Context, path string) (string, error) {
	run, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	out, err := os.MkdirTemp("", "zap-whisper-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(out)

	args := append(append([]string{}, t.args...),
		"--model", t.Model,
		"--output-format", "txt",
		"--output-dir", out,
		path)
	cmd := exec.CommandContext(run, t.bin, args...)
	if logs, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("mlx_whisper: %w: %s", err, lastLine(string(logs)))
	}
	files, err := filepath.Glob(filepath.Join(out, "*.txt"))
	if err != nil || len(files) == 0 {
		return "", fmt.Errorf("mlx_whisper wrote no transcript")
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "", fmt.Errorf("empty transcript")
	}
	return text, nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

func (w *WhatsApp) QueueStats(ctx context.Context) ([]QueueStat, error) {
	rows, err := w.db.QueryContext(ctx,
		`SELECT state, count(*) FROM audio_queue GROUP BY state ORDER BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []QueueStat
	for rows.Next() {
		var s QueueStat
		if err := rows.Scan(&s.State, &s.Count); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Retry puts failed transcriptions back in line, for when the model was simply
// not installed yet.
func (w *WhatsApp) Retry(ctx context.Context) (int, error) {
	res, err := w.db.ExecContext(ctx, `UPDATE audio_queue SET state = 'pending', error = NULL WHERE state IN ('failed', 'running')`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func extensionFor(mime string) string {
	switch {
	case strings.Contains(mime, "ogg"):
		return ".ogg"
	case strings.Contains(mime, "mp4"), strings.Contains(mime, "m4a"):
		return ".m4a"
	case strings.Contains(mime, "mpeg"):
		return ".mp3"
	default:
		return ".bin"
	}
}
