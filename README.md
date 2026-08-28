# zap

WhatsApp as unix primitives. One Go process holds every paired number and
serves them over a unix socket; every verb is a thin client of that socket.

```
zap serve                       # holds the numbers, listens on ~/.zap/zap.sock
zap pair                        # QR in the terminal, redrawn in place
zap send --to +55… "oi"
zap tail | jq -r 'select(.type == "message_received") | "\(.sender_name): \(.text)"'
zap on message_received -- ./answer.sh
```

On a terminal it prints for a human; through a pipe it prints ndjson on stdout
and nothing else, so every verb composes. Exit 0 ok, 1 failed, 2 wrong usage.

## Why one process

Measured, not assumed — the [benchmark](bench/) holds N whatsmeow clients two
ways and samples RSS, CPU, goroutines and spin-up:

```
topo    mode  clients  procs      RSS    per-client   goroutines  spinup
inproc  ws       1000      1   100.3 MB    103 KB           2002   202ms
proc    ws        100    100  1595.6 MB  16339 KB            400   967ms
proc    ws       1000   1000   SKIPPED: projected 12.4 GB > 10 GB budget
```

**87 KB and two goroutines per number in one process, against 15.9 MB per
number as a process** — a factor of 180. A thousand numbers is 100 MB and 0%
idle CPU; the same thousand as processes projects 12.4 GB, which on a 16 GB
machine measures swap instead of WhatsApp.

## The socket is the API

No protocol was invented: `net.Listen("unix", …)` plus `net/http`, so a
consumer in any language — or plain curl — already speaks it.

```
curl --unix-socket ~/.zap/zap.sock http://zap/accounts
```

Streams (`/tail`, `/log`, `/pair`) are ndjson with a flush per line.
[`schema.json`](zap/schema.json) is reflected from the Go structs, so it cannot
drift: a test fails when a model changes and the schema does not.

## Events

Facts are named EventStorming style — subject plus a past-tense verb — because
`connected` is a state you argue about and `account_connected` is a thing that
happened at a time. `zap events` prints the catalogue from the binary.

```
account_connected · account_disconnected · account_logged_out
message_received · message_sent · message_delivered · message_read
message_reacted · message_edited · message_revoked · message_undecryptable
media_received · audio_queued · audio_transcribed · history_synced
group_joined · group_members_added · group_admins_promoted · group_renamed …
chat_archived · chat_typing · contact_name_changed · call_offered …
session_opened · session_closed          ← ours, not whatsmeow's
```

Anything whatsmeow emits that zap has not curated still reaches `zap tail` as
`wa_<TypeName>`. Nothing is invisible.

## Sessions

A session is a stretch of conversation cut wherever the chat went quiet longer
than a gap (24h by default). It is a **view derived from the stored messages**,
not a table — `--gap 4h` and `--gap 24h` read the same rows and answer
differently, with nothing to migrate.

```
zap sessions --mine --plain -n 20
zap sessions --number +55… --count
```

## Voice notes

whatsmeow keeps sessions, never message bodies, so zap stores what arrives in
sqlite. A voice note is downloaded on the event path — the media URL expires,
the transcription can wait — queued, and run through mlx-whisper on Apple
Silicon, one at a time. The transcript replaces `[media]` in the message
itself, so every verb reads it.

## Layout

```
whatsapp/
 ├ bench/     the footprint measurement above
 ├ zap/       main.go (CLI) + core/ (library)
 └ tests/     its own module: tests import zap, zap cannot import tests
```

Each directory carries a `CONTEXT.md` with the rules that hold there and the
failures they prevent.

## Requirements

Go 1.26+, and `mlx_whisper` or `uvx` on PATH if you want transcription.

## Status

Working against a real account: pairing, sending, groups, contacts, history
sync, sessions and transcription are all exercised end to end. The on-demand
history pull (`zap sync`) is built but unverified against a real chat.

Unofficial client — WhatsApp does not support this, and using it carries a ban
risk that is yours to accept.
