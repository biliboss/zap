---
name: zap
description: Drive WhatsApp from the shell with the `zap` CLI — send and read messages, list contacts and groups, stream events as ndjson, run a command per event, read the local message store, derive conversation sessions, and transcribe voice notes. Use whenever the task touches WhatsApp on this machine: "manda no whatsapp", "lê a conversa com X", "quem me mandou mensagem", "transcreve o áudio", "quantas sessões", "exporta o histórico", "roda algo quando chegar mensagem", "zap".
---

# zap

**One process holds every paired number and serves them over a unix socket;
every verb is a thin client of it.** `zap serve` must be running or every other
verb answers `no daemon on ~/.zap/zap.sock`.

## The rule that governs every output

**On a terminal zap prints for a human; through a pipe it prints ndjson on
stdout and nothing else.** Captions, totals, progress and errors always go to
stderr, so `zap x | jq` never has to parse decoration.

```sh
zap accounts                 # +5511900000000  online  Gabriel
zap accounts | jq -r .number # +5511900000000
```

Exit 0 ok, 1 failed, 2 wrong usage. Numbers are given as `+55…` or as a full
JID. With one number paired `--from` is optional; with several, pass `--from`
or pick a default once with `zap use +55…` — zap never guesses which number
sends.

## The verbs

| verb | what it does |
| --- | --- |
| `zap serve [--transcribe] [--history-days N]` | holds the numbers, listens on the socket |
| `zap pair` · `zap logout` | link a number (QR redrawn in place) · unlink it |
| `zap accounts` · `zap use <num>` | who is linked · which one acts by default |
| `zap send --to <num>\|--myself [text]` | text from the argument or from stdin |
| `zap typing --to <num> [--off]` · `zap presence [--away]` · `zap status <text>` | the bubble · the dot · the 24h story |
| `zap contacts [busca]` · `zap groups` · `zap group <ação> <grupo>` | the address book · the groups · info/create/add/remove/name/topic/link/leave |
| `zap messages [--chat X] [-n N] [--plain-text]` | what is stored, newest first |
| `zap sessions [--mine] [--gap 24h] [--count] [--plain [-d]]` | conversations cut by silence |
| `zap tail` · `zap log` · `zap on <evento> -- cmd` | facts · requests · react to facts |
| `zap events` · `zap schema` · `zap queue [--retry]` | the catalogue · the json schema · the audio queue |

`zap -h` is the authority; this table is the map.

## Recipes

**Copy the block for the scenario you are in.** Every one of these is a whole
answer — no flags left as an exercise.

Answer someone who just wrote, with the context of what was said before:

```sh
zap messages --chat +5511900000000 -n 30 --plain-text     # read first
zap typing  --to  +5511900000000                          # while you think
zap send    --to  +5511900000000 --id "reply-<inbound_id>" "sua resposta"
zap typing  --to  +5511900000000 --off
```

Find the person before you write to them:

```sh
zap contacts fonseca                     # name, push name or number
zap contacts | jq -r 'select(.name|test("(?i)thais")) | .number'
```

Catch up on everything that happened while you were away — this is the path
that never loses a message, because the store is written before the event:

```sh
zap messages --since 2h --limit 500 | jq -c 'select(.from_me|not)'
zap messages --since 1787930000 --limit 500      # or a unix cursor
```

React to a message as it arrives, without polling:

```sh
zap on message_received -- ./handle.sh   # event as ndjson on stdin
                                         # plus ZAP_CHAT ZAP_SENDER ZAP_TEXT
zap tail | jq -r 'select(.type=="message_received") | "\(.sender_name): \(.text)"'
```

Work with a group by its name, never by its id:

```sh
zap groups | jq -r '.name'
zap group info "A Grande Família"
zap send --to "$(zap groups | jq -r 'select(.name=="Obra") | .jid')" "aviso"
zap group add "Obra" --who +5511900000000
```

Read a voice note: it is already text if the daemon runs with `--transcribe`.

```sh
zap queue                       # pending / running / done / failed
zap queue --retry               # re-arm what failed
zap messages --chat +55… -n 5 --plain-text   # the transcript replaced [media]
```

Summarise a relationship instead of reading everything:

```sh
zap sessions --number +5511900000000 --count            # how many conversations
zap sessions --number +5511900000000 --plain -d -n 3    # the last three, whole
zap sessions --mine --gap 4h | jq -r 'select(.inbound==0) | .number'  # no answer
```

Check the daemon before blaming the code:

```sh
zap accounts        # connected: true?  push_name right?
zap log             # every request, live, with status and ms
zap events          # what can show up on tail, and what each means
```

## Reading a conversation

**`--plain-text` is the shape you hand to a human or to a model:** one line per
message, `name: text`, oldest first, newlines collapsed.

```sh
zap messages --chat +5511900000001 -n 40 --plain-text
zap sessions --mine --plain -n 10           # a summary per session, ---- between
zap sessions --mine --plain --detail -n 3   # the whole transcript instead
```

A session is a stretch of conversation cut wherever the chat went quiet longer
than the gap. It is **derived from the stored messages on every call**, so
`--gap 4h` and `--gap 24h` read the same rows and answer differently. An active
group never goes 24h silent, so one session of 900 messages is correct, not a
bug — narrow the gap when that happens.

## Reacting to events

**`zap tail` is an endless ndjson of facts; `zap on` runs a command per fact.**
Every event carries the `account` it belongs to — without that field two
inboxes silently become one.

```sh
zap tail | jq -r 'select(.type=="message_received") | "\(.sender_name): \(.text)"'
zap on message_received -- ./answer.sh      # event on stdin + ZAP_* env vars
zap log | jq -r '"\(.status) \(.ms)ms \(.method) \(.path)"'
```

Names are EventStorming — subject plus past-tense verb: `message_received`,
`group_members_added`, `session_opened`. `zap events` prints the catalogue with
what each one means. Anything whatsmeow emits that zap has not curated still
arrives as `wa_<TypeName>`, so nothing is invisible.

## What will bite you

**History is negotiated AT PAIRING TIME.** `zap serve --history-days 365` only
widens numbers paired after the flag; a number already linked keeps the window
it was linked with. The bundle also arrives over several minutes — a count
taken right after pairing is not the final count.

**A `@lid` is not a phone number.** Group members arrive as internal ids;
`number` and `sender_name` are already resolved for you, but the raw `jid`
field is not dialable. Never print a `@lid`, a `@g.us` or a `@broadcast` with a
`+` in front.

**Voice notes only become text if `zap serve --transcribe` is running** with
`mlx_whisper` or `uvx` on PATH; until then they read as `[media]`. The audio is
downloaded either way — the media URL expires, the transcription can wait.
`zap queue` shows pending/running/done/failed, `zap queue --retry` re-arms.

## From another language

The socket speaks plain HTTP, so nothing needs a client library.

```sh
curl --unix-socket ~/.zap/zap.sock http://zap/accounts
curl --unix-socket ~/.zap/zap.sock -N http://zap/tail        # ndjson stream
```

`zap schema` emits the JSON Schema of every record, reflected from the Go
structs — feed it to `json-schema-to-typescript` instead of hand-writing types.
