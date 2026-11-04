# zap — recipes

**Copy the block for the scenario you are in.** Every chat below must be a
**followed** chat (`scripts/follow.sh`): zap stores, tails and forwards nothing
of any other. Sending is the exception, it works to any chat.

Numbers here are placeholders from the reserved fictional range.

## Answer someone who just wrote, with the context of what was said before

```sh
zap messages --chat +15550100001 -n 30 --plain-text     # read first
zap typing  --to  +15550100001                          # while you think
zap send    --to  +15550100001 --id "reply-<inbound_id>" "your reply"
zap typing  --to  +15550100001 --off
```

## Find the person before you write to them

```sh
zap contacts ana                         # name, push name or number
zap contacts | jq -r 'select(.name|test("(?i)ana")) | .number'
```

## Catch up on what happened while you were away

This is the path that never loses a message, because the store is written
before the event:

```sh
zap messages --since 2h --limit 500 | jq -c 'select(.from_me|not)'
zap messages --since 1787930000 --limit 500      # or a unix cursor
```

## React to a message as it arrives, without polling

```sh
zap on message_received -- ./handle.sh   # event as ndjson on stdin
                                         # plus ZAP_CHAT ZAP_SENDER ZAP_TEXT
zap tail | jq -r 'select(.type=="message_received") | "\(.sender_name): \(.text)"'
```

## Work with a group by its name, never by its id

```sh
zap groups | jq -r '.name'
zap group info "Book club"
zap send --to "$(zap groups | jq -r 'select(.name=="Book club") | .jid')" "notice"
zap group add "Book club" --who +15550100001
```

## Read a voice note

It is already text if the daemon runs with `--transcribe`.

```sh
zap queue                       # pending / running / done / failed
zap queue --retry               # re-arm what failed
zap messages --chat +15550100001 -n 5 --plain-text   # the transcript replaced [media]
```

## Summarise a relationship instead of reading everything

```sh
zap sessions --number +15550100001 --count            # how many conversations
zap sessions --number +15550100001 --plain -d -n 3    # the last three, whole
zap sessions --mine --gap 4h | jq -r 'select(.inbound==0) | .number'  # no answer
```

A session is a stretch of conversation cut wherever the chat went quiet longer
than the gap. It is **derived from the stored messages on every call**, so
`--gap 4h` and `--gap 24h` read the same rows and answer differently. An active
group never goes 24h silent, so one session of 900 messages is correct, not a
bug: narrow the gap when that happens.

## Check the daemon before blaming the code

```sh
zap accounts        # connected: true?  push_name right?
zap log             # every request, live, with status and ms (message text is replaced by its length)
zap events          # what can show up on tail, and what each means
zap follows         # who is followed; an empty list means zap listens to nobody
```

## From another language

The socket speaks plain HTTP, so nothing needs a client library.

```sh
curl --unix-socket "$ZAP_HOME/zap.sock" http://zap/accounts
curl --unix-socket "$ZAP_HOME/zap.sock" -N http://zap/tail        # ndjson stream
```

`zap schema` emits the JSON Schema of every record, reflected from the Go
structs: feed it to `json-schema-to-typescript` instead of hand-writing types.
