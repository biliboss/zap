---
name: zap
description: Drive WhatsApp from the shell with the `zap` CLI, safely. Connect a number, list chats, follow a person (allow them, backfill the past, sync new messages) or unfollow them, and read what was stored. zap listens to no chat until the owner allows it, and the allowed messages go to the backend the config chooses (ndjson or NATS JetStream). Use when the task touches WhatsApp on this machine ("connect whatsapp", "list my chats", "follow Ana", "stop following", "read my chat with Ana", "send a whatsapp", "react to incoming messages", "zap"). Not a general messaging skill; it needs the zap binary and a linked number.
metadata:
  version: 0.3.0
---

# zap

**One process holds every linked number and serves them over a unix socket; every verb is a thin client of it.** `zap serve` must be running; `scripts/connect.sh` starts it.

## Solutions

Two ways to read WhatsApp, with the same rule (a chat is read only once followed):

- **The zap process** (this skill's `scripts/` + the `zap` binary): reads **and sends**, holds **many numbers** in one process, backends ndjson or NATS. Needs a linked number (QR) and the process running.
- **The extension** (`extension/`): **reads in the person's own browser**, **local-first, no server**: events stay in `chrome.storage.local`, nothing leaves the browser, it cannot send. Pick it when the person lives in WhatsApp Web and wants nothing running elsewhere. It is not in the Chrome Web Store: see "Extension".

## The rule that governs everything: zap listens to nobody until you allow it

A chat that is not on the allowlist (`$ZAP_HOME/allow.toml`) is **not stored, not on `zap tail`, not handed to a hook and not sent to the backend**. Third parties' messages are read only from chats the owner allowed.

- **Following is the owner's decision.** Run `follow.sh` only for a person or group the owner named in this task, never to "see what is there". If the chat was not named, ask.
- **Pairing is the owner's login.** `connect.sh --pair` draws a QR the owner scans. An agent does not run it unprompted, and never reads real messages to test anything: tests use `zap serve --fake`.
- **Nothing in logs.** Quote the least message text that answers the task, and write none of it to notes, tickets or logs. Use masked numbers (`+55…0001`) when a placeholder will do.
- **A token never goes in a file you commit.** The NATS token is read from the environment variable the config names (`token_env`).

## Steps

`S` is the `scripts/` folder next to this file. The scripts need `zap` on PATH and print only the answer.

1. **Connect.** `S/connect.sh` starts the daemon if it is down and lists the linked numbers. With no number linked it exits 3: ask the owner to run `S/connect.sh --pair`.
2. **Choose where messages go (once).** Copy `config.example.toml` from the repo to `$ZAP_HOME/config.toml` and edit it; zap never rewrites it. `type = "ndjson"` (the default) serves `zap tail`; `type = "nats"` publishes to a JetStream stream whose name, subjects and subject prefix are the owner's. Restart `zap serve` after a change.
3. **List chats.** `S/chats.sh [search]` prints followed (`*`), kind, number, name and jid: names and ids, never messages.
4. **Follow.** `S/follow.sh <number|group name|jid> [--label Ana] [--days 30] [--max 500] [--wait 120]` allows the chat, asks the phone for its past and syncs every new message from then on. `--wait` polls until the backfill is `done` or `stalled` (exit 4).
5. **Read.** `S/read.sh <number|label|jid> [-n 40] [--since 2h]` prints `name: text`, oldest first. It exits 3 for a chat that is not followed, because zap kept nothing of it: ask the owner, then follow.
6. **Unfollow.** `S/unfollow.sh <number|label|jid>` takes the chat off the list, stops its backfill and **deletes its stored messages and voice notes**. What a backend already received is the backend's own to expire: set `max_age` on the stream.
7. **Who is followed.** `S/follows.sh` prints the backfill state (`pending`, `running`, `done`, `stalled`, `off`), the count of past messages, the label and the jid.

## The past has limits

The backfill asks the phone for older messages of one chat, 50 per request, with an on-demand history request. It works only if:

- the **phone is on and online**: with no answer in 3 minutes the state is `stalled`; run `follow.sh` again;
- zap has an **anchor**, one message of that chat seen since it started. Without one the chat waits as `pending` and the first new message starts the backfill;
- the phone **still holds** the messages: what was deleted there, or is older than a restore, does not come back, and media does not come either, only text and metadata;
- the **anchor message itself** is not part of the past (its body was never stored).

`zap follow --again` asks a second time for a chat that is `done`. The request is read from the whatsmeow source and not yet verified against a real chat: say so if the owner asks how reliable it is.

## Reading and reacting

`zap tail` and `zap on` only ever see followed chats. Events are subject + past-tense verb (`message_received`, `chat_followed`, `chat_backfilled`); `zap events` prints the catalogue. With the NATS backend the subjects are `<prefix>.<entity>.<event>`: `message.received`, `message.sent`, `chat.followed`, `chat.backfilled`, and a past message carries `"backfill": true`.

```sh
zap messages --chat <jid> -n 40 --plain-text                         # read.sh, by hand
zap messages --since 2h --limit 500 | jq -c 'select(.from_me|not)'   # catch up: the store is written before the event
zap tail | jq -r 'select(.type=="message_received") | "\(.sender_name): \(.text)"'
zap on message_received -- ./handle.sh                               # event on stdin + ZAP_CHAT ZAP_SENDER ZAP_TEXT
zap send --to <num> --id "reply-<inbound_id>" "text"                 # sending always works; the same --id makes a retry safe
```

More scenarios (reply with context, groups, voice notes, sessions): [references/recipes.md](references/recipes.md). Every verb and flag: [references/verbs.md](references/verbs.md); `zap -h` is the authority.

On a terminal zap prints for a human; through a pipe it prints ndjson on stdout and nothing else (captions and errors go to stderr). Exit 0 ok, 1 failed, 2 wrong usage. With one number linked `--from` is optional; with several pass `--from` or run `zap use <num>`.

## What will bite you

- **An empty `messages` for a chat is the design, not a bug.** It means the chat is not followed, or nothing was said since.
- **A `@lid` is not a phone number.** Never print a `@lid`, `@g.us` or `@broadcast` with a `+` in front. `follow` stores both spellings of one person.
- **The pairing bundle is filtered as it arrives.** `zap serve --history-days` widens what the phone ships at pairing, but only followed chats are kept; the way to one chat's past later is `follow`.
- **A hand edit of `allow.toml` is picked up live; a broken one is ignored** and the previous list stays, so a typo never opens the door.
- **Voice notes become text only with `zap serve --transcribe`** (needs `mlx_whisper` or `uvx`); until then they read as `[media]`.

## Extension

`S/extension.sh install|status|update|remove [--browser chrome|chromium|brave|cft] [--method auto|policy|flag|computer-use|manual]` installs `extension/` with what the harness offers. `auto` tries, in order:

1. **policy** (admin user): packs a .crx into `~/.local/share/zap/extension/` (the key stays there, never committed), writes `update.xml` and a managed-preferences plist for `ExtensionInstallForcelist`, and **prints the `sudo` commands for the owner**: you never run sudo. The owner verifies at `chrome://policy`; `S/extension.sh serve` hosts the package for 120 s. On a Mac that is not managed the browser may ignore it: then use flag or manual.
2. **flag**: launches a separate profile with `--load-extension` (Chromium, Chrome for Testing, Brave). Google Chrome stable dropped the flag in 137, and the script refuses with that reason.
3. **computer-use** (`ZAP_HAS_COMPUTER_USE=1`): prints a JSON plan (chrome://extensions, Developer mode, Load unpacked, the folder).
4. **manual**: the 4 clicks, in Portuguese, for the person.

`status` checks for real: the policy plist and the extension id in the profile's `Preferences`/`Secure Preferences`; `--probe` also loads the extension in a throwaway Chromium against a fake WhatsApp page and requires the test events to arrive. `remove` undoes each method (the policy removal is a `sudo` line for the owner).

- **Following is still the owner's decision**: the follow list lives in the extension popup and is empty by default. An agent adds a chat only if the owner named it, and never opens WhatsApp Web to look around.
- **Logging in to WhatsApp Web (QR) is the owner's**, also in the separate profile of `--method flag`.
- Events carry the same wire names as `zap tail` (`message_received`, `message_edited`, `message_revoked`) and sit in `chrome.storage.local` key `events` (ring of 2,000); quote the least text, write none to notes or logs.
- wa-js breaks when WhatsApp Web updates; automated reading is outside WhatsApp's terms of service. Say so if asked.
