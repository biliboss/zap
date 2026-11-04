# zap — every verb and every flag

Generated from the binary: `zap <verb> -h`. If this disagrees with the
binary, the binary is right.

## zap serve
```
  -allow string
    	allowlist file: the chats zap may listen to (default "~/.zap/allow.toml")
  -config string
    	config file: which backend allowed messages go to (default "~/.zap/config.toml")
  -db string
    	sqlite store path (default "~/.zap/store.db")
  -history-days int
    	days of history to ask the phone for, on numbers paired from now on
  -history-per-chat int
    	max messages per chat in the initial sync
  -media string
    	where voice notes are kept (default "~/.zap/media")
  -session-gap duration
    	silence that closes a session (default 24h0m0s)
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
  -transcribe
    	transcribe incoming voice notes with mlx-whisper
  -whisper-model string
    	mlx model to transcribe with (default "mlx-community/whisper-large-v3-turbo")
```

## zap pair
```
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap logout
```
  -from string
    	account to unlink
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap accounts
```
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap use
```
  -clear
    	forget the default account
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap send
```
  -from string
    	sending account, required when several are paired
  -id string
    	message id — pass the same one to retry safely
  -myself
    	send to your own chat
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
  -to string
    	recipient number or JID
```

## zap typing
```
  -audio
    	show 'recording audio' instead
  -from string
    	sending account
  -off
    	stop typing
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
  -to string
    	chat to appear typing in
```

## zap presence
```
  -away
    	appear offline (and let the phone push again)
  -from string
    	account
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap status
```
  -from string
    	account
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap contacts
```
  -from string
    	account to read
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap groups
```
  -from string
    	account to ask
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap group
```
  -from string
    	sending account
  -reset
    	link: revoke the old link
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
  -who string
    	comma separated numbers
```

## zap messages
```
  -chat string
    	only this chat (number or group jid)
  -from string
    	account to read
  -limit int
    	how many, newest first (default 50)
  -n int
    	alias for --limit (default 50)
  -plain-text
    	transcript form: 'name: message', oldest first
  -since string
    	cursor: unix seconds, or a duration like 2h — reads forward, oldest first
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap sessions
```
  -count
    	print how many sessions, nothing else
  -d	alias for --detail
  -detail
    	with --plain, print the whole transcript instead of a summary
  -from string
    	account to read
  -gap duration
    	silence that starts a new session (default 24h0m0s)
  -limit int
    	how many, newest first
  -mine
    	only sessions you spoke in
  -n int
    	alias for --limit
  -number string
    	only this contact or group
  -plain
    	print each session as text, separated by ----
  -socket string
```

## zap sync
```
  -chat string
    	the chat to pull older messages for
  -count int
    	how many older messages to ask for (default 50)
  -from string
    	account to ask with
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap tail
```
  -from string
    	only events of this account
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap log
```
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap on
```
  -from string
    	only events of this account
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap events
```
{"meaning":"the account came online","source":"socket","type":"account_connected"}
{"meaning":"old messages arrived and went into the store","source":"phone","type":"history_synced"}
{"meaning":"someone wrote to us","source":"phone","type":"message_received"}
{"meaning":"we wrote to someone","source":"zap","type":"message_sent"}
{"meaning":"the other side's phone got it","source":"phone","type":"message_delivered"}
{"meaning":"the other side opened it","source":"phone","type":"message_read"}
{"meaning":"an emoji landed on a message","source":"phone","type":"message_reacted"}
{"meaning":"the author rewrote a message","source":"phone","type":"message_edited"}
{"meaning":"the author deleted it for everyone","source":"phone","type":"message_revoked"}
{"meaning":"a message arrived that we could not open","source":"phone","type":"message_undecryptable"}
{"meaning":"an image, video, document or sticker arrived","source":"phone","type":"media_received"}
{"meaning":"a voice note was downloaded and parked","source":"queue","type":"audio_queued"}
{"meaning":"a voice note became text in the store","source":"queue","type":"audio_transcribed"}
{"meaning":"we entered a group","source":"phone","type":"group_joined"}
{"meaning":"the member list changed","source":"group","type":"group_members_added"}
{"meaning":"the member list changed","source":"group","type":"group_members_removed"}
{"meaning":"who can administer changed","source":"group","type":"group_admins_promoted"}
{"meaning":"who can administer changed","source":"group","type":"group_admins_demoted"}
{"meaning":"the group itself changed","source":"group","type":"group_renamed"}
{"meaning":"the group itself changed","source":"group","type":"group_topic_changed"}
```

## zap queue
```
  -retry
    	put failed and stuck transcriptions back in line
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap chats
```
  -from string
    	account to read
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap follow
```
  -again
    	ask for the past again on a chat that already has it
  -days int
    	how far back to ask the phone for the past (default 30)
  -from string
    	account that follows
  -label string
    	a name for the chat, to find it again
  -max int
    	at most this many past messages (default 500)
  -no-backfill
    	follow from now on, without the past
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap unfollow
```
  -from string
    	account that follows
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap follows
```
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap purge
```
  -socket string
    	unix socket path (default "~/.zap/zap.sock")
```

## zap schema
```
{
  "$defs": {
    "Account": {
      "additionalProperties": false,
      "properties": {
        "connected": {
          "type": "boolean"
        },
        "jid": {
          "type": "string"
        },
        "number": {
          "type": "string"
        },
        "push_name": {
          "type": "string"
        }
      },
      "required": [
        "number",
```

