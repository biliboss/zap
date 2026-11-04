#!/bin/sh
# chats.sh [search]: the chats the phone knows, with who is followed.
#   One tab-separated line per chat: followed (* or blank), kind, number, name, jid.
#   Names and ids only, never a message. A search matches name, number or jid.
set -eu
command -v zap >/dev/null 2>&1 || { echo "chats: zap is not on PATH" >&2; exit 1; }
if command -v jq >/dev/null 2>&1; then
  zap chats "$@" | jq -r '[(if .followed then "*" else " " end), .kind, (.number // ""), (.name // "-"), .jid] | @tsv'
else
  zap chats "$@"
fi
