#!/bin/sh
# read.sh <number|label|jid> [-n N] [--since 2h]: the stored messages of one FOLLOWED chat.
#   One line per message, `name: text`, oldest first. Refuses a chat that is not followed:
#   zap stores nothing of a chat nobody allowed, so there is nothing to read, and the answer
#   is to ask the owner before running follow.sh.
#   -n N      how many messages (default 40)
#   --since   only what is newer: a duration like 2h or 30m, or a unix time
set -eu
command -v zap >/dev/null 2>&1 || { echo "read: zap is not on PATH" >&2; exit 1; }
[ $# -ge 1 ] || { echo "usage: read.sh <number|label|jid> [-n N] [--since 2h]" >&2; exit 2; }

who=$1
shift
n=40
since=""
while [ $# -gt 0 ]; do
  case "$1" in
    -n) n=${2:?-n needs a number}; shift 2 ;;
    --since) since=${2:?--since needs a value}; shift 2 ;;
    *) echo "read: unknown option $1" >&2; exit 2 ;;
  esac
done

# find the followed chat by number, label or jid, and read it by its jid
digits=$(printf '%s' "$who" | tr -cd '0-9')
if [ "${#digits}" -ge 6 ]; then
  line=$(zap follows | grep -F "$digits" | head -n 1 || true)
else
  line=$(zap follows | grep -iF "\"label\":\"$who\"" | head -n 1 || true)
fi
chat=$(printf '%s' "$line" | sed -n 's/.*"chat":"\([^"]*\)".*/\1/p')
if [ -z "$chat" ]; then
  echo "read: \"$who\" is not followed, so zap keeps nothing of it. Ask the owner; then: follow.sh $who" >&2
  exit 3
fi

if [ -n "$since" ]; then
  exec zap messages --chat "$chat" --since "$since" --limit "$n" --plain-text
fi
exec zap messages --chat "$chat" --limit "$n" --plain-text
