#!/bin/sh
# unfollow.sh <number|label|jid>: undo follow.sh.
#   Takes the chat off the allowlist, stops its backfill and DELETES its stored messages
#   and voice notes. What a backend (e.g. a JetStream stream) already received is the
#   backend's own to expire: the stream's retention is the defense, not this script.
set -eu
command -v zap >/dev/null 2>&1 || { echo "unfollow: zap is not on PATH" >&2; exit 1; }
[ $# -ge 1 ] || { echo "usage: unfollow.sh <number|label|jid>" >&2; exit 2; }
exec zap unfollow "$@"
