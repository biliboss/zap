#!/bin/sh
# follow.sh <number|group|jid> [--label X] [--days N] [--max N] [--no-backfill] [--wait SECONDS]
#   Follow one chat: allow it, ask the phone for its past, sync every new message from now on.
#   Run it only for a chat the OWNER named: following is what lets zap listen to a third party.
#   --wait polls until the backfill finishes (done or stalled) or the time is up; the phone
#   answers minutes later, and only if it is on and online.
#   Prints the result line, and the final state of the backfill when --wait is used.
set -eu
command -v zap >/dev/null 2>&1 || { echo "follow: zap is not on PATH" >&2; exit 1; }
[ $# -ge 1 ] || { echo "usage: follow.sh <number|group|jid> [--label X] [--days N] [--max N] [--no-backfill] [--wait S]" >&2; exit 2; }

wait=0
args=""
while [ $# -gt 0 ]; do
  case "$1" in
    --wait) wait=${2:?--wait needs seconds}; shift 2 ;;
    *) args="$args
$1"; shift ;;
  esac
done

# one argument per line keeps a group name with spaces in one piece
oldifs=$IFS
IFS='
'
# shellcheck disable=SC2086
set -- $args
IFS=$oldifs

res=$(zap follow "$@")
printf '%s\n' "$res"
[ "$wait" -gt 0 ] || exit 0

chat=$(printf '%s' "$res" | sed -n 's/.*"chat":"\([^"]*\)".*/\1/p')
state=$(printf '%s' "$res" | sed -n 's/.*"backfill":"\([^"]*\)".*/\1/p')
end=$(( $(date +%s) + wait ))
while [ "$state" = running ] || [ "$state" = pending ]; do
  [ "$(date +%s)" -lt "$end" ] || break
  sleep 2
  state=$(zap follows | grep -F "\"chat\":\"$chat\"" | sed -n 's/.*"backfill":"\([^"]*\)".*/\1/p')
done
echo "backfill: $state" >&2
[ "$state" != stalled ] || exit 4
