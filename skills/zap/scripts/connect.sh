#!/bin/sh
# connect.sh [--pair]: make sure the zap daemon is running, and show who is linked.
#   Starts `zap serve` in the background when no daemon answers: stdout goes nowhere
#   (the messages of followed chats are not for a log) and stderr to $ZAP_HOME/serve.log
#   (counts and error classes only, never message text).
#   --pair  link a number: runs `zap pair`, which draws a QR the OWNER scans on the phone.
#           Pairing is the owner's login; an agent never runs it without being asked to.
#   Even with a number linked, zap listens to NO chat until follow.sh allows one.
set -eu
pair=0
case "${1:-}" in
  --pair) pair=1 ;;
  "") ;;
  *) echo "usage: connect.sh [--pair]" >&2; exit 2 ;;
esac
command -v zap >/dev/null 2>&1 || { echo "connect: zap is not on PATH (build it: just build)" >&2; exit 1; }
home=${ZAP_HOME:-$HOME/.zap}

if ! zap accounts >/dev/null 2>&1; then
  mkdir -p "$home"
  chmod 700 "$home"
  nohup zap serve >/dev/null 2>"$home/serve.log" </dev/null &
  i=0
  until zap accounts >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -le 25 ] || { echo "connect: the daemon did not come up; see $home/serve.log" >&2; exit 1; }
    sleep 0.2
  done
  echo "connect: daemon started (log: $home/serve.log)" >&2
fi

if [ "$pair" -eq 1 ]; then
  exec zap pair
fi

n=$(zap accounts | grep -c . || true)
if [ "$n" -eq 0 ]; then
  echo "connect: no number is linked. Ask the owner to run: connect.sh --pair" >&2
  exit 3
fi
zap accounts
