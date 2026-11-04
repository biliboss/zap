#!/bin/sh
# follows.sh: who is followed, and how each backfill stands.
#   One tab-separated line per chat: backfill state, past messages brought in, label, jid.
#   States: pending (waiting for a first message to ask from), running, done, stalled
#   (the phone did not answer: run follow.sh again), off (followed without the past).
set -eu
command -v zap >/dev/null 2>&1 || { echo "follows: zap is not on PATH" >&2; exit 1; }
if command -v jq >/dev/null 2>&1; then
  zap follows | jq -r '[.backfill, .backfilled, (.label // "-"), .chat] | @tsv'
else
  zap follows
fi
