# Changelog

All notable changes to the zap skill. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] - 2026-11-04

### Added
- `scripts/`: `connect.sh` (start the daemon, show numbers, `--pair`), `chats.sh`, `follow.sh`, `unfollow.sh`, `follows.sh`, `read.sh`.
- The safe-by-default rule (nothing is listened to until a chat is followed), the limits of the backfill, and the backend choice (ndjson or NATS JetStream).
- `references/verbs.md`: `chats`, `follow`, `unfollow`, `follows`, `purge` and the `--config`/`--allow` flags of `serve`.

### Changed
- The examples are generic: no real names, numbers or hosts.
- History: a single chat's past is asked for after pairing, by `follow`, instead of re-pairing with a wider window.
