# Changelog

All notable changes to the zap skill. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.3.0] - 2026-11-06

### Added
- `extension/`: a read-only MV3 extension for WhatsApp Web (wa-js 4.7.2, Apache-2.0, credited in `extension/NOTICE`). Reads only followed chats, emits `message_received`, `message_edited`, `message_revoked`, keeps a ring of 2,000 events in `chrome.storage.local`, no network.
- `scripts/extension.sh install|status|update|remove|serve` with methods `policy` (managed-preferences force-install, admin commands printed, never run), `flag` (separate profile with `--load-extension`; Google Chrome 137+ refused), `computer-use` (machine-readable plan) and `manual` (4 clicks, in Portuguese).
- SKILL.md: a "Solutions" section (the zap process vs the extension) and an "Extension" section.
- Tests: fake WhatsApp page under Playwright + Chromium, unit test of update.xml and the policy plist.

## [0.2.0] - 2026-11-04

### Added
- `scripts/`: `connect.sh` (start the daemon, show numbers, `--pair`), `chats.sh`, `follow.sh`, `unfollow.sh`, `follows.sh`, `read.sh`.
- The safe-by-default rule (nothing is listened to until a chat is followed), the limits of the backfill, and the backend choice (ndjson or NATS JetStream).
- `references/verbs.md`: `chats`, `follow`, `unfollow`, `follows`, `purge` and the `--config`/`--allow` flags of `serve`.

### Changed
- The examples are generic: no real names, numbers or hosts.
- History: a single chat's past is asked for after pairing, by `follow`, instead of re-pairing with a wider window.
