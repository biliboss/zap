# zap extension (read only)

An MV3 extension that reads **only the chats you follow**, inside your own WhatsApp Web tab, and keeps
the events in `chrome.storage.local`. **Nothing leaves the browser**: no network call exists in the
code, the extension pages carry `connect-src 'none'`, and the page-side guard drops any request that
is not aimed at whatsapp.com. It sends nothing: no message, no read receipt, no typing.

## How it works

```
page (world MAIN)             isolated world         service worker
wa-js ─ chat.new_message ┐
        chat.msg_edited  ├─▶ page.js ──▶ bridge.js ──▶ background.js ─▶ chrome.storage.local
        chat.msg_revoke  ┘   (filters)               (filters again)    events: ring of 2,000
```

- WhatsApp Web's IndexedDB bodies are encrypted; wa-js reads the already decrypted message in memory.
- The follow list is filtered twice (page, then worker). **Empty list = nothing is read** (the default).
- Events use zap's wire names and ndjson shape: `message_received`, `message_edited`,
  `message_revoked`, with `account`, `type`, `chat`, `sender`, `id`, `text`, `timestamp`, `target`,
  `media`, `sender_name`. Media bodies are never read (a caption is kept as `text`); messages you
  sent are skipped.
- wa-js's own analytics, link-preview servers and device rename are switched off in `config.js`.

## Use

Popup: type a number (`5511…`) or a jid (`…@g.us`), optionally a label, press Follow. The popup shows
the followed chats and the count of stored events; **Clear** empties the ring. Read the events:
DevTools of the service worker (chrome://extensions → zap → service worker) →
`chrome.storage.local.get('events')`.

Install it with `skills/zap/scripts/extension.sh` (it is not in the Chrome Web Store); tests with
`just ext-test`.

## Vendored code

`vendor/wppconnect-wa.js` is [@wppconnect/wa-js](https://github.com/wppconnect-team/wa-js) 4.7.2 by the
WPPConnect Team, Apache-2.0, unmodified (see `NOTICE`, `vendor/LICENSE-wa-js`). It breaks when
WhatsApp Web updates: bump it by replacing that file with a newer release build.

Risk, said plainly: automated reading of WhatsApp Web is outside WhatsApp's terms of service; a
read-only extension lowers the ban risk, it does not remove it.
