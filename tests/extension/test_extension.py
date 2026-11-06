# Loads extension/ into Chromium with a fake WhatsApp Web page and checks the read-only rules.
#   PLAYWRIGHT_BROWSERS_PATH=... uv run --with playwright python -I tests/extension/test_extension.py [extension-dir]
# The page is served by a route for https://web.whatsapp.com/: no real WhatsApp, no real profile.
import json, sys, tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
EXT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else HERE.parent.parent / "extension"
FAKE = (HERE / "fake-wa.html").read_text()
ME = "5511900000000@c.us"
A = "5511900000001@c.us"  # never followed
B = "5511900000002@c.us"  # followed (by bare number)
G = "120363000000000001@g.us"  # followed (by jid)


def key(chat, mid, me=False):
    return {"fromMe": me, "remote": {"_serialized": chat}, "id": mid, "_serialized": f"{'true' if me else 'false'}_{chat}_{mid}"}


def new(chat, mid, text, **kw):
    return {"id": key(chat, mid), "from": {"_serialized": chat}, "author": kw.get("author"), "type": kw.get("type", "chat"),
            "body": text, "t": 1762400000, "notifyName": "Ana", "isNewMsg": True, **({"caption": kw["caption"]} if "caption" in kw else {})}


def drive(page, chat, tag):
    page.evaluate("""([chat, tag, key]) => {
      const k = (id) => ({ fromMe: false, remote: { _serialized: chat }, id, _serialized: 'false_' + chat + '_' + id });
      __fakeEmit('chat.new_message', { id: k('m-' + tag), from: { _serialized: chat }, type: 'chat', body: 'hello ' + tag, t: 1762400000, notifyName: 'Ana', isNewMsg: true });
      __fakeEmit('chat.msg_edited', { chat: { _serialized: chat }, id: 'false_' + chat + '_m-' + tag, msg: { id: k('m-' + tag), from: { _serialized: chat }, body: 'edited ' + tag, t: 1762400100 } });
      __fakeEmit('chat.msg_revoke', { from: { _serialized: chat }, to: { _serialized: '5511900000000@c.us' }, id: k('r-' + tag), refId: k('m-' + tag), type: 'revoke' });
    }""", [chat, tag, None])


def main():
    seen = []
    with sync_playwright() as p, tempfile.TemporaryDirectory() as prof:
        ctx = p.chromium.launch_persistent_context(prof, channel="chromium", headless=True, args=[
            f"--disable-extensions-except={EXT}", f"--load-extension={EXT}"])
        ctx.on("request", lambda r: seen.append(r.url))
        ctx.route("**/*", lambda r: r.fulfill(body=FAKE, content_type="text/html") if r.request.url.startswith("https://web.whatsapp.com/")
                  else r.abort())
        sw = ctx.service_workers[0] if ctx.service_workers else ctx.wait_for_event("serviceworker", timeout=15000)
        page = ctx.new_page()
        page.goto("https://web.whatsapp.com/")
        page.wait_for_function("window.__fakeBound && window.__fakeBound()['chat.new_message'] === 1", timeout=10000)

        get = lambda: sw.evaluate("chrome.storage.local.get(['events']).then(r => r.events || [])")
        # 1. default: nothing followed -> nothing stored
        drive(page, A, "a0"); drive(page, B, "b0")
        page.wait_for_timeout(700)
        assert get() == [], f"empty allowlist must store nothing, got {get()}"
        print("ok: empty follow list stores nothing")

        # 2. follow B (bare number) and the group (jid)
        sw.evaluate("chrome.storage.local.set({follows: [{id: '5511900000002', label: 'Bia'}, {id: '%s', label: ''}]})" % G)
        for i in range(20):  # bounded wait for the follow list to reach the page
            drive(page, B, f"probe{i}")
            page.wait_for_timeout(250)
            if get():
                break
        assert get(), "follow list never reached the page"
        sw.evaluate("chrome.storage.local.set({events: []})")
        page.wait_for_timeout(300)

        drive(page, A, "a1"); drive(page, B, "b1"); drive(page, G, "g1")
        page.evaluate("""([g]) => __fakeEmit('chat.new_message', { id: { fromMe: false, remote: { _serialized: g }, id: 'img1', _serialized: 'false_' + g + '_img1' },
          from: { _serialized: g }, author: { _serialized: '5511900000003@c.us' }, type: 'image', body: 'BASE64THUMBNAILSECRET', caption: 'planta', t: 1762400200 })""", [G])
        page.evaluate("""([b]) => __fakeEmit('chat.new_message', { id: { fromMe: true, remote: { _serialized: b }, id: 'mine', _serialized: 'true_' + b + '_mine' }, type: 'chat', body: 'my own', t: 1762400300 })""", [B])
        page.wait_for_timeout(700)
        ev = get()
        chats = {e["chat"] for e in ev}
        assert A not in chats, "unfollowed chat leaked"
        assert chats == {B, G}, chats
        by = lambda c, t: [e for e in ev if e["chat"] == c and e["type"] == t]
        rec, edi, rev = by(B, "message_received"), by(B, "message_edited"), by(B, "message_revoked")
        assert len(rec) == 1 and len(edi) == 1 and len(rev) == 1, [e["type"] for e in ev]
        assert rec[0]["text"] == "hello b1" and rec[0]["id"] == f"false_{B}_m-b1" and rec[0]["account"] == "5511900000000"
        assert rec[0]["sender_name"] == "Ana" and rec[0]["timestamp"] == "2025-11-06T03:33:20.000Z", rec[0]
        assert edi[0]["text"] == "edited b1" and edi[0]["target"] == f"false_{B}_m-b1"
        assert rev[0]["target"] == f"false_{B}_m-b1" and "text" not in rev[0]
        img = [e for e in by(G, "message_received") if e.get("media")]
        assert img and img[0]["media"] == "image" and img[0]["text"] == "planta" and img[0]["sender"] == "5511900000003@c.us"
        assert "BASE64THUMBNAILSECRET" not in json.dumps(ev), "media body leaked"
        assert not any(e["text"] == "my own" for e in ev if "text" in e), "own message stored as received"
        names = {e["type"] for e in ev}
        assert names == {"message_received", "message_edited", "message_revoked"}, names
        print("ok: followed chats stored with zap names:", sorted(names), "| unfollowed chat stored nothing |", len(ev), "events")

        # 3. ring of 2000
        sw.evaluate("chrome.storage.local.set({events: Array.from({length: 2000}, (_, i) => ({type: 'message_received', chat: '%s', id: 'old' + i, text: 'x'}))})" % B)
        drive(page, B, "ring")
        page.wait_for_timeout(700)
        ev = get()
        assert len(ev) == 2000 and ev[-1]["type"] == "message_revoked" and ev[0]["id"] != "old0", (len(ev), ev[0])
        print("ok: ring capped at 2000, oldest dropped")

        # 4. unfollow stops reading
        sw.evaluate("chrome.storage.local.set({follows: [], events: []})")
        page.wait_for_timeout(500)
        drive(page, B, "after")
        page.wait_for_timeout(700)
        assert get() == [], get()
        print("ok: unfollow stops reading")

        foreign = [u for u in seen if not u.startswith(("https://web.whatsapp.com/", "chrome-extension://", "data:", "blob:"))]
        assert not foreign, f"network calls to foreign hosts: {foreign}"
        print("ok: no foreign network requests;", len(seen), "requests, all web.whatsapp.com or chrome-extension://")
        ctx.close()


main()
