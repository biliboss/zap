// Runs in the page (world MAIN), next to wa-js. Listens to three wa-js events and hands over
// ONLY those of followed chats, in zap's wire shape. The list arrives from bridge.js.
// Text of a message is read only after its chat passed the filter; media bodies are never read.
(() => {
  if (window.__zapPage) return;
  window.__zapPage = true;
  let follows = [];
  window.addEventListener('message', (e) => {
    const d = e.data;
    if (e.source === window && d && d.zap === 'follows' && Array.isArray(d.chats)) {
      follows = d.chats.filter((s) => typeof s === 'string');
    }
  });
  window.postMessage({ zap: 'hello' }, location.origin);

  const jid = (w) => (!w ? '' : typeof w === 'string' ? w : w._serialized || (w.user && w.server ? `${w.user}@${w.server}` : ''));
  const followed = (j) => !!j && globalThis.ZapFollow.matches(follows, j);
  const account = () => {
    try { const w = window.WPP.conn.getMyUserId(); return (w && w.user) || 'web'; } catch (_) { return 'web'; }
  };
  const MEDIA = { image: 'image', video: 'video', ptt: 'audio', audio: 'audio', document: 'document', sticker: 'sticker' };
  const emit = (event) => window.postMessage({ zap: 'event', event }, location.origin);
  const iso = (sec) => new Date(sec ? sec * 1000 : Date.now()).toISOString();
  const strip = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined && v !== '' && v !== false));

  const body = (m) => {
    if (m.type === 'chat') return { text: m.body || '' };
    return { media: MEDIA[m.type] || String(m.type || ''), text: m.caption || '' };
  };

  const onNew = (m) => {
    if (!m || !m.id || m.id.fromMe || m.isNewMsg === false) return;
    const chat = jid(m.id.remote);
    if (!followed(chat)) return;
    emit(strip({
      account: account(), type: 'message_received', chat, sender: jid(m.author || m.from),
      id: m.id._serialized, ...body(m), timestamp: iso(m.t), sender_name: m.notifyName,
    }));
  };
  const onEdit = (e) => {
    if (!e) return;
    const chat = jid(e.chat);
    if (!followed(chat)) return;
    const m = e.msg || {};
    const target = typeof e.id === 'string' ? e.id : jid(e.id);
    emit(strip({
      account: account(), type: 'message_edited', chat, sender: jid(m.author || m.from),
      id: (m.id && m.id._serialized) || target, target, text: m.body || m.caption || '',
      timestamp: iso(m.t), sender_name: m.notifyName,
    }));
  };
  const onRevoke = (e) => {
    if (!e) return;
    const chat = [jid(e.from), jid(e.to)].find(followed);
    if (!chat) return;
    const target = jid(e.refId);
    emit(strip({
      account: account(), type: 'message_revoked', chat, sender: jid(e.author || e.from),
      id: target, target, timestamp: iso(0),
    }));
  };

  let bound = null;
  const bind = () => {
    const w = window.WPP;
    if (!w || w === bound || !w.isReady || typeof w.on !== 'function') return;
    bound = w;
    w.on('chat.new_message', onNew);
    w.on('chat.msg_edited', onEdit);
    w.on('chat.msg_revoke', onRevoke);
  };
  bind();
  const t = setInterval(bind, 500);
  window.addEventListener('pagehide', () => clearInterval(t));
})();
