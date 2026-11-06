// Which chats are followed. Shared by the page script, the bridge, the worker and the popup.
// An entry is a jid ("5511900000001@c.us", "1203...@g.us") or a bare phone number.
// An empty list follows nothing: that is the default and the rule.
(() => {
  const split = (jid) => {
    const s = String(jid || '').trim().toLowerCase();
    const i = s.indexOf('@');
    return i < 0 ? { user: s, server: '' } : { user: s.slice(0, i), server: s.slice(i + 1) };
  };
  const person = (server) => server === 'c.us' || server === 's.whatsapp.net';
  const matches = (entries, jid) => {
    const c = split(jid);
    if (!c.user || !c.server || !Array.isArray(entries)) return false;
    return entries.some((e) => {
      if (typeof e !== 'string') return false;
      if (e.includes('@')) {
        const x = split(e);
        return x.user === c.user && (x.server === c.server || (person(x.server) && person(c.server)));
      }
      const digits = e.replace(/\D/g, '');
      return digits !== '' && person(c.server) && c.user === digits;
    });
  };
  globalThis.ZapFollow = { matches, MAX_EVENTS: 2000 };
})();
