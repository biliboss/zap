// Isolated world: the only script that talks to the extension. It pushes the follow list into
// the page and forwards the page's events to the worker, which checks the list again.
(() => {
  const KEY = 'follows';
  const send = () =>
    chrome.storage.local.get(KEY).then((r) => {
      const chats = (r[KEY] || []).map((f) => f.id);
      window.postMessage({ zap: 'follows', chats }, location.origin);
    }).catch(() => {});
  window.addEventListener('message', (e) => {
    if (e.source !== window || !e.data) return;
    if (e.data.zap === 'hello') send();
    if (e.data.zap === 'event' && e.data.event && typeof e.data.event === 'object') {
      chrome.runtime.sendMessage({ zap: 'store', event: e.data.event }).catch(() => {});
    }
  });
  chrome.storage.onChanged.addListener((c, area) => { if (area === 'local' && c[KEY]) send(); });
  send();
})();
