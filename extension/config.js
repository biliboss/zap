// Runs in the page BEFORE wa-js. Turns off everything in wa-js that talks to a network:
// Google Analytics, link-preview servers, and the "WPPConnect" device rename.
// The beacon/fetch/XHR guard is a second lock: anything not aimed at whatsapp.com is dropped.
(() => {
  window.WPPConfig = {
    disableGoogleAnalytics: true,
    googleAnalyticsId: null,
    googleAnalyticsUserProperty: {},
    linkPreviewApiServers: null,
    poweredBy: null,
    deviceName: false,
    sendStatusToDevice: false,
  };
  const ok = (u) => {
    try {
      const h = new URL(String(u), location.href).hostname;
      return h === 'whatsapp.com' || h.endsWith('.whatsapp.com') || h.endsWith('.whatsapp.net');
    } catch (_) { return false; }
  };
  if (navigator.sendBeacon) {
    const b = navigator.sendBeacon.bind(navigator);
    navigator.sendBeacon = (u, d) => (ok(u) ? b(u, d) : false);
  }
  const f = window.fetch;
  if (f) {
    window.fetch = function (input, init) {
      const u = input && input.url ? input.url : input;
      if (!ok(u)) return Promise.reject(new TypeError('zap: blocked request to a foreign host'));
      return f.apply(this, arguments);
    };
  }
  const open = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (m, u) {
    if (!ok(u)) throw new DOMException('zap: blocked request to a foreign host', 'NetworkError');
    return open.apply(this, arguments);
  };
})();
