// The single writer of the event ring. No network call exists in this extension.
importScripts('follow.js');
const NAMES = new Set(['message_received', 'message_edited', 'message_revoked']);
const FIELDS = ['account', 'type', 'chat', 'sender', 'id', 'text', 'timestamp', 'target', 'media', 'sender_name'];
let queue = Promise.resolve();

const store = async (raw) => {
  if (!raw || !NAMES.has(raw.type) || typeof raw.chat !== 'string') return;
  const { follows = [], events = [] } = await chrome.storage.local.get(['follows', 'events']);
  if (!globalThis.ZapFollow.matches(follows.map((f) => f.id), raw.chat)) return; // second lock
  const ev = {};
  for (const k of FIELDS) if (typeof raw[k] === 'string' && raw[k] !== '') ev[k] = raw[k].slice(0, 20000);
  if (events.some((o) => o.type === ev.type && o.id === ev.id && o.text === ev.text)) return;
  events.push(ev);
  await chrome.storage.local.set({ events: events.slice(-globalThis.ZapFollow.MAX_EVENTS) });
};

chrome.runtime.onMessage.addListener((msg, sender) => {
  if (!msg || msg.zap !== 'store' || !sender.url || new URL(sender.url).origin !== 'https://web.whatsapp.com') return;
  queue = queue.then(() => store(msg.event)).catch(() => {});
});
