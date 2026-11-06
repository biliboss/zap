const $ = (id) => document.getElementById(id);
const render = async () => {
  const { follows = [], events = [] } = await chrome.storage.local.get(['follows', 'events']);
  const ul = $('follows');
  ul.replaceChildren(...follows.map((f) => {
    const li = document.createElement('li');
    const s = document.createElement('span');
    s.textContent = f.label ? `${f.label} (${f.id})` : f.id;
    const b = document.createElement('button');
    b.textContent = 'Unfollow';
    b.onclick = async () => {
      await chrome.storage.local.set({ follows: follows.filter((x) => x.id !== f.id) });
      render();
    };
    li.append(s, b);
    return li;
  }));
  $('count').textContent = String(events.length);
};
$('add').onsubmit = async (e) => {
  e.preventDefault();
  const id = $('id').value.trim();
  const { follows = [] } = await chrome.storage.local.get('follows');
  if (id && !follows.some((f) => f.id === id)) {
    follows.push({ id, label: $('label').value.trim() });
    await chrome.storage.local.set({ follows });
  }
  $('id').value = $('label').value = '';
  render();
};
$('clear').onclick = async () => { await chrome.storage.local.set({ events: [] }); render(); };
chrome.storage.onChanged.addListener(render);
render();
