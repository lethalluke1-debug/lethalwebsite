// Lets Chrome install Lethal Chat as an app. Always loads the newest page when online.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', e => {
  if (e.request.mode !== 'navigate') return;
  e.respondWith(fetch(e.request).catch(() => new Response('<p style="font:16px system-ui;padding:20px">You are offline. Lethal Chat needs the internet to show chat.</p>', { headers: { 'content-type': 'text/html' } })));
});
