const CACHE_NAME = 'rapidxai-v1';
const SHELL_ASSETS = ['/', '/index.html', '/static/js/main.js', '/static/css/main.css'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_ASSETS)));
});

self.addEventListener('fetch', (e) => {
  if (e.request.url.includes('/api/')) {
    e.respondWith(
      fetch(e.request).catch(
        () =>
          new Response(JSON.stringify({ data: null, error: 'offline' }), {
            headers: { 'Content-Type': 'application/json' },
          })
      )
    );
    return;
  }

  e.respondWith(caches.match(e.request).then((cached) => cached || fetch(e.request)));
});
