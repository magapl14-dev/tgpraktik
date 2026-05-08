// Service Worker — базовый кеш статики для оффлайн-фоллбека.
// Web Push добавится отдельным этапом. Версию кеша поднимаем при крупных правках,
// чтобы старые клиенты получили свежий HTML.
const CACHE = 'practices-v1';
const PRECACHE = [
  '/app',
  '/login',
  '/manifest.webmanifest',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/icon-180.png',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) => cache.addAll(PRECACHE).catch(() => {}))
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);

  // API-запросы и аутентификация — всегда через сеть, не кешируем.
  if (url.pathname.startsWith('/api/') || url.pathname === '/sw.js') return;
  // Фото практик — лучше всегда из сети, чтобы свежие подгружались.
  if (url.pathname.startsWith('/photos/')) return;

  // Network-first с fallback на кеш — для HTML/статики.
  event.respondWith(
    fetch(event.request)
      .then((res) => {
        if (res && res.status === 200 && res.type === 'basic') {
          const cloned = res.clone();
          caches.open(CACHE).then((c) => c.put(event.request, cloned));
        }
        return res;
      })
      .catch(() => caches.match(event.request).then((hit) => hit || caches.match('/app')))
  );
});
