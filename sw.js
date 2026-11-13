/* SINUGRAVAS · Sistema de Gestión de Flota · sw.js
   La app de los conductores («?conductor») sin señal (Juan, 12/11/2026). Es el único archivo aparte del
   index.html y se sube una sola vez: no cambia con las actualizaciones de la plataforma.
   1. Guarda la app para abrirla sin señal: primero la pide a internet (y la guarda); si no hay señal, o si
      tarda más de 4 segundos, abre la guardada.
   2. Entrega el manifiesto y el ícono que la app dejó guardados, para instalarla como una app.
   3. Lo que el conductor marcó sin señal queda en el celular (IndexedDB «sigf-conductor»). Cuando vuelve la
      señal, el navegador despierta este archivo («sync») y lo manda, aunque la app esté cerrada. Cada envío
      trae sus pasos ya armados por la app (la dirección, los encabezados y el cuerpo): aquí solo se mandan,
      en orden, y el paso que ya llegó no se repite.
   La plataforma (sin «?conductor») no pasa por aquí: todo sigue igual. */
const CACHE = 'sigf-conductor-v1';

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));

self.addEventListener('fetch', e => {
  const r = e.request;
  if (r.method !== 'GET') return;
  let u;
  try { u = new URL(r.url); } catch (x) { return; }
  if (u.origin !== self.location.origin) return;
  if (r.mode === 'navigate' && u.searchParams.has('conductor')) { e.respondWith(app(e)); return; }
  if (/\/conductor(\.webmanifest|-192\.png|-512\.png)$/.test(u.pathname)) {
    e.respondWith(caches.open(CACHE).then(c => c.match(r.url)).then(x => x || fetch(r)));
  }
});

async function app(e) {
  const cache = await caches.open(CACHE);
  const red = fetch(e.request).then(res => {
    if (res && res.ok) return cache.put('app', res.clone()).then(() => res, () => res);
    return res;
  });
  e.waitUntil(red.catch(() => null));
  const guardada = await cache.match('app');
  if (!guardada) return red;
  /* Sin señal o con señal lenta: la guardada. La de internet, si llega, queda para la próxima vez */
  try {
    const res = await Promise.race([red, new Promise(ok => setTimeout(() => ok(null), 4000))]);
    return res && res.ok ? res : guardada;
  } catch (x) {
    return guardada;
  }
}

self.addEventListener('sync', e => { if (e.tag === 'sigf-cola') e.waitUntil(mandar()); });
self.addEventListener('message', e => { if (e.data && e.data.tipo === 'mandar') e.waitUntil(mandar().catch(() => null)); });

function base() {
  return new Promise((ok, mal) => {
    const q = indexedDB.open('sigf-conductor', 1);
    q.onupgradeneeded = () => q.result.createObjectStore('envios', { keyPath: 'id' });
    q.onsuccess = () => ok(q.result);
    q.onerror = () => mal(q.error);
  });
}
function hacer(db, modo, f) {
  return new Promise((ok, mal) => {
    const t = db.transaction('envios', modo), r = f(t.objectStore('envios'));
    t.oncomplete = () => ok(r && r.result);
    t.onerror = t.onabort = () => mal(t.error);
  });
}
async function mandar() {
  const db = await base();
  const lista = ((await hacer(db, 'readonly', s => s.getAll())) || []).sort((a, b) => a.orden - b.orden);
  let sinRed = false;
  for (const it of lista) {
    if (it.error || (it.espera && it.espera > Date.now())) continue;
    try {
      await uno(it, x => hacer(db, 'readwrite', s => s.put(x)));
      await hacer(db, 'readwrite', s => s.delete(it.id));
    } catch (x) {
      if (x.red) { sinRed = true; break; }
      if (x.luego || x.cedula) continue;
      it.error = x.message;
      await hacer(db, 'readwrite', s => s.put(it));
    }
  }
  const apps = await self.clients.matchAll({ includeUncontrolled: true });
  apps.forEach(c => c.postMessage({ tipo: 'enviado' }));
  /* Sin señal todavía: el navegador lo vuelve a intentar más tarde */
  if (sinRed) throw new Error('Sin señal');
}
async function uno(it, guardar) {
  for (let i = it.hecho || 0; i < it.pasos.length; i++) {
    const p = it.pasos[i];
    let res;
    try {
      res = await fetch(p.url, { method: p.metodo, headers: p.encabezados, body: p.bytes ? new Blob([p.bytes], { type: p.tipo }) : p.cuerpo });
    } catch (x) {
      throw Object.assign(new Error('Sin señal'), { red: true });
    }
    const txt = await res.text().catch(() => '');
    let j = null;
    try { j = txt ? JSON.parse(txt) : null; } catch (x) { /* no era JSON */ }
    if (!res.ok) {
      const ya = (p.okSi || []).includes(res.status) || (p.okTexto && new RegExp(p.okTexto, 'i').test(txt));
      if (!ya) {
        if (res.status >= 500 || res.status === 408 || res.status === 429) throw Object.assign(new Error('El sistema no respondió'), { red: true });
        throw new Error((j && (j.message || j.msg || j.error)) || 'El sistema respondió ' + res.status);
      }
    } else if (p.exige) {
      if (j && j.ok === false && j.motivo === 'cedula') throw Object.assign(new Error('cedula'), { cedula: true });
      if (!j || !(j.recibidos || []).includes(p.exige)) {
        const x = ((j && j.rechazados) || []).find(y => y.id === p.exige);
        throw new Error(x ? x.motivo : (j && j.motivo) || 'El sistema no lo recibió');
      }
    } else if (p.exigeOk && (!j || j.ok !== true)) {
      if (j && j.motivo === 'cedula') throw Object.assign(new Error('cedula'), { cedula: true });
      throw Object.assign(new Error((j && j.motivo) || 'El sistema no lo confirmó'), { luego: true });
    }
    it.hecho = i + 1;
    await guardar(it);
  }
}
