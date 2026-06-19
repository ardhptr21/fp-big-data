(function () {
  const PREFIX = 'slummap-api-cache:';
  const DEFAULT_TTL = 60 * 1000;
  const DEFAULT_STALE_TTL = 24 * 60 * 60 * 1000;

  function normalizeKey(url) {
    const parsed = new URL(url, window.location.origin);
    return `${parsed.pathname}${parsed.search}`;
  }

  function storageKey(key) {
    return `${PREFIX}${key}`;
  }

  function read(key) {
    try {
      const raw = localStorage.getItem(storageKey(key));
      if (!raw) return null;
      const cached = JSON.parse(raw);
      if (!cached || typeof cached.savedAt !== 'number') return null;
      return cached;
    } catch {
      return null;
    }
  }

  function write(key, data) {
    try {
      localStorage.setItem(storageKey(key), JSON.stringify({
        savedAt: Date.now(),
        data,
      }));
    } catch {
      // Storage can be full or disabled; network data should still render.
    }
  }

  async function fetchFresh(url, fetchOptions = {}) {
    const res = await fetch(url, { ...fetchOptions, cache: 'no-cache' });
    if (!res.ok) throw new Error(`Status ${res.status}`);
    return res.json();
  }

  function changed(a, b) {
    try {
      return JSON.stringify(a) !== JSON.stringify(b);
    } catch {
      return true;
    }
  }

  async function getJSON(url, options = {}) {
    const key = options.cacheKey || normalizeKey(url);
    const ttl = options.ttl ?? DEFAULT_TTL;
    const staleTtl = options.staleTtl ?? DEFAULT_STALE_TTL;
    const cached = read(key);
    const now = Date.now();
    const age = cached ? now - cached.savedAt : Infinity;
    const usableCache = cached && age <= staleTtl;
    const fetchOptions = options.fetchOptions || {};

    const refresh = async () => {
      const fresh = await fetchFresh(url, fetchOptions);
      if (!cached || changed(cached.data, fresh)) {
        write(key, fresh);
        if (cached && typeof options.onUpdate === 'function') {
          options.onUpdate(fresh);
        }
        window.dispatchEvent(new CustomEvent('api-cache:update', {
          detail: { key, url, data: fresh },
        }));
      } else {
        write(key, fresh);
      }
      return fresh;
    };

    if (usableCache && (age <= ttl || options.background !== false)) {
      if (options.background !== false) refresh().catch(() => {});
      return cached.data;
    }

    try {
      return await refresh();
    } catch (err) {
      if (usableCache) return cached.data;
      throw err;
    }
  }

  function invalidate(match) {
    const needle = typeof match === 'string' ? match : null;
    Object.keys(localStorage)
      .filter(key => key.startsWith(PREFIX))
      .filter(key => !needle || key.includes(needle))
      .forEach(key => localStorage.removeItem(key));
  }

  window.apiCache = {
    getJSON,
    invalidate,
    keyFor: normalizeKey,
  };
})();
