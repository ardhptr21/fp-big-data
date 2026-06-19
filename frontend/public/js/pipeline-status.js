(function () {
  const API_ROOT = '/api';
  const POLL_MS = 5000;
  const RECONNECT_MS = 5000;

  let source = null;
  let reconnectTimer = null;
  let pollTimer = null;
  let connected = false;
  let latestPipeline = null;
  let watch = null;
  let localOverlay = null;

  function isActive(pipeline) {
    if (!pipeline) return false;
    return Boolean(pipeline.running || pipeline.pending || ['queued', 'running'].includes(pipeline.state));
  }

  function progressValue(pipeline) {
    if (!pipeline) return 0;
    const value = Number(pipeline.progress || 0);
    return Math.max(0, Math.min(100, Number.isFinite(value) ? value : 0));
  }

  function statusLabel(pipeline) {
    if (!pipeline) return 'Siap menerima pembaruan';
    if (pipeline.phase_label) return pipeline.phase_label;
    if (pipeline.state === 'queued') return 'Menunggu data siap diproses';
    if (pipeline.state === 'running') return 'Memproses data';
    if (pipeline.state === 'failed') return 'Pemrosesan belum berhasil';
    if (pipeline.state === 'succeeded') return 'Pembaruan selesai';
    return 'Siap menerima pembaruan';
  }

  function createNode(tag, className, attrs = {}) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
    return node;
  }

  function ensureNavStatus() {
    document.querySelectorAll('[data-pipeline-nav-status]').forEach(node => node.remove());
    return null;
  }

  function ensureGlobalPanel() {
    let panel = document.getElementById('pipeline-status-panel');
    if (!panel) {
      panel = createNode('div', 'pipeline-status-panel', {
        id: 'pipeline-status-panel',
        hidden: '',
        'aria-live': 'polite',
      });
      panel.innerHTML = `
        <div class="pipeline-status-title">Status pembaruan data</div>
        <div class="pipeline-status-message" data-pipeline-panel-message></div>
        <div class="pipeline-progress-percent" data-pipeline-panel-percent></div>
        <div class="pipeline-progress-track">
          <div class="pipeline-progress-fill" data-pipeline-panel-progress></div>
        </div>
      `;
      document.body.appendChild(panel);
    }
    return panel;
  }

  function ensureOverlay() {
    let overlay = document.getElementById('pipeline-progress-overlay');
    if (!overlay) {
      overlay = createNode('div', 'pipeline-progress-overlay', {
        id: 'pipeline-progress-overlay',
        hidden: '',
        role: 'status',
        'aria-live': 'polite',
      });
      overlay.innerHTML = `
        <div class="pipeline-progress-card">
          <div class="pipeline-status-title" data-pipeline-overlay-title>Memproses data</div>
          <div class="pipeline-status-message" data-pipeline-overlay-message></div>
          <div class="pipeline-progress-percent" data-pipeline-overlay-percent></div>
          <div class="pipeline-progress-track">
            <div class="pipeline-progress-fill" data-pipeline-overlay-progress></div>
          </div>
          <div class="pipeline-overlay-actions" data-pipeline-overlay-actions></div>
        </div>
      `;
      document.body.appendChild(overlay);
    }
    return overlay;
  }

  function ensureUi() {
    ensureNavStatus();
    ensureGlobalPanel();
    ensureOverlay();
  }

  function renderNav() {
    ensureNavStatus();
  }

  function renderGlobalPanel() {
    const panel = ensureGlobalPanel();
    const active = isActive(latestPipeline);
    panel.hidden = !active || Boolean(watch);
    if (panel.hidden) return;

    const message = panel.querySelector('[data-pipeline-panel-message]');
    const progress = panel.querySelector('[data-pipeline-panel-progress]');
    const percent = panel.querySelector('[data-pipeline-panel-percent]');
    const value = Math.max(progressValue(latestPipeline), 8);
    if (message) message.textContent = statusLabel(latestPipeline);
    if (progress) progress.style.width = `${value}%`;
    if (percent) percent.textContent = `${Math.round(value)}%`;
  }

  function overlayState() {
    if (localOverlay) return localOverlay;
    if (!watch) return null;
    if (!latestPipeline || !isActive(latestPipeline)) {
      return {
        title: watch.title || 'Mengirim data',
        message: 'Menunggu pemrosesan dimulai.',
        progress: 12,
        state: 'queued',
      };
    }
    return {
      title: watch.title || 'Memproses data',
      message: statusLabel(latestPipeline),
      progress: Math.max(progressValue(latestPipeline), 18),
      state: latestPipeline.state,
    };
  }

  function renderOverlay() {
    const overlay = ensureOverlay();
    const state = overlayState();
    overlay.hidden = !state;
    if (!state) return;

    const title = overlay.querySelector('[data-pipeline-overlay-title]');
    const message = overlay.querySelector('[data-pipeline-overlay-message]');
    const progress = overlay.querySelector('[data-pipeline-overlay-progress]');
    const percent = overlay.querySelector('[data-pipeline-overlay-percent]');
    const actions = overlay.querySelector('[data-pipeline-overlay-actions]');
    const value = Math.max(state.progress || 0, 5);
    if (title) title.textContent = state.title || 'Memproses data';
    if (message) message.textContent = state.message || 'Memproses data';
    if (progress) progress.style.width = `${value}%`;
    if (percent) percent.textContent = `${Math.round(value)}%`;
    if (actions) {
      if (state.failed || state.cancelled || state.state === 'completed') {
        actions.innerHTML = '<button type="button" class="btn btn-secondary btn-sm" data-pipeline-close>Tutup</button>';
      } else {
        actions.innerHTML = '<button type="button" class="btn btn-secondary btn-sm" data-pipeline-cancel>Batal</button>';
      }
    }

    overlay.querySelector('[data-pipeline-close]')?.addEventListener('click', clearWatch);
    overlay.querySelector('[data-pipeline-cancel]')?.addEventListener('click', cancelProcessing);
  }

  function render() {
    if (document.readyState === 'loading') return;
    ensureUi();
    renderNav();
    renderGlobalPanel();
    renderOverlay();
  }

  function startPolling() {
    if (pollTimer) return;
    pollTimer = setInterval(fetchStatus, POLL_MS);
  }

  function stopPolling() {
    if (!pollTimer) return;
    clearInterval(pollTimer);
    pollTimer = null;
  }

  async function fetchStatus() {
    try {
      const res = await fetch(`${API_ROOT}/pipeline/status`, { cache: 'no-store' });
      if (!res.ok) return;
      syncPipeline(await res.json());
    } catch (err) {
      console.warn('Status pembaruan gagal dimuat:', err);
    }
  }

  function affectedIdsFrom(detail, pipeline = latestPipeline) {
    const ids = [
      ...(detail?.affected_ids || []),
      ...(pipeline?.affected_ids || []),
      ...(pipeline?.running_affected_ids || []),
      ...(pipeline?.all_affected_ids || []),
    ];
    return [...new Set(ids.filter(Boolean).map(String))];
  }

  function matchesWatch(detail, pipeline = latestPipeline) {
    if (!watch) return false;
    if (!watch.affectedIds || watch.affectedIds.length === 0) return true;
    const ids = new Set(affectedIdsFrom(detail, pipeline));
    return watch.affectedIds.some(id => ids.has(id));
  }

  function isFreshCompletion(pipeline) {
    if (!watch?.minCompletedAt) return true;
    const completedAt = Date.parse(pipeline?.last_completed_at || '');
    return !Number.isFinite(completedAt) || completedAt >= watch.minCompletedAt;
  }

  function completeWatch(detail) {
    if (!watch || !matchesWatch(detail, detail?.pipeline)) return;
    const pipeline = detail?.pipeline || latestPipeline;
    const version = Number(pipeline?.version || 0);
    if (version <= watch.startVersion || !isFreshCompletion(pipeline)) return;
    const redirectTo = watch.redirectTo;
    localOverlay = {
      title: watch.title || 'Pembaruan selesai',
      message: redirectTo
        ? 'Data siap ditampilkan. Membuka peta terbaru...'
        : 'Data selesai diproses dan hasil terbaru sudah tersedia.',
      progress: 100,
      state: 'completed',
    };
    render();
    watch = null;
    if (redirectTo) {
      window.setTimeout(() => {
        window.location.assign(redirectTo);
      }, 900);
    }
  }

  function failWatch(detail) {
    if (!watch || !matchesWatch(detail, detail?.pipeline)) return;
    const pipeline = detail?.pipeline || latestPipeline;
    const version = Number(pipeline?.version || 0);
    if (version <= watch.startVersion || !isFreshCompletion(pipeline)) return;
    localOverlay = {
      title: 'Pemrosesan belum berhasil',
      message: 'Data sudah dikirim, tetapi hasil terbaru belum dapat diterbitkan. Coba lagi setelah layanan siap.',
      progress: 100,
      state: 'failed',
      failed: true,
    };
    watch = null;
    render();
  }

  function syncPipeline(pipeline, detail = null) {
    const previousVersion = Number(latestPipeline?.version || 0);
    latestPipeline = pipeline;

    if (isActive(pipeline) && !connected) startPolling();
    else stopPolling();

    render();

    const nextVersion = Number(pipeline?.version || 0);
    if (pipeline?.state === 'succeeded' && watch && nextVersion > watch.startVersion && nextVersion >= previousVersion && isFreshCompletion(pipeline)) {
      completeWatch(detail || { pipeline, affected_ids: pipeline.affected_ids || [] });
    }
    if (pipeline?.state === 'failed' && watch && nextVersion > watch.startVersion) {
      failWatch(detail || { pipeline, affected_ids: pipeline.affected_ids || [] });
    }
  }

  function handleMessage(data) {
    window.dispatchEvent(new CustomEvent('live:update', { detail: data }));
    if (data.pipeline) syncPipeline(data.pipeline, data);

    if (data.type === 'map_updated') {
      window.dispatchEvent(new CustomEvent('pipeline:complete', { detail: data }));
      completeWatch(data);
    } else if (data.type === 'processing_failed') {
      window.dispatchEvent(new CustomEvent('pipeline:failed', { detail: data }));
      failWatch(data);
    } else if (data.type === 'processing_cancelled') {
      window.dispatchEvent(new CustomEvent('pipeline:cancelled', { detail: data }));
      cancelWatch(data);
    } else if (data.type === 'processing_started') {
      window.dispatchEvent(new CustomEvent('pipeline:started', { detail: data }));
    } else if (data.type === 'processing_queued' || data.type === 'processing_progress') {
      window.dispatchEvent(new CustomEvent('pipeline:progress', { detail: data }));
    }
  }

  function connect() {
    if (source) return;
    source = new EventSource(`${API_ROOT}/stream/updates`);
    source.onopen = () => {
      connected = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      stopPolling();
      render();
    };
    source.onmessage = event => {
      try {
        handleMessage(JSON.parse(event.data));
      } catch (err) {
        console.warn('Pembaruan langsung tidak valid:', err);
      }
    };
    source.onerror = () => {
      connected = false;
      render();
      source.close();
      source = null;
      reconnectTimer = setTimeout(connect, RECONNECT_MS);
      startPolling();
    };
  }

  function beginSubmission(options = {}) {
    const currentVersion = Number(latestPipeline?.version || 0);
    watch = {
      affectedIds: (options.affectedIds || []).map(String),
      redirectTo: options.redirectTo || null,
      startVersion: currentVersion + (isActive(latestPipeline) ? 1 : 0),
      minCompletedAt: Date.now() - 5000,
      title: options.title || 'Mengirim data',
    };
    localOverlay = {
      title: watch.title,
      message: options.message || 'Mengirim data ke server...',
      progress: options.progress || 5,
      state: 'accepted',
    };
    render();
  }

  function awaitCompletion(options = {}) {
    if (!watch) beginSubmission(options);
    watch.affectedIds = (options.affectedIds || watch.affectedIds || []).map(String);
    watch.redirectTo = options.redirectTo ?? watch.redirectTo;
    watch.title = options.title || watch.title;
    if (options.submittedAt) {
      const submittedAt = Date.parse(options.submittedAt);
      if (Number.isFinite(submittedAt)) watch.minCompletedAt = submittedAt - 1000;
    }
    localOverlay = null;
    render();
    fetchStatus();
  }

  function failSubmission(message) {
    localOverlay = {
      title: 'Gagal mengirim data',
      message: message || 'Data belum dapat dikirim. Periksa koneksi dan coba lagi.',
      progress: 100,
      state: 'failed',
      failed: true,
    };
    watch = null;
    render();
  }

  function cancelWatch() {
    localOverlay = {
      title: 'Pemrosesan dibatalkan',
      message: 'Pembaruan dihentikan. Data yang belum diproses tidak diterbitkan ke peta.',
      progress: 100,
      state: 'cancelled',
      cancelled: true,
    };
    watch = null;
    render();
  }

  async function cancelProcessing() {
    localOverlay = {
      title: 'Membatalkan pemrosesan',
      message: 'Mengirim permintaan pembatalan ke server...',
      progress: Math.max(progressValue(latestPipeline), 5),
      state: 'cancelling',
    };
    render();
    try {
      const res = await fetch(`${API_ROOT}/pipeline/cancel`, { method: 'POST' });
      if (!res.ok) throw new Error(`Status ${res.status}`);
      const data = await res.json();
      if (data.pipeline) syncPipeline(data.pipeline);
      if (data.status === 'cancelled' || data.status === 'idle') cancelWatch(data);
    } catch (err) {
      localOverlay = {
        title: 'Gagal membatalkan',
        message: err.message,
        progress: Math.max(progressValue(latestPipeline), 5),
        state: 'failed',
        failed: true,
      };
      render();
    }
  }

  function clearWatch() {
    watch = null;
    localOverlay = null;
    render();
  }

  function createIdempotencyKey(prefix = 'request') {
    if (window.crypto?.randomUUID) return `${prefix}-${window.crypto.randomUUID()}`;
    return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  window.pipelineStatus = {
    connect,
    fetchStatus,
    beginSubmission,
    awaitCompletion,
    failSubmission,
    cancelProcessing,
    clearWatch,
    createIdempotencyKey,
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      ensureUi();
      connect();
      fetchStatus();
    });
  } else {
    ensureUi();
    connect();
    fetchStatus();
  }
})();
