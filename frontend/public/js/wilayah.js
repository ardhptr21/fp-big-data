/**
 * wilayah.js - Wilayah history page logic
 */

const API_BASE = '/api';

function getJSON(url, options = {}) {
  if (window.apiCache?.getJSON) return window.apiCache.getJSON(url, options);
  return fetch(url).then(res => {
    if (!res.ok) throw new Error(`Status ${res.status}`);
    return res.json();
  });
}

function getWilayahIdFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return params.get('id');
}

async function loadWilayahHistory() {
  const id = getWilayahIdFromUrl();
  if (!id) {
    document.getElementById('page-title').textContent = 'ID Wilayah tidak ditemukan';
    return;
  }

  document.getElementById('wilayah-id-display').textContent = id;

  try {
    const historyUrl = `${API_BASE}/wilayah/${encodeURIComponent(id)}/history`;
    const latestUrl = `${API_BASE}/wilayah/${encodeURIComponent(id)}/latest`;
    const [histData, latestData] = await Promise.all([
      getJSON(historyUrl, {
        ttl: 60 * 1000,
        staleTtl: 24 * 60 * 60 * 1000,
        onUpdate: data => renderHistory(data.history || []),
      }),
      getJSON(latestUrl, {
        ttl: 60 * 1000,
        staleTtl: 24 * 60 * 60 * 1000,
        onUpdate: data => {
          renderHeader(id, data);
          if (typeof createIndicatorRadar === 'function') {
            createIndicatorRadar('radar-chart', data);
          }
        },
      }).catch(() => null),
    ]);

    renderHeader(id, latestData);
    renderHistory(histData.history || []);
    renderCharts(histData.history || [], latestData);
  } catch (e) {
    console.error(e);
    document.getElementById('history-table-body').innerHTML =
      `<tr><td colspan="10" style="text-align:center;color:var(--text-muted);">Gagal memuat data: ${e.message}</td></tr>`;
  }
}

function renderHeader(id, latest) {
  const el = document.getElementById('page-title');
  if (!latest) {
    el.textContent = id;
    return;
  }
  const rt = latest.rt || '';
  const rw = latest.rw || '';
  const kel = latest.kelurahan || '';
  const kec = latest.kecamatan || '';
  el.textContent = `RT ${rt} / RW ${rw} - ${kel} - ${kec}`;

  // Latest score card
  const scoreEl = document.getElementById('latest-score');
  if (scoreEl && latest.risk_score !== undefined) {
    const score = parseFloat(latest.risk_score || 0).toFixed(1);
    const level = latest.risk_level || 'Belum Didata';
    scoreEl.innerHTML = `
      <div style="font-size:3rem;font-weight:800;color:var(--text-primary);line-height:1;">${score}</div>
      <div style="font-size:0.75rem;color:var(--text-muted);margin-top:0.25rem;">/100</div>
      <div style="margin-top:0.5rem;"><span class="badge ${getBadgeClass(level)}">${level}</span></div>
    `;
  }
}

function getBadgeClass(level) {
  const map = {
    'Ringan': 'badge-ringan',
    'Sedang': 'badge-sedang',
    'Berat': 'badge-berat',
    'Sangat Berat': 'badge-sangat-berat',
  };
  return map[level] || 'badge-none';
}

function formatDate(dt) {
  if (!dt) return '--';
  try {
    return new Date(dt).toLocaleString('id-ID', {
      day: '2-digit', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  } catch { return dt; }
}

function scoreBadge(v) {
  const colors = ['var(--risk-ringan)', 'var(--risk-sedang)', 'var(--risk-berat)', 'var(--risk-sangat-berat)'];
  const n = parseInt(v);
  const c = colors[Math.min(n, 3)] || 'var(--text-muted)';
  return `<span style="color:${c};font-weight:700;">${n}</span>`;
}

function renderHistory(history) {
  const tbody = document.getElementById('history-table-body');
  const countEl = document.getElementById('history-count');
  if (countEl) countEl.textContent = `${history.length} survei`;

  if (history.length === 0) {
    tbody.innerHTML = `
      <tr><td colspan="10">
        <div class="empty-state">
          <div class="empty-title">Belum ada data survei</div>
          <div class="empty-msg">Kirim data survei pertama untuk wilayah ini.</div>
        </div>
      </td></tr>
    `;
    return;
  }

  tbody.innerHTML = history.map((h, i) => {
    const score = h.risk_score_saat_itu ?? h.risk_score ?? '--';
    const level = h.risk_level_saat_itu ?? h.risk_level ?? 'Belum Didata';
    return `
      <tr>
        <td class="td-primary" style="font-family:monospace;font-size:0.75rem;">${formatDate(h.recorded_at)}</td>
        <td>${scoreBadge(h.skor_bangunan)}</td>
        <td>${scoreBadge(h.skor_jalan)}</td>
        <td>${scoreBadge(h.skor_drainase)}</td>
        <td>${scoreBadge(h.skor_air_limbah)}</td>
        <td>${scoreBadge(h.skor_sampah)}</td>
        <td>${scoreBadge(h.skor_kebakaran)}</td>
        <td>${scoreBadge(h.skor_air_minum)}</td>
        <td class="td-primary">${parseFloat(score).toFixed(1)}</td>
        <td><span class="badge ${getBadgeClass(level)}">${level}</span></td>
        <td style="color:var(--text-muted);font-size:0.75rem;">${h.catatan || '--'}</td>
        <td style="color:var(--text-muted);font-size:0.75rem;">${h.recorded_by || '--'}</td>
      </tr>
    `;
  }).join('');
}

function renderCharts(history, latest) {
  if (typeof createTrendChart === 'function' && history.length > 0) {
    createTrendChart('trend-chart', history);
  }
  if (typeof createIndicatorRadar === 'function' && latest) {
    createIndicatorRadar('radar-chart', latest);
  }
}

document.addEventListener('DOMContentLoaded', loadWilayahHistory);
window.addEventListener('pipeline:complete', (event) => {
  const id = getWilayahIdFromUrl();
  const affected = [
    ...(event.detail?.affected_ids || []),
    ...(event.detail?.pipeline?.affected_ids || []),
    ...(event.detail?.pipeline?.all_affected_ids || []),
  ].filter(Boolean);
  if (!id || affected.length === 0 || affected.includes(id)) {
    loadWilayahHistory();
  }
});
