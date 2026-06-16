/**
 * map.js — Leaflet.js choropleth map with real-time SSE updates
 * Geospatial Big Data Analytics - Pemetaan Permukiman Kumuh Surabaya
 */

const API_BASE = '/api';

// ============================================================
// MAP INITIALIZATION
// ============================================================
const map = L.map('main-map', {
  center: [-7.2575, 112.7521],
  zoom: 12,
  zoomControl: true,
  preferCanvas: false,
});

// Base tile layers
const tiles = {
  dark: L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '© OpenStreetMap contributors © CARTO',
    subdomains: 'abcd',
    maxZoom: 19,
  }),
  satellite: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: '© Esri',
    maxZoom: 19,
  }),
};
tiles.dark.addTo(map);
window._activeTile = 'dark';

// ============================================================
// RISK COLOR SCHEME
// ============================================================
const RISK_COLORS = {
  'Ringan':       { fill: '#22c55e', stroke: '#16a34a' },
  'Sedang':       { fill: '#eab308', stroke: '#ca8a04' },
  'Berat':        { fill: '#f97316', stroke: '#ea580c' },
  'Sangat Berat': { fill: '#ef4444', stroke: '#dc2626' },
  'Belum Didata': { fill: '#475569', stroke: '#334155' },
};

function getRiskColor(level) {
  return RISK_COLORS[level] || RISK_COLORS['Belum Didata'];
}

function getRiskBadgeClass(level) {
  const map = {
    'Ringan': 'badge-ringan',
    'Sedang': 'badge-sedang',
    'Berat': 'badge-berat',
    'Sangat Berat': 'badge-sangat-berat',
  };
  return map[level] || 'badge-none';
}

function getRiskBarClass(level) {
  const map = {
    'Ringan': 'ringan',
    'Sedang': 'sedang',
    'Berat': 'berat',
    'Sangat Berat': 'sangat-berat',
  };
  return map[level] || 'none';
}

// ============================================================
// GEOJSON LAYER
// ============================================================
let geojsonLayer = null;
let rawGeoData = null;
let selectedLayer = 'risk'; // 'risk' | 'prediction'

function styleFeature(feature) {
  const props = feature.properties;
  const level = props.risk_level || 'Belum Didata';
  const colors = getRiskColor(level);
  return {
    fillColor: colors.fill,
    weight: 1.5,
    opacity: 0.85,
    color: colors.stroke,
    fillOpacity: 0.65,
  };
}

function highlightFeature(e) {
  const layer = e.target;
  layer.setStyle({
    weight: 2.5,
    opacity: 1,
    fillOpacity: 0.85,
  });
  layer.bringToFront();
}

function resetHighlight(e) {
  geojsonLayer.resetStyle(e.target);
}

function buildPopup(props) {
  const score = props.risk_score !== null ? `${props.risk_score?.toFixed(1)}` : '—';
  const level = props.risk_level || 'Belum Didata';
  const proba = props.proba_kumuh !== null ? `${(props.proba_kumuh * 100)?.toFixed(1)}%` : '—';
  const jiwa = (props.total_jiwa || 0).toLocaleString('id-ID');

  const factors = [props.top_faktor_1, props.top_faktor_2, props.top_faktor_3]
    .filter(Boolean)
    .map(f => `<span style="background:rgba(59,130,246,0.15);color:#93c5fd;padding:0.15rem 0.5rem;border-radius:4px;font-size:0.7rem;">${f}</span>`)
    .join(' ');

  return `
    <div class="popup-header">
      <div class="popup-title">RT ${props.rt} / RW ${props.rw}</div>
      <div class="popup-subtitle">Kel. ${props.kelurahan} · Kec. ${props.kecamatan}</div>
    </div>
    <div class="popup-body">
      <div class="popup-row">
        <span class="popup-row-label">Risk Level</span>
        <span class="badge ${getRiskBadgeClass(level)}">${level}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Risk Score</span>
        <span class="popup-row-value">${score}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Prob. Kumuh</span>
        <span class="popup-row-value">${proba}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Jiwa</span>
        <span class="popup-row-value">${jiwa}</span>
      </div>
      ${factors ? `<div style="margin-top:0.5rem;display:flex;gap:0.25rem;flex-wrap:wrap;">${factors}</div>` : ''}
    </div>
    <div class="popup-footer">
      <a href="/wilayah.html?id=${props.id_wilayah}" class="btn btn-primary btn-sm" style="width:100%;text-align:center;">
        📊 Lihat Riwayat
      </a>
    </div>
  `;
}

function onEachFeature(feature, layer) {
  const props = feature.properties;
  layer.bindPopup(buildPopup(props), { maxWidth: 280 });

  layer.on({
    mouseover: highlightFeature,
    mouseout: resetHighlight,
  });
}

async function loadMapData() {
  showMapLoading(true);
  try {
    const res = await fetch(`${API_BASE}/map/risk-score`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    rawGeoData = await res.json();
    renderMapLayer();
    updateSummaryStats();
  } catch (err) {
    console.error('Failed to load map data:', err);
    showToast('error', 'Gagal memuat peta', err.message);
  } finally {
    showMapLoading(false);
  }
}

function renderMapLayer() {
  if (!rawGeoData) return;
  if (geojsonLayer) {
    map.removeLayer(geojsonLayer);
  }
  geojsonLayer = L.geoJSON(rawGeoData, {
    style: styleFeature,
    onEachFeature,
  }).addTo(map);

  if (rawGeoData.features && rawGeoData.features.length > 0) {
    try {
      map.fitBounds(geojsonLayer.getBounds(), { padding: [20, 20] });
    } catch (e) { /* ignore */ }
  }

  updateLayerList(rawGeoData.features || []);
}

// ============================================================
// SIDEBAR
// ============================================================
function updateLayerList(features) {
  const list = document.getElementById('layer-list');
  if (!list) return;

  if (!features || features.length === 0) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">🗺️</div><div class="empty-msg">Belum ada data wilayah. Daftarkan wilayah dan masukkan data survei.</div></div>';
    return;
  }

  // Sort by risk_score desc
  const sorted = [...features].sort((a, b) => {
    return (b.properties.risk_score || 0) - (a.properties.risk_score || 0);
  });

  list.innerHTML = sorted.slice(0, 20).map(f => {
    const p = f.properties;
    const level = p.risk_level || 'Belum Didata';
    const score = p.risk_score !== null ? p.risk_score?.toFixed(0) : '—';
    const barClass = getRiskBarClass(level);
    const pct = p.risk_score ? Math.min(p.risk_score, 100) : 0;
    return `
      <div class="layer-item" onclick="focusWilayah('${p.id_wilayah}')" style="padding:0.75rem;border-bottom:1px solid var(--border);cursor:pointer;transition:background 0.15s;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem;">
          <span style="font-size:0.8125rem;font-weight:600;color:var(--text-primary);">RT${p.rt}/RW${p.rw} ${p.kelurahan}</span>
          <span class="badge ${getRiskBadgeClass(level)}" style="font-size:0.6875rem;">${score}</span>
        </div>
        <div class="risk-bar-container">
          <div class="risk-bar ${barClass}" style="width:${pct}%;"></div>
        </div>
      </div>
    `;
  }).join('');
}

function focusWilayah(id) {
  if (!geojsonLayer) return;
  geojsonLayer.eachLayer(layer => {
    if (layer.feature && layer.feature.properties.id_wilayah === id) {
      map.fitBounds(layer.getBounds(), { padding: [60, 60] });
      layer.openPopup();
    }
  });
}
window.focusWilayah = focusWilayah;

// ============================================================
// SUMMARY STATS
// ============================================================
async function updateSummaryStats() {
  try {
    const res = await fetch(`${API_BASE}/summary`);
    if (!res.ok) return;
    const data = await res.json();

    const setEl = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };

    setEl('stat-total-wilayah', (data.total_wilayah || 0).toLocaleString('id-ID'));
    setEl('stat-total-kumuh', (data.total_kumuh || 0).toLocaleString('id-ID'));
    setEl('stat-jiwa', (data.total_jiwa_terdampak || 0).toLocaleString('id-ID'));
    setEl('stat-events', (data.total_survey_events || 0).toLocaleString('id-ID'));
  } catch (e) {
    console.warn('Stats update failed:', e);
  }
}

// ============================================================
// REAL-TIME SSE
// ============================================================
let sseSource = null;
let reconnectTimer = null;

function connectSSE() {
  if (sseSource) {
    sseSource.close();
  }

  sseSource = new EventSource(`${API_BASE}/stream/updates`);

  sseSource.onopen = () => {
    console.log('SSE connected');
    updateSSEStatus(true);
    if (reconnectTimer) clearTimeout(reconnectTimer);
  };

  sseSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleSSEMessage(data);
    } catch (e) {
      console.warn('SSE parse error:', e);
    }
  };

  sseSource.onerror = () => {
    updateSSEStatus(false);
    sseSource.close();
    // Reconnect after 5 seconds
    reconnectTimer = setTimeout(connectSSE, 5000);
  };
}

function handleSSEMessage(data) {
  console.log('SSE event:', data.type, data);

  if (data.type === 'map_updated') {
    showToast('success', 'Peta diperbarui', 'Data baru telah diproses dan peta telah diupdate.');
    loadMapData();
  } else if (data.type === 'processing_started') {
    showToast('info', 'Processing...', 'Pipeline sedang memproses data baru...');
    updateProcessingIndicator(true);
  } else if (data.type === 'heartbeat') {
    // Keep alive, no UI action
  } else if (data.type === 'connected') {
    console.log('SSE stream connected at', data.timestamp);
  }
}

function updateSSEStatus(live) {
  const dot = document.getElementById('sse-status-dot');
  const label = document.getElementById('sse-status-label');
  if (dot) dot.className = `status-dot ${live ? 'live' : ''}`;
  if (label) label.textContent = live ? 'Live' : 'Reconnecting...';
}

function updateProcessingIndicator(active) {
  const el = document.getElementById('processing-indicator');
  if (el) {
    el.style.display = active ? 'flex' : 'none';
  }
}

// ============================================================
// TOAST NOTIFICATIONS
// ============================================================
function showToast(type, title, msg, duration = 4000) {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span class="toast-icon">${icons[type] || 'ℹ️'}</span>
    <div class="toast-content">
      <div class="toast-title">${title}</div>
      ${msg ? `<div class="toast-msg">${msg}</div>` : ''}
    </div>
  `;
  container.appendChild(toast);

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(100%)';
    toast.style.transition = 'all 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, duration);
}
window.showToast = showToast;

// ============================================================
// LAYER TOGGLE
// ============================================================
function setLayerToggle(mode) {
  selectedLayer = mode;
  document.querySelectorAll('.toggle-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.layer === mode);
  });
  renderMapLayer();
}
window.setLayerToggle = setLayerToggle;

// ============================================================
// MAP LOADING INDICATOR
// ============================================================
function showMapLoading(show) {
  const el = document.getElementById('map-loading');
  if (el) el.style.display = show ? 'flex' : 'none';
}

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  loadMapData();
  connectSSE();

  // Toggle tile buttons
  document.getElementById('btn-tile-dark')?.addEventListener('click', () => {
    if (window._activeTile !== 'dark') {
      map.removeLayer(tiles.satellite);
      tiles.dark.addTo(map);
      window._activeTile = 'dark';
    }
  });

  document.getElementById('btn-tile-satellite')?.addEventListener('click', () => {
    if (window._activeTile !== 'satellite') {
      map.removeLayer(tiles.dark);
      tiles.satellite.addTo(map);
      window._activeTile = 'satellite';
    }
  });

  // Layer toggle buttons
  document.querySelectorAll('.toggle-btn[data-layer]').forEach(btn => {
    btn.addEventListener('click', () => setLayerToggle(btn.dataset.layer));
  });
});
