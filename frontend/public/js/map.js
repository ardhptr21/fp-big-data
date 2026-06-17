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
  preferCanvas: true,
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
let selectedPoint = null;
let mapLoadToken = 0;
let nextMapOffset = 0;
const MAP_PAGE_SIZE = 50;

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

function pointBounds(latlng, feature) {
  const area = Number(feature.properties?.luas_m2 || 0);
  let sideM = area > 0 ? Math.sqrt(area) : 450;
  sideM = Math.min(Math.max(sideM, 120), 700);
  const halfLat = (sideM / 2) / 111320;
  const metersPerLng = 111320 * Math.max(Math.cos(latlng.lat * Math.PI / 180), 0.2);
  const halfLng = (sideM / 2) / metersPerLng;
  return [
    [latlng.lat - halfLat, latlng.lng - halfLng],
    [latlng.lat + halfLat, latlng.lng + halfLng],
  ];
}

function layerBounds(layer) {
  if (layer.getBounds) return layer.getBounds();
  if (layer.getLatLng) {
    const latlng = layer.getLatLng();
    return L.latLngBounds([latlng, latlng]);
  }
  return null;
}

function buildPopup(props) {
  const score = props.risk_score != null ? `${props.risk_score?.toFixed(1)}` : '—';
  const level = props.risk_level || 'Belum Didata';
  const proba = props.proba_kumuh != null ? `${(props.proba_kumuh * 100)?.toFixed(1)}%` : '—';
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
    click: (e) => L.DomEvent.stopPropagation(e),
  });
}

function getMapQuery(offset = 0) {
  const params = new URLSearchParams({
    limit: String(MAP_PAGE_SIZE),
    offset: String(offset),
  });

  if (selectedPoint) {
    params.set('lat', String(selectedPoint.lat));
    params.set('lng', String(selectedPoint.lng));
  } else {
    const b = map.getBounds();
    params.set('bbox', [
      b.getWest().toFixed(6),
      b.getSouth().toFixed(6),
      b.getEast().toFixed(6),
      b.getNorth().toFixed(6),
    ].join(','));
  }

  return params.toString();
}

function mergeGeoData(data, append) {
  if (!append || !rawGeoData) {
    rawGeoData = {
      type: 'FeatureCollection',
      features: [],
      total: data.total || 0,
      returned: 0,
      has_more: false,
    };
  }

  const indexById = new Map((rawGeoData.features || []).map((f, index) => [f.properties?.id_wilayah, index]));
  (data.features || []).forEach(feature => {
    const id = feature.properties?.id_wilayah;
    if (!id) return;
    if (indexById.has(id)) {
      rawGeoData.features[indexById.get(id)] = feature;
    } else {
      indexById.set(id, rawGeoData.features.length);
      rawGeoData.features.push(feature);
    }
  });

  rawGeoData.total = data.total || rawGeoData.total || 0;
  rawGeoData.returned = rawGeoData.features.length;
  rawGeoData.has_more = Boolean(data.has_more);
}

async function loadMapData({ append = false, token = null } = {}) {
  const activeToken = append ? token : ++mapLoadToken;
  const offset = append ? nextMapOffset : 0;
  if (!append) {
    nextMapOffset = 0;
    showMapLoading(true);
  }

  try {
    const res = await fetch(`${API_BASE}/map/risk-score?${getMapQuery(offset)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (activeToken !== mapLoadToken) return;

    mergeGeoData(data, append);
    nextMapOffset = offset + (data.features || []).length;
    renderMapLayer();
    if (!append) updateSummaryStats();

    if (data.has_more && (data.features || []).length > 0) {
      setTimeout(() => loadMapData({ append: true, token: activeToken }), 250);
    } else {
      showMapLoading(false);
    }
  } catch (err) {
    console.error('Failed to load map data:', err);
    if (!append) showToast('error', 'Gagal memuat peta', err.message);
    showMapLoading(false);
  }
}

async function loadWilayahByIds(ids, { focusFirst = false } = {}) {
  const uniqueIds = [...new Set((ids || []).filter(Boolean))];
  if (uniqueIds.length === 0) return;

  const params = new URLSearchParams({
    ids: uniqueIds.join(','),
    limit: String(Math.max(uniqueIds.length, MAP_PAGE_SIZE)),
    offset: '0',
  });

  try {
    const res = await fetch(`${API_BASE}/map/risk-score?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    mergeGeoData(data, true);
    renderMapLayer();
    if (focusFirst && uniqueIds[0]) {
      focusWilayah(uniqueIds[0], { openPopup: false });
    }
  } catch (err) {
    console.warn('Failed to load affected wilayah:', err);
  }
}

async function refreshMapAfterPipeline(affectedIds = []) {
  selectedPoint = null;
  await loadMapData();
  await loadWilayahByIds(affectedIds, { focusFirst: affectedIds.length === 1 });
}

function renderMapLayer() {
  if (!rawGeoData) return;
  if (geojsonLayer) {
    map.removeLayer(geojsonLayer);
  }
  geojsonLayer = L.geoJSON(rawGeoData, {
    style: styleFeature,
    onEachFeature,
    pointToLayer: (feature, latlng) => L.rectangle(pointBounds(latlng, feature), styleFeature(feature)),
  }).addTo(map);

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
    const score = p.risk_score != null ? p.risk_score?.toFixed(0) : '—';
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

function focusWilayah(id, options = {}) {
  if (!geojsonLayer) return;
  const openPopup = options.openPopup !== false;
  geojsonLayer.eachLayer(layer => {
    if (layer.feature && layer.feature.properties.id_wilayah === id) {
      const bounds = layerBounds(layer);
      if (bounds) map.fitBounds(bounds, { padding: [60, 60], maxZoom: 16 });
      if (openPopup) layer.openPopup();
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
let pipelinePollTimer = null;
let pipelineVersion = null;
let pipelineActive = false;

function isPipelineActive(pipeline) {
  if (!pipeline) return false;
  return Boolean(pipeline.running || pipeline.pending || ['queued', 'running'].includes(pipeline.state));
}

function startPipelinePolling() {
  if (pipelinePollTimer) return;
  pipelinePollTimer = setInterval(fetchPipelineStatus, 5000);
}

function stopPipelinePolling() {
  if (!pipelinePollTimer) return;
  clearInterval(pipelinePollTimer);
  pipelinePollTimer = null;
}

async function fetchPipelineStatus() {
  try {
    const res = await fetch(`${API_BASE}/pipeline/status`, { cache: 'no-store' });
    if (!res.ok) return;
    syncPipelineStatus(await res.json());
  } catch (err) {
    console.warn('Pipeline status check failed:', err);
  }
}

function syncPipelineStatus(pipeline) {
  if (!pipeline) return;

  const wasActive = pipelineActive;
  const active = isPipelineActive(pipeline);
  const version = Number(pipeline.version || 0);
  pipelineActive = active;
  updateProcessingIndicator(active);

  if (active) {
    startPipelinePolling();
  } else {
    stopPipelinePolling();
  }

  if (pipelineVersion == null) {
    pipelineVersion = version;
    return;
  }

  if (!active && version > pipelineVersion && wasActive) {
    pipelineVersion = version;
    refreshMapAfterPipeline(pipeline.affected_ids || []);
    return;
  }

  if (version > pipelineVersion) {
    pipelineVersion = version;
  }
}

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
    pipelineActive = false;
    if (data.pipeline?.version != null) pipelineVersion = Number(data.pipeline.version);
    updateProcessingIndicator(false);
    stopPipelinePolling();
    refreshMapAfterPipeline(data.affected_ids || data.pipeline?.affected_ids || []);
  } else if (data.type === 'processing_failed') {
    showToast('error', 'Pipeline gagal', 'Pemrosesan selesai dengan error. Cek log API/consumer.');
    pipelineActive = false;
    if (data.pipeline?.version != null) pipelineVersion = Number(data.pipeline.version);
    updateProcessingIndicator(false);
    stopPipelinePolling();
  } else if (data.type === 'wilayah_registered') {
    loadWilayahByIds(data.affected_ids || [data.id_wilayah]);
  } else if (data.type === 'processing_started') {
    showToast('info', 'Processing...', 'Pipeline sedang memproses data baru...');
    syncPipelineStatus(data.pipeline || { state: 'running', running: true });
  } else if (data.type === 'processing_queued') {
    updateProcessingIndicator(true);
    syncPipelineStatus(data.pipeline || { state: 'queued', pending: true });
  } else if (data.type === 'heartbeat') {
    // Keep alive, no UI action
  } else if (data.type === 'connected') {
    console.log('SSE stream connected at', data.timestamp);
    syncPipelineStatus(data.pipeline);
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

function debounce(fn, delay = 400) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  map.whenReady(() => loadMapData());
  connectSSE();
  fetchPipelineStatus();

  map.on('moveend', debounce(() => {
    selectedPoint = null;
    loadMapData();
  }, 500));

  map.on('click', (e) => {
    selectedPoint = { lat: e.latlng.lat, lng: e.latlng.lng };
    loadMapData();
  });

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
