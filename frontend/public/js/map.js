/**
 * map.js - Leaflet.js choropleth map with live updates
 * Pemetaan Permukiman Kumuh Surabaya
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

function standardTileUrl() {
  const theme = document.documentElement.dataset.theme;
  const variant = theme === 'dark' ? 'dark_all' : 'light_all';
  return `https://{s}.basemaps.cartocdn.com/${variant}/{z}/{x}/{y}{r}.png`;
}

function createStandardTileLayer() {
  return L.tileLayer(standardTileUrl(), {
    attribution: '© OpenStreetMap contributors © CARTO',
    subdomains: 'abcd',
    maxZoom: 19,
  });
}

// Base tile layers
const tiles = {
  standard: createStandardTileLayer(),
  satellite: L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: '© Esri',
    maxZoom: 19,
  }),
};
tiles.standard.addTo(map);
window._activeTile = 'standard';

// ============================================================
// RISK COLOR SCHEME
// ============================================================
function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function getRiskColor(level) {
  const colors = {
    'Ringan':       { fill: cssVar('--risk-ringan'), stroke: cssVar('--risk-ringan') },
    'Sedang':       { fill: cssVar('--risk-sedang'), stroke: cssVar('--risk-sedang') },
    'Berat':        { fill: cssVar('--risk-berat'), stroke: cssVar('--risk-berat') },
    'Sangat Berat': { fill: cssVar('--risk-sangat-berat'), stroke: cssVar('--risk-sangat-berat') },
    'Belum Didata': { fill: cssVar('--risk-none'), stroke: cssVar('--risk-none') },
  };
  return colors[level] || colors['Belum Didata'];
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

function getJSON(url, options = {}) {
  if (window.apiCache?.getJSON) return window.apiCache.getJSON(url, options);
  return fetch(url, options.fetchOptions || {}).then(res => {
    if (!res.ok) throw new Error(`Status ${res.status}`);
    return res.json();
  });
}

function activeMapEndpoint() {
  return selectedLayer === 'prediction' ? 'prediction' : 'risk-score';
}

function styleFeature(feature) {
  const props = feature.properties;
  if (selectedLayer === 'prediction') {
    const colors = getPredictionColor(props);
    return {
      fillColor: colors.fill,
      weight: 1.5,
      opacity: 0.9,
      color: colors.stroke,
      fillOpacity: colors.opacity,
      dashArray: props.label_prediksi == null ? '4 4' : null,
    };
  }

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

function getPredictionColor(props) {
  if (props.label_prediksi == null && props.proba_kumuh == null) {
    return { fill: cssVar('--risk-none'), stroke: cssVar('--risk-none'), opacity: 0.35 };
  }

  const proba = Number(props.proba_kumuh);
  if (Number.isFinite(proba)) {
    if (proba >= 0.7) return { fill: cssVar('--risk-sangat-berat'), stroke: cssVar('--risk-sangat-berat'), opacity: 0.72 };
    if (proba >= 0.5) return { fill: cssVar('--risk-berat'), stroke: cssVar('--risk-berat'), opacity: 0.68 };
    return { fill: cssVar('--risk-ringan'), stroke: cssVar('--risk-ringan'), opacity: 0.6 };
  }

  return Number(props.label_prediksi) === 1
    ? { fill: cssVar('--risk-sangat-berat'), stroke: cssVar('--risk-sangat-berat'), opacity: 0.7 }
    : { fill: cssVar('--risk-ringan'), stroke: cssVar('--risk-ringan'), opacity: 0.6 };
}

function getPredictionBadgeClass(props) {
  if (props.label_prediksi == null && props.proba_kumuh == null) return 'badge-none';
  return Number(props.label_prediksi) === 1 || Number(props.proba_kumuh) >= 0.5
    ? 'badge-sangat-berat'
    : 'badge-ringan';
}

function getPredictionLabel(props) {
  if (props.label_prediksi == null && props.proba_kumuh == null) return 'Belum Ada Prediksi';
  return Number(props.label_prediksi) === 1 || Number(props.proba_kumuh) >= 0.5
    ? 'Berpotensi Kumuh'
    : 'Tidak Kumuh';
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
  const score = props.risk_score != null ? `${props.risk_score?.toFixed(1)}` : '--';
  const level = props.risk_level || 'Belum Didata';
  const proba = props.proba_kumuh != null ? `${(props.proba_kumuh * 100)?.toFixed(1)}%` : '--';
  const jiwa = (props.total_jiwa || 0).toLocaleString('id-ID');
  const predictionLabel = getPredictionLabel(props);
  const predictionBadge = getPredictionBadgeClass(props);

  const factors = [props.top_faktor_1, props.top_faktor_2, props.top_faktor_3]
    .filter(Boolean)
    .map(f => `<span style="background:var(--civic-blue-soft);color:var(--civic-blue);padding:0.18rem 0.5rem;border-radius:999px;font-size:0.7rem;font-weight:700;">${f}</span>`)
    .join(' ');

  return `
    <div class="popup-header">
      <div class="popup-title">RT ${props.rt} / RW ${props.rw}</div>
      <div class="popup-subtitle">Kel. ${props.kelurahan} - Kec. ${props.kecamatan}</div>
    </div>
    <div class="popup-body">
      <div class="popup-row">
        <span class="popup-row-label">Tingkat Risiko</span>
        <span class="badge ${getRiskBadgeClass(level)}">${level}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Skor Risiko</span>
        <span class="popup-row-value">${score}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Probabilitas Kumuh</span>
        <span class="popup-row-value">${proba}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Prediksi</span>
        <span class="badge ${predictionBadge}">${predictionLabel}</span>
      </div>
      <div class="popup-row">
        <span class="popup-row-label">Jiwa</span>
        <span class="popup-row-value">${jiwa}</span>
      </div>
      ${factors ? `<div style="margin-top:0.5rem;display:flex;gap:0.25rem;flex-wrap:wrap;">${factors}</div>` : ''}
    </div>
    <div class="popup-footer">
      <a href="/wilayah.html?id=${props.id_wilayah}" class="btn btn-primary btn-sm" style="width:100%;text-align:center;">
        Lihat Riwayat
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

function applyMapPage(data, append, activeToken, offset, fromBackground = false) {
  if (activeToken !== mapLoadToken) return;

  mergeGeoData(data, append);
  nextMapOffset = offset + (data.features || []).length;
  renderMapLayer();
  if (!append) updateSummaryStats();

  if (!fromBackground && data.has_more && (data.features || []).length > 0) {
    setTimeout(() => loadMapData({ append: true, token: activeToken }), 250);
  } else if (!fromBackground) {
    showMapLoading(false);
  }
}

async function loadMapData({ append = false, token = null } = {}) {
  const activeToken = append ? token : ++mapLoadToken;
  const offset = append ? nextMapOffset : 0;
  if (!append) {
    nextMapOffset = 0;
    showMapLoading(true);
  }

  try {
    const url = `${API_BASE}/map/${activeMapEndpoint()}?${getMapQuery(offset)}`;
    const data = await getJSON(url, {
      ttl: 45 * 1000,
      staleTtl: 24 * 60 * 60 * 1000,
      onUpdate: fresh => applyMapPage(fresh, append, activeToken, offset, true),
    });
    applyMapPage(data, append, activeToken, offset);
  } catch (err) {
    console.error('Failed to load map data:', err);
    if (!append && !rawGeoData) showToast('error', 'Gagal memuat peta', err.message);
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
    const data = await getJSON(`${API_BASE}/map/${activeMapEndpoint()}?${params.toString()}`, {
      ttl: 30 * 1000,
      staleTtl: 24 * 60 * 60 * 1000,
      onUpdate: fresh => {
        mergeGeoData(fresh, true);
        renderMapLayer();
      },
    });
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
    list.innerHTML = '<div class="empty-state"><div class="empty-msg">Belum ada data wilayah. Daftarkan wilayah dan masukkan data survei.</div></div>';
    return;
  }

  const sorted = [...features].sort((a, b) => {
    if (selectedLayer === 'prediction') {
      return (b.properties.proba_kumuh || 0) - (a.properties.proba_kumuh || 0);
    }
    return (b.properties.risk_score || 0) - (a.properties.risk_score || 0);
  });

  list.innerHTML = sorted.slice(0, 20).map(f => {
    const p = f.properties;
    const level = p.risk_level || 'Belum Didata';
    const score = p.risk_score != null ? p.risk_score?.toFixed(0) : '--';
    const barClass = getRiskBarClass(level);
    const pct = p.risk_score ? Math.min(p.risk_score, 100) : 0;
    const probaPct = p.proba_kumuh != null ? Math.round(p.proba_kumuh * 100) : null;
    const predictionLabel = getPredictionLabel(p);
    const predictionBadge = getPredictionBadgeClass(p);
    const primaryValue = selectedLayer === 'prediction' ? (probaPct != null ? `${probaPct}%` : '--') : score;
    const primaryBadge = selectedLayer === 'prediction' ? predictionBadge : getRiskBadgeClass(level);
    const barWidth = selectedLayer === 'prediction' ? (probaPct || 0) : pct;
    const barStyle = selectedLayer === 'prediction'
      ? `background:${getPredictionColor(p).fill};`
      : '';
    const subtitle = selectedLayer === 'prediction' ? predictionLabel : level;
    return `
      <div class="layer-item" onclick="focusWilayah('${p.id_wilayah}')" style="padding:0.75rem;border-bottom:1px solid var(--border);cursor:pointer;transition:background 0.15s;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem;">
          <span style="font-size:0.8125rem;font-weight:600;color:var(--text-primary);">RT${p.rt}/RW${p.rw} ${p.kelurahan}</span>
          <span class="badge ${primaryBadge}" style="font-size:0.6875rem;">${primaryValue}</span>
        </div>
        <div style="font-size:0.72rem;color:var(--text-muted);margin-bottom:0.35rem;">${subtitle}</div>
        <div class="risk-bar-container">
          <div class="risk-bar ${barClass}" style="width:${barWidth}%;${barStyle}"></div>
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
    const data = await getJSON(`${API_BASE}/summary`, {
      ttl: 60 * 1000,
      staleTtl: 24 * 60 * 60 * 1000,
      onUpdate: renderSummaryStats,
    });
    renderSummaryStats(data);
  } catch (e) {
    console.warn('Stats update failed:', e);
  }
}

function renderSummaryStats(data) {
  const setEl = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };

  setEl('stat-total-wilayah', (data.total_wilayah || 0).toLocaleString('id-ID'));
  setEl('stat-total-kumuh', (data.total_kumuh || 0).toLocaleString('id-ID'));
  setEl('stat-jiwa', (data.total_jiwa_terdampak || 0).toLocaleString('id-ID'));
  setEl('stat-events', (data.total_survey_events || 0).toLocaleString('id-ID'));
}

// ============================================================
// TOAST NOTIFICATIONS
// ============================================================
function showToast(type, title, msg, duration = 4000) {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const icons = { success: 'SUKSES', error: 'GAGAL', info: 'INFO', warning: 'PERLU CEK' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <span class="toast-icon">${icons[type] || 'INFO'}</span>
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
  document.querySelectorAll('.toggle-btn[data-layer]').forEach(b => {
    b.classList.toggle('active', b.dataset.layer === mode);
  });
  updateLegend();
  showMapLoading(true, mode === 'prediction' ? 'Menampilkan prediksi...' : 'Menampilkan skor risiko...');
  renderMapLayer();
  setTimeout(() => showMapLoading(false), 250);
}
window.setLayerToggle = setLayerToggle;

function updateLegend() {
  const title = document.getElementById('legend-title');
  const legend = document.getElementById('map-legend');
  if (!legend) return;

  if (selectedLayer === 'prediction') {
    if (title) title.textContent = 'Legenda Prediksi';
    legend.innerHTML = `
      <div class="legend-item">
        <div class="legend-color" style="background:var(--risk-ringan);"></div>
        <span>Tidak kumuh (&lt;50%)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color" style="background:var(--risk-berat);"></div>
        <span>Potensi sedang (50-70%)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color" style="background:var(--risk-sangat-berat);"></div>
        <span>Potensi tinggi (>=70%)</span>
      </div>
      <div class="legend-item">
        <div class="legend-color" style="background:var(--risk-none);"></div>
        <span>Belum ada prediksi</span>
      </div>
    `;
    return;
  }

  if (title) title.textContent = 'Legenda Risiko';
  legend.innerHTML = `
    <div class="legend-item">
      <div class="legend-color" style="background:var(--risk-ringan);"></div>
      <span>Ringan (0-25)</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background:var(--risk-sedang);"></div>
      <span>Sedang (25-50)</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background:var(--risk-berat);"></div>
      <span>Berat (50-75)</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background:var(--risk-sangat-berat);"></div>
      <span>Sangat Berat (75-100)</span>
    </div>
    <div class="legend-item">
      <div class="legend-color" style="background:var(--risk-none);"></div>
      <span>Belum Didata</span>
    </div>
  `;
}

function setTileToggle(mode) {
  if (mode === window._activeTile) return;

  if (mode === 'standard') {
    map.removeLayer(tiles.satellite);
    tiles.standard.addTo(map);
  } else if (mode === 'satellite') {
    map.removeLayer(tiles.standard);
    tiles.satellite.addTo(map);
  } else {
    return;
  }

  window._activeTile = mode;
  document.querySelectorAll('.toggle-btn[data-tile]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tile === mode);
  });
}
window.setTileToggle = setTileToggle;

function refreshStandardTiles() {
  if (map.hasLayer(tiles.standard)) {
    map.removeLayer(tiles.standard);
    tiles.standard = createStandardTileLayer();
    tiles.standard.addTo(map);
    return;
  }

  tiles.standard = createStandardTileLayer();
}

// ============================================================
// MAP LOADING INDICATOR
// ============================================================
function showMapLoading(show, label = 'Memuat peta...') {
  const el = document.getElementById('map-loading');
  const text = document.getElementById('map-loading-text');
  if (text) text.textContent = label;
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
  updateLegend();
  map.whenReady(() => loadMapData());

  window.addEventListener('pipeline:complete', (event) => {
    refreshMapAfterPipeline(event.detail?.affected_ids || event.detail?.pipeline?.affected_ids || []);
  });

  window.addEventListener('pipeline:failed', () => {});

  window.addEventListener('live:update', (event) => {
    const data = event.detail || {};
    if (data.type === 'wilayah_registered') {
      loadWilayahByIds(data.affected_ids || [data.id_wilayah]);
    }
  });

  map.on('moveend', debounce(() => {
    selectedPoint = null;
    loadMapData();
  }, 500));

  map.on('click', (e) => {
    selectedPoint = { lat: e.latlng.lat, lng: e.latlng.lng };
    loadMapData();
  });

  // Tile toggle buttons
  document.querySelectorAll('.toggle-btn[data-tile]').forEach(btn => {
    btn.addEventListener('click', () => setTileToggle(btn.dataset.tile));
  });

  // Layer toggle buttons
  document.querySelectorAll('.toggle-btn[data-layer]').forEach(btn => {
    btn.addEventListener('click', () => setLayerToggle(btn.dataset.layer));
  });

  window.addEventListener('themechange', () => {
    refreshStandardTiles();
    renderMapLayer();
  });
});
