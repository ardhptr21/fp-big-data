/**
 * input.js — Survey form logic
 */

const API_BASE = '/api';
const optionCache = new Map();

// ============================================================
// LOAD WILAYAH
// ============================================================
async function loadWilayah() {
  try {
    const kecs = await fetchWilayahOptions('kecamatan');
    setSelectOptions(document.getElementById('sel-kecamatan'), '— Pilih Kecamatan —', kecs);
  } catch (e) {
    console.error('Failed to load wilayah:', e);
    showToast('error', 'Gagal memuat daftar wilayah', e.message);
  }
}

async function fetchWilayahOptions(level, filters = {}) {
  const params = new URLSearchParams({ level });
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const cacheKey = params.toString();
  if (optionCache.has(cacheKey)) return optionCache.get(cacheKey);

  const res = await fetch(`${API_BASE}/wilayah/options?${cacheKey}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  const options = data.options || [];
  optionCache.set(cacheKey, options);
  return options;
}

function setSelectOptions(select, placeholder, options, labelFn = v => v) {
  if (!select) return;
  select.innerHTML = '';
  select.add(new Option(placeholder, ''));
  options.forEach(value => {
    select.add(new Option(labelFn(value), value));
  });
}

function setSelectLoading(select, label = 'Memuat...') {
  if (!select) return;
  select.innerHTML = '';
  select.add(new Option(label, ''));
}

function resetWilayahSelection(from = 'kecamatan') {
  const levels = {
    kecamatan: [
      ['sel-kelurahan', '— Pilih Kecamatan dulu —'],
      ['sel-rw', '— Pilih Kelurahan dulu —'],
      ['sel-rt', '— Pilih RW dulu —'],
    ],
    kelurahan: [
      ['sel-rw', '— Pilih Kelurahan dulu —'],
      ['sel-rt', '— Pilih RW dulu —'],
    ],
    rw: [
      ['sel-rt', '— Pilih RW dulu —'],
    ],
  };
  (levels[from] || []).forEach(([id, placeholder]) => {
    setSelectOptions(document.getElementById(id), placeholder, []);
  });
  document.getElementById('selected-wilayah-id').value = '';
  const preview = document.getElementById('wilayah-preview');
  if (preview) preview.textContent = 'Pilih kecamatan → kelurahan → RW → RT di atas';
}

async function onKecamatanChange() {
  const kec = document.getElementById('sel-kecamatan').value;
  const sel = document.getElementById('sel-kelurahan');
  resetWilayahSelection('kecamatan');
  if (!kec) return;
  setSelectLoading(sel);
  try {
    const kels = await fetchWilayahOptions('kelurahan', { kecamatan: kec });
    setSelectOptions(sel, '— Pilih Kelurahan —', kels);
  } catch (e) {
    showToast('error', 'Gagal mengambil data kelurahan', e.message);
    setSelectOptions(sel, '— Pilih Kelurahan —', []);
  }
}

async function onKelurahanChange() {
  const kec = document.getElementById('sel-kecamatan').value;
  const kel = document.getElementById('sel-kelurahan').value;
  const sel = document.getElementById('sel-rw');
  resetWilayahSelection('kelurahan');
  if (!kec || !kel) return;
  setSelectLoading(sel);
  try {
    const rws = await fetchWilayahOptions('rw', { kecamatan: kec, kelurahan: kel });
    setSelectOptions(sel, '— Pilih RW —', rws, r => `RW ${r}`);
  } catch (e) {
    showToast('error', 'Gagal mengambil data RW', e.message);
    setSelectOptions(sel, '— Pilih RW —', []);
  }
}

async function onRwChange() {
  const kec = document.getElementById('sel-kecamatan').value;
  const kel = document.getElementById('sel-kelurahan').value;
  const rw = document.getElementById('sel-rw').value;
  const sel = document.getElementById('sel-rt');
  resetWilayahSelection('rw');
  if (!kec || !kel || !rw) return;
  setSelectLoading(sel);
  try {
    const rts = await fetchWilayahOptions('rt', { kecamatan: kec, kelurahan: kel, rw });
    setSelectOptions(sel, '— Pilih RT —', rts, r => `RT ${r}`);
  } catch (e) {
    showToast('error', 'Gagal mengambil data RT', e.message);
    setSelectOptions(sel, '— Pilih RT —', []);
  }
}

async function onRtChange() {
  const kec = document.getElementById('sel-kecamatan').value;
  const kel = document.getElementById('sel-kelurahan').value;
  const rw = document.getElementById('sel-rw').value;
  const rt = document.getElementById('sel-rt').value;
  document.getElementById('selected-wilayah-id').value = '';
  if (!kec || !kel || !rw || !rt) return;

  const params = new URLSearchParams({ kecamatan: kec, kelurahan: kel, rw, rt });
  try {
    const res = await fetch(`${API_BASE}/wilayah/lookup?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const found = await res.json();
    document.getElementById('selected-wilayah-id').value = found.id_wilayah;
    document.getElementById('wilayah-preview').innerHTML =
      `<span style="color:var(--accent-teal);">✓ ${found.id_wilayah}</span> &nbsp; RT ${rt} / RW ${rw} &nbsp;·&nbsp; ${kel} &nbsp;·&nbsp; ${kec}`;
  } catch (e) {
    showToast('error', 'Gagal mengambil data wilayah', e.message);
  }
}

// ============================================================
// COMPUTE LIVE SCORE PREVIEW
// ============================================================
function computePreviewScore() {
  const indicators = [
    'skor_bangunan', 'skor_jalan', 'skor_drainase',
    'skor_air_limbah', 'skor_sampah', 'skor_kebakaran', 'skor_air_minum'
  ];
  let total = 0;
  let count = 0;
  for (const ind of indicators) {
    const checked = document.querySelector(`input[name="${ind}"]:checked`);
    if (checked) {
      total += parseInt(checked.value);
      count++;
    }
  }
  if (count === 0) return;

  const score = (total / (count * 3)) * 100;
  const el = document.getElementById('score-preview');
  if (!el) return;

  let level = 'Belum Lengkap';
  let cls = 'badge-none';
  if (count === 7) {
    if (score < 25) { level = 'Ringan'; cls = 'badge-ringan'; }
    else if (score < 50) { level = 'Sedang'; cls = 'badge-sedang'; }
    else if (score < 75) { level = 'Berat'; cls = 'badge-berat'; }
    else { level = 'Sangat Berat'; cls = 'badge-sangat-berat'; }
  }

  el.innerHTML = `
    <span style="font-size:1.5rem;font-weight:700;color:var(--text-primary);">${score.toFixed(0)}</span>
    <span style="font-size:0.75rem;color:var(--text-muted);">/100</span>
    <span class="badge ${cls}" style="margin-left:0.5rem;">${level}</span>
    <span style="font-size:0.75rem;color:var(--text-muted);margin-left:0.5rem;">(${count}/7 indikator)</span>
  `;
}

// ============================================================
// SUBMIT
// ============================================================
async function submitSurvey(e) {
  e.preventDefault();

  const id_wilayah = document.getElementById('selected-wilayah-id').value;
  if (!id_wilayah) {
    showToast('warning', 'Pilih wilayah', 'Silakan pilih Kecamatan → Kelurahan → RW → RT terlebih dahulu.');
    return;
  }

  const getRadio = name => {
    const checked = document.querySelector(`input[name="${name}"]:checked`);
    return checked ? parseInt(checked.value) : null;
  };

  const indicators = {
    skor_bangunan: getRadio('skor_bangunan'),
    skor_jalan: getRadio('skor_jalan'),
    skor_drainase: getRadio('skor_drainase'),
    skor_air_limbah: getRadio('skor_air_limbah'),
    skor_sampah: getRadio('skor_sampah'),
    skor_kebakaran: getRadio('skor_kebakaran'),
    skor_air_minum: getRadio('skor_air_minum'),
  };

  const missing = Object.entries(indicators).filter(([, v]) => v === null).map(([k]) => k);
  if (missing.length > 0) {
    showToast('warning', 'Form belum lengkap', `Isi semua 7 indikator (${missing.join(', ')})`);
    return;
  }

  const payload = {
    id_wilayah,
    ...indicators,
    jumlah_kk: parseInt(document.getElementById('jumlah_kk')?.value || 0),
    jumlah_jiwa: parseInt(document.getElementById('jumlah_jiwa')?.value || 0),
    pernah_banjir: document.getElementById('pernah_banjir')?.value === 'true',
    frekuensi_banjir: parseInt(document.getElementById('frekuensi_banjir')?.value || 0),
    sosek_dominan: document.getElementById('sosek_dominan')?.value || 'menengah',
    catatan: document.getElementById('catatan')?.value || '',
    recorded_by: document.getElementById('recorded_by')?.value || '',
  };

  const btn = document.getElementById('submit-btn');
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<div class="spinner"></div> Mengirim...';
  }

  try {
    const res = await fetch(`${API_BASE}/survey`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'Submit failed');
    }

    const data = await res.json();
    showToast('success', 'Data diterima!',
      `Event ID: ${data.event_id?.slice(0, 8)}... — Pipeline akan berjalan otomatis setelah data masuk Bronze.`);

    // Show success banner
    const banner = document.getElementById('success-banner');
    if (banner) {
      banner.innerHTML = `
        <div style="background:rgba(34,197,94,0.1);border:1px solid rgba(34,197,94,0.3);border-radius:10px;padding:1rem 1.25rem;margin-top:1.5rem;display:flex;align-items:center;gap:0.75rem;">
          <span style="font-size:1.5rem;">✅</span>
          <div>
            <div style="font-weight:600;color:var(--text-primary);">Data berhasil dikirim!</div>
            <div style="font-size:0.8125rem;color:var(--text-secondary);">
              Event ID: <code style="font-family:monospace;color:var(--accent-teal);">${data.event_id}</code><br>
              Pipeline Spark akan berjalan otomatis dan peta diperbarui setelah proses selesai.
            </div>
          </div>
          <a href="/" class="btn btn-sm btn-secondary" style="margin-left:auto;">🗺️ Lihat Peta</a>
        </div>
      `;
      banner.style.display = 'block';
    }

  } catch (err) {
    showToast('error', 'Gagal mengirim data', err.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '🚀 Submit Data Survei';
    }
  }
}

// ============================================================
// TOAST (if not already defined by map.js)
// ============================================================
if (!window.showToast) {
  window.showToast = function(type, title, msg, duration = 4000) {
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
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
    }, duration);
  };
}

// ============================================================
// INIT
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  loadWilayah();

  document.getElementById('sel-kecamatan')?.addEventListener('change', onKecamatanChange);
  document.getElementById('sel-kelurahan')?.addEventListener('change', onKelurahanChange);
  document.getElementById('sel-rw')?.addEventListener('change', onRwChange);
  document.getElementById('sel-rt')?.addEventListener('change', onRtChange);
  document.getElementById('survey-form')?.addEventListener('submit', submitSurvey);

  // Live score preview
  document.querySelectorAll('input[type="radio"]').forEach(r => {
    r.addEventListener('change', computePreviewScore);
  });
});
