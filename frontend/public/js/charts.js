/**
 * charts.js — Chart.js utilities for trend visualization
 */

const CHART_DEFAULTS = {
  colors: {
    blue: 'rgba(59, 130, 246, 1)',
    blueAlpha: 'rgba(59, 130, 246, 0.15)',
    teal: 'rgba(20, 184, 166, 1)',
    tealAlpha: 'rgba(20, 184, 166, 0.15)',
    red: 'rgba(239, 68, 68, 1)',
    redAlpha: 'rgba(239, 68, 68, 0.15)',
    orange: 'rgba(249, 115, 22, 1)',
    yellow: 'rgba(234, 179, 8, 1)',
    green: 'rgba(34, 197, 94, 1)',
    greenAlpha: 'rgba(34, 197, 94, 0.15)',
    muted: 'rgba(100, 116, 139, 0.3)',
  },
  textColor: '#94a3b8',
  gridColor: 'rgba(99, 115, 160, 0.1)',
};

Chart.defaults.color = CHART_DEFAULTS.textColor;
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 12;

/**
 * Create a line chart for risk score trend
 * @param {string} canvasId
 * @param {Array} historyData - array of {recorded_at, risk_score_saat_itu}
 */
function createTrendChart(canvasId, historyData) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;

  const ctx = canvas.getContext('2d');

  // Sort ascending by recorded_at
  const sorted = [...historyData].sort((a, b) =>
    new Date(a.recorded_at) - new Date(b.recorded_at)
  );

  const labels = sorted.map(d => {
    const dt = new Date(d.recorded_at);
    return dt.toLocaleDateString('id-ID', { day: '2-digit', month: 'short', year: '2-digit' });
  });

  const scores = sorted.map(d => {
    const s = d.risk_score_saat_itu ?? d.risk_score ?? null;
    return s !== null ? parseFloat(s).toFixed(1) : null;
  });

  // Destroy existing if any
  const existing = Chart.getChart(canvasId);
  if (existing) existing.destroy();

  return new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Risk Score',
        data: scores,
        borderColor: CHART_DEFAULTS.colors.blue,
        backgroundColor: CHART_DEFAULTS.colors.blueAlpha,
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointBackgroundColor: scores.map(s => {
          if (s === null) return CHART_DEFAULTS.colors.muted;
          const v = parseFloat(s);
          if (v < 25) return CHART_DEFAULTS.colors.green;
          if (v < 50) return CHART_DEFAULTS.colors.yellow;
          if (v < 75) return CHART_DEFAULTS.colors.orange;
          return CHART_DEFAULTS.colors.red;
        }),
        pointRadius: 5,
        pointHoverRadius: 7,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1a2234',
          borderColor: 'rgba(99,115,160,0.3)',
          borderWidth: 1,
          titleColor: '#e2e8f0',
          bodyColor: '#94a3b8',
          callbacks: {
            label: ctx => `Risk Score: ${ctx.parsed.y}`,
          },
        },
      },
      scales: {
        x: {
          grid: { color: CHART_DEFAULTS.gridColor },
          ticks: { maxTicksLimit: 8, maxRotation: 30 },
        },
        y: {
          min: 0,
          max: 100,
          grid: { color: CHART_DEFAULTS.gridColor },
          ticks: {
            callback: v => `${v}`,
          },
        },
      },
      interaction: {
        intersect: false,
        mode: 'index',
      },
    },
  });
}

/**
 * Create a radar chart for 7 PUPR indicator scores
 * @param {string} canvasId
 * @param {Object} latestData
 */
function createIndicatorRadar(canvasId, latestData) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;

  const ctx = canvas.getContext('2d');
  const existing = Chart.getChart(canvasId);
  if (existing) existing.destroy();

  const labels = [
    'Bangunan', 'Jalan', 'Drainase',
    'Air Limbah', 'Sampah', 'Kebakaran', 'Air Minum'
  ];
  const keys = [
    'skor_bangunan', 'skor_jalan', 'skor_drainase',
    'skor_air_limbah', 'skor_sampah', 'skor_kebakaran', 'skor_air_minum'
  ];
  const values = keys.map(k => parseFloat(latestData[k] || 0));

  return new Chart(ctx, {
    type: 'radar',
    data: {
      labels,
      datasets: [{
        label: 'Skor Indikator',
        data: values,
        backgroundColor: 'rgba(59, 130, 246, 0.15)',
        borderColor: CHART_DEFAULTS.colors.blue,
        borderWidth: 2,
        pointBackgroundColor: CHART_DEFAULTS.colors.blue,
        pointRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
      },
      scales: {
        r: {
          min: 0,
          max: 3,
          ticks: {
            stepSize: 1,
            color: CHART_DEFAULTS.textColor,
            backdropColor: 'transparent',
          },
          grid: { color: CHART_DEFAULTS.gridColor },
          angleLines: { color: CHART_DEFAULTS.gridColor },
          pointLabels: {
            color: CHART_DEFAULTS.textColor,
            font: { size: 11 },
          },
        },
      },
    },
  });
}

/**
 * Create a horizontal bar chart for priority ranking
 * @param {string} canvasId
 * @param {Array} data - array of {kelurahan, risk_score}
 */
function createPriorityChart(canvasId, data) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;

  const ctx = canvas.getContext('2d');
  const existing = Chart.getChart(canvasId);
  if (existing) existing.destroy();

  const top = data.slice(0, 10);
  const labels = top.map(d => `${d.kelurahan || d.id_wilayah}`);
  const scores = top.map(d => parseFloat(d.risk_score || 0).toFixed(1));
  const colors = top.map(d => {
    const s = parseFloat(d.risk_score || 0);
    if (s < 25) return CHART_DEFAULTS.colors.green;
    if (s < 50) return CHART_DEFAULTS.colors.yellow;
    if (s < 75) return CHART_DEFAULTS.colors.orange;
    return CHART_DEFAULTS.colors.red;
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Risk Score',
        data: scores,
        backgroundColor: colors,
        borderRadius: 4,
        borderSkipped: false,
      }],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1a2234',
          borderColor: 'rgba(99,115,160,0.3)',
          borderWidth: 1,
          callbacks: {
            label: ctx => `Risk Score: ${ctx.parsed.x}`,
          },
        },
      },
      scales: {
        x: {
          min: 0,
          max: 100,
          grid: { color: CHART_DEFAULTS.gridColor },
        },
        y: {
          grid: { display: false },
        },
      },
    },
  });
}

// Export for use in other files
window.createTrendChart = createTrendChart;
window.createIndicatorRadar = createIndicatorRadar;
window.createPriorityChart = createPriorityChart;
