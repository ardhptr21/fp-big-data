/**
 * charts.js - Chart.js utilities for trend visualization
 */

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function chartTheme() {
  return {
    colors: {
      blue: cssVar('--civic-blue'),
      blueAlpha: cssVar('--civic-blue-soft'),
      teal: cssVar('--civic-green'),
      tealAlpha: cssVar('--civic-green-soft'),
      red: cssVar('--risk-sangat-berat'),
      redAlpha: cssVar('--risk-sangat-berat-bg'),
      orange: cssVar('--risk-berat'),
      yellow: cssVar('--risk-sedang'),
      green: cssVar('--risk-ringan'),
      greenAlpha: cssVar('--risk-ringan-bg'),
      muted: cssVar('--risk-none'),
    },
    textColor: cssVar('--text-muted'),
    gridColor: cssVar('--line-faint'),
    tooltipBg: cssVar('--surface'),
    tooltipBorder: cssVar('--line-strong'),
    tooltipTitle: cssVar('--text-primary'),
    tooltipBody: cssVar('--text-secondary'),
  };
}

const CHART_DEFAULTS = chartTheme();

Chart.defaults.color = CHART_DEFAULTS.textColor;
Chart.defaults.font.family = '"Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
Chart.defaults.font.size = 12;

/**
 * Create a line chart for risk score trend
 * @param {string} canvasId
 * @param {Array} historyData - array of {recorded_at, risk_score_saat_itu}
 */
function createTrendChart(canvasId, historyData) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  const theme = chartTheme();

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
        label: 'Skor Risiko',
        data: scores,
        borderColor: theme.colors.blue,
        backgroundColor: theme.colors.blueAlpha,
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointBackgroundColor: scores.map(s => {
          if (s === null) return theme.colors.muted;
          const v = parseFloat(s);
          if (v < 25) return theme.colors.green;
          if (v < 50) return theme.colors.yellow;
          if (v < 75) return theme.colors.orange;
          return theme.colors.red;
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
          backgroundColor: theme.tooltipBg,
          borderColor: theme.tooltipBorder,
          borderWidth: 1,
          titleColor: theme.tooltipTitle,
          bodyColor: theme.tooltipBody,
          callbacks: {
            label: ctx => `Skor Risiko: ${ctx.parsed.y}`,
          },
        },
      },
      scales: {
        x: {
          grid: { color: theme.gridColor },
          ticks: { maxTicksLimit: 8, maxRotation: 30 },
        },
        y: {
          min: 0,
          max: 100,
          grid: { color: theme.gridColor },
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
  const theme = chartTheme();

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
        backgroundColor: theme.colors.tealAlpha,
        borderColor: theme.colors.teal,
        borderWidth: 2,
        pointBackgroundColor: theme.colors.teal,
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
            color: theme.textColor,
            backdropColor: cssVar('--transparent'),
          },
          grid: { color: theme.gridColor },
          angleLines: { color: theme.gridColor },
          pointLabels: {
            color: theme.textColor,
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
  const theme = chartTheme();

  const ctx = canvas.getContext('2d');
  const existing = Chart.getChart(canvasId);
  if (existing) existing.destroy();

  const top = data.slice(0, 10);
  const labels = top.map(d => `${d.kelurahan || d.id_wilayah}`);
  const scores = top.map(d => parseFloat(d.risk_score || 0).toFixed(1));
  const colors = top.map(d => {
    const s = parseFloat(d.risk_score || 0);
    if (s < 25) return theme.colors.green;
    if (s < 50) return theme.colors.yellow;
    if (s < 75) return theme.colors.orange;
    return theme.colors.red;
  });

  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Skor Risiko',
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
          backgroundColor: theme.tooltipBg,
          borderColor: theme.tooltipBorder,
          borderWidth: 1,
          titleColor: theme.tooltipTitle,
          bodyColor: theme.tooltipBody,
          callbacks: {
            label: ctx => `Skor Risiko: ${ctx.parsed.x}`,
          },
        },
      },
      scales: {
        x: {
          min: 0,
          max: 100,
          grid: { color: theme.gridColor },
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

window.addEventListener('themechange', () => {
  if (typeof Chart === 'undefined') return;
  const charts = Object.keys(Chart.instances || {});
  if (charts.length > 0) window.location.reload();
});
