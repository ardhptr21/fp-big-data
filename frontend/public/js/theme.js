(function () {
  const STORAGE_KEY = 'slummap-theme';
  const root = document.documentElement;

  function resolveTheme(value) {
    if (value === 'dark' || value === 'light') return value;
    return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function applyTheme(theme) {
    const next = resolveTheme(theme);
    root.dataset.theme = next;
    root.style.colorScheme = next;
    localStorage.setItem(STORAGE_KEY, next);
    document.querySelectorAll('[data-theme-toggle]').forEach(button => {
      button.setAttribute('aria-pressed', next === 'dark' ? 'true' : 'false');
      button.textContent = next === 'dark' ? 'Terang' : 'Gelap';
      button.title = next === 'dark' ? 'Gunakan mode terang' : 'Gunakan mode gelap';
    });
  }

  function installToggle() {
    const nav = document.querySelector('.nav');
    if (!nav || nav.querySelector('[data-theme-toggle]')) return;

    let actions = nav.querySelector('.nav-actions');
    if (!actions) {
      actions = document.createElement('div');
      actions.className = 'nav-actions';
      const status = nav.querySelector('.nav-status');
      if (status) actions.appendChild(status);
      nav.appendChild(actions);
    }

    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'theme-toggle';
    button.setAttribute('data-theme-toggle', '');
    button.addEventListener('click', () => {
      applyTheme(root.dataset.theme === 'dark' ? 'light' : 'dark');
      window.dispatchEvent(new CustomEvent('themechange', { detail: { theme: root.dataset.theme } }));
    });
    actions.appendChild(button);
    applyTheme(root.dataset.theme);
  }

  applyTheme(localStorage.getItem(STORAGE_KEY));

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', installToggle);
  } else {
    installToggle();
  }
})();
