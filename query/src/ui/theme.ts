// The page's theme, as upstream Legend Query has it: DARK unless the person picks light (its
// sun/moon switch: default-dark <-> legacy-light; the system's setting plays no part). The choice
// is kept in this browser. The embedded DataCube's theme (`data-dc-theme`,
// datacube/src/theme.css) is kept the same.

const KEY = 'legend-query.theme';

function stored(): 'light' | 'dark' {
  try {
    return localStorage.getItem(KEY) === 'light' ? 'light' : 'dark';
  } catch {
    return 'dark';     // storage refused (a private window): the default
  }
}

function apply(theme: 'light' | 'dark'): void {
  const root = document.documentElement;
  // upstream's name for Query's light theme (legend-art/src/tokens.css)
  if (theme === 'light') root.dataset['theme'] = 'legacy-light';
  else delete root.dataset['theme'];
  root.dataset['dcTheme'] = theme;
}

/** The theme now. */
export function theme(): 'light' | 'dark' {
  return document.documentElement.dataset['theme'] === 'legacy-light' ? 'light' : 'dark';
}

/** Put the kept theme on the page (dark when none was kept). */
export function followTheme(): void {
  apply(stored());
}

/** The sun/moon switch: the other one, kept. */
export function toggleTheme(): void {
  const next = theme() === 'dark' ? 'light' : 'dark';
  apply(next);
  try {
    localStorage.setItem(KEY, next);
  } catch {
    // not kept (storage refused): it holds for this page
  }
}
