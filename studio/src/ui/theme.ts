// The page's theme, as upstream Studio has it: DARK unless the person picks light with the activity bar's sun/moon
// switch (default-dark <-> default-light, census UPSTREAM_STUDIO_LOOK.md 0.1; the system's setting plays no part).
// The choice is kept in this browser. The colours are legend-art's tokens (`data-theme` on <html>).

const KEY = 'legend-studio.theme';

export type Theme = 'dark' | 'light';

function stored(): Theme {
  try {
    return localStorage.getItem(KEY) === 'light' ? 'light' : 'dark';
  } catch {
    return 'dark';     // storage refused (a private window): the default
  }
}

function apply(theme: Theme): void {
  const root = document.documentElement;
  if (theme === 'light') root.dataset['theme'] = 'default-light';
  else delete root.dataset['theme'];
}

/** The theme now. */
export function theme(): Theme {
  return document.documentElement.dataset['theme'] === 'default-light' ? 'light' : 'dark';
}

/** Put the kept theme on the page (dark when none was kept). */
export function followTheme(): void {
  apply(stored());
}

/** Switch, and keep the choice. */
export function toggleTheme(): Theme {
  const next: Theme = theme() === 'dark' ? 'light' : 'dark';
  apply(next);
  try {
    localStorage.setItem(KEY, next);
  } catch {
    // not kept (a private window): the switch still holds for this page
  }
  return next;
}
