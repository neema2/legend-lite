// THE BROWSER'S OWN THINGS (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.2, the browser service): handing a file to
// the person, the clipboard, and the grids' settings kept between visits -- the same in a tab and under a notebook's
// cell, so every host's services give them from here (src/services/browser.ts, src/services/python-host.ts's hosts).

import type { BrowserService } from '../app/services.ts';

/** Hand a file to the user. Its address is let go of a little later, not at once: some browsers start the download
 *  after the click returns (Python's pages always waited; the tab let go at once until 2026-10-12, step 2a). */
function download(name: string, mime: string, content: string | Uint8Array): void {
  const url = URL.createObjectURL(new Blob([typeof content === 'string' ? content : content.slice()], { type: mime }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

/** Text onto the clipboard, or why the browser does not let the page copy. */
async function writeClipboard(text: string): Promise<void> {
  const clipboard = navigator.clipboard;
  if (!clipboard) throw new Error('this browser does not let the page copy (a page not served over https or from this machine)');
  await clipboard.writeText(text);
}

const SETTINGS_KEY = 'dataCube.settings';

/** The settings this browser kept, or none. */
function readSettings(key: string): Record<string, unknown> | undefined {
  try {
    const raw = window.localStorage.getItem(key);
    const parsed: unknown = raw === null ? undefined : JSON.parse(raw);
    return parsed !== null && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}

function writeSettings(key: string, values: Record<string, unknown>): void {
  try {
    window.localStorage.setItem(key, JSON.stringify(values));
  } catch {
    // storage refused (private window, quota): the settings hold for this visit
  }
}

/** The browser's own things every host has; a host adds its address and its tab's title where it has them. */
export function browserBasicsFor(settingsKey: string): Pick<BrowserService, 'download' | 'writeClipboard' | 'readSettings' | 'writeSettings'> {
  return { download, writeClipboard, readSettings: () => readSettings(settingsKey), writeSettings: (values) => writeSettings(settingsKey, values) };
}

export const browserBasics = browserBasicsFor(SETTINGS_KEY);

/** This tab's title, as the page sets it (its name, whether it changed). */
export const tabTitle = (): NonNullable<BrowserService['title']> => ({
  base: document.title,
  set: (text) => { document.title = text; },
});
