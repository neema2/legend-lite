// DATACUBE ON AN ENGINE, IN A TAB: one cube over a Legend engine that RUNS its queries
// (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md) -- Python's engine, which serves this page beside its API
// (`legend_lite.show`). The cube is engine-cube.ts's, in remote-run mode: nothing runs in this tab. So the page loads
// neither DuckDB-WASM nor the compiler's module: its script is the grid and the remote client (budgeted,
// test/bundle-budget.test.ts), and it needs no WebAssembly GC.
//
// What the page is told, and how:
//   - the engine: this page's own origin (one origin, no cross-origin call);
//   - the engine's token: the link's fragment (`#token=...`), which a browser never sends to a server, as the
//     warehouse's launch key is given; every call carries it;
//   - the cube: `cube.json?table=<name>` (asked with the token; engine-cube.ts);
//   - that the frame changed: `version.json?table=<name>`, asked about once a second (follow, below).
//
// Or A PAGE (`?page=<name>`, Python's `ll.Page`; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md): engine-page.ts's, its
// sheets, grids and charts, each grid over a frame. Fetched only then: one cube's tab loads none of the page's code.

import { EngineCube, type EngineLink } from './engine-cube.ts';

function refuse(message: string): never {
  const host = document.getElementById('cube');
  if (host) host.textContent = message;
  throw new Error(message);
}

/** What the link says: the engine's token (its fragment) and the frame to show. It chooses nothing about who runs. */
function linked(): EngineLink {
  const token = new URLSearchParams(location.hash.slice(1)).get('token');
  if (!token) refuse('this page opens from the link the engine gave (it carries the engine\'s token)');
  // (a page's link names no table: `?page=`)
  return {
    baseUrl: location.origin,
    fetch: (input, init) => fetch(input, init),
    authorization: `Bearer ${token}`,
    table: new URLSearchParams(location.search).get('table') ?? '',
  };
}

/** How often the page asks whether its frame changed: a brief call on this machine, nothing held open (ms). */
const ASK_EVERY = 1000;

const pause = (ms: number): Promise<void> => new Promise((done) => { setTimeout(done, ms); });

/** Once the tab is shown: a hidden page has no grid to keep current, and asks again when it is looked at. */
const shown = (): Promise<void> => (document.hidden
  ? new Promise((done) => { document.addEventListener('visibilitychange', () => done(), { once: true }); })
  : Promise.resolve());

/**
 * FOLLOWING THE FRAME: about once a second the page asks the engine its frame's version (`version.json`), which
 * Python moves after a notebook cell, a `cube.update`, a `cube.refresh`, a Live frame's new columns, or a close. A
 * brief call, nothing held open: cubes in many tabs never use up the browser's six connections to one origin (the
 * audit of show(), 2026-10-08). A new version re-reads the cube (EngineCube.reread). A frame closed in Python, or an
 * engine that stopped, leaves the page showing what it showed, saying it no longer follows.
 */
async function follow(link: EngineLink, cube: EngineCube): Promise<void> {
  const stopped = (why: string): void => { document.title = `${cube.config.title} (not followed: ${why})`; };
  for (;;) {
    await pause(ASK_EVERY);
    await shown();
    const answer = await fetch(`version.json?table=${encodeURIComponent(link.table)}`,
      { headers: { Authorization: link.authorization ?? '' } }).catch(() => undefined);
    if (answer === undefined) return stopped('the engine stopped');
    if (answer.status === 404) return stopped('the frame was closed');
    if (!answer.ok) return stopped(`the engine answered ${answer.status}`);
    const { version } = (await answer.json()) as { version: number };
    if (version === cube.config.version) continue;
    const why = await cube.reread();
    if (why !== undefined) return stopped(why);
    document.title = cube.config.title;
  }
}

async function open(link: EngineLink): Promise<void> {
  const page = new URLSearchParams(location.search).get('page');
  if (page !== null) {
    await openPage({ baseUrl: link.baseUrl, fetch: link.fetch, ...(link.authorization ? { authorization: link.authorization } : {}), page });
    return;
  }
  const cube = await EngineCube.open(document.getElementById('cube')!, link);
  document.title = cube.config.title;
  await follow(link, cube);
}

/**
 * A PAGE ON THE ENGINE, followed as one cube is: about once a second its versions (`version.json?page=`), which Python
 * moves when it changes the page or a frame on it (EnginePage.follow).
 */
async function openPage(link: import('./engine-page.ts').PageLink): Promise<void> {
  const { EnginePage, askedVersions } = await import('./engine-page.ts');
  const page = await EnginePage.open(document.getElementById('cube')!, link);
  document.title = page.title;
  const stopped = (why: string): void => { document.title = `${page.title} (not followed: ${why})`; };
  for (;;) {
    await pause(ASK_EVERY);
    await shown();
    const versions = await askedVersions(link);
    if (typeof versions === 'string') return stopped(versions);
    const why = await page.follow(versions);
    if (why !== undefined) return stopped(why);
    document.title = page.title;
  }
}

// a page that cannot open says why, on the page (the cube's own failures it shows in its status bar)
open(linked()).catch((e: unknown) => {
  const host = document.getElementById('cube');
  if (host && host.childElementCount === 0) host.textContent = e instanceof Error ? e.message : String(e);
  throw e;
});
