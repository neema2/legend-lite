// DATACUBE ON PYTHON, IN A TAB (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §3.3; step 2a): DataCube's app
// (src/app/datacube-app.ts) over Python's services (src/services/python-host.ts) -- Python's engine serving this page
// beside its API (`legend_lite.show`). Every grid's rows are Python's: nothing is planned or run in this tab, so the page
// loads neither DuckDB-WASM nor the compiler's module (budgeted, test/bundle-budget.test.ts), and needs no WebAssembly GC.
//
// What the page is told, and how:
//   - the engine: this page's own origin (one origin, no cross-origin call);
//   - the engine's token: the link's fragment (`#token=...`), which a browser never sends to a server, as the
//     warehouse's launch key is given; every call carries it;
//   - what to show: a page (`?page=<key>`: Python's `ll.Page`, or `ll.show(df)`'s page of one grid over the frame);
//   - what Python's host offers beside its frames (`services.json`: whether it reads a page's file), asked once;
//   - that Python changed it: its versions (`version.json`), asked about once a second while the tab is shown.

import { openDataCube } from '../src/app/datacube-app.ts';
import type { Versions } from '../src/app/services.ts';
import { browserBasics, tabTitle } from '../src/services/browser-basics.ts';
import {
  askedOffers, askedSettings, askedVersions, pythonServices, type PageFollower, type PythonLink, type PythonShows,
} from '../src/services/python-host.ts';

/** How often the page asks whether Python changed what it shows: a brief call on this machine, nothing held open (ms). */
const ASK_EVERY = 1000;
/** How often a hidden page says it is still open on Python's page (Python keeps the copies it shows for five minutes
 *  after it last said: python/legend_lite/keeper.py FOLLOWED_FOR) -- about as often as a browser runs a hidden tab's
 *  timers (ms). */
const SAY_HIDDEN = 60_000;

const pause = (ms: number): Promise<void> => new Promise((done) => { setTimeout(done, ms); });

/** While the tab is hidden: until it is shown, or `ms` have passed. */
const hiddenFor = (ms: number): Promise<void> => new Promise((done) => {
  const over = (): void => {
    clearTimeout(timer);
    document.removeEventListener('visibilitychange', over);
    done();
  };
  const timer = setTimeout(over, ms);
  document.addEventListener('visibilitychange', over);
});

/**
 * FOLLOWING PYTHON: about once a second the page asks the engine the versions of what it shows (`version.json`), which
 * Python moves after a notebook cell, a `cube.update`, a page changed, a Live frame's new columns, or a close -- saying,
 * as it asks, who it is and the version it shows (`follower`). A brief call, nothing held open: pages in many tabs never
 * use up the browser's six connections to one origin (the audit of show(), 2026-10-08). A hidden page has no grid to
 * keep current: it follows when it is looked at, and meanwhile only says, now and then, that it is still open.
 */
function polled(link: PythonLink, shows: PythonShows) {
  return (moved: (now: Versions | string) => void, follower: PageFollower): (() => void) => {
    let stopped = false;
    void (async () => {
      for (;;) {
        const now = await askedVersions(link, shows, follower);
        if (stopped) return;
        if (typeof now === 'string') {
          moved(now);
          return;
        }
        if (!document.hidden) moved(now);
        await (document.hidden ? hiddenFor(SAY_HIDDEN) : pause(ASK_EVERY));
        if (stopped) return;
      }
    })();
    return () => { stopped = true; };
  };
}

/** The page, said wrong when it cannot open: on the page itself. */
function refuse(host: HTMLElement, message: string): never {
  host.textContent = message;
  throw new Error(message);
}

async function start(): Promise<void> {
  const host = document.getElementById('cube')!;
  const token = new URLSearchParams(location.hash.slice(1)).get('token');
  if (!token) refuse(host, 'this page opens from the link the engine gave (it carries the engine\'s token)');
  const params = new URLSearchParams(location.search);
  const page = params.get('page');
  if (page === null) refuse(host, 'this page opens from the link the engine gave (it names the page to show)');
  const shows: PythonShows = { page };
  const link: PythonLink = { baseUrl: location.origin, fetch: (input, init) => fetch(input, init), authorization: `Bearer ${token}` };
  const status = document.createElement('span');
  status.textContent = 'asking Python…';
  const settled = { show: (_reason: string): void => {} };
  try {
    // what Python offers, and the page's settings as Python keeps them (none read, they start afresh), asked once
    const [offers, settings] = await Promise.all([askedOffers(link), askedSettings(link).catch(() => undefined)]);
    // where it plans, in its one word (the status bar's readout)
    status.textContent = offers.plane.word;
    await openDataCube({
      services: pythonServices({
        link, shows, offers, ...(settings ? { settings } : {}), watch: polled(link, shows), browser: { ...browserBasics, title: tabTitle() },
      }),
      host,
      status,
      windows: document.body,
      start: { kind: 'live' },
      taken: [],
      settled,
    });
  } catch (e) {
    const why = `failed to start: ${e instanceof Error ? e.message : String(e)}`;
    settled.show(why);
    // no page to say it on: the tab says it
    if (host.childElementCount === 0) host.textContent = why;
    throw e;
  }
}

void start();
