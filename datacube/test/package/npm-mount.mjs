// THE NPM PACKAGE IN AN APPLICATION'S PAGE, in a real browser (//datacube:npm_package; npm/README.md). The package's
// folder is served as an application would serve an install of it -- its module and styles under /dist/, its assets
// under /assets/ -- to a page that is the application's own: its own title, its own markup, a field of its own, and a
// script that imports the module and hands it data. Nothing else is served, so what the page shows was planned and run
// by what the package carries, and a request for anything the package does not hold fails the test.
//
// What it holds, in the order below:
//   - what must be refused before anything starts is, in words, the application's element left as it was -- a folder
//     of assets that is not there among them, told by the file it missed;
//   - a mount that fails (a table that cannot be read) or is stopped (its signal) leaves nothing: no element of
//     DataCube's, no worker, and the page may mount again at once;
//   - the app opens on the records handed over (a bigint kept exact, a Date a timestamp), the blank page never shown
//     on the way; more data comes in after, two tables asked at once both landing; one that cannot be read is refused
//     and leaves the page as it was;
//   - DataCube offers no entry for what this host does not give (the model actions);
//   - the application's title, markup, address and focus are its own, and no request leaves the package's folder;
//   - dispose leaves the element empty, takes off the page what DataCube laid over it (a dialog left open), ends its
//     workers, and leaves the page ready to mount again.
//
//   bazel test //datacube:npm_package_browser_test

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../../tools/browser/pinned-chromium.mjs';
import { chromium } from 'playwright';
import { checks, serve } from '../../demo/harness.mjs';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const ROOT = runfileFromEnv('NPM_PACKAGE');

// the application: its own page around an element it gives DataCube
const APP = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>An application</title>
<link rel="stylesheet" href="/dist/datacube.css">
<link rel="stylesheet" href="/dist/fonts.css">
<style>
  body { margin: 0; font: 16px/1.4 Georgia, serif; }
  #cube { width: 1100px; height: 620px; }
</style>
</head>
<body>
<h1 id="own">The application's own heading</h1>
<input id="field" value="the application's own field">
<div id="cube"><p id="placeholder">loading…</p></div>
<script type="module">
  import { mountDataCube } from '/dist/datacube.js';
  window.mountDataCube = mountDataCube;
  // a refusal read in the page: \`.then(() => null, window.said)\` gives its reason, or null when it was not refused
  window.said = (e) => (e?.name === 'AbortError' ? 'AbortError' : String(e?.message ?? e));
  window.cubeElement = () => document.getElementById('cube');
  // twelve records as an application's fetch would leave them: a bigint no double holds, a Date
  window.orders = () => Array.from({ length: 12 }, (_, i) => ({
    id: 9007199254740993n + BigInt(i),
    region: ['EMEA', 'APAC', 'AMER'][i % 3],
    product: i % 2 === 0 ? 'Widget' : 'Gadget',
    qty: 10 + i,
    price: 2.5 * (i + 1),
    placed: new Date(Date.UTC(2026, 0, 1 + i, 9, 30)),
  }));
</script>
</body>
</html>`;

const server = await serve(ROOT, {
  route: (_req, res, url) => {
    // the application's page; and, under /routed/, an application's router: every address answered with its page
    if (url.pathname !== '/app.html' && !url.pathname.startsWith('/routed/')) return false;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(APP);
    return true;
  },
});
const { check, done } = checks();
const browser = await chromium.launch();
const page = await browser.newPage();

const problems = [];
const requested = [];
page.on('request', (r) => requested.push(new URL(r.url())));
page.on('console', (m) => {
  if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) problems.push(`console: ${m.text()}`);
});
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

/** The grids on DataCube's page, and the rows and text the element shows. */
const shown = () => page.evaluate(() => ({
  grids: window.__dataPage?.grids.length ?? 0,
  rows: document.querySelectorAll('#cube .dc-row').length,
  text: document.getElementById('cube').textContent ?? '',
}));
/** Nothing of DataCube's in the page: the element empty, none of its windows, menus or dialogs, none of its names. */
const nothingLeft = () => page.evaluate(() => document.getElementById('cube').childElementCount === 0
  && document.querySelectorAll('.hostwin, .dc-app, .dc-page, .dc-menu, .dc-picker-backdrop').length === 0
  && window.__dataPage === undefined && window.__dataCube === undefined);
/** Its workers ended: the compiler's and the database's (a worker that is ending is still listed for a moment). */
const noWorkers = async () => {
  await page.waitForFunction(() => true);
  for (let i = 0; i < 200 && page.workers().length > 0; i++) await new Promise((resolve) => { setTimeout(resolve, 50); });
  return page.workers().length === 0;
};
/** The workers still running, by their scripts: what a failed check says. */
const workersLeft = () => page.workers().map((w) => new URL(w.url()).pathname).join(' ') || 'none';

try {
  await page.goto(`${server.origin}/app.html`, { waitUntil: 'load', timeout: 60_000 });
  await page.waitForFunction(() => typeof window.mountDataCube === 'function', undefined, { timeout: 60_000 });

  // REFUSED BEFORE ANYTHING STARTS, each in words, the element left as it was
  const refusals = await page.evaluate(async () => {
    const tried = (options) => window.mountDataCube(window.cubeElement(), options).then(() => null, window.said);
    return {
      elsewhere: await tried({ assets: 'https://cdn.example/datacube/' }),
      query: await tried({ assets: '/assets/?v=2' }),
      empty: await tried({ assets: '/assets/', data: [{ name: 'orders', rows: [] }] }),
      ending: await tried({ assets: '/assets/', data: [new File(['a\n1\n'], 'sheet.xlsx')] }),
      content: await tried({ assets: '/assets/', data: [{ name: 'late', format: 'csv', content: fetch('/app.html') }] }),
      nowhere: await tried({ assets: '/not-here/' }),
      routed: await tried({ assets: '/routed/assets/' }),
      stopped: await tried({ assets: '/assets/', signal: AbortSignal.abort() }),
    };
  });
  check('assets on another origin are refused, saying why', /own origin/.test(refusals.elsewhere ?? ''), refusals.elsewhere ?? 'not refused');
  check('an assets address with a query is refused', /names a folder/.test(refusals.query ?? ''), refusals.query ?? 'not refused');
  check('a table of no rows is refused, saying why', /orders has no rows/.test(refusals.empty ?? ''), refusals.empty ?? 'not refused');
  check('a file whose name has no ending DataCube reads is refused', /sheet\.xlsx.*name's ending/.test(refusals.ending ?? ''), refusals.ending ?? 'not refused');
  check('content that is not text or bytes is refused', /late: its content is not/.test(refusals.content ?? ''), refusals.content ?? 'not refused');
  check('assets that are not there are refused, by the file missed',
    /assets are not at .*\/not-here\/: (planner-worker|duckdb-browser-eh\.worker)\.js is not there/.test(refusals.nowhere ?? ''), refusals.nowhere ?? 'not refused');
  check("an application's router answering for a missing file is told apart from the file",
    /assets are not at .*\/routed\/assets\/: .* is answered with a page/.test(refusals.routed ?? ''), refusals.routed ?? 'not refused');
  check('a mount whose signal is already aborted is not begun', refusals.stopped === 'AbortError', refusals.stopped ?? 'not refused');
  check('a refusal leaves the element as the application had it',
    await page.evaluate(() => document.getElementById('placeholder') !== null));
  const early = requested.map((u) => u.pathname).filter((p) => p.startsWith('/assets/') && !/worker\.js$/.test(p));
  check('nothing was started for a mount that was refused', early.length === 0 && page.workers().length === 0,
    `${early.join(' ')} (${page.workers().length} workers)`);

  // A MOUNT THAT FAILS -- its second table is no Parquet -- leaves nothing, and the page may mount again
  const failed = await page.evaluate(() => window.mountDataCube(window.cubeElement(), {
    assets: '/assets/', data: [{ name: 'orders', rows: window.orders() }, { name: 'broken', format: 'parquet', content: 'this is not Parquet' }],
  }).then(() => null, window.said));
  check('a mount whose table cannot be read is rejected, saying why', /broken\.parquet/.test(failed ?? ''), failed ?? 'not rejected');
  check('and leaves nothing of DataCube in the page', await nothingLeft());
  check('and none of its workers', await noWorkers(), workersLeft());

  // A MOUNT STOPPED WHILE IT LOADS (a component taken down), and another begun at once in its place
  const stopped = await page.evaluate(async () => {
    const stop = new AbortController();
    const first = window.mountDataCube(window.cubeElement(), { assets: '/assets/', data: [{ name: 'orders', rows: window.orders() }], signal: stop.signal })
      .then(() => 'resolved', window.said);
    await new Promise((resolve) => { setTimeout(resolve, 40); });
    stop.abort();
    // no wait between: the page is free as the signal is aborted
    const second = window.mountDataCube(window.cubeElement(), { assets: '/assets/' }).then((cube) => { cube.dispose(); return 'mounted'; }, window.said);
    return { first: await first, second: await second };
  });
  check('a mount stopped by its signal is rejected with the abort', stopped.first === 'AbortError', stopped.first);
  check('and the page mounts again at once', stopped.second === 'mounted', stopped.second);
  check('and nothing of either is left', await nothingLeft());
  check('nor a worker of either', await noWorkers(), workersLeft());

  // THE APP, OPENED ON THE APPLICATION'S RECORDS, the blank page's card never shown on the way
  const sawBlank = await page.evaluate(async () => {
    let blank = false;
    let mounting = true;
    const look = () => {
      const card = document.querySelector('#cube .dc-blank-actions');
      if (card !== null && card.closest('[hidden]') === null && getComputedStyle(card).visibility !== 'hidden') blank = true;
      if (mounting) requestAnimationFrame(look);
    };
    look();
    window.cube = await window.mountDataCube(window.cubeElement(), { assets: '/assets/', data: [{ name: 'orders', rows: window.orders() }] });
    look();
    mounting = false;
    return blank;
  });
  check('nothing shows until the data is on the page: never the blank page', sawBlank === false);
  await page.waitForFunction(() => document.querySelectorAll('#cube .dc-row').length >= 12, undefined, { timeout: 60_000 });
  const first = await shown();
  check('the app opens on the table handed over: one grid, its twelve rows', first.grids === 1 && first.rows >= 12,
    `${first.grids} grid(s), ${first.rows} row(s)`);
  check('its values are the records\'', first.text.includes('EMEA') && first.text.includes('Gadget'));
  // 9007199254740993 is no double: read as one it would show ...992
  check('a bigint is kept exact', first.text.replace(/[\s,]/g, '').includes('9007199254740993'),
    (first.text.replace(/[\s,]/g, '').match(/90071992547\d+/) ?? ['no id shown'])[0]);
  // a Date is written as its ISO text, which the database reads as a timestamp: the column's type says so, and the
  // grid shows a date and a time, not the text
  const placed = await page.evaluate(() => JSON.stringify(window.__dataCube.snapshot.columns.find((c) => c.name === 'placed') ?? null));
  check('a Date is a timestamp', /DateTime|Timestamp/i.test(placed) && !first.text.includes('2026-01-01T09:30'), `${placed} ${(first.text.match(/.{0,14}2026.{0,14}/) ?? ['no date shown'])[0]}`);
  check('its grid is titled by the file the table is read as', first.text.includes('orders.json'));

  // NO ENTRY FOR WHAT THIS HOST DOES NOT GIVE: the model actions (Create a Class...) need a model home
  await page.click('#cube .dc-row .dc-cell', { button: 'right' });
  await page.waitForSelector('.dc-menu', { timeout: 30_000 });
  const create = page.locator('.dc-menu .dc-menu-item', { hasText: /^Create/ }).first();
  if (await create.count() > 0) await create.hover();
  const menus = (await page.locator('.dc-menu').allTextContents()).join(' | ');
  check('the grid\'s menu offers no model action', menus.length > 0 && !/Class…|Association…|Data Space…/.test(menus), menus.slice(0, 400));
  await page.keyboard.press('Escape');
  await page.keyboard.press('Escape');

  // DATACUBE'S OWN PLACE, AND NOTHING ELSE OF THE PAGE
  check("the application's title is its own", (await page.title()) === 'An application', await page.title());
  check("the application's markup is as it was", await page.evaluate(() =>
    document.getElementById('own')?.textContent === "The application's own heading"
    && getComputedStyle(document.body).fontFamily.includes('Georgia')));
  check('the address is as it was', page.url() === `${server.origin}/app.html`, page.url());

  // MORE DATA, AFTER: two tables asked at once -- a file's content, a File -- each a grid beside the first
  await page.evaluate(() => Promise.all([
    window.cube.add({ name: 'rates', format: 'csv', content: 'ccy,rate\nUSD,1\nEUR,1.08\nGBP,1.27\n' }),
    window.cube.add(new File(['city,pop\nOslo,700000\nBergen,290000\n'], 'cities.csv', { type: 'text/csv' })),
  ]));
  await page.waitForFunction(() => {
    const text = document.getElementById('cube').textContent ?? '';
    return window.__dataPage?.grids.length === 3 && text.includes('GBP') && text.includes('Bergen');
  }, undefined, { timeout: 60_000 });
  check('two tables added at once are both on the page', (await shown()).grids === 3);

  // nothing so far went wrong, and nothing was asked of anywhere but the package's folder
  check('no error on the page', problems.length === 0, problems.join(' | '));
  const outside = requested.filter((u) => u.protocol !== 'blob:' && u.protocol !== 'data:' && u.origin !== server.origin);
  check('no request left the application\'s origin', outside.length === 0, outside.map((u) => u.href).join(' '));
  const paths = new Set(requested.map((u) => u.pathname));
  check('DuckDB, the compiler and its worker came from the assets folder',
    ['/assets/duckdb-eh.wasm', '/assets/duckdb-browser-eh.worker.js', '/assets/classes.wasm', '/assets/wasm-gc-module-runtime.js', '/assets/planner-worker.js']
      .every((p) => paths.has(p)), [...paths].filter((p) => p.startsWith('/assets/')).join(' '));

  // A TABLE THAT CANNOT BE READ is refused with the reason, and the page is as it was
  const bad = await page.evaluate(() => window.cube.add({ name: 'broken', format: 'parquet', content: 'this is not Parquet' })
    .then(() => null, window.said));
  check('a table that cannot be read is refused, saying why', /broken\.parquet/.test(bad ?? ''), bad ?? 'not refused');
  check('and leaves the page as it was', (await shown()).grids === 3);
  const again = await page.evaluate(() => window.mountDataCube(window.cubeElement(), { assets: '/assets/' }).then(() => null, window.said));
  check('a second DataCube in the page is refused', /already mounted/.test(again ?? ''), again ?? 'not refused');

  // DISPOSE, a table still being read: it is told, the element is empty, the workers are ended
  const during = await page.evaluate(async () => {
    const reading = window.cube.add({ name: 'more', rows: window.orders() }).then(() => 'added', window.said);
    // (a moment, so the read is under way: its file handed to the app, the database at work)
    await new Promise((resolve) => { setTimeout(resolve, 15); });
    window.cube.dispose();
    return reading;
  });
  check('a table being read as DataCube is disposed is told so', /taken down/.test(during), during);
  check('dispose leaves nothing of DataCube in the page', await nothingLeft());
  check('and ends its workers', await noWorkers(), workersLeft());
  const gone = await page.evaluate(() => window.cube.add({ name: 'late', format: 'csv', content: 'a\n1\n' }).then(() => null, window.said));
  check('a DataCube taken down takes no data', /taken down/.test(gone ?? ''), gone ?? 'not refused');

  // MOUNTED AGAIN, with no data: the blank page, the application's focus left where it was
  await page.focus('#field');
  await page.evaluate(async () => {
    window.cube = await window.mountDataCube(window.cubeElement(), { assets: '/assets/' });
  });
  check('with no data it opens on the blank page', await page.evaluate(() => {
    const actions = document.querySelector('#cube .dc-blank-actions');
    return actions !== null && actions.closest('[hidden]') === null && window.__dataPage.grids.length === 0;
  }));
  check("the application's focus is where it was", await page.evaluate(() => document.activeElement?.id === 'field'),
    await page.evaluate(() => `${document.activeElement?.tagName}#${document.activeElement?.id} .${document.activeElement?.className}`));
  // two tables asked at once of a blank page: the first in its place, the second beside it -- neither lost
  await page.evaluate(() => Promise.all([
    window.cube.add({ name: 'rates', format: 'csv', content: 'ccy,rate\nUSD,1\nEUR,1.08\n' }),
    window.cube.add({ name: 'cities', format: 'csv', content: 'city,pop\nOslo,700000\nBergen,290000\n' }),
  ]));
  await page.waitForFunction(() => {
    const text = document.getElementById('cube').textContent ?? '';
    return window.__dataPage?.grids.length === 2 && text.includes('EUR') && text.includes('Bergen');
  }, undefined, { timeout: 60_000 });
  check('two tables asked at once of a blank page are both on it', (await shown()).grids === 2);

  // A DIALOG LEFT OPEN AS DATACUBE IS DISPOSED: it is laid over the whole page, and goes with it
  await page.evaluate(() => window.cube.dispose());
  await page.evaluate(async () => {
    window.cube = await window.mountDataCube(window.cubeElement(), { assets: '/assets/' });
  });
  await page.click('#cube .dc-blank-actions .dc-primary');
  await page.waitForSelector('.dc-picker-backdrop', { timeout: 30_000 });
  await page.evaluate(() => window.cube.dispose());
  check('a dialog open as DataCube is disposed goes with it', await nothingLeft(),
    await page.evaluate(() => [...document.body.children].map((e) => `${e.tagName}.${e.className}`).join(' ')));
  check('and the application can be used: its field takes a click', await page.click('#field', { timeout: 5_000 }).then(() => true, () => false));
  check('no worker is left at the end', await noWorkers(), workersLeft());

  // only what was meant to go wrong did: the two tables that are no Parquet (the database says so on the console)
  const unexpected = problems.filter((p) => !/broken|Parquet|magic bytes/i.test(p));
  check('no error on the page but the ones the tables that cannot be read made', unexpected.length === 0, unexpected.join(' | '));
} catch (e) {
  check('the page ran to its end', false, `${e instanceof Error ? e.message : e}${problems.length ? ` (${problems.join(' | ')})` : ''}`);
} finally {
  await browser.close();
  await server.close();
}
done();
