// THE NPM PACKAGE IN AN APPLICATION'S PAGE, in a real browser (//datacube:npm_package; npm/README.md). The package's
// folder is served as an application would serve an install of it -- its module and styles under /dist/, its assets
// under /assets/ -- to a page that is the application's own: its own title, its own markup, and a script that imports
// the module and hands it data. Nothing else is served, so what the page shows was planned and run by what the
// package carries, and a request for anything the package does not hold fails the test.
//
// What it holds: the app opens on the tables handed over (records, a bigint among them, kept exact); more data comes in
// after (a file's content, a File); a table that cannot be read is refused and leaves the page as it was; DataCube
// touches nothing of the page but the element it was given, and makes no request beyond the package's folder;
// dispose leaves the element empty and the page ready to mount again; and what it must refuse before starting, it does.
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
<div id="cube"><p id="placeholder">loading…</p></div>
<script type="module">
  import { mountDataCube } from '/dist/datacube.js';
  window.mountDataCube = mountDataCube;
</script>
</body>
</html>`;

const server = await serve(ROOT, {
  route: (_req, res, url) => {
    if (url.pathname !== '/app.html') return false;
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
const missing = [];
page.on('request', (r) => requested.push(new URL(r.url())));
page.on('response', (r) => { if (r.status() >= 400) missing.push(new URL(r.url()).pathname); });
page.on('console', (m) => {
  if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) problems.push(`console: ${m.text()}`);
});
page.on('pageerror', (e) => problems.push(`pageerror: ${e.message}`));

/** The grids on DataCube's page, and the rows the page shows. */
const shown = () => page.evaluate(() => ({
  grids: window.__dataPage?.grids.length ?? 0,
  rows: document.querySelectorAll('#cube .dc-row').length,
  text: document.getElementById('cube').textContent ?? '',
}));
// (a refusal is read in the page: `.then(() => null, said)` gives the reason it was refused with, or null when it was not)
const SAID = '(e) => String(e?.message ?? e)';
await page.addInitScript(`window.said = ${SAID};`);

try {
  await page.goto(`${server.origin}/app.html`, { waitUntil: 'load', timeout: 60_000 });
  await page.waitForFunction(() => typeof window.mountDataCube === 'function', undefined, { timeout: 60_000 });

  // REFUSED BEFORE ANYTHING STARTS, each in words, the element left as it was
  const elsewhere = await page.evaluate(() => window.mountDataCube(document.getElementById('cube'), { assets: 'https://cdn.example/datacube/' })
    .then(() => null, window.said));
  check('assets on another origin are refused, saying why', /own origin/.test(elsewhere ?? ''), elsewhere ?? 'not refused');
  const empty = await page.evaluate(() => window.mountDataCube(document.getElementById('cube'), { assets: '/assets/', data: [{ name: 'orders', rows: [] }] })
    .then(() => null, window.said));
  check('a table of no rows is refused, saying why', /orders has no rows/.test(empty ?? ''), empty ?? 'not refused');
  check('a refusal leaves the element as the application had it',
    await page.evaluate(() => document.getElementById('placeholder') !== null));
  check('nothing was fetched for a mount refused before it started',
    !requested.some((u) => u.pathname.startsWith('/assets/')), requested.map((u) => u.pathname).filter((p) => p.startsWith('/assets/')).join(' '));

  // THE APP, OPENED ON THE APPLICATION'S RECORDS: built in the page, as an application's fetch would leave them
  await page.evaluate(async () => {
    const regions = ['EMEA', 'APAC', 'AMER'];
    const rows = Array.from({ length: 12 }, (_, i) => ({
      id: 9007199254740993n + BigInt(i),
      region: regions[i % 3],
      product: i % 2 === 0 ? 'Widget' : 'Gadget',
      qty: 10 + i,
      price: 2.5 * (i + 1),
      placed: new Date(Date.UTC(2026, 0, 1 + i, 9, 30)),
    }));
    window.cube = await window.mountDataCube(document.getElementById('cube'), { assets: '/assets/', data: [{ name: 'orders', rows }] });
  });
  await page.waitForFunction(() => document.querySelectorAll('#cube .dc-row').length >= 12, undefined, { timeout: 60_000 });
  const first = await shown();
  check('the app opens on the table handed over: one grid, its twelve rows', first.grids === 1 && first.rows >= 12,
    `${first.grids} grid(s), ${first.rows} row(s)`);
  check('its values are the records\'', first.text.includes('EMEA') && first.text.includes('Gadget'));
  // 9007199254740993 is no double: read as one it would show ...992
  check('a bigint is kept exact', first.text.replace(/[\s,]/g, '').includes('9007199254740993'),
    (first.text.replace(/[\s,]/g, '').match(/90071992547\d+/) ?? ['no id shown'])[0]);
  check('nothing shows until the data is on the page: no blank page',
    await page.evaluate(() => document.querySelector('#cube .dc-blank-actions') === null
      || document.querySelector('#cube .dc-blank-actions').closest('[hidden]') !== null));

  // DATACUBE'S OWN PLACE, AND NOTHING ELSE OF THE PAGE
  check("the application's title is its own", (await page.title()) === 'An application', await page.title());
  check("the application's markup is as it was", await page.evaluate(() =>
    document.getElementById('own')?.textContent === "The application's own heading"
    && getComputedStyle(document.body).fontFamily.includes('Georgia')));
  check('the address is as it was', page.url() === `${server.origin}/app.html`, page.url());

  // MORE DATA, AFTER: a file's content the application fetched, a grid of its own beside the first
  await page.evaluate(() => window.cube.add({ name: 'rates', format: 'csv', content: 'ccy,rate\nUSD,1\nEUR,1.08\nGBP,1.27\n' }));
  await page.waitForFunction(() => window.__dataPage?.grids.length === 2 && (document.getElementById('cube').textContent ?? '').includes('GBP'),
    undefined, { timeout: 60_000 });
  check('a table added later is a grid beside the first', (await shown()).grids === 2);

  // nothing so far went wrong, and nothing was asked of anywhere but the package's folder
  check('no error on the page', problems.length === 0, problems.join(' | '));
  check('nothing the page asked for was missing', missing.length === 0, missing.join(' '));
  const outside = requested.filter((u) => u.protocol !== 'blob:' && u.protocol !== 'data:' && u.origin !== server.origin);
  check('no request left the application\'s origin', outside.length === 0, outside.map((u) => u.href).join(' '));
  const paths = new Set(requested.map((u) => u.pathname));
  check('DuckDB, the compiler and its worker came from the assets folder',
    ['/assets/duckdb-eh.wasm', '/assets/duckdb-browser-eh.worker.js', '/assets/classes.wasm', '/assets/wasm-gc-module-runtime.js', '/assets/planner-worker.js']
      .every((p) => paths.has(p)), [...paths].filter((p) => p.startsWith('/assets/')).join(' '));

  // A TABLE THAT CANNOT BE READ is refused with the reason, and the page is as it was
  const bad = await page.evaluate(() => window.cube.add({ name: 'broken', format: 'parquet', content: 'this is not Parquet' })
    .then(() => null, window.said));
  check('a table that cannot be read is refused, saying why', bad !== null && bad.length > 0, bad ?? 'not refused');
  check('and leaves the page as it was', (await shown()).grids === 2);
  const again = await page.evaluate(() => window.mountDataCube(document.getElementById('cube'), { assets: '/assets/' })
    .then(() => null, window.said));
  check('a second DataCube in the page is refused', /already mounted/.test(again ?? ''), again ?? 'not refused');

  // DISPOSE: the element empty, as the application gave it, and the page ready to mount again
  await page.evaluate(() => window.cube.dispose());
  check('dispose leaves the element empty', await page.evaluate(() => document.getElementById('cube').childElementCount === 0
    && document.querySelectorAll('.hostwin, .dc-app, .dc-page').length === 0));
  const gone = await page.evaluate(() => window.cube.add({ name: 'late', format: 'csv', content: 'a\n1\n' })
    .then(() => null, window.said));
  check('a DataCube taken down takes no data', /taken down/.test(gone ?? ''), gone ?? 'not refused');

  // MOUNTED AGAIN, with no data: the blank page, then a File as a file input gives one
  await page.evaluate(async () => {
    window.cube = await window.mountDataCube(document.getElementById('cube'), { assets: '/assets/' });
  });
  check('with no data it opens on the blank page', await page.evaluate(() => {
    const actions = document.querySelector('#cube .dc-blank-actions');
    return actions !== null && actions.closest('[hidden]') === null && window.__dataPage.grids.length === 0;
  }));
  await page.evaluate(() => window.cube.add(new File(['city,pop\nOslo,700000\nBergen,290000\n'], 'cities.csv', { type: 'text/csv' })));
  await page.waitForFunction(() => window.__dataPage?.grids.length === 1 && (document.getElementById('cube').textContent ?? '').includes('Bergen'),
    undefined, { timeout: 60_000 });
  check('a File opens in place of the blank page', (await shown()).grids === 1);
  await page.evaluate(() => window.cube.dispose());
} catch (e) {
  check('the page ran to its end', false, `${e instanceof Error ? e.message : e}${problems.length ? ` (${problems.join(' | ')})` : ''}`);
} finally {
  await browser.close();
  await server.close();
}
done();
