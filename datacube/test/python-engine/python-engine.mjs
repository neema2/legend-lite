// DATACUBE AGAINST PYTHON'S ENGINE, run in the pinned Chromium (//datacube:python_engine_test). Node here only drives:
// it starts Python's engine (engine_fixture.py, beside it: the cube corpus's rows as a Live frame, DataCube's site
// served beside the API, so the page and the engine are one origin), hands the browser the test page (Playwright's
// routing, for /python-engine.html, its bundle and upstream's recorded Arrow answer) with the engine's token, and
// reports what the page found (test/python-engine/page.ts: every check runs there). Then the page a person opens
// (demo/engine.html), and a notebook's cubes (demo/widget-loader.ts and widget.ts, as anywidget runs a widget), their
// messages carried between the page and Python's widgets as a notebook's channel carries them.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const fixture = runfileFromEnv('ENGINE_FIXTURE');
const site = runfileFromEnv('DATACUBE_SITE');
const bundle = readFileSync(runfileFromEnv('PYTHON_ENGINE_PAGE'));
const upstream = readFileSync(runfileFromEnv('UPSTREAM_ARROW_ANSWER'));

// the library by its runfiles path, as //python's tests are given it: a program a test starts gets no BUILD env
const engine = spawn(fixture, ['--site', site], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'), PYTHONUTF8: '1',
    PYTHONDONTWRITEBYTECODE: '1' },
});
// the engine's own account, in this test's log: a server-side reason is then never invisible
engine.stderr.on('data', (b) => { for (const line of String(b).split('\n')) if (line) process.stderr.write(`[engine] ${line}\n`); });
const stopped = new Promise((done) => engine.once('exit', done));
// the engine's lines: the first says what it serves, each after it answers a command (`done <command>`); and, as they
// come, what its notebook cubes send their page (`sent ...`, `trait ...`), handed to the widget page as a notebook's
// channel would
const waiting = [];
const queued = [];
let toWidgets = () => {};
createInterface({ input: engine.stdout }).on('line', (line) => {
  if (line.startsWith('sent ') || line.startsWith('trait ')) {
    toWidgets(line);
    return;
  }
  const next = waiting.shift();
  if (next) next(line);
  else queued.push(line);
});
const exited = new Promise((_, fail) => engine.once('exit', (code) => fail(new Error(`the engine exited (${code})`))));
const nextLine = () => Promise.race([
  queued.length > 0 ? Promise.resolve(queued.shift()) : new Promise((done) => { waiting.push(done); }),
  exited,
]);
// (show() says its link first, "DataCube: <url>", as it does for a person)
let first = await nextLine();
while (first !== undefined && !first.startsWith('{')) first = await nextLine();
const served = JSON.parse(first);
const origin = served.url;

let failed = false;
let browser;
try {
  browser = await chromium.launch();
  const page = await browser.newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  page.on('requestfailed', (r) => console.log(`request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  // what the page is handed: the token, and the frame the engine serves (the address is the page's own origin)
  const { url: _origin, link: _link, ...given } = served;
  await page.addInitScript((g) => { window.__pythonEngineServed = g; }, given);
  // the test page, its bundle and upstream's recorded answer; everything else (the site, the API) is the engine's
  await page.route(`${origin}/python-engine.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>DataCube on Python</title><script type="module" src="python-engine.js"></script>',
  }));
  await page.route(`${origin}/python-engine.js`, (r) => r.fulfill({ contentType: 'text/javascript', body: bundle }));
  await page.route(`${origin}/upstream.arrows.zst`, (r) => r.fulfill({ contentType: 'application/octet-stream', body: upstream }));
  await page.goto(`${origin}/python-engine.html`);
  await page.waitForFunction(() => window.__pythonEngine?.done === true, undefined, { timeout: 280_000 }).catch(async (e) => {
    console.log(`the page did not finish; it was at: ${await page.evaluate(() => window.__pythonEngineStage ?? '(no stage)')}`);
    throw e;
  });
  const verdict = await page.evaluate(() => window.__pythonEngine);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name} (${Math.round(o.ms)} ms)${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  if (verdict.outcomes.length !== 4) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not 4`);
    failed = true;
  }

  // THE PAGE A PERSON OPENS (demo/engine.html, from the engine's site): the link names the frame and carries the
  // token in its fragment; the cube opens over the engine and shows the frame's rows, with nothing run in the tab
  const cube = await browser.newPage();
  cube.on('pageerror', (e) => { console.log(`engine page error: ${e.message}`); failed = true; });
  cube.on('requestfailed', (r) => console.log(`engine page request failed: ${r.url()} -- ${r.failure()?.errorText ?? ''}`));
  cube.on('console', (m) => console.log(`engine page console ${m.type()}: ${m.text()}`));
  // every file and call the page asks for is answered: a refusal (a missing file, a refused call) fails the test
  cube.on('response', (r) => {
    if (r.status() >= 400) { console.log(`FAIL engine page answer ${r.status()}: ${r.url()}`); failed = true; }
  });
  // the link show() returned (cube.url)
  await cube.goto(served.link);
  const shown = await cube.waitForFunction(() => /\b10 rows\b/.test(document.body.innerText)
    && document.body.innerText.includes('APAC'), undefined, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${shown ? 'ok  ' : 'FAIL'} the engine's page opens the frame's cube and shows its rows`);
  if (!shown) {
    console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText)).slice(0, 400)}`);
    failed = true;
  }

  // FOLLOWING THE FRAME (show's cube.update): Python changes it, and the open page shows the change by itself
  const after = async (command, name, test) => {
    engine.stdin.write(`${command}\n`);
    if ((await nextLine()) !== `done ${command}`) throw new Error(`the engine did not do ${command}`);
    const ok = await cube.waitForFunction(test, undefined, { timeout: 30_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await cube.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await after('update', 'a frame updated in Python shows on the open page by itself (the same columns: the view re-run)',
    () => /\b11 rows\b/.test(document.body.innerText));
  await after('columns', 'a frame with a new column opens the cube again over its new model',
    () => document.body.innerText.includes('trader'));

  // A PAGE FROM PYTHON (docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md, step 1): a page of the engine's two frames
  // on two sheets, written by DataCube's own writers (the test page's __pythonPageDocument), served by Python as `q3`,
  // and opened as a person opens it (demo/engine.html?page=q3); then a frame updated, and the page changed, in Python
  const doc = await page.evaluate(() => window.__pythonPageDocument());
  engine.stdin.write(`page ${JSON.stringify(doc)}\n`);
  if ((await nextLine()) !== 'done page') throw new Error('the engine did not serve the page');
  const tabbed = await browser.newPage();
  tabbed.on('pageerror', (e) => { console.log(`page tab error: ${e.message}`); failed = true; });
  tabbed.on('console', (m) => { if (m.type() === 'error') console.log(`page tab console: ${m.text()}`); });
  tabbed.on('response', (r) => {
    if (r.status() >= 400) { console.log(`FAIL page tab answer ${r.status()}: ${r.url()}`); failed = true; }
  });
  await tabbed.goto(`${origin}/engine.html?page=q3${new URL(served.link).hash}`);
  const sheetNames = () => [...document.querySelectorAll('.dc-sheet-tab .dc-sheet-label')].map((t) => t.textContent).join('|');
  const seen = async (name, test, arg) => {
    const ok = await tabbed.waitForFunction(test, arg, { timeout: 60_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await tabbed.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await seen('a page from Python opens in a tab: its sheets, the first one\'s grid over its frame and the chart beside it',
    `(${String(sheetNames)})() === 'trades|Desks' && document.querySelector('[data-tile="grid-1"]')?.innerText.includes('EMEA')
      && document.querySelector('[data-tile="chart-1"]') !== null`);
  await tabbed.locator('.dc-sheet-tab', { hasText: 'Desks' }).click();
  await seen('its second sheet shows the grid over the second frame',
    () => document.querySelector('[data-tile="grid-2"]')?.innerText.includes('Credit') === true);
  const python = async (command) => {
    engine.stdin.write(`${command}\n`);
    if ((await nextLine()) !== `done ${command}`) throw new Error(`the engine did not do ${command}`);
  };
  await python('desks-update');
  await seen('a frame updated in Python shows on the open page by itself',
    () => document.querySelector('[data-tile="grid-2"]')?.innerText.includes('Equity') === true);
  await python('page-again');
  await seen('the page changed in Python opens again, on the sheet it showed',
    `(${String(sheetNames)})() === 'trades|Desks (v2)' && document.querySelector('[data-tile="grid-2"]')?.offsetParent !== null`);

  // A PAGE BUILT WITH ll.Page (step 2): built and shown in Python as a person does; its link is the one show() gives.
  // Then a sheet added in Python (the open page opens it again), and a sheet renamed in DataCube read back by
  // page.read() (the open page reports its document as it changes)
  engine.stdin.write('py-page\n');
  const built = await nextLine();
  if (!built.startsWith('done py-page ')) throw new Error(`the engine did not build the page: ${built}`);
  await tabbed.goto(JSON.parse(built.slice('done py-page '.length)));
  await seen('a page built with ll.Page opens in a tab: its sheets, its grid over its frame and the chart beside it',
    `(${String(sheetNames)})() === 'pytrades|Desks' && document.querySelector('[data-tile="grid-1"]')?.innerText.includes('EMEA')
      && document.querySelector('[data-tile="chart-1"]') !== null`);
  await python('py-page-sheet');
  await seen('a sheet added to it in Python shows on the open page', `(${String(sheetNames)})() === 'pytrades|Desks|Added in Python'`);
  await tabbed.locator('.dc-sheet-tab', { hasText: 'Desks' }).dblclick();
  await tabbed.locator('.dc-sheet-tab input').fill('Renamed in DataCube');
  await tabbed.locator('.dc-sheet-tab input').press('Enter');
  const read = await (async () => {
    // the page reports its document once a change settles (half a second): asked until it has, or ten seconds
    for (let tries = 0; tries < 20; tries += 1) {
      await new Promise((done) => { setTimeout(done, 500); });
      engine.stdin.write('py-page-read\n');
      const line = await nextLine();
      if (!line.startsWith('done py-page-read ')) throw new Error(`the engine did not read the page: ${line}`);
      const names = JSON.parse(line.slice('done py-page-read '.length));
      if (names.includes('Renamed in DataCube')) return names;
    }
    return undefined;
  })();
  console.log(`${read ? 'ok  ' : 'FAIL'} a sheet renamed in DataCube is in page.read() in Python${read ? ` (${read.join(', ')})` : ''}`);
  if (!read) failed = true;

  // A NOTEBOOK'S CUBES (legend_lite.notebook.DataCube): two widgets on one page, each its script loaded as anywidget
  // loads one (its text as a module), their messages carried to Python's widgets and back as a notebook's channel
  // carries them. DataCube's module comes over the channel once for the page; no call goes over HTTP.
  const widgets = await browser.newPage();
  widgets.on('pageerror', (e) => { console.log(`widget page error: ${e.message}`); failed = true; });
  widgets.on('console', (m) => { if (m.type() === 'error') console.log(`widget page console: ${m.text()}`); });
  const http = [];
  widgets.on('request', (r) => { if (!r.url().endsWith('/widgets.html')) http.push(r.url()); });
  const fetched = new Map();
  await widgets.exposeFunction('__widgetSend', (name, message) => {
    const { path } = JSON.parse(message);
    fetched.set(path, (fetched.get(path) ?? 0) + 1);
    engine.stdin.write(`widget ${name} ${message}\n`);
  });
  // what Python's widgets send goes to every page with a view of that widget, as a notebook's channel brings it to
  // every view of a model
  const widgetPages = [widgets];
  toWidgets = (line) => {
    const space = line.indexOf(' ');
    for (const page of widgetPages) {
      page.evaluate(([kind, json]) => {
        const m = JSON.parse(json);
        const model = window.__models?.[m.widget];
        if (model === undefined) return;
        if (kind === 'trait') model.set(m.name, m.value);
        else {
          const views = m.buffers.map((b) => new DataView(Uint8Array.from(atob(b), (c) => c.charCodeAt(0)).buffer));
          model.fire('msg:custom', m.content, views);
        }
      }, [line.slice(0, space), line.slice(space + 1)]).catch((e) => { console.log(`FAIL a widget message: ${e}`); failed = true; });
    }
  };
  await widgets.route(`${origin}/widgets.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>notebook cubes</title><div id="nb" style="width:1000px"></div>'
      + '<div id="nb2" style="width:1000px"></div><div id="nbp" style="width:1000px"></div>',
  }));
  await widgets.goto(`${origin}/widgets.html`);
  await widgets.evaluate(async (given) => {
    window.__models = {};
    window.__heardAbove = [];
    document.addEventListener('keydown', (e) => window.__heardAbove.push(e.key));
    for (const [name, { esm, state }] of Object.entries(given)) {
      // the part of anywidget's model a widget uses, as anywidget's front end gives it
      const listeners = new Map();
      const values = { ...state };
      const model = {
        get: (key) => values[key],
        on: (event, f) => { if (!listeners.has(event)) listeners.set(event, new Set()); listeners.get(event).add(f); },
        off: (event, f) => { listeners.get(event)?.delete(f); },
        send: (content) => { window.__widgetSend(name, JSON.stringify(content)); },
        fire: (event, ...args) => { for (const f of listeners.get(event) ?? []) f(...args); },
        set: (key, value) => { values[key] = value; model.fire(`change:${key}`); },
      };
      window.__models[name] = model;
      // as anywidget loads a widget's script: its text as a module (a blob URL), imported
      const url = URL.createObjectURL(new Blob([esm], { type: 'text/javascript' }));
      const widget = (await import(url)).default;
      URL.revokeObjectURL(url);
      widget.render({ model, el: document.getElementById(name) });
    }
  }, served.widgets);
  const check = async (name, test, arg) => {
    const ok = await widgets.waitForFunction(test, arg, { timeout: 60_000 }).then(() => true, () => false);
    console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}`);
    if (!ok) {
      console.log(`  the page showed: ${(await widgets.evaluate(() => document.body.innerText)).slice(0, 400)}`);
      failed = true;
    }
  };
  await check('two notebook cubes show their frames\' rows, over the widget\'s channel',
    () => /\b10 rows\b/.test(document.getElementById('nb').innerText) && document.getElementById('nb').innerText.includes('APAC')
      && /\b3 rows\b/.test(document.getElementById('nb2').innerText));
  const once = fetched.get('/widget.js') === 1 && fetched.get('/widget.css') === 1;
  console.log(`${once ? 'ok  ' : 'FAIL'} DataCube's module came once for the page (${fetched.get('/widget.js')} widget.js, ${fetched.get('/widget.css')} widget.css)`);
  if (!once) failed = true;
  console.log(`${http.length === 0 ? 'ok  ' : 'FAIL'} the notebook cubes made no HTTP call${http.length ? `: ${http.join(', ')}` : ''}`);
  if (http.length) failed = true;
  // A NOTEBOOK'S PAGE (ll.Page under a cell, notebook.PageCube): its sheet and its grid, over the widget's channel; a
  // sheet added in Python shows by itself (its versions followed, no polling)
  const pageTabs = () => [...document.getElementById('nbp').querySelectorAll('.dc-sheet-tab .dc-sheet-label')]
    .map((t) => t.textContent).join('|');
  await check('a notebook\'s page shows its sheet and its grid over its frame, over the widget\'s channel',
    `(${String(pageTabs)})() === 'nbpage' && document.getElementById('nbp').innerText.includes('EMEA')`);
  engine.stdin.write('widget-page-sheet\n');
  if ((await nextLine()) !== 'done widget-page-sheet') throw new Error('the engine did not do widget-page-sheet');
  await check('a sheet added to the notebook\'s page in Python shows by itself (its versions followed)',
    `(${String(pageTabs)})() === 'nbpage|Added in Python'`);
  engine.stdin.write('widget-update\n');
  if ((await nextLine()) !== 'done widget-update') throw new Error('the engine did not do widget-update');
  await check('a notebook cube\'s frame updated in Python shows by itself (its version followed, no polling)',
    () => /\b11 rows\b/.test(document.getElementById('nb').innerText));
  const kept = await widgets.evaluate(() => {
    const el = document.getElementById('nb');
    el.querySelector('*')?.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true }));
    return { marked: el.dataset.lmSuppressShortcuts === 'true', heardAbove: window.__heardAbove.length };
  });
  const keys = kept.marked && kept.heardAbove === 0;
  console.log(`${keys ? 'ok  ' : 'FAIL'} a key pressed in a notebook cube stays with it (${JSON.stringify(kept)})`);
  if (!keys) failed = true;

  // A VIEW TAKEN DOWN WHILE DATACUBE LOADS (the audit of step 7, S2): on a fresh page, two views of one widget; the
  // first starts the module's load over its channel and is taken down at once (its cell run again), which cuts its
  // calls short. The second loads the module over its own channel, and shows its rows.
  const cut = await browser.newPage();
  cut.on('pageerror', (e) => { console.log(`second widget page error: ${e.message}`); failed = true; });
  const cutFetched = new Map();
  await cut.exposeFunction('__widgetSend', (name, message) => {
    const { path } = JSON.parse(message);
    cutFetched.set(path, (cutFetched.get(path) ?? 0) + 1);
    engine.stdin.write(`widget ${name} ${message}\n`);
  });
  await cut.route(`${origin}/cut.html`, (r) => r.fulfill({
    contentType: 'text/html',
    body: '<!doctype html><meta charset="utf-8"><title>a view taken down</title><div id="a" style="width:1000px"></div>'
      + '<div id="b" style="width:1000px"></div>',
  }));
  await cut.goto(`${origin}/cut.html`);
  widgetPages.push(cut);
  await cut.evaluate(async ({ esm, state }) => {
    const listeners = new Map();
    const values = { ...state };
    const model = {
      get: (key) => values[key],
      on: (event, f) => { if (!listeners.has(event)) listeners.set(event, new Set()); listeners.get(event).add(f); },
      off: (event, f) => { listeners.get(event)?.delete(f); },
      send: (content) => { window.__widgetSend('nb2', JSON.stringify(content)); },
      fire: (event, ...args) => { for (const f of listeners.get(event) ?? []) f(...args); },
      set: (key, value) => { values[key] = value; model.fire(`change:${key}`); },
    };
    window.__models = { nb2: model };
    const url = URL.createObjectURL(new Blob([esm], { type: 'text/javascript' }));
    const widget = (await import(url)).default;
    URL.revokeObjectURL(url);
    const takeDownFirst = widget.render({ model, el: document.getElementById('a') });
    widget.render({ model, el: document.getElementById('b') });
    takeDownFirst();
  }, served.widgets.nb2);
  const rerun = await cut.waitForFunction(() => /\b3 rows\b/.test(document.getElementById('b').innerText), undefined,
    { timeout: 60_000 }).then(() => true, () => false);
  const loads = cutFetched.get('/widget.js');
  const shownAfterCut = rerun && loads === 2;
  console.log(`${shownAfterCut ? 'ok  ' : 'FAIL'} a view taken down mid-load leaves the other view to load DataCube itself (${loads} widget.js asked)`);
  if (!shownAfterCut) {
    console.log(`  the second view showed: ${(await cut.evaluate(() => document.getElementById('b').innerText)).slice(0, 300)}`);
    failed = true;
  }
} finally {
  await browser?.close();
  // stopped and WAITED for: the engine serves until its input closes, and is gone before the test reports
  engine.stdin.end();
  await stopped;
}
process.exit(failed ? 1 : 0);
