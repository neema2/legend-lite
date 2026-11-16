// A package consumer, not another checkout entry point: install the release tarball offline into an empty
// directory, typecheck and bundle its imports, and serve only that application's files. Every calculation below
// must be planned and executed in its browser; the HTTP server answers static files and nothing else.
import '../../tools/browser/pinned-chromium.mjs';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { basename, join, sep } from 'node:path';
import { promisify } from 'node:util';
import { test } from 'node:test';
import { chromium } from 'playwright';
import { outPath, serve, tmpDir } from '../demo/harness.mjs';
import { runfile, runfileFromEnv, runfilesFromEnv } from '../../tools/js/runfiles.mts';

const exec = promisify(execFile);

/** A tool is named by the test target; no executable or package is found through the host's PATH. */
async function command(executable, args, cwd, extraEnv = {}) {
  try {
    return await exec(executable, args, {
      cwd,
      env: { ...process.env, NODE_PATH: '', NODE_OPTIONS: '', ...extraEnv },
      maxBuffer: 4 * 1024 * 1024,
      timeout: 90_000,
      windowsHide: true,
    });
  } catch (reason) {
    throw new Error(`${basename(executable)} ${args.join(' ')} failed\n${reason.stdout ?? ''}${reason.stderr ?? ''}`, { cause: reason });
  }
}

function typescript() {
  const links = runfilesFromEnv('CONSUMER_TYPESCRIPT').filter((path) =>
    path.split(sep).join('/').endsWith('/datacube/node_modules/typescript'));
  assert.equal(links.length, 1, 'the test declares exactly one public TypeScript package link');
  return join(links[0], 'bin', 'tsc');
}

/** Small exact integers make the visible cells an unambiguous check of the user's result. */
async function rowsEqual(page, columns, expected) {
  await page.waitForFunction(({ columns, expected }) => {
    const rows = [...document.querySelectorAll('#cube .dc-row:not(.dc-total)')].map((row) =>
      columns.map((name) => [...row.querySelectorAll('.dc-cell')]
        .find((cell) => cell.getAttribute('data-column') === name)?.textContent?.trim() ?? ''));
    const key = (row) => JSON.stringify(row);
    return JSON.stringify(rows.map(key).sort()) === JSON.stringify(expected.map(key).sort());
  }, { columns, expected }, { timeout: 60_000 });
}

async function menu(page, column, path, value) {
  const cells = page.locator(`#cube .dc-row:not(.dc-total) .dc-cell[data-column="${column}"]`);
  await (value === undefined ? cells : cells.filter({ hasText: value })).first().click({ button: 'right' });
  for (let i = 0; i < path.length; i += 1) {
    const text = path[i];
    const exact = typeof text === 'string' ? new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) : text;
    const item = page.locator('.dc-menu-item > .dc-menu-label').filter({ hasText: exact }).first();
    if (i + 1 === path.length) await item.click();
    else await item.hover();
  }
}

async function pageMenu(page, text) {
  await page.locator('#cube .dc-titlebar-menu').click();
  await page.locator('.dc-menu-item > .dc-menu-label').filter({ hasText: new RegExp(`^${text}$`) }).click();
}

/** Abort while a startup download never answers, then verify cleanup before allowing that request to finish. */
async function abortHeldStartup(page, context, pattern, asset) {
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  const holdDownload = async (route) => {
    await held;
    // A terminated worker may already have cancelled its request.
    await route.abort('aborted').catch(() => undefined);
  };
  await context.route(pattern, holdDownload);
  try {
    const loading = context.waitForEvent('request', {
      predicate: (request) => request.url().endsWith(asset), timeout: 60_000,
    });
    await page.locator('#start-abort').click();
    await loading;
    await page.locator('#abort').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'aborted', null, { timeout: 15_000 });
    assert.equal(await page.locator('#expected-error').textContent(), 'AbortError: Consumer cancelled startup');
    assert.equal(await page.evaluate(() => window.__consumerWorkerCount), 0, `aborted startup releases both workers before ${asset} answers`);
    assert.equal(await page.locator('#cube .dc-browser').count(), 0, 'aborted startup releases the host');
  } finally {
    release();
    await context.unroute(pattern, holdDownload);
  }
}

/** A pending IndexedDB open cannot be cancelled by the browser. The creator must stop waiting and close it later. */
async function abortBlockedStorage(page) {
  const databaseName = 'independent-npm-consumer.pages';
  await page.evaluate(async (name) => {
    const holding = indexedDB.open(name);
    const held = await new Promise((resolve, reject) => {
      holding.onsuccess = () => resolve(holding.result);
      holding.onerror = () => reject(holding.error);
    });
    // Simulate another tab that keeps its connection open while an upgrade or deletion is waiting.
    held.onversionchange = () => {};
    const originalOpen = indexedDB.open;
    const state = window.__consumerStorageBlock = {
      blocked: false, deleted: false, requested: 0, settled: 0,
      release() { indexedDB.open = originalOpen; held.close(); },
    };
    indexedDB.open = function (...args) {
      const request = originalOpen.apply(this, args);
      if (args[0] === name) {
        state.requested += 1;
        request.addEventListener('success', () => { state.settled += 1; });
        request.addEventListener('error', () => { state.settled += 1; });
      }
      return request;
    };
    const deletion = indexedDB.deleteDatabase(name);
    await new Promise((resolve, reject) => {
      deletion.onblocked = () => { state.blocked = true; resolve(); };
      deletion.onerror = () => reject(deletion.error);
      deletion.onsuccess = () => {
        state.deleted = true;
        if (!state.blocked) reject(new Error('the storage blocker did not keep its database open'));
      };
    });
  }, databaseName);
  try {
    await page.locator('#start-abort').click();
    // This open happens after the planner and DuckDB have started. Waiting for it proves this abort tests storage,
    // rather than repeating the module-download cancellation cases above.
    await page.waitForFunction(() => window.__consumerStorageBlock.requested > 0, null, { timeout: 60_000 });
    assert.deepEqual(await page.evaluate(() => ({
      blocked: window.__consumerStorageBlock.blocked,
      deleted: window.__consumerStorageBlock.deleted,
      settled: window.__consumerStorageBlock.settled,
    })), { blocked: true, deleted: false, settled: 0 });
    assert.equal(await page.locator('#state').textContent(), 'starting-abort');
    assert.ok(await page.evaluate(() => window.__consumerWorkerCount >= 2), 'both runtimes started before storage blocked');
    await page.locator('#abort').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'aborted', null, { timeout: 15_000 });
    assert.equal(await page.locator('#expected-error').textContent(), 'AbortError: Consumer cancelled startup');
    assert.equal(await page.evaluate(() => window.__consumerWorkerCount), 0, 'storage cancellation releases both workers before IndexedDB answers');
    assert.equal(await page.locator('#cube .dc-browser').count(), 0);
    assert.equal(await page.evaluate(() => window.__consumerStorageBlock.deleted), false, 'the blocker is still held when cleanup finishes');
  } finally {
    await page.evaluate(() => window.__consumerStorageBlock.release());
  }
  await page.waitForFunction(() => window.__consumerStorageBlock.deleted
    && window.__consumerStorageBlock.settled === window.__consumerStorageBlock.requested);
  // If an abandoned open eventually succeeded without closing, this deletion would block on its leaked connection.
  await page.evaluate((name) => new Promise((resolve, reject) => {
    const deletion = indexedDB.deleteDatabase(name);
    deletion.onsuccess = () => resolve();
    deletion.onerror = () => reject(deletion.error);
    deletion.onblocked = () => reject(new Error('aborted startup retained its late IndexedDB connection'));
  }), databaseName);
  assert.equal(await page.locator('#cube .dc-browser').count(), 0, 'a late storage answer does not recreate the disposed view');
  await page.locator('#mount').click();
  await page.waitForFunction(() => document.querySelector('#state').value === 'ready', null, { timeout: 60_000 });
  await page.locator('#dispose').click();
  await page.waitForFunction(() => document.querySelector('#state').value === 'disposed');
  assert.equal(await page.evaluate(() => window.__consumerWorkerCount), 0, 'remount after blocked storage also disposes cleanly');
}

test('the installed DataCube package analyzes a local file without a backend', { timeout: 240_000 }, async (t) => {
  const application = await tmpDir('datacube-npm-consumer-');
  const web = join(application, 'public');
  // A subpath catches loaders that accidentally assume assets live at the site's root.
  const site = join(web, 'embedded', 'analysis');
  await mkdir(site, { recursive: true });
  const fixtures = runfilesFromEnv('CONSUMER_FIXTURES');
  for (const source of fixtures) {
    const name = basename(source).replace(/\.txt$/, '');
    await copyFile(source, join(name === 'index.html' ? site : application, name));
  }
  await writeFile(join(application, 'package.json'), JSON.stringify({ name: 'independent-consumer', private: true, type: 'module' }));
  await writeFile(join(application, 'user.npmrc'), '');
  await writeFile(join(application, 'global.npmrc'), '');
  const { npmCli } = JSON.parse(await readFile(runfileFromEnv('CONSUMER_TOOLS'), 'utf8'));
  await command(process.execPath, [
    runfile(npmCli), 'install', runfileFromEnv('NPM_ARCHIVE'), '--offline', '--ignore-scripts', '--no-audit', '--no-fund',
    '--cache', join(application, 'npm-cache'), '--userconfig', join(application, 'user.npmrc'),
    '--globalconfig', join(application, 'global.npmrc'), '--registry', 'http://127.0.0.1:9',
  ], application, { npm_config_update_notifier: 'false' });

  const installed = join(application, 'node_modules', '@legend-lite', 'datacube');
  const manifest = JSON.parse(await readFile(join(installed, 'package.json'), 'utf8'));
  assert.equal(manifest.name, '@legend-lite/datacube');
  assert.ok(!manifest.private, 'the release manifest must be publishable');
  // The compiler sees only the consumer and declarations in its real npm installation. No skipLibCheck hides a
  // declaration's private source import, and the fixture does not borrow the repo's TypeScript configuration.
  await command(process.execPath, [typescript(), '-p', application], application);
  await command(process.execPath, [join(installed, 'bin', 'copy-assets.mjs'), join(site, 'assets')], application);
  await command(runfileFromEnv('CONSUMER_ESBUILD'), [
    'main.ts', '--bundle', '--format=esm', '--platform=browser', '--target=es2022', '--splitting',
    '--outdir=public/embedded/analysis', '--chunk-names=chunks/[name]-[hash]', '--log-level=warning',
  ], application);

  const forbidden = [];
  const requested = [];
  const failures = [];
  const server = await serve(web, {
    route(req, res, url) {
      requested.push(url.pathname);
      if (!['GET', 'HEAD'].includes(req.method) || !url.pathname.startsWith('/embedded/analysis/')) {
        forbidden.push(`${req.method} ${url.pathname}`);
        res.writeHead(403);
        res.end('this test serves only the consumer application static files');
        return true;
      }
      return false;
    },
  });
  let browser;
  let context;
  t.after(async () => {
    await context?.close();
    await browser?.close();
    await server.close();
  });
  browser = await chromium.launch();
  context = await browser.newContext({ viewport: { width: 1440, height: 940 }, serviceWorkers: 'block' });
  const page = await context.newPage();
  page.setDefaultTimeout(15_000);
  page.on('pageerror', (reason) => failures.push(reason.message));
  page.on('console', (message) => {
    if (message.type() === 'error') failures.push(message.text());
  });
  context.on('response', (response) => {
    if (response.status() >= 400) failures.push(`${response.status()} ${response.url()}`);
  });
  await context.route('**/*', (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== server.origin) {
      forbidden.push(route.request().url());
      return route.abort('blockedbyclient');
    }
    return route.continue();
  });
  // Track native worker lifetimes from before the package loads, independently of its internal references.
  await page.addInitScript(() => {
    const OriginalWorker = window.Worker;
    const active = new Set();
    Object.defineProperty(window, '__consumerWorkerCount', { get: () => active.size });
    window.Worker = class extends OriginalWorker {
      constructor(...args) {
        super(...args);
        active.add(this);
      }
      terminate() {
        active.delete(this);
        super.terminate();
      }
    };
  });
  try {
    const address = `${server.origin}/embedded/analysis/index.html?host=kept#application-route`;
    await page.goto(address, { waitUntil: 'load', timeout: 60_000 });
    await page.waitForFunction(() => document.querySelector('#state').value === 'ready', null, { timeout: 60_000 });
    assert.equal(await page.title(), 'Independent application');
    assert.equal(page.url(), address, 'mounting DataCube must preserve its host route');
    const databases = await page.evaluate(async () => (await indexedDB.databases()).map((database) => database.name));
    assert.ok(databases.includes('independent-npm-consumer.pages'), 'saved pages use the application storage namespace');
    assert.ok(databases.every((name) => name?.startsWith('independent-npm-consumer.')), 'the library creates no shared default store');
    assert.equal(await page.evaluate(() => '__dataCube' in window), false, 'the package does not publish demo globals');
    assert.ok(await page.evaluate(() => window.__consumerWorkerCount >= 2), 'planner and database run in workers');
    await page.locator('#file').setInputFiles(join(application, 'sales.csv'));
    await page.waitForFunction(() => document.querySelector('#state').value === 'opened', null, { timeout: 60_000 });
    const all = [
      ['East', 'Q1', '10'], ['East', 'Q1', '20'], ['East', 'Q2', '30'],
      ['West', 'Q1', '40'], ['West', 'Q2', '50'], ['West', 'Q2', '60'],
    ];
    await rowsEqual(page, ['region', 'quarter', 'amount'], all);

    await menu(page, 'region', ['Filter', /^Add Filter: region = /], 'East');
    await rowsEqual(page, ['region', 'quarter', 'amount'], all.slice(0, 3));
    await menu(page, 'region', ['Filter', 'Clear All Filters']);
    await rowsEqual(page, ['region', 'quarter', 'amount'], all);

    await menu(page, 'region', ['Pivot', 'Vertical Pivot on region']);
    await rowsEqual(page, ['__tree', 'amount'], [['East', '60'], ['West', '150']]);
    await menu(page, 'quarter', ['Pivot', 'Horizontal Pivot on quarter']);
    await rowsEqual(page, ['__tree', 'Q1__|__amount', 'Q2__|__amount'], [['East', '30', '30'], ['West', '40', '110']]);

    // Saved pages run their real rules from the package's second WASM module. Restore an edited view so the test
    // proves both writing and reading, rather than just showing a saved record in a list.
    await pageMenu(page, 'Save');
    await page.locator('.dc-save-input').fill('Consumer quarter totals');
    await page.locator('.dc-save-input').press('Enter');
    await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 60_000 });
    await menu(page, '__tree', ['Pivot', 'Clear All Horizontal Pivots']);
    await rowsEqual(page, ['__tree', 'amount'], [['East', '60'], ['West', '150']]);
    await pageMenu(page, 'Open…');
    const saved = page.locator('.dc-lib-row').filter({ has: page.locator('.dc-lib-row-name', { hasText: 'Consumer quarter totals' }) });
    await saved.getByRole('button', { name: 'Open', exact: true }).click();
    // The page owner also confirms replacing unsaved changes. Playwright dismisses native dialogs unless handled.
    const confirming = page.waitForEvent('dialog');
    const opening = page.locator('.dc-lib-row').getByRole('button', { name: 'Open anyway', exact: true }).click();
    const dialog = await confirming;
    assert.equal(dialog.type(), 'confirm');
    assert.match(dialog.message(), /unsaved changes.*Consumer quarter totals/);
    await dialog.accept();
    await opening;
    await rowsEqual(page, ['__tree', 'Q1__|__amount', 'Q2__|__amount'], [['East', '30', '30'], ['West', '40', '110']]);
    await page.waitForFunction(() => !document.querySelector('.dc-open')
      || document.querySelector('.dc-lib-message')?.textContent?.startsWith('opened "Consumer quarter totals"'));
    // Reopening a saved snapshot can leave the existing explanatory note visible after its rows are restored.
    if (await page.locator('.dc-open').count()) await page.locator('.dc-open .dc-picker-close').click();
    await page.locator('.dc-open').waitFor({ state: 'detached' });

    await page.locator('#invalid').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'refused', null, { timeout: 60_000 });
    assert.match(await page.locator('#expected-error').textContent(), /parquet/i, 'openFile rejects a malformed file by name');
    await rowsEqual(page, ['__tree', 'Q1__|__amount', 'Q2__|__amount'], [['East', '30', '30'], ['West', '40', '110']]);
    assert.equal(await page.locator('#error').textContent(), '');
    assert.equal(await page.title(), 'Independent application');
    assert.equal(page.url(), address, 'file operations must preserve its host route');

    await page.locator('#dispose').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'disposed');
    assert.equal(await page.locator('#cube .dc-app').count(), 0);
    assert.equal(await page.evaluate(() => window.__consumerWorkerCount), 0, 'unmount releases both workers');
    await page.locator('#mount').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'ready', null, { timeout: 60_000 });
    await page.locator('#file').setInputFiles([]);
    await page.locator('#file').setInputFiles(join(application, 'sales.csv'));
    await rowsEqual(page, ['region', 'quarter', 'amount'], all);
    await page.locator('#hold-file').click();
    await page.waitForFunction(() => document.querySelector('#pending-open').value === 'reading');
    await page.locator('#dispose').click();
    await page.waitForFunction(() => document.querySelector('#state').value === 'disposed');
    await page.waitForFunction(() => document.querySelector('#pending-open').value === 'rejected');
    assert.match(await page.locator('#expected-error').textContent(), /^AbortError:/, 'disposal rejects a file read that never answers');
    assert.equal(await page.evaluate(() => window.__consumerWorkerCount), 0, 'a second mount also releases its workers');
    assert.equal(await page.locator('#cube .dc-browser').count(), 0);
    await page.locator('#release-file').click();
    await page.waitForFunction(() => document.querySelector('#pending-read').value === 'released');
    // Let the pending stream and its promise continuations run after disposal. Its eventual result must not remount.
    await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    assert.equal(await page.locator('#cube .dc-browser').count(), 0, 'a late file read does not recreate the disposed view');
    assert.equal(await page.locator('#error').textContent(), '');

    // Exercise both startup phases: first the module download, then extension setup on an instantiated database.
    // Releasing either response before the abort finishes would conceal an uninterruptible startup.
    await abortHeldStartup(page, context, '**/assets/duckdb-eh.wasm', '/assets/duckdb-eh.wasm');
    await abortHeldStartup(page, context, '**/assets/duckdb-extensions/**/icu.duckdb_extension.wasm', '/icu.duckdb_extension.wasm');
    await abortBlockedStorage(page);

    assert.ok(requested.some((path) => path.endsWith('/assets/classes.wasm')), 'the installed planner actually loaded');
    assert.ok(requested.some((path) => path.endsWith('/assets/duckdb-eh.wasm')), 'the installed database actually loaded');
    assert.ok(requested.some((path) => path.endsWith('/assets/queries/classes.wasm')), 'saved pages ran the installed rules locally');
    assert.deepEqual(forbidden, [], 'no backend, CDN, extension registry, or other remote service is contacted');
    assert.deepEqual(failures, [], 'no missing assets, browser errors, or rejected queries');
  } catch (reason) {
    const details = {
      forbidden, requested, failures,
      hostError: await page.locator('#error').textContent().catch(() => ''),
      expectedError: await page.locator('#expected-error').textContent().catch(() => ''),
      state: await page.locator('#state').textContent().catch(() => ''),
      pendingOpen: await page.locator('#pending-open').textContent().catch(() => ''),
      blockedStorage: await page.evaluate(() => {
        const state = window.__consumerStorageBlock;
        return state && { blocked: state.blocked, deleted: state.deleted, requested: state.requested, settled: state.settled };
      }).catch(() => undefined),
    };
    await writeFile(outPath('npm-consumer.json'), JSON.stringify(details, null, 2));
    await page.screenshot({ path: outPath('npm-consumer.png'), fullPage: true });
    throw new Error(`${reason.message}\n${JSON.stringify(details, null, 2)}`, { cause: reason });
  }
});
