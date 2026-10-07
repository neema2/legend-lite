// The single-user app, end to end (docs/DATACUBE_APP_PLAN_2026_10_02.md, A2 and A3): the folder //datacube:app
// builds -- the native warehouse beside DuckDB's library, its postgres extension and the site -- copied to a plain
// directory and started there with --app, as a package's user starts it, nothing of Bazel around the server (the
// build rebuild's L1c). Its printed address is opened the way `--open` opens it. It starts from the launch key,
// never from the sample; it opens the table asked for, or offers the tables; a reload signs in again; a wrong key
// and an unknown table leave the blank page saying why.
//
//   DATACUBE_APP_PG=postgresql://reader:secret@127.0.0.1:5432/shop \
//   DATACUBE_APP_TABLE=sales.orders DATACUBE_APP_GROUP=channel bazel run //datacube:verify_app
//
// Under `bazel run` it needs a Postgres (16+) that the URL's user can read, as //warehouse:postgres_live does.
// As //datacube:verify_app_test (Bazel workplan P1-14b) it starts its own: :app_postgres, the pinned Postgres
// 16 loaded with demo/sample-shop.sql.

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { cpSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv, runfilesRoot } from '../../tools/js/runfiles.mts';

// the runfiles tree, through the runfiles helper (never this file's own location): the test's Postgres's RUNFILES_DIR
const RUNFILES = runfilesRoot();

let PG = process.env.DATACUBE_APP_PG;
let TABLE = process.env.DATACUBE_APP_TABLE;
let GROUP = process.env.DATACUBE_APP_GROUP;

// THE TEST'S OWN POSTGRES (verify_app_test): APP_POSTGRES is the rlocation of :app_postgres's executable (a
// script on Linux and macOS, an .exe on Windows), SAMPLE_SQL the sample's; both resolved through the runfiles
// (tools/js/runfiles.mts, Bazel workplan P1-24)
let postgres;
if (process.env.APP_POSTGRES) {
  postgres = spawn(runfileFromEnv('APP_POSTGRES'), [runfileFromEnv('SAMPLE_SQL')], {
    env: { ...process.env, RUNFILES_DIR: RUNFILES, JAVA_RUNFILES: RUNFILES },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  // a test that fails part-way, even before Postgres is ready, leaves no Postgres behind: its stdin closes
  // with this process
  process.on('exit', () => postgres.stdin.end());
  const port = await new Promise((done, fail) => {
    let said = '';
    postgres.stdout.on('data', (b) => {
      said += b.toString();
      const m = /postgres port (\d+)/.exec(said);
      if (m) done(m[1]);
    });
    postgres.on('exit', (code) => fail(new Error(`the test's Postgres exited (${code}) before it was ready:
${said}`)));
    setTimeout(() => fail(new Error(`the test's Postgres was not ready in 180s:
${said}`)), 180_000);
  });
  // the sample's database, login and table (demo/sample-shop.sql)
  PG = `postgresql://reader:secret@127.0.0.1:${port}/shop`;
  TABLE ??= 'sales.orders';
  GROUP ??= 'channel';
  console.log(`ok: the test's Postgres 16 is up on port ${port}, the sample loaded`);
}

if (!PG || !TABLE || !GROUP) {
  console.error('set DATACUBE_APP_PG (a postgresql:// URL with its password), DATACUBE_APP_TABLE (schema.name)'
    + ' and DATACUBE_APP_GROUP (a text column of that table to group by)');
  process.exit(2);
}

// THE APP FOLDER (//datacube:app, warehouse/defs.bzl): APP is the rlocation of its executable. The folder around it --
// the server, DuckDB's library, the postgres extension, site/ -- is copied to a directory of its own and the server
// started from there, with none of Bazel's variables: no runfiles, no output tree, nothing a package's user has.
const APP = process.env.APP;
if (!APP) {
  console.error('run this as `bazel run //datacube:verify_app`: APP names the app\'s executable');
  process.exit(2);
}
const built = runfileFromEnv('APP');
const folder = mkdtempSync(join(process.env.TEST_TMPDIR ?? tmpdir(), 'datacube-app-'));
// dereference: in a runfiles tree the folder's entries are links to the built files
cpSync(dirname(built), folder, { recursive: true, dereference: true });
const app = join(folder, basename(built));
const plainEnv = Object.fromEntries(Object.entries(process.env)
  .filter(([k]) => !/^(RUNFILES_|JAVA_RUNFILES|BUILD_WORKING_DIRECTORY|BUILD_WORKSPACE_DIRECTORY|TEST_SRCDIR|TEST_WORKSPACE)/.test(k)));

let failed = false;
const bad = (m) => { console.log(`FAIL: ${m}`); failed = true; };
const ok = (m) => console.log(`ok: ${m}`);

// The app as `bazel run //datacube:app` starts it (--app: one user, the site beside the executable), without --open:
// the address is read from what it prints. Started in its own folder, as a user would.
const server = spawn(app, ['--port', '0', '--app', PG], { cwd: folder, env: plainEnv, stdio: ['ignore', 'ignore', 'pipe'] });
let printed = '';
const address = await new Promise((done, fail) => {
  server.stderr.on('data', (b) => {
    printed += b.toString();
    const m = /DataCube: (http:\/\/127\.0\.0\.1:\d+\/#key=[A-Za-z0-9_-]+)/.exec(printed);
    if (m) done(m[1]);
  });
  server.on('exit', (code) => fail(new Error(`the warehouse exited (${code}):\n${printed}`)));
  setTimeout(() => fail(new Error(`no address in 60s:\n${printed}`)), 60_000);
});
ok(`the warehouse printed ${address.replace(/key=.*/, 'key=…')}, started from ${folder}`);
// no --data: the server said its data is a fresh directory, removed when it stops (checked at the end)
const dataDir = /warehouse data in (.+) \(temporary: removed when the warehouse stops\)/.exec(printed)?.[1];
if (dataDir) ok(`its data is temporary: ${dataDir}`);
else bad(`the app did not say its data is temporary:\n${printed}`);

let browser;
try {
  // inside the try: a launch failure still stops the warehouse below
  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const statuses = [];
  page.on('pageerror', (e) => bad(`page error: ${e.message}`));
  await page.exposeFunction('__status', (t) => statuses.push(t));
  await page.addInitScript(() => {
    new MutationObserver(() => {
      const s = document.getElementById('status');
      if (s) window.__status(s.textContent ?? '');
    }).observe(document, { subtree: true, childList: true, characterData: true });
  });
  // Live: the title bar's plane button says so, and the status bar's receipt names the warehouse
  const live = () => page.waitForFunction(() =>
    document.querySelector('.dc-titlebar-toggle')?.textContent?.trim() === 'Live'
    && /the warehouse/.test(document.querySelector('.dc-status-receipt')?.textContent ?? ''), undefined,
  { timeout: 120_000 });

  // 1. THE TABLE ASKED FOR, opened Live, and nothing generated first
  await page.goto(`${address}&table=${TABLE}`);
  await live();
  const title = await page.evaluate(() => window.__dataCube?.configuration.reportTitle);
  if (title !== TABLE) bad(`the cube is ${title}, not ${TABLE}`); else ok(`opened ${TABLE} Live`);
  if (statuses.some((t) => /generating/.test(t))) bad('the sample was generated before the table opened');
  else ok('no sample was generated');

  // 2. GROUPED in Postgres: one row per value, no error
  const cols = await page.$$eval('.dc-th[data-column]', (els) => els.map((e) => e.dataset.column));
  const at = cols.indexOf(GROUP);
  if (at < 0) bad(`${GROUP} is not a column (${cols.join(', ')})`);
  else {
    await page.locator('.dc-row').nth(0).locator('.dc-cell').nth(at).click({ button: 'right' });
    const own = (t) => `.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(t)}))`;
    await page.locator(own('Pivot')).first().hover();
    await page.locator(own(`Vertical Pivot on ${GROUP}`)).first().click();
    await page.waitForFunction(() => document.querySelectorAll('.dc-row[aria-expanded]').length > 0, undefined,
      { timeout: 60_000 }).catch(() => bad(`grouping by ${GROUP} shows no groups`));
    if (await page.locator('text=Data Fetch Failure').count()) {
      // the failure dialog's own words: what Postgres (or the planner) said
      const said = (await page.locator('text=Data Fetch Failure').locator('xpath=ancestor::*[contains(@class, "dc-")][1]').innerText())
        .replace(/\s+/g, ' ').slice(-900);
      bad(`grouping by ${GROUP} failed in Postgres: ${said}`);
    }
    else ok(`grouped by ${GROUP}: ${await page.locator('.dc-row[aria-expanded]').count()} groups`);
  }

  // 2b. SNAPPED: the plane button copies the table into the tab; the copy shows the same groups,
  // planned for the tab's DuckDB, with no failure (every column of the table, as Postgres read it)
  const groups = await page.locator('.dc-row[aria-expanded]').count();
  await page.click('.dc-titlebar-toggle');
  await page.waitForFunction(() => window.__dataCube?.controller.snaps.state.mode === 'snapped'
    && !window.__dataCube.busy, null, { timeout: 120_000 })
    .catch(() => bad('the table did not snap'));
  if (await page.locator('text=Data Fetch Failure').count()) {
    bad(`the snapped copy failed: ${(await page.locator('text=Data Fetch Failure')
      .locator('xpath=ancestor::*[contains(@class, "dc-")][1]').innerText()).replace(/\s+/g, ' ').slice(-900)}`);
  } else if (await page.locator('.dc-row[aria-expanded]').count() !== groups) {
    bad(`the snapped copy shows ${await page.locator('.dc-row[aria-expanded]').count()} groups, Live showed ${groups}`);
  } else ok(`snapped: the copy shows the same ${groups} groups`);

  // 3. A RELOAD signs in again with the key still in the address
  await page.reload();
  await live();
  ok('a reload signed in again');

  // Each start below is a NEW page: going to an address that differs only in its fragment does not
  // load the page again, so the start would never run.
  const fresh = async () => {
    const p = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    p.on('pageerror', (e) => bad(`page error: ${e.message}`));
    return p;
  };

  // 4. NO TABLE: the tables are offered, already signed in
  const offered = await fresh();
  await offered.goto(address);
  await offered.locator('.dc-picker-row[data-object]').first().waitFor({ timeout: 60_000 })
    .then(async () => ok(`the tables are offered: ${await offered.locator('.dc-picker-row[data-object]').count()}`))
    .catch(() => bad('without a table, no table list was offered'));
  if (await offered.locator('.dc-picker-form input[type=password]').count()) bad('the picker asked for a password');

  // 5. A WRONG KEY and 6. AN UNKNOWN TABLE: the blank page says why
  for (const [url, why] of [
    [address.replace(/key=.*/, 'key=not-the-key'), /wrong launch key/],
    [`${address}&table=no_such.table`, /no_such\.table is not a table you may read here/],
  ]) {
    const refused = await fresh();
    await refused.goto(url);
    const said = await refused.locator('.dc-blank-reason').textContent({ timeout: 60_000 }).catch(() => '');
    if (!why.test(said ?? '')) bad(`${url.replace(/key=[^&]*/, 'key=…')}: the blank page said "${said}"`);
    else ok(`the blank page says: ${said}`);
  }
} finally {
  await browser?.close();
  // The server is this process's own child on every platform (no launcher since L1c), so kill() stops it; the folder
  // copied above is under TEST_TMPDIR either way.
  server.kill('SIGTERM');
  await new Promise((done) => {
    if (server.exitCode !== null || server.signalCode !== null) return done();
    server.on('exit', done);
    setTimeout(done, 15_000);
  });
  if (dataDir) {
    // a removal that fails (Windows: a handle released late, an antivirus scan) is a finding, not a thrown error
    // that would skip stopping the test's Postgres below
    const remove = () => {
      try { rmSync(dataDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 }); return true; }
      catch (e) { bad(`could not remove the temporary data ${dataDir}: ${e.message}`); return false; }
    };
    if (process.platform === 'win32') {
      // Windows: kill() is TerminateProcess, under which the server's own removal of its temporary data cannot run
      // (as under the taskkill /f of before), so the test removes it (L1d; a desk's Ctrl+C is a separate check)
      if (remove()) ok('the temporary data removed by the test (Windows: a hard kill runs no cleanup)');
    } else if (existsSync(dataDir)) {
      bad(`the temporary data is still there after SIGTERM: ${dataDir}`);
      remove();
    } else ok('the temporary data is gone: the server removed it on SIGTERM');
  }
}
if (postgres) {
  // closing its stdin stops it, and Postgres with it
  postgres.stdin.end();
  const code = await new Promise((done) => {
    if (postgres.exitCode !== null) done(postgres.exitCode);
    postgres.on('exit', done);
    setTimeout(() => done('still running after 60 s'), 60_000);
  });
  if (code !== 0) bad(`the test's Postgres did not stop cleanly: ${code}`);
}
process.exit(failed ? 1 : 0);
