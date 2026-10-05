// //studio:verify_test: Legend Studio end to end in the pinned Chromium (//tools/browser), the whole loop --
// the demo projects published, a workspace on trading compiled in the tab with its dependencies from Depot, an
// element added and saved, a review created and committed onto the project line, a version released --
// twice: with no server at all (level 0: the SDLC and Depot are WebAssembly in the page), and against the model
// home's server over a git repository on disk (level 1; SDLC_SERVER, started by sdlc-client's test helper).
// Screenshots go to the test's undeclared outputs (bazel-testlogs/studio/verify_test/test.outputs/).

import '../../tools/browser/pinned-chromium.mjs';
import { strict as assert } from 'node:assert';
import { mkdirSync } from 'node:fs';
import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { chromium } from 'playwright';

import { startSdlcServer } from '../../sdlc-client/test/sdlc-server.ts';

// the site: this package's own files, beside this harness (as datacube/demo/verify-smoke.mjs serves its own)
const ROOT = fileURLToPath(new URL('..', import.meta.url)).replace(/[\\/]$/, '');

const OUT = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? join(process.env.TEST_TMPDIR ?? tmpdir(), 'studio-verify');
mkdirSync(OUT, { recursive: true });

const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm',
  '.json': 'application/json', '.pure': 'text/plain', '.woff2': 'font/woff2',
};
const site = createServer(async (req, res) => {
  const { pathname } = new URL(req.url ?? '/', 'http://x');
  try {
    const base = pathToFileURL(`${ROOT}${sep}`);
    const file = fileURLToPath(new URL(`.${pathname}`, base));
    if (!file.startsWith(ROOT)) throw new Error('outside');
    await stat(file);
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream' }).end(await readFile(file));
  } catch {
    res.writeHead(404).end('not found');
  }
});
// port 0: the system's choice, never a guess that collides
const SITE = await new Promise((resolve) => site.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${site.address().port}`)));

const TRADING = 'org.finos.lite.demo:trading';

async function loop(browser, name, query) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  // generous waits: under a full gate run the machine is loaded, and the in-tab compile and the page's SDLC take
  // longer than Playwright's 30 s default (//studio:verify_test failed only then, 2026-10-05)
  page.setDefaultTimeout(120_000);
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const shot = (n) => page.screenshot({ path: join(OUT, `${name}-${n}.png`) });
  // the status bar's problems button says when the compiler is done and how many errors it found (data-state,
  // data-errors): upstream's bar shows icons and counts, not words
  const waitCompiled = () => page.waitForFunction(() => { const b = document.querySelector('[data-testid=problems-count]'); return b?.dataset.state === 'idle' && b.dataset.errors === '0'; }, undefined, { timeout: 120_000 });
  const statusOf = async (id) => ((await page.getByTestId(id).textContent()) ?? '').trim();
  const waitStatus = (id, pattern) => page.waitForFunction(([id, source]) => new RegExp(source).test(document.querySelector(`[data-testid=${id}]`)?.textContent ?? ''), [id, pattern.source], { timeout: 120_000 });
  try {
    await page.goto(`${SITE}/demo/index.html${query}`);
    // 1. the demo projects, published through the SDLC (every release through the compile gate)
    await page.getByTestId('load-demo').click({ timeout: 60_000 });
    await waitStatus('demo-status', /Demo projects published/);
    await page.getByTestId('project-selector').click();
    await page.locator(`[data-testid=project-selector-menu] [data-id="${TRADING}"]`).click();
    await shot('1-projects');
    // 2. a workspace on trading: it compiles in the tab, with its dependencies from Depot
    await page.getByTestId('new-workspace').click();
    await page.locator('.dialog input').fill('dev');
    await shot('1-dialog');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    await waitCompiled();
    // 3. an element using a type from a dependency, then saved
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-path').fill('demo::trading::Desk');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    // one input event, as a paste: keystroke by keystroke, Monaco's bracket auto-closing raced the typed '}' (a
    // stray second brace, 2026-10-04)
    await page.keyboard.insertText('// a trading desk, quoting in one currency\nClass demo::trading::Desk\n{\nname: String[1];\nbase: demo::types::Currency[1];\n}\n');
    await waitCompiled();
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no changes detected/);
    // the status bar's problems counts open upstream's Problems panel
    await page.getByTestId('problems-count').click();
    await page.getByText('No problems have been detected in the workspace.').waitFor();
    await shot('2-saved');
    await page.locator('.panel-group__action[title=Close]').click();
    // 3b. a function, run in the tab (plan A3): the planner writes its SQL, DuckDB here runs it on party's own rows --
    // its Data element, which trading has through its dependency (plan A2)
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-kind').selectOption({ label: 'Function' });
    await page.getByTestId('new-path').fill('demo::trading::parties');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('// every party, by name: run in the tab\nfunction demo::trading::parties(): meta::pure::metamodel::relation::Relation<Any>[1]\n{\ndemo::party::Party.all()->project(~[name: p | $p.name, country: p | $p.country])->from(demo::party::PartyMapping, demo::party::Runtime)\n}\n');
    await waitCompiled();
    await page.getByTestId('run-function').click();
    await page.waitForFunction(() => !/^Running/.test(document.querySelector('[data-testid=run-status]')?.textContent ?? 'Running'), undefined, { timeout: 120_000 });
    const ran = await statusOf('run-status');
    if (!/^5 rows in \d+ ms/.test(ran)) throw new Error(`the function's run said: ${ran}`);
    assert.ok((await page.getByTestId('run-rows').textContent()).includes('Banque Lumière'), 'the run shows the party rows');
    await shot('2b-ran');
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no changes detected/);
    await page.locator('.panel-group__action[title=Close]').click();
    // 4. a review, committed onto the project line (the workspace closes)
    await page.locator('[data-activity=review]').click();
    await page.getByTestId('review-title').fill('Add the desk');
    await page.getByTestId('create-review').click();
    await page.getByTestId('commit-review').click({ timeout: 30_000 });
    await page.waitForSelector('[data-testid=new-workspace]', { timeout: 60_000 });
    // 5. a version of the line's head, released through the gate
    await page.getByTestId('new-workspace').click();
    await page.locator('.dialog input').fill('release');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    assert.ok((await page.getByTestId('explorer').textContent()).includes('Desk'), 'the committed element is on the project line');
    await page.locator('[data-activity=project]').click();
    await page.locator('[data-project-tab=release]').click();
    await page.getByTestId('release-notes').fill('the desk');
    await page.getByTestId('release-minor').click();
    await page.waitForFunction(() => /1\.1\.0/.test(document.querySelector('[data-testid=latest-release]')?.textContent ?? ''), null, { timeout: 60_000 });
    await shot('3-released');
    await page.locator('[data-project-tab=versions]').click();
    await page.getByTestId('versions').waitFor();
    assert.match(await page.getByTestId('versions').textContent(), /1\.1\.0/);
    await page.locator('[data-project-tab=overview]').click();
    await page.getByTestId('dependencies').waitFor();
    assert.match(await page.getByTestId('dependencies').textContent(), /org\.finos\.lite\.demo:party : 1\.0\.0/);
    // the activity bar's sun/moon switch: upstream's default-light, kept, and back
    await page.getByTestId('theme-toggle').click();
    assert.equal(await page.evaluate(() => document.documentElement.dataset.theme), 'default-light');
    await shot('4-light');
    await page.getByTestId('theme-toggle').click();
    assert.equal(await page.evaluate(() => document.documentElement.dataset.theme), undefined);
    assert.deepEqual(errors, []);
    console.log(`${name}: the loop passed (${await page.getByTestId('problems-count').getAttribute('data-errors')} errors)`);
  } catch (e) {
    await shot('failed');
    throw new Error(`${name}: ${e.message}\npage errors: ${errors.join('\n')}`);
  } finally {
    await page.close();
  }
}

const browser = await chromium.launch();
let home;
try {
  // level 0: nothing but the page (each browser context has its own IndexedDB)
  await loop(await browser.newContext().then((c) => ({ newPage: (o) => c.newPage(o) })), 'page', '');
  // level 1: the model home over HTTP, a git repository on disk
  home = await startSdlcServer('studio-verify');
  await loop(await browser.newContext().then((c) => ({ newPage: (o) => c.newPage(o) })), 'server',
    `?sdlc=${encodeURIComponent(`${home.base}/sdlc/api`)}`);
  console.log(`studio verify: passed; screenshots in ${OUT}`);
} finally {
  await browser.close();
  site.close();
  await home?.stop();
}
