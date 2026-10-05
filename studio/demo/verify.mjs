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
  const errors = [];
  page.on('pageerror', (e) => errors.push(e.message));
  const shot = (n) => page.screenshot({ path: join(OUT, `${name}-${n}.png`) });
  const statusText = (id) => page.getByTestId(id).textContent();
  const waitStatus = (id, pattern) => page.waitForFunction(([id, source]) => new RegExp(source).test(document.querySelector(`[data-testid=${id}]`)?.textContent ?? ''), [id, pattern.source], { timeout: 120_000 });
  try {
    await page.goto(`${SITE}/demo/index.html${query}`);
    // 1. the demo projects, published through the SDLC (every release through the compile gate)
    await page.getByTestId('load-demo').click({ timeout: 60_000 });
    await waitStatus('demo-status', /Demo projects published/);
    await page.locator(`[data-testid=projects] .setup-item[data-id="${TRADING}"]`).click();
    await shot('1-projects');
    // 2. a workspace on trading: it compiles in the tab, with its dependencies from Depot
    await page.getByTestId('new-workspace').click();
    await page.locator('.dialog input').fill('dev');
    await page.locator('.dialog .btn-primary').click();
    await page.waitForSelector('[data-testid=explorer] .element');
    await waitStatus('problems-count', /^0 problems/);
    // 3. an element using a type from a dependency, then saved
    await page.getByTestId('new-element').click();
    await page.getByTestId('new-path').fill('demo::trading::Desk');
    await page.locator('.dialog .btn-primary').click();
    await page.locator('.monaco-editor .view-lines').click();
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.type('// a trading desk, quoting in one currency\nClass demo::trading::Desk\n{\nname: String[1];\nbase: demo::types::Currency[1];\n}\n');
    await waitStatus('problems-count', /^0 problems/);
    await page.getByTestId('save-status').click();
    await page.locator('.dialog .btn-primary').click();
    await waitStatus('changes-count', /no local changes/);
    await shot('2-saved');
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
    await page.getByTestId('release-notes').fill('the desk');
    await page.getByTestId('release-minor').click();
    await page.waitForFunction(() => /1\.1\.0/.test(document.querySelector('[data-testid=versions]')?.textContent ?? ''), null, { timeout: 60_000 });
    await shot('3-released');
    assert.match(await page.getByTestId('dependencies').textContent(), /org\.finos\.lite\.demo:party : 1\.0\.0/);
    assert.deepEqual(errors, []);
    console.log(`${name}: the loop passed (${await statusText('problems-count')})`);
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
