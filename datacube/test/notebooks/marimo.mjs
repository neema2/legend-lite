// A NOTEBOOK'S CUBE IN A REAL MARIMO NOTEBOOK, run in the pinned Chromium (//datacube:marimo_test). Node here only drives
// the browser (Playwright): it starts marimo (notebook_fixture.py, beside it: legend-lite's wheel and marimo
// pip-installed into a fresh environment, offline, as a developer installs them) running the notebook
// (marimo_notebook.py) as an app, and checks what a person would see: the cube as the cell's output, with the frame's
// rows; a marimo slider re-running the cells, the cube then showing the new frame under the same name (the cell's run
// closed the old cube, its frame and name with it); and the cube's keys its own. The design:
// docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a marimo notebook".
//
// The notebook's cells: imports; a slider `rows` (1 to 4, at 4); `df`, the frame's first `rows.value` rows; `ll.show(df)`.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

// the wheels by their runfiles paths (LEGEND_LITE_WHEEL, LEGEND_LITE_DEPENDENCY_WHEELS), resolved by the fixture
const fixture = spawn(runfileFromEnv('NOTEBOOK_FIXTURE'), ['--app', 'marimo', '--notebook', runfileFromEnv('NOTEBOOK')], {
  stdio: ['pipe', 'pipe', 'pipe'],
});
// marimo's own account, in this test's log
fixture.stderr.on('data', (b) => { for (const line of String(b).split('\n')) if (line) process.stderr.write(`[marimo] ${line}\n`); });
const stopped = new Promise((done) => fixture.once('exit', done));
const exited = new Promise((_, fail) => fixture.once('exit', (code) => fail(new Error(`marimo exited (${code})`))));
const lines = createInterface({ input: fixture.stdout })[Symbol.asyncIterator]();
let first;
do first = (await Promise.race([lines.next(), exited])).value; while (first !== undefined && !first.startsWith('{'));
const served = JSON.parse(first);

let failed = false;
const say = (ok, what, detail = '') => {
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}${detail ? ` (${detail})` : ''}`);
  if (!ok) failed = true;
};
let browser;
let page;
try {
  browser = await chromium.launch();
  page = await browser.newPage({ viewport: { width: 1400, height: 1600 } });
  page.on('pageerror', (e) => { say(false, `no error in the page: ${e.message}`); });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text().slice(0, 300)}`); });
  await page.goto(`${served.url}?access_token=${served.token}`);
  // the cube, wherever marimo puts a widget (Playwright's locators see into shadow roots)
  const cube = page.locator('.dc-app').first();
  // a check that fails ends the run (the rest would wait out their own timeouts): what showed is reported below
  const until = async (what, test) => {
    const ok = await test().then(() => true, () => false);
    say(ok, what);
    if (!ok) throw new Error(`stopped at: ${what}`);
  };
  await until('the cube is the cell\'s output, with the frame\'s 4 rows',
    () => cube.getByText(/\b4 rows\b/).waitFor({ timeout: 120_000 }));
  await until('the cube says where its rows came from: the engine, over marimo\'s channel',
    () => cube.getByText('the engine at kernel').waitFor({ timeout: 10_000 }));
  // DataCube's theme sets --dc-font on the cube: unset, the cube's styles never reached it (marimo puts it in a
  // shadow root, which a page's styles do not enter)
  await until('the cube is styled: DataCube\'s styles reach it inside marimo\'s shadow root',
    () => cube.evaluate((app) => {
      if (getComputedStyle(app).getPropertyValue('--dc-font').trim() === '') throw new Error('unstyled');
    }));
  // the slider, from 4 to 2: marimo runs the frame's cell and the cube's again
  const slider = page.getByRole('slider').first();
  await slider.focus();
  await page.keyboard.press('ArrowLeft');
  await page.keyboard.press('ArrowLeft');
  await until('a marimo control re-running the cells shows the new frame in the cube',
    () => page.locator('.dc-app').first().getByText(/\b2 rows\b/).waitFor({ timeout: 60_000 }));
  const title = await page.locator('.dc-app').first().innerText();
  say(/\bframe \(on the engine\)/.test(title) && !/frame_2/.test(title),
    'the re-run cube has the same name: the cell\'s run closed the old cube and freed its name');
  say(await page.locator('.dc-app').count() === 1, 'one cube on the page after the re-run');
  // the re-run cube styled too: the page's DataCube module and its styles reused, not fetched again
  say(await page.locator('.dc-app').first().evaluate((app) => getComputedStyle(app).getPropertyValue('--dc-font').trim() !== ''),
    'the re-run cube is styled too');
  // a key in the cube is the grid's: its selection moves down a row, the focus stays in the grid
  await page.locator('.dc-app').first().locator('.dc-cell', { hasText: /^FX$/ }).first().click({ timeout: 10_000 });
  const keys = () => page.locator('.dc-app').first().evaluate((app) => ({
    selected: app.querySelector('.dc-cell.dc-selected')?.textContent ?? null,
    inGrid: (app.getRootNode().activeElement ?? document.activeElement)?.closest('.dc-grid') !== null,
  }));
  const before = await keys();
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(300);
  const after = await keys();
  say(before.selected === 'FX' && after.selected === 'EQ' && after.inGrid, 'an arrow key moves the grid\'s selection',
    `${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
} catch (e) {
  say(false, e instanceof Error ? e.message : String(e));
} finally {
  // what the page showed, whatever happened: the screenshot (the test's output) and, on a failure, its text
  if (page && process.env.TEST_UNDECLARED_OUTPUTS_DIR) {
    await page.screenshot({ path: join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, 'marimo.png'), fullPage: true })
      .catch(() => {});
  }
  if (page && failed) {
    console.log(`the page showed:\n${(await page.evaluate(() => document.body.innerText).catch(() => '')).slice(0, 1500)}`);
  }
  await browser?.close();
  // stopped and WAITED for: marimo serves until its launcher's input closes
  fixture.stdin.end();
  await stopped;
}
process.exit(failed ? 1 : 0);
