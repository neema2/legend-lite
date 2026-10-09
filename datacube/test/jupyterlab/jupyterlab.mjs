// A NOTEBOOK'S CUBE IN A REAL JUPYTERLAB, run in the pinned Chromium (//datacube:jupyterlab_test). Node here only
// drives the browser (Playwright): it starts JupyterLab (jupyterlab_fixture.py, beside it: legend-lite's wheel and
// JupyterLab pip-installed into a fresh environment, offline, as a developer installs them), opens the notebook
// (datacube.ipynb), runs its cells one by one, and
// checks what a person would see -- the parts the other tests stand in for: anywidget's own front end loading the cube,
// ipywidgets' channel to a real kernel, the cell outputs, and JupyterLab's keyboard shortcuts. The manual check it
// replaces found a real fault (docs/datacube-python-show/jupyterlab-check/).
//
// The notebook's cells: 0 a DataFrame; 1 `cube = ll.show(df)`; 2 `ll.show(df, name='again')` as the cell's last line;
// 3 an in-place change (`df.loc[0, 'qty'] = 999.5`); 4 `cube.update(df.head(2))`.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

// the wheels by their runfiles paths (LEGEND_LITE_WHEEL, LEGEND_LITE_DEPENDENCY_WHEELS), resolved by the fixture: a
// program a test starts gets the test's environment and runfiles, no BUILD env of its own
const fixture = spawn(runfileFromEnv('JUPYTERLAB_FIXTURE'), ['--notebook', runfileFromEnv('NOTEBOOK')], {
  stdio: ['pipe', 'pipe', 'pipe'],
});
// JupyterLab's own account (and its kernel's), in this test's log: a server-side reason is then never invisible
fixture.stderr.on('data', (b) => { for (const line of String(b).split('\n')) if (line) process.stderr.write(`[jupyter] ${line}\n`); });
const stopped = new Promise((done) => fixture.once('exit', done));
const exited = new Promise((_, fail) => fixture.once('exit', (code) => fail(new Error(`JupyterLab exited (${code})`))));
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
try {
  browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1400, height: 2200 } });
  page.on('pageerror', (e) => { say(false, `no error in the page: ${e.message}`); });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text().slice(0, 300)}`); });
  await page.goto(`${served.url}lab/tree/datacube.ipynb?token=${served.token}`);
  // the notebook's kernel started and done with what it was given
  const idle = () => page.waitForFunction(() => {
    const nb = window.jupyterapp?.shell?.currentWidget;
    return nb?.sessionContext?.session?.kernel?.status === 'idle' && !nb.sessionContext.pendingInput;
  }, undefined, { timeout: 120_000 });
  await idle();
  const run = async (i) => {
    await page.evaluate(async (i) => {
      const nb = window.jupyterapp.shell.currentWidget;
      nb.content.activeCellIndex = i;
      await window.jupyterapp.commands.execute('notebook:run-cell');
    }, i);
    await page.waitForTimeout(300);
    await idle();
  };
  const cellText = (i) => page.evaluate((i) => window.jupyterapp.shell.currentWidget.content.widgets[i].node.innerText, i);
  // a check that fails ends the run (the rest would wait out their own timeouts): what showed is reported below
  const until = async (what, test, arg) => {
    const ok = await page.waitForFunction(test, arg, { timeout: 60_000 }).then(() => true, () => false);
    say(ok, what);
    if (!ok) throw new Error(`stopped at: ${what}`);
  };
  for (const i of [0, 1, 2]) await run(i);
  await until('two cubes under their cells show the frame\'s 4 rows', () => {
    const cells = window.jupyterapp.shell.currentWidget.content.widgets;
    return [1, 2].every((i) => /\b4 rows\b/.test(cells[i].node.innerText));
  });
  say((await cellText(1)).includes('the engine at kernel'), 'the cube says where its rows came from: the kernel');
  await run(3);
  await until('an in-place change shows after its cell, by itself', () =>
    window.jupyterapp.shell.currentWidget.content.widgets[1].node.innerText.includes('999.50'));
  await run(4);
  await until('cube.update() shows the new frame in the same cube', () =>
    /\b2 rows\b/.test(window.jupyterapp.shell.currentWidget.content.widgets[1].node.innerText));
  const outputs = await page.evaluate(() => window.jupyterapp.shell.currentWidget.content.widgets
    .map((c) => c.model.outputs?.length ?? 0));
  say(JSON.stringify(outputs) === '[0,1,1,0,0]',
    'one cube under each show(), none under the change or the update', JSON.stringify(outputs));
  // a key in the cube is the grid's, not the notebook's: the grid's selection moves down a row, the focus stays in the
  // grid, and the notebook's active cell stays (clicking an output selects its cell, as JupyterLab does)
  await page.locator('.jp-Cell').nth(1).locator('.dc-cell', { hasText: /^FX$/ }).first().click({ timeout: 10_000 });
  const keys = () => page.evaluate(() => {
    const cell = window.jupyterapp.shell.currentWidget.content.widgets[1].node;
    return {
      selected: cell.querySelector('.dc-cell.dc-selected')?.textContent ?? null,
      inGrid: document.activeElement?.closest('.dc-grid') !== null,
      active: window.jupyterapp.shell.currentWidget.content.activeCellIndex,
    };
  });
  const before = await keys();
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(300);
  const after = await keys();
  say(before.selected === 'FX' && after.selected === 'EQ' && after.inGrid && before.active === after.active,
    'an arrow key moves the grid\'s selection, not the notebook\'s cell', `${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
} catch (e) {
  say(false, e instanceof Error ? e.message : String(e));
} finally {
  // what the page showed, whatever happened: the screenshot (the test's output) and, on a failure, the cube's text
  const page = browser?.contexts()[0]?.pages()[0];
  if (page && process.env.TEST_UNDECLARED_OUTPUTS_DIR) {
    await page.screenshot({ path: join(process.env.TEST_UNDECLARED_OUTPUTS_DIR, 'jupyterlab.png'), fullPage: true })
      .catch(() => {});
  }
  if (page && failed) {
    console.log(`the page showed:\n${(await page.evaluate(() => document.body.innerText).catch(() => '')).slice(0, 1500)}`);
  }
  await browser?.close();
  // stopped and WAITED for: JupyterLab serves until its input closes, its kernels gone with it, before the test reports
  fixture.stdin.end();
  await stopped;
}
process.exit(failed ? 1 : 0);
