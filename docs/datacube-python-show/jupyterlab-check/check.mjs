// A MANUAL CHECK of a notebook's cube in a real JupyterLab (not a test; README.md beside it): a fresh kernel, the
// notebook's cells run one by one (check.ipynb), what shows reported, and two screenshots.
//   PLAYWRIGHT=<.../playwright/index.mjs> node check.mjs <a Chromium executable>
// Playwright from the repository's own install (bazel-bin/datacube/node_modules/playwright), by its path
const { chromium } = await import(process.env.PLAYWRIGHT);

const browser = await chromium.launch({ executablePath: process.argv[2] });
const page = await browser.newPage({ viewport: { width: 1400, height: 2200 } });
page.on('pageerror', (e) => console.log('pageerror', e.message));
page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log('console', m.type(), m.text().slice(0, 300)); });
await page.goto('http://127.0.0.1:8899/lab/tree/check.ipynb?token=checktoken');
const idle = () => page.waitForFunction(() => {
  const nb = window.jupyterapp?.shell?.currentWidget;
  return nb?.sessionContext?.session?.kernel?.status === 'idle' && !nb.sessionContext.pendingInput;
}, null, { timeout: 120_000 });
await idle();
// a fresh kernel (the code just installed) and no outputs left from a run before
await page.evaluate(async () => {
  const nb = window.jupyterapp.shell.currentWidget;
  await nb.sessionContext.session.kernel.restart();
  await window.jupyterapp.commands.execute('notebook:clear-all-cell-outputs');
});
await page.waitForTimeout(500);
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
const outputs = () => page.evaluate(() => window.jupyterapp.shell.currentWidget.content.widgets.map((c) => c.model.outputs?.length ?? 0));
const text = (i) => page.evaluate((i) => window.jupyterapp.shell.currentWidget.content.widgets[i].node.querySelector('.jp-OutputArea')?.innerText ?? '', i);
const until = async (what, test) => {
  const ok = await page.waitForFunction(test, null, { timeout: 60_000 }).then(() => true, () => false);
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}`);
};
for (const i of [0, 1, 2]) await run(i);
await until('both cubes show 4 rows', () => {
  const cells = window.jupyterapp.shell.currentWidget.content.widgets;
  return [1, 2].every((i) => /\b4 rows\b/.test(cells[i].node.innerText));
});
console.log('outputs per cell', JSON.stringify(await outputs()));
await page.screenshot({ path: 'shot-1.png', fullPage: true });
await run(3);
await until('the Live change shows after the cell (999.5)', () => window.jupyterapp.shell.currentWidget.content.widgets[1].node.innerText.includes('999.5'));
await run(4);
await until('cube.update shows 2 rows', () => /\b2 rows\b/.test(window.jupyterapp.shell.currentWidget.content.widgets[1].node.innerText));
console.log('outputs per cell, all run', JSON.stringify(await outputs()));
// a key in the cube: the notebook's active cell does not move
const cell = page.locator('.jp-Cell').nth(1).locator('.dc-grid').first();
await cell.click({ position: { x: 60, y: 60 } }).catch((e) => console.log('click', e.message));
const before = await page.evaluate(() => window.jupyterapp.shell.currentWidget.content.activeCellIndex);
await page.keyboard.press('ArrowDown');
await page.keyboard.press('ArrowDown');
await page.waitForTimeout(500);
const after = await page.evaluate(() => window.jupyterapp.shell.currentWidget.content.activeCellIndex);
console.log(`${before === after ? 'ok  ' : 'FAIL'} arrow keys in the cube leave the notebook's active cell (${before} -> ${after})`);
console.log('cell 1 text:', (await text(1)).slice(0, 300).replace(/\n/g, ' | '));
await page.screenshot({ path: 'shot-2.png', fullPage: true });
await browser.close();
