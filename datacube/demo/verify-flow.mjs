// A FLOW AS A PERSON BUILDS ONE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1, its "done when"): Create ▸ Flow, then each
// box by drag and drop alone -- an Input dragged onto the canvas, a Filter onto the Input, a Formula onto the Filter, a
// Summarize onto the Formula -- each set in the settings pane by the grid's own window docked there, each previewed in
// the results pane; a box moved and the move undone; the flow saved, the tab reloaded and the flow opened again (no box
// opened until it is shown, the last box's rows as before); shared, the link opening the same flow in a fresh tab; and a
// file from the desktop dropped onto the canvas, an Input over it. And its lines (M2): one drawn from a box's output onto
// another, and undone; one taken off, the box it went into frozen, and linked again from its settings; a tool dropped on
// a line, between its boxes, and undone in one step. And its Join and Union boxes (M2): each over two boxes, live -- an
// edit of the Filter reaches the Join -- and set again from its settings, undone in one step.
//
//   bazel test //datacube:verify_flow_test        bazel run //datacube:verify_flow (by hand)

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { chromium } from 'playwright';
import { outPath, serve, siteRoot } from './harness.mjs';

let failed = false;
function bad(m) { console.log(`FAIL: ${m}`); failed = true; }
function check(ok, m) { if (ok) console.log(`ok: ${m}`); else bad(m); }

const { port, close } = await serve(siteRoot());
const browser = await chromium.launch();
// the clipboard: Share… writes the link there
const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, permissions: ['clipboard-read', 'clipboard-write'] });
const page = await context.newPage();
const errors = [];
const alerts = [];
page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
page.on('dialog', async (d) => {
  // a page about to be left asks first (beforeunload) while it holds work; an open over unsaved changes is yes
  if (d.type() === 'beforeunload' || d.type() === 'confirm') { await d.accept(); return; }
  alerts.push(d.message());
  await d.dismiss();
});

const item = (label) => page.locator('.dc-menu .dc-menu-item', { has: page.locator(`:scope > .dc-menu-label:text-is(${JSON.stringify(label)})`) }).first();
/** The page's menu: `path`, each submenu pointed at, the last clicked. */
async function burger(...path) {
  await page.click('.dc-titlebar-menu');
  for (const label of path.slice(0, -1)) await item(label).hover();
  await item(path.at(-1)).click();
}
const boxes = (tab = page) => tab.locator('.dc-flow-box').evaluateAll((els) => els.map((e) => ({
  id: e.dataset.tile, tool: e.dataset.tool, says: e.querySelector('.dc-flow-says')?.textContent ?? '', shown: e.classList.contains('dc-flow-shown'),
})));
const boxCount = (n, tab = page) => tab.waitForFunction((k) => document.querySelectorAll('.dc-flow-box').length === k, n, { timeout: 60_000 });
/** The results pane's rows: its grid's status ("2,404 rows", or "the first 1,000 of 5,000 rows"), once it has some. */
async function resultRows(tab = page) {
  const text = await tab.waitForFunction(() => {
    const t = document.querySelector('.dc-flow-results')?.textContent ?? '';
    const m = /of ([\d,]+) rows/.exec(t) ?? /([\d,]+) rows ×/.exec(t);
    return m ? m[1] : false;
  }, undefined, { timeout: 60_000 }).then((h) => h.jsonValue());
  return Number(String(text).replace(/,/g, ''));
}
/** Until the shown box's grid has opened and gone quiet: its query landed, nothing changing for a moment. */
async function settled(tab = page) {
  await tab.waitForFunction(() => {
    const signal = window.__dataCubeSignal;
    const now = performance.now();
    const w = (window.__flowWatch ??= { changes: signal?.changes, since: now });
    if (!signal || signal.changes !== w.changes) {
      w.changes = signal?.changes;
      w.since = now;
      return false;
    }
    return now - w.since >= 500;
  }, null, { timeout: 60_000 });
  await tab.evaluate(() => { window.__flowWatch = undefined; });
}
/** A tool dragged from the palette and let go on `target` (a box's id, or the canvas), as a person drags one. */
async function drag(tool, target) {
  await page.locator(`.dc-flow-tool[data-tool="${tool}"]`).scrollIntoViewIfNeeded();
  const from = await page.locator(`.dc-flow-tool[data-tool="${tool}"]`).boundingBox();
  const onto = await page.locator(target === 'canvas' ? '.dc-flow-canvas' : `.dc-flow-box[data-tile="${target}"]`).boundingBox();
  if (!from || !onto) throw new Error(`nothing to drag ${tool} from, or onto ${target}`);
  const to = target === 'canvas' ? { x: onto.x + onto.width / 2, y: onto.y + onto.height * 0.75 } : { x: onto.x + onto.width / 2, y: onto.y + onto.height / 2 };
  await page.mouse.move(from.x + from.width / 2, from.y + from.height / 2);
  await page.mouse.down();
  await page.mouse.move(to.x, to.y, { steps: 12 });
  await page.mouse.up();
}
/** The lines drawn (each beside the wider stroke a pointer finds). */
const lineCount = () => page.locator('.dc-flow-lines path.dc-flow-line').count();
/** What a box reads, as the page has it: each side's live grid, or 'copy'. */
const readsOf = (id) => page.evaluate((tile) => {
  const doc = window.__dataPage.document('x');
  const view = doc.views.find((v) => v.id === tile);
  const source = doc.cubes.find((c) => c.id === view.cube).cube.source;
  const side = (i) => ('grid' in i ? (i.port ? `${i.grid}:${i.port}` : i.grid) : 'copy');
  return source._type === 'rows' ? side(source.input)
    : source._type === 'join' ? [side(source.left), side(source.right), ...(source.more ?? []).map(side)] : null;
}, id);
/** The middle of the line into `tile`, in the window's points. */
const lineMiddle = (tile) => page.evaluate((t) => {
  const hit = document.querySelector(`.dc-flow-lines path.dc-flow-hit[data-tile="${t}"]`);
  const p = hit.getPointAtLength(hit.getTotalLength() / 2);
  const r = document.querySelector('.dc-flow-plane').getBoundingClientRect();
  return { x: r.left + p.x, y: r.top + p.y };
}, tile);
/** A point of the window on the canvas, away from every box, port and line: where a line let go makes a box. */
const emptyCanvasPoint = () => page.evaluate(() => {
  const c = document.querySelector('.dc-flow-canvas').getBoundingClientRect();
  for (let y = c.bottom - 30; y > c.top + 60; y -= 24) {
    for (let x = c.left + 100; x < c.right - 100; x += 24) {
      const hit = document.elementsFromPoint(x, y);
      if (hit.some((e) => e.closest?.('.dc-flow-box, .dc-flow-note, .dc-flow-toolbar, .dc-flow-minimap') || e.classList?.contains('dc-flow-hit'))) continue;
      return { x, y, canvasX: x - document.querySelector('.dc-flow-plane').getBoundingClientRect().left };
    }
  }
  throw new Error('no empty place on the canvas');
});
/** A box shown, its rows once they have come. */
async function show(id) {
  await page.locator(`.dc-flow-box[data-tile="${id}"]`).click();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.classList.contains('dc-flow-shown'), id);
  await settled();
  return resultRows();
}
/** The window docked in the settings pane, once it is there. */
const docked = (name) => page.locator(`.dc-flow-dock > .dc-app-overlay[data-window^="${name}"]`).waitFor({ timeout: 20_000 });
/** The box that is not one of `known`: the one just added. */
async function added(known) {
  const now = await boxes();
  const fresh = now.find((b) => !known.includes(b.id));
  if (!fresh) throw new Error(`no box added: ${JSON.stringify(now)}`);
  return fresh;
}

try {
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });

  // CREATE ▸ FLOW: a sheet of its own, its palette and an empty canvas
  await burger('Create', 'Flow');
  await page.locator('.dc-flow-palette').waitFor({ timeout: 20_000 });
  const tabs = await page.locator('.dc-sheet-tab .dc-sheet-label').allTextContents();
  check(tabs.at(-1) === 'Flow 2', `a flow sheet, called a flow: ${tabs}`);
  check(await page.locator('.dc-flow-hint').isVisible(), 'an empty canvas says how to start');

  // AN INPUT, dragged onto the canvas: the source picker, an example of 5,000 rows
  await drag('input', 'canvas');
  await page.locator('.dc-picker').waitFor({ timeout: 10_000 });
  await page.click('.dc-picker-nav button:has-text("Examples")');
  await page.locator('.dc-picker-card').first().click();
  await page.fill('.dc-picker-rows', '5000');
  await page.click('.dc-picker-choice-actions button:has-text("Open")');
  await boxCount(1);
  await settled();
  const input = (await boxes())[0];
  check(input.tool === 'input' && input.shown, `an Input box, shown: ${JSON.stringify(input)}`);
  const all = await resultRows();
  check(all === 5000, `its rows in the results pane: ${all}`);

  // A FILTER, dragged onto the Input: a box after it, its Filter window docked, a condition set there
  await drag('filter', input.id);
  await boxCount(2);
  const filter = await added([input.id]);
  check(filter.tool === 'filter' && filter.says === 'not set yet', `a Filter box, to be set: ${JSON.stringify(filter)}`);
  await docked('Filters');
  await page.locator('.dc-flow-dock button', { hasText: 'Create New Filter' }).click();
  await page.locator('.dc-flow-dock .dc-filter-column').first().selectOption('notional');
  await page.locator('.dc-flow-dock .dc-filter-op').first().selectOption('greaterThan');
  const n = page.locator('.dc-flow-dock .dc-filter-number').first();
  await n.fill('500000');
  await n.press('Enter');
  await page.locator('.dc-flow-dock button', { hasText: /^OK$/ }).click();
  await settled();
  const filtered = await resultRows();
  const set = (await boxes()).find((b) => b.id === filter.id);
  check(/notional .* 500000/.test(set?.says ?? ''), `the Filter says what it keeps: ${set?.says}`);
  check(filtered > 0 && filtered < all, `its rows, the Input's filtered: ${filtered} of ${all}`);
  check(await lineCount() === 1, 'a line from the Input into the Filter');

  // A FORMULA, dragged onto the Filter: a calculated column, in its editor docked
  await drag('formula', filter.id);
  await boxCount(3);
  const formula = await added([input.id, filter.id]);
  await docked('column:');
  await page.fill('.dc-flow-dock .dc-calc-input-name', 'fee');
  await page.fill('.dc-flow-dock .dc-calc-input-expr', '$x.notional * 0.01');
  // a box's columns are its input's relation's, which may be empty: the editor asks what an empty one gives
  const empty = page.locator('.dc-flow-dock .dc-calc-fix-button[data-as="zero"]');
  await empty.waitFor({ timeout: 30_000 }).then(() => empty.click(), () => undefined);
  await page.waitForFunction(() => {
    const ok = document.querySelector('.dc-flow-dock .dc-calc-ok');
    return ok instanceof HTMLButtonElement && !ok.disabled;
  }, undefined, { timeout: 60_000 });
  await page.locator('.dc-flow-dock .dc-calc-ok').click();
  await settled();
  const formulaRows = await resultRows();
  const calc = (await boxes()).find((b) => b.id === formula.id);
  check(calc?.tool === 'formula' && calc.says === 'fee', `the Formula says its column: ${JSON.stringify(calc)}`);
  check(formulaRows === filtered, `a calculated column keeps its input's rows: ${formulaRows}`);
  check(await page.locator('.dc-flow-results .dc-th[data-column="fee"]').count() > 0, 'its column in its rows');

  // A SUMMARIZE, dragged onto the Formula: its window docked (Alteryx's: a row a column, its action), region grouped by,
  // each number aggregated as its column says until told otherwise
  await drag('summarize', formula.id);
  await boxCount(4);
  const summary = await added([input.id, filter.id, formula.id]);
  await docked('Summarize');
  const okSummary = page.locator('.dc-flow-dock .dc-sample-foot .dc-primary');
  check(await okSummary.isDisabled(), 'a Summarize waits for a group');
  const actions = await page.locator('.dc-flow-dock .dc-summarize-row[data-column="notional"] .dc-summarize-action option').allTextContents();
  check(actions.includes('Group by') && actions.some((a) => /^Sum/.test(a)) && actions.includes('Weighted average'), `a number's actions: ${actions.join(', ')}`);
  // an aggregate of two columns: its second chosen beside it, then let be
  const notionalAction = page.locator('.dc-flow-dock .dc-summarize-row[data-column="notional"] .dc-summarize-action');
  await notionalAction.selectOption('wavg');
  check(await page.locator('.dc-flow-dock .dc-summarize-row[data-column="notional"] .dc-summarize-second option').count() > 0,
    'a weighted average\'s weight offered beside it');
  await notionalAction.selectOption('sum');
  await page.locator('.dc-flow-dock .dc-summarize-row[data-column="region"] .dc-summarize-action').selectOption('group');
  await okSummary.click();
  await settled();
  const groups = await resultRows();
  const summed = (await boxes()).find((b) => b.id === summary.id);
  check(summed?.tool === 'summarize' && summed.says === 'by region', `the Summarize says its groups: ${JSON.stringify(summed)}`);
  check(groups > 0 && groups < 10, `a row per region: ${groups}`);
  // its Chart (M5): its rows drawn -- a chart of its regions -- its options a click away; then its rows again
  await page.locator('.dc-flow-mode[data-mode="chart"]').click();
  await page.locator('.dc-flow-chart .dc-chartpanel-chart canvas, .dc-flow-chart .dc-chartpanel-chart svg').first().waitFor({ timeout: 60_000 });
  check(await page.locator('.dc-flow-results .dc-flow-chart-options').isVisible(), 'its chart\'s Options beside the title');
  check(JSON.stringify(await page.locator('.dc-flow-mode.dc-chosen:visible').allTextContents()) === '["Chart"]',
    `Chart the one chosen: ${await page.locator('.dc-flow-mode.dc-chosen:visible').allTextContents()}`);
  await page.screenshot({ path: outPath('flow-chart.png') });
  await page.locator('.dc-flow-mode[data-mode="rows"]').click();
  check(await page.locator('.dc-flow-chart').count() === 0 && await resultRows() === groups, 'its rows again, the chart taken down');
  // its Code (M5, FL12): the query its grid ran, as Pure, and the SQL the database ran -- each with its Copy
  await page.locator('.dc-flow-mode[data-mode="code"]').click();
  await page.locator('.dc-flow-code .dc-flow-code-text').first().waitFor({ timeout: 30_000 });
  const [pure, sql] = await page.locator('.dc-flow-code .dc-flow-code-text').allTextContents();
  check(/region/.test(pure ?? '') && /notional/.test(pure ?? ''), `its Pure, by region over notional: ${(pure ?? '').slice(0, 300)}`);
  check(/select/i.test(sql ?? '') && /group by/i.test(sql ?? ''), `its SQL, grouped: ${(sql ?? '').slice(0, 300)}`);
  await page.locator('.dc-flow-code .dc-flow-code-copy').first().click();
  await page.waitForFunction(() => document.querySelector('.dc-flow-code .dc-flow-code-said')?.textContent === 'Copied', undefined, { timeout: 5_000 });
  await page.screenshot({ path: outPath('flow-code.png') });
  await page.locator('.dc-flow-mode[data-mode="rows"]').click();
  check(await page.locator('.dc-flow-code').count() === 0 && await resultRows() === groups, 'its rows again, its code taken down');
  // THE COMMAND PALETTE (M7): Ctrl+K, a command found by name, Enter runs it -- the gesture of its button, its palette
  // entry or a click; Escape closes it
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  const command = async (typed) => {
    await page.keyboard.press('Control+k');
    await page.locator('.dc-flow-commands').waitFor({ state: 'visible', timeout: 5_000 });
    await page.locator('.dc-flow-commands-find').fill(typed);
    return page.locator('.dc-flow-command.dc-active .dc-flow-command-label').textContent();
  };
  check(await command('zoom in') === 'Zoom in', 'Zoom in found');
  await page.keyboard.press('Enter');
  check(await page.locator('.dc-flow-commands').isHidden(), 'run, and closed');
  check(await page.locator('.dc-flow-zoom-level').textContent() === '110%', `zoomed in: ${await page.locator('.dc-flow-zoom-level').textContent()}`);
  check(await command('own size') === 'Zoom to its own size', 'its own size found');
  await page.keyboard.press('Enter');
  check(await page.locator('.dc-flow-zoom-level').textContent() === '100%', 'its own size again');
  const beforeCommand = (await boxes()).map((b) => b.id);
  check(await command('browse') === 'Add Browse', 'a tool found');
  await page.keyboard.press('Enter');
  await boxCount(beforeCommand.length + 1);
  const browsed = await added(beforeCommand);
  check(browsed.tool === 'browse' && await readsOf(browsed.id) === summary.id, `a Browse after the box shown: ${JSON.stringify(browsed)}`);
  check(await command('show filter') === 'Show Filter', 'a box found by its name');
  await page.screenshot({ path: outPath('flow-commands.png') });
  await page.keyboard.press('Enter');
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.classList.contains('dc-flow-shown'), filter.id, { timeout: 10_000 });
  await command('');
  await page.keyboard.press('Escape');
  check(await page.locator('.dc-flow-commands').isHidden(), 'Escape closes it');
  await page.keyboard.press('Control+z');
  await boxCount(beforeCommand.length);
  await show(summary.id);
  check(await lineCount() === 3, 'a line into each box after the first');

  // EACH BOX PREVIEWED, the one shown before closed: only the shown box has a grid
  const previews = {};
  for (const b of [input, filter, formula, summary]) {
    await page.locator(`.dc-flow-box[data-tile="${b.id}"]`).click();
    await page.waitForFunction((id) => document.querySelector(`.dc-flow-box[data-tile="${id}"]`)?.classList.contains('dc-flow-shown'), b.id);
    await settled();
    previews[b.id] = await resultRows();
  }
  const want = { [input.id]: all, [filter.id]: filtered, [formula.id]: formulaRows, [summary.id]: groups };
  check(JSON.stringify(previews) === JSON.stringify(want), `each box's rows, shown again: ${JSON.stringify(previews)}`);
  const open = await page.evaluate(() => window.__dataPage.grids);
  check(open.includes(summary.id) && ![input.id, filter.id, formula.id].some((id) => open.includes(id)),
    `one box's grid made, the shown one's: ${open}`);

  // A LINE DRAWN by hand: from the Input's output onto the Formula, which reads the Input from then on -- one step, undone
  // (the canvas scrolled back to its start: a box added was scrolled into view, the Input's port under the palette)
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const outPort = await page.locator(`.dc-flow-box[data-tile="${input.id}"] .dc-flow-port-out:not(.dc-flow-port-false):not(.dc-flow-port-unmatched)`).boundingBox();
  const onto = await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).boundingBox();
  await page.mouse.move(outPort.x + outPort.width / 2, outPort.y + outPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(onto.x + onto.width / 2, onto.y + onto.height / 2, { steps: 12 });
  await page.mouse.up();
  check(await readsOf(formula.id) === input.id, `the Formula reads the Input now: ${await readsOf(formula.id)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Link Box', 'one step: Link Box');
  check(await show(formula.id) === all, 'its rows the Input\'s, every one');
  check(await lineCount() === 3, 'still a line into each box after the first');
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('Control+z');
  check(await readsOf(formula.id) === filter.id, `undone: the Formula reads the Filter again: ${await readsOf(formula.id)}`);
  await settled();
  check(await resultRows() === filtered, 'its rows the Filter\'s again');

  // A LINE TAKEN OFF: clicked, then Delete -- the Formula reads a copy of what the Filter was (frozen), its rows the same
  const middle = await lineMiddle(formula.id);
  await page.mouse.click(middle.x, middle.y);
  // a level line's box has no height: attached, not "visible"
  await page.locator('.dc-flow-lines path.dc-flow-line-selected').waitFor({ state: 'attached', timeout: 5_000 });
  check(await page.locator('.dc-flow-unlink').isVisible(), 'a selected line shows its ×');
  await page.screenshot({ path: outPath('flow-line-selected.png') });
  await page.keyboard.press('Delete');
  check(await readsOf(formula.id) === 'copy', `taken off: the Formula reads a copy: ${await readsOf(formula.id)}`);
  const left = await lineCount();
  check(left === 2, `its line gone: ${left} lines`);
  check(await page.locator(`.dc-flow-box[data-tile="${formula.id}"].dc-flow-frozen`).count() === 1, 'the Formula drawn frozen');
  await page.screenshot({ path: outPath('flow-frozen.png') });
  await settled();
  check(await resultRows() === filtered, `frozen, its rows as they were: ${filtered}`);
  // linked again from its settings: Input, the Filter
  await page.locator('.dc-flow-inputs select').selectOption(`grid:${filter.id}`);
  check(await readsOf(formula.id) === filter.id, 'linked again from its settings: it reads the Filter');
  const back3 = await lineCount();
  check(back3 === 3, `its line back: ${back3} lines`);
  await settled();

  // A TOOL DROPPED ON A LINE: a Sort between the Filter and the Formula -- the Formula reads the Sort; one step, undone
  // the palette scrolled where the tool is in sight (it is taller than its pane)
  await page.locator('.dc-flow-tool[data-tool="sort"]').scrollIntoViewIfNeeded();
  const on = await lineMiddle(formula.id);
  const sortTool = await page.locator('.dc-flow-tool[data-tool="sort"]').boundingBox();
  await page.mouse.move(sortTool.x + sortTool.width / 2, sortTool.y + sortTool.height / 2);
  await page.mouse.down();
  await page.mouse.move(on.x, on.y, { steps: 12 });
  check(await page.locator('.dc-flow-lines path.dc-flow-line-target').count() === 1, 'the line under the tool marked');
  await page.screenshot({ path: outPath('flow-insert.png') });
  await page.mouse.up();
  await boxCount(5);
  const sorted = await added([input.id, filter.id, formula.id, summary.id]);
  check(sorted.tool === 'sort', `a Sort box: ${JSON.stringify(sorted)}`);
  check(await readsOf(sorted.id) === filter.id && await readsOf(formula.id) === sorted.id,
    `between them: the Sort reads the Filter, the Formula the Sort: ${await readsOf(sorted.id)}, ${await readsOf(formula.id)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Insert Box', 'one step: Insert Box');
  check(await lineCount() === 4, 'a line into each box after the first, the Sort\'s too');
  await settled();
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('Control+z');
  await boxCount(4);
  check(await readsOf(formula.id) === filter.id, `undone in one step: the Sort gone, the Formula reads the Filter: ${await readsOf(formula.id)}`);
  await settled();

  // A BOX MOVED BY HAND, and the move undone
  const placeOf = () => page.evaluate((id) => {
    const flow = window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow');
    return flow.layout.boxes.find((b) => b.tile === id);
  }, summary.id);
  const was = await placeOf();
  await page.locator(`.dc-flow-box[data-tile="${summary.id}"]`).scrollIntoViewIfNeeded();
  const card = await page.locator(`.dc-flow-box[data-tile="${summary.id}"]`).boundingBox();
  await page.mouse.move(card.x + 30, card.y + 20);
  await page.mouse.down();
  await page.mouse.move(card.x + 30, card.y + 200, { steps: 10 });
  await page.mouse.up();
  const moved = await placeOf();
  check(moved.y > was.y && moved.x === was.x, `moved down by hand: (${was.x}, ${was.y}) to (${moved.x}, ${moved.y})`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Move Box', 'one step: Move Box');
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('Control+z');
  const back = await placeOf();
  check(back.x === was.x && back.y === was.y, `undone: back at (${back.x}, ${back.y})`);

  // SAVED, RELOADED, OPENED AGAIN: the flow as it was, no box opened until it is shown
  await burger('Save As…');
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', 'My flow');
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
  await page.goto(`http://127.0.0.1:${port}/demo/index.html`, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0, undefined, { timeout: 120_000 });
  await burger('Open…');
  const row = page.locator('#cubelib .dc-lib-row', { hasText: 'My flow' });
  await row.waitFor({ timeout: 10_000 });
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  await page.waitForFunction(() => /opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''), undefined, { timeout: 60_000 });
  await page.locator('#cubelib .dc-lib-close, #cubelib [aria-label="Close"]').first().click().catch(() => undefined);
  await page.locator('.dc-sheet-tab', { hasText: 'Flow 2' }).click();
  await boxCount(4);
  const reopened = await boxes();
  check(JSON.stringify(reopened.map((b) => [b.tool, b.says])) === JSON.stringify([['input', input.says], ['filter', set.says], ['formula', 'fee'], ['summarize', 'by region']]),
    `reopened, each box as it was: ${JSON.stringify(reopened.map((b) => [b.tool, b.says]))}`);
  const openNow = await page.evaluate(() => window.__dataPage.grids);
  check(!reopened.some((b) => openNow.includes(b.id)), `no box opened as the page opened: ${openNow}`);
  await page.locator(`.dc-flow-box[data-tile="${reopened[3].id}"]`).click();
  await settled();
  check(await resultRows() === groups, `the last box's rows, as before: ${groups}`);

  // SHARED: the link opens the same flow in a fresh tab
  await burger('Share…');
  await page.waitForFunction(() => /^Copied/.test(document.querySelector('#sharenote')?.textContent ?? ''), null, { timeout: 10_000 });
  const url = await page.evaluate(() => navigator.clipboard.readText());
  await page.click('#sharewin .hostwin-close');
  const tab = await context.newPage();
  tab.on('pageerror', (e) => errors.push(`(shared tab) pageerror: ${e.message}`));
  try {
    await tab.goto(url);
    await tab.waitForFunction(() => /opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? '')
      || document.querySelectorAll('.dc-sheet-tab').length > 1, undefined, { timeout: 120_000 });
    await tab.locator('.dc-sheet-tab', { hasText: 'Flow 2' }).click();
    await boxCount(4, tab);
    const shared = await boxes(tab);
    check(JSON.stringify(shared.map((b) => b.says)) === JSON.stringify(reopened.map((b) => b.says)), `shared, the same boxes: ${shared.map((b) => b.says)}`);
    await tab.locator(`.dc-flow-box[data-tile="${shared[3].id}"]`).click();
    await settled(tab);
    check(await resultRows(tab) === groups, 'the shared flow\'s last box, the same rows');
  } finally {
    await tab.close();
  }

  // A FILE FROM THE DESKTOP, dropped onto the canvas: an Input over it, its rows shown
  const csv = 'region,target\nEMEA,10\nAMER,20\nAPAC,30\nLATAM,40\n';
  const dt = await page.evaluateHandle((text) => {
    const d = new DataTransfer();
    d.items.add(new File([text], 'targets.csv', { type: 'text/csv' }));
    return d;
  }, csv);
  await page.dispatchEvent('.dc-flow-canvas', 'dragover', { dataTransfer: dt });
  await page.dispatchEvent('.dc-flow-canvas', 'drop', { dataTransfer: dt });
  await boxCount(5);
  await settled();
  const dropped = (await boxes()).at(-1);
  check(dropped?.tool === 'input' && dropped.says === 'targets.csv' && dropped.shown, `a file dropped, an Input over it: ${JSON.stringify(dropped)}`);
  check(await resultRows() === 4, 'its rows in the results pane');

  // A JOIN BOX, dragged onto the Filter: the Join window over it (its Left) and the Input (its Right), matching trades
  const joinWindow = page.locator('.dc-join');
  const known = (await boxes()).map((b) => b.id);
  await drag('join', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-picker-subtitle').textContent().then((t) => /another box of the flow/.test(t ?? '')), 'the Join window, over the flow\'s boxes');
  await joinWindow.locator('.dc-join-their').selectOption(input.id);
  await joinWindow.locator('.dc-join-kind[data-kind="inner"]').click();
  await page.waitForFunction(() => document.querySelectorAll('.dc-join .dc-join-left option').length > 1, undefined, { timeout: 60_000 });
  await joinWindow.locator('.dc-join-left').first().selectOption('trade_id');
  await joinWindow.locator('.dc-join-right').first().selectOption('trade_id');
  const addJoin = joinWindow.locator('.dc-save-foot .dc-primary');
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await addJoin.textContent() === 'Add Join', 'its button says what it does: Add Join');
  await addJoin.click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(known.length + 1);
  const join = await added(known);
  check(join.tool === 'join' && join.shown, `a Join box, shown: ${JSON.stringify(join)}`);
  check(JSON.stringify(await readsOf(join.id)) === JSON.stringify([filter.id, input.id]), `it reads the Filter and the Input, live: ${JSON.stringify(await readsOf(join.id))}`);
  await settled();
  check(await resultRows() === filtered, `each of the Filter's trades matched once: ${filtered}`);
  check(await page.locator(`.dc-flow-lines path.dc-flow-hit[data-tile="${join.id}"]`).count() === 2, 'two lines into it, its Left and its Right');
  await page.screenshot({ path: outPath('flow-join.png') });

  // AN EDIT UPSTREAM REACHES IT: the Filter's threshold raised, the Join's rows fewer
  await show(filter.id);
  await page.locator(`.dc-flow-box[data-tile="${filter.id}"]`).click();
  await docked('Filters').catch(() => undefined);
  if (await page.locator('.dc-flow-dock .dc-filter-number').count() === 0) {
    await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
    await docked('Filters');
  }
  const threshold = page.locator('.dc-flow-dock .dc-filter-number').first();
  await threshold.fill('900000');
  await threshold.press('Enter');
  await page.locator('.dc-flow-dock button', { hasText: /^OK$/ }).click();
  await settled();
  const fewer = await resultRows();
  check(fewer < filtered, `the Filter keeps fewer: ${fewer}`);
  check(await show(join.id) === fewer, `the Join's rows follow it: ${fewer}`);

  // SET AGAIN from its settings: Mine with a match, one step, undone
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-join-kind[data-kind="inner"]').getAttribute('aria-checked') === 'true', 'it opens at its own join');
  await joinWindow.locator('.dc-join-kind[data-kind="semi"]').click();
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-save-foot .dc-primary').textContent() === 'Apply', 'set again: Apply');
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Set Join', 'one step: Set Join');
  const semi = (await boxes()).find((b) => b.id === join.id);
  check(/with a match/.test(semi?.says ?? ''), `it says its join: ${semi?.says}`);
  await settled();
  check(await resultRows() === fewer, 'its rows: the Filter\'s with a match');
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('Control+z');
  const inner = (await boxes()).find((b) => b.id === join.id);
  check(/matching rows only/.test(inner?.says ?? ''), `undone: matching rows only again: ${inner?.says}`);

  // A UNION BOX, dragged onto the Filter: the window at Append, the Input under the Filter's rows
  const before = (await boxes()).map((b) => b.id);
  await drag('union', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-join-kind[data-kind="append"]').getAttribute('aria-checked') === 'true', 'a Union\'s window starts at Append');
  await joinWindow.locator('.dc-join-their').selectOption(input.id);
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(before.length + 1);
  const union = await added(before);
  check(union.tool === 'union', `a Union box: ${JSON.stringify(union)}`);
  await settled();
  check(await resultRows() === fewer + all, `the rows of both: ${fewer} + ${all}`);
  await page.screenshot({ path: outPath('flow-union.png') });

  // A UNION OF MORE THAN TWO (M2): a line from the Input let go on the Union -- one input more, its ports 1, 2 and 3 and
  // its +, its rows the three's; one taken off from its settings (its ×), one added there (Add an input…); undone
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const inPort = await page.locator(`.dc-flow-box[data-tile="${input.id}"] .dc-flow-port-out:not(.dc-flow-port-false):not(.dc-flow-port-unmatched)`).boundingBox();
  const onUnion = await page.locator(`.dc-flow-box[data-tile="${union.id}"]`).boundingBox();
  await page.mouse.move(inPort.x + inPort.width / 2, inPort.y + inPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(onUnion.x + onUnion.width / 2, onUnion.y + onUnion.height / 2, { steps: 12 });
  await page.mouse.up();
  const reads = async (id) => JSON.stringify(await readsOf(id));
  check(await reads(union.id) === JSON.stringify([filter.id, input.id, input.id]), `a third input: ${await reads(union.id)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Link Box', 'one step: Link Box');
  // said to a screen reader (M7): what changed, by the flow shown's live region
  const told = await page.evaluate(() => [...document.querySelectorAll('.dc-flow')].find((f) => f.offsetParent !== null)
    ?.querySelector(':scope > .dc-board-live')?.textContent ?? '');
  check(told === 'Union reads Filter and Input and Input now.', `its change said: ${told}`);
  check(JSON.stringify(await page.locator(`.dc-flow-box[data-tile="${union.id}"] > .dc-flow-port-in:visible`).allTextContents()) === '["1","2","3","+"]',
    'its ports numbered, and its +');
  check(await show(union.id) === fewer + 2 * all, `the rows of all three: ${fewer} + ${all} + ${all}`);
  check(/the rows of all 3/.test((await boxes()).find((b) => b.id === union.id)?.says ?? ''), 'it says all three');
  await page.screenshot({ path: outPath('flow-union-three.png') });
  const offs = page.locator('.dc-flow-inputs .dc-flow-input-off');
  check(await offs.count() === 3, `a × on each of its three inputs: ${await offs.count()}`);
  await offs.nth(1).click();
  check(await reads(union.id) === JSON.stringify([filter.id, input.id]), `its Input 2 taken off, its third moved up: ${await reads(union.id)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Take Off Input', 'one step: Take Off Input');
  check(await offs.count() === 0, 'two inputs: none to take off (a line taken off freezes)');
  await page.locator('.dc-flow-input-add select').selectOption(`grid:${filter.id}`);
  check(await reads(union.id) === JSON.stringify([filter.id, input.id, filter.id]), `added from its settings: ${await reads(union.id)}`);
  await settled();
  check(await resultRows() === 2 * fewer + all, `the Filter's rows twice and the Input's: ${2 * fewer + all}`);
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  for (const want of [[filter.id, input.id], [filter.id, input.id, input.id], [filter.id, input.id]]) {
    await page.keyboard.press('Control+z');
    check(await reads(union.id) === JSON.stringify(want), `undone: ${await reads(union.id)}`);
  }
  check(await show(union.id) === fewer + all, 'a Union of two again: the rows of both');

  // A DYNAMIC INPUT (M8, dynamic-input.ts): the palette's Dynamic Input dragged onto the Filter -- the window at Append,
  // the Input picked; made, a Union that keeps the rows of the one a new parameter of the page names, `source` (the two
  // boxes no one named, read under one file's name, named as the canvas names them in the same step), the Filter's
  // first; its value set on the parameters bar, the Input's rows; its settings' Keep the rows of, every input's again;
  // each undone, the box, its parameter and the names gone
  const beforeDynamic = (await boxes()).map((b) => b.id);
  await drag('dynamicInput', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-join-kind[data-kind="append"]').getAttribute('aria-checked') === 'true', 'a Dynamic Input\'s window starts at Append');
  await joinWindow.locator('.dc-join-their').selectOption(input.id);
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-save-foot .dc-primary').textContent() === 'Add Dynamic Input', 'its window adds a Dynamic Input');
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(beforeDynamic.length + 1);
  const dynamic = await added(beforeDynamic);
  check(dynamic.tool === 'dynamicInput' && dynamic.says === 'the rows of the one $source names', `a Dynamic Input box: ${JSON.stringify(dynamic)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Dynamic Input', 'one step: Add Dynamic Input');
  const sourceChoice = page.locator('.dc-page-parameter[data-parameter="source"] select');
  await sourceChoice.waitFor({ timeout: 20_000 });
  check(JSON.stringify(await sourceChoice.locator('option').allTextContents()) === '["Filter","Input"]', `its choices, its inputs' names: ${await sourceChoice.locator('option').allTextContents()}`);
  check(await show(dynamic.id) === fewer, `the Filter's rows, the first it names: ${fewer}`);
  await sourceChoice.selectOption({ label: 'Input' });
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Set source', undefined, { timeout: 20_000 });
  await settled();
  check(await resultRows() === all, `its value set, the Input's rows: ${all}`);
  const keep = page.locator('.dc-flow-pick-select');
  check(await keep.inputValue() === 'p:source', 'its settings say what picks it');
  await keep.selectOption('');
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Union', undefined, { timeout: 20_000 });
  await settled();
  check(await resultRows() === fewer + all, 'every input\'s rows again: a Union');
  check((await boxes()).find((b) => b.id === dynamic.id)?.tool === 'union', 'drawn as a Union');
  await page.screenshot({ path: outPath('flow-dynamic-input.png') });
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  for (let i = 0; i < 3; i++) await page.keyboard.press('Control+z');
  await boxCount(beforeDynamic.length);
  check(await page.locator('.dc-page-parameter[data-parameter="source"]').count() === 0, 'undone: its parameter gone');
  check(await page.evaluate((ids) => ids.map((id) => window.__dataPage.document('x').views.find((v) => v.id === id)?.title ?? null), [filter.id, input.id])
    .then((t) => t.every((x) => x === null)), 'undone: the names it gave gone');

  // AN APPEND FIELDS BOX, dragged onto the Filter: the window at Every pair, no condition asked; the dropped targets'
  // four rows beside each of the Filter's
  const beforeAppend = (await boxes()).map((b) => b.id);
  await drag('appendFields', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-join-kind[data-kind="cross"]').getAttribute('aria-checked') === 'true', 'an Append Fields\' window starts at Every pair');
  await joinWindow.locator('.dc-join-their').selectOption(dropped.id);
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-join-condition').isHidden(), 'no condition asked');
  check(await joinWindow.locator('.dc-save-foot .dc-primary').textContent() === 'Add Append Fields', 'its button: Add Append Fields');
  await page.screenshot({ path: outPath('flow-append-fields.png') });
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(beforeAppend.length + 1);
  const appended = await added(beforeAppend);
  check(appended.tool === 'appendFields' && appended.says === 'each row with every row of theirs', `an Append Fields box: ${JSON.stringify(appended)}`);
  await settled();
  check(await resultRows() === fewer * 4, `each of the Filter's ${fewer} rows beside each of the 4 targets: ${fewer * 4}`);

  // A FUZZY MATCH BOX, dragged onto the Filter: the window at matching only, its comparison similar; the dropped
  // targets' regions within one edit of the Filter's
  const beforeFuzzy = (await boxes()).map((b) => b.id);
  await drag('fuzzyMatch', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  await joinWindow.locator('.dc-join-their').selectOption(dropped.id);
  // their rows' columns come when theirs is read: its target among them (another box's first, read before, has none)
  await page.waitForFunction(() => document.querySelector('.dc-join .dc-join-right option[value="target"]') !== null, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-join-op').first().inputValue() === '~' && await joinWindow.locator('.dc-join-within').first().isVisible(),
    'a Fuzzy Match\'s window starts at similar, its edits apart beside it');
  await joinWindow.locator('.dc-join-left').first().selectOption('region');
  await joinWindow.locator('.dc-join-right').first().selectOption('region');
  await joinWindow.locator('.dc-join-within').first().fill('1');
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-save-foot .dc-primary').textContent() === 'Add Fuzzy Match', 'its button: Add Fuzzy Match');
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(beforeFuzzy.length + 1);
  const fuzzy = await added(beforeFuzzy);
  check(fuzzy.tool === 'fuzzyMatch' && fuzzy.says === 'matching rows only, on region ≈ region (within 1), scored as score', `a Fuzzy Match box: ${JSON.stringify(fuzzy)}`);
  await settled();
  check(await resultRows() > 0, 'its pairs, alike regions matched');
  // each pair scored as its window started (Alteryx's Score): the database's likeness, 0 to 100
  check(await page.locator('.dc-flow-results:visible .dc-th[data-column="score"]').count() === 1, 'its pairs scored: a score column');

  // A FIND REPLACE (M2), dragged onto the Filter: a file of region names dropped, the Join window at all of mine on
  // region, then its window docked -- region replaced by their name where found
  const regionCsv = 'region,name\nEMEA,Europe\nAMER,Americas\n';
  const named = await page.evaluateHandle((text) => {
    const d = new DataTransfer();
    d.items.add(new File([text], 'regions.csv', { type: 'text/csv' }));
    return d;
  }, regionCsv);
  const beforeNames = (await boxes()).map((b) => b.id);
  await page.dispatchEvent('.dc-flow-canvas', 'dragover', { dataTransfer: named });
  await page.dispatchEvent('.dc-flow-canvas', 'drop', { dataTransfer: named });
  await boxCount(beforeNames.length + 1);
  const regions = await added(beforeNames);
  const beforeFind = (await boxes()).map((b) => b.id);
  await drag('findReplace', filter.id);
  await joinWindow.waitFor({ timeout: 20_000 });
  check(await joinWindow.locator('.dc-join-kind[data-kind="left"]').getAttribute('aria-checked') === 'true', 'a Find Replace\'s window starts at all of mine');
  await joinWindow.locator('.dc-join-their').selectOption(regions.id);
  await page.waitForFunction(() => document.querySelector('.dc-join .dc-join-right option[value="name"]') !== null, undefined, { timeout: 60_000 });
  await joinWindow.locator('.dc-join-left').first().selectOption('region');
  await joinWindow.locator('.dc-join-right').first().selectOption('region');
  await page.waitForFunction(() => { const b = document.querySelector('.dc-join .dc-save-foot .dc-primary'); return b instanceof HTMLButtonElement && !b.disabled; }, undefined, { timeout: 60_000 });
  check(await joinWindow.locator('.dc-save-foot .dc-primary').textContent() === 'Add Find Replace', 'its button: Add Find Replace');
  await joinWindow.locator('.dc-save-foot .dc-primary').click();
  await joinWindow.waitFor({ state: 'detached', timeout: 60_000 });
  await boxCount(beforeFind.length + 1);
  const finder = await added(beforeFind);
  await docked('Find Replace');
  check(await page.locator('.dc-flow-dock .dc-find-column').inputValue() === 'region' && await page.locator('.dc-flow-dock .dc-find-with').inputValue() === 'name',
    'its window starts at its Join\'s key and the last of theirs');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const replacing = (await boxes()).find((b) => b.id === finder.id);
  check(replacing?.tool === 'findReplace' && replacing.says === 'region replaced by name', `a Find Replace box: ${JSON.stringify(replacing)}`);
  check(await resultRows() === fewer, `each of the Filter's rows once: ${fewer}`);
  check(/Europe|Americas/.test(await page.locator('.dc-flow-results').textContent() ?? ''), 'its regions found, replaced by their names');
  check(await page.locator('.dc-flow-results .dc-th[data-column="name"]').count() === 0, 'theirs left out');

  // COUNT RECORDS (M4), dragged onto the Filter: no settings -- made, it counts; one row, its count
  const beforeCount = (await boxes()).map((b) => b.id);
  await drag('countRecords', filter.id);
  await boxCount(beforeCount.length + 1);
  const counter = await added(beforeCount);
  await page.waitForFunction((id) => document.querySelector(`.dc-flow-box[data-tile="${id}"] .dc-flow-says`)?.textContent === 'its rows counted: Count',
    counter.id, { timeout: 60_000 });
  await settled();
  check(await resultRows() === 1, 'one row');
  check((await page.locator('.dc-flow-results').textContent() ?? '').includes(fewer.toLocaleString('en-US')), `its count: the Filter's ${fewer}`);

  // A RECORD ID (M3), dragged onto the Filter: the column editor docked at a row number, ordered by trade_id here
  const beforeId = (await boxes()).map((b) => b.id);
  await drag('recordId', filter.id);
  await boxCount(beforeId.length + 1);
  const numbered = await added(beforeId);
  check(numbered.tool === 'recordId', `a Record ID box: ${JSON.stringify(numbered)}`);
  await docked('column:');
  check(await page.locator('.dc-flow-dock .dc-calc-input-name').inputValue() === 'RecordID', 'its column named RecordID');
  check(await page.locator('.dc-flow-dock .dc-win-fn').inputValue() === 'rowNumber', 'a row number, to start');
  await page.locator('.dc-flow-dock .dc-win-order-add').click();
  await page.locator('.dc-flow-dock .dc-win-order-column').first().selectOption('trade_id');
  await page.waitForFunction(() => { const ok = document.querySelector('.dc-flow-dock .dc-calc-ok'); return ok instanceof HTMLButtonElement && !ok.disabled; }, undefined, { timeout: 60_000 });
  await page.locator('.dc-flow-dock .dc-calc-ok').click();
  await settled();
  const set1 = (await boxes()).find((b) => b.id === numbered.id);
  check(set1?.tool === 'recordId' && set1.says === 'RecordID', `it says its column: ${JSON.stringify(set1)}`);
  check(await resultRows() === fewer, 'its input\'s rows, each numbered');
  check(await page.locator('.dc-flow-results .dc-th[data-column="RecordID"]').count() > 0, 'its column in its rows');

  // A CROSS TAB, dragged onto the Filter: Properties docked at its column pivots
  const beforeTab = (await boxes()).map((b) => b.id);
  await drag('crosstab', filter.id);
  await boxCount(beforeTab.length + 1);
  const crossTab = await added(beforeTab);
  check(crossTab.tool === 'crosstab' && crossTab.says === 'not set yet', `a Cross Tab box, to be set: ${JSON.stringify(crossTab)}`);
  await docked('Properties');
  check(await page.locator('.dc-flow-dock [role="tab"][aria-selected="true"]').getAttribute('data-tab') === 'Horizontal Pivots',
    'Properties at its column labels (its column pivots)');
  await page.screenshot({ path: outPath('flow-tools.png') });

  // ITS COLUMNS FIXED (FL2): pivoted on region and grouped by desk in its docked Properties -- its columns its values,
  // found as it runs -- then Load values pins them (asked of the database): a Browse after it reads each region a column
  const zone = async (tab, column) => {
    await page.locator(`.dc-flow-dock .dc-editor-tab[data-tab="${tab}"]`).click();
    await page.locator(`.dc-flow-dock .dc-pane-available .dc-selector-row[data-column="${column}"]:visible`).click();
    await page.locator('.dc-flow-dock .dc-selector-move:visible').first().click();
  };
  await zone('Horizontal Pivots', 'region');
  await zone('Vertical Pivots', 'desk');
  await page.locator('.dc-flow-dock .dc-editor-footer .dc-primary').click();
  await settled();
  await page.waitForFunction(() => /found as it runs/.test(document.querySelector('.dc-flow-pin-words')?.textContent ?? ''), undefined, { timeout: 30_000 });
  check(await page.locator('.dc-flow-pin-unfix').count() === 0, 'nothing fixed yet: no Unfix');
  await page.locator('.dc-flow-pin-load').click();
  await page.waitForFunction(() => /^Its columns: /.test(document.querySelector('.dc-flow-pin-words')?.textContent ?? ''), undefined, { timeout: 30_000 });
  const pinned = await page.evaluate((tile) => {
    const doc = window.__dataPage.document('x');
    return doc.cubes.find((c) => c.id === doc.views.find((v) => v.id === tile).cube).cube.query.pivotValues;
  }, crossTab.id);
  check(Array.isArray(pinned) && pinned.length > 0, `its values pinned in its query: ${JSON.stringify(pinned)}`);
  const pinWords = await page.locator('.dc-flow-pin-words').textContent();
  check(/^Its columns: [A-Z]+/.test(pinWords ?? ''), `Load values: its columns named, ${pinWords} (${JSON.stringify(pinned)})`);
  check(await page.locator('.dc-flow-pin-unfix').count() === 1, 'fixed: Unfix offered');
  const beforeCrossed = (await boxes()).map((b) => b.id);
  await drag('browse', crossTab.id);
  await boxCount(beforeCrossed.length + 1);
  const crossed = await added(beforeCrossed);
  await settled();
  const crossedHeads = await page.locator('.dc-flow-results .dc-th[data-column]').evaluateAll((els) => els.map((e) => e.dataset.column));
  const firstRegion = (pinWords ?? '').replace(/^Its columns: /, '').split(/[,.]/)[0]?.trim();
  check(crossedHeads.includes('desk') && crossedHeads.some((c) => c === firstRegion || c.startsWith(`${firstRegion}_`)),
    `a Browse after it reads its fixed columns: ${JSON.stringify(crossedHeads)}`);
  check(await readsOf(crossed.id) === crossTab.id, 'it reads the Cross Tab');

  // A FILTER'S FALSE (FL7): a line from its F port let go on the canvas -- a Browse of the rows its condition leaves out
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const beforeFalse = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${filter.id}"]`).scrollIntoViewIfNeeded();
  const fPort = await page.locator(`.dc-flow-box[data-tile="${filter.id}"] .dc-flow-port-false`).boundingBox();
  const freeSpot = await emptyCanvasPoint();
  await page.mouse.move(fPort.x + fPort.width / 2, fPort.y + fPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(freeSpot.x, freeSpot.y, { steps: 12 });
  await page.mouse.up();
  await boxCount(beforeFalse.length + 1);
  const leftOut = await added(beforeFalse);
  check(leftOut.tool === 'browse' && await readsOf(leftOut.id) === `${filter.id}:false`, `a Browse of the Filter's False: ${JSON.stringify(leftOut)}, ${await readsOf(leftOut.id)}`);
  await settled();
  check(await resultRows() === all - fewer, `the rows it leaves out: ${all} - ${fewer}`);
  const leftAt = await page.evaluate((id) => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes.find((b) => b.tile === id), leftOut.id);
  check(Math.abs(leftAt.x + 92 - freeSpot.canvasX) <= 2, `made where its line was let go: (${leftAt.x}, ${leftAt.y}), its middle at ${freeSpot.canvasX}`);
  await page.screenshot({ path: outPath('flow-false.png') });

  // A JOIN'S R (§3.3): a line from its R port let go on the canvas -- a Browse of the Input's rows no row of the Filter's
  // pairs with (the Filter's trades matched on trade_id: every other trade)
  const beforeR = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${join.id}"]`).scrollIntoViewIfNeeded();
  check(JSON.stringify(await page.locator(`.dc-flow-box[data-tile="${join.id}"] .dc-flow-port-out:visible`).allTextContents()) === '["J","L","R"]',
    'a Join draws its J, L and R');
  const rPort = await page.locator(`.dc-flow-box[data-tile="${join.id}"] .dc-flow-port-r`).boundingBox();
  const rSpot = await emptyCanvasPoint();
  await page.mouse.move(rPort.x + rPort.width / 2, rPort.y + rPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(rSpot.x, rSpot.y, { steps: 12 });
  await page.mouse.up();
  await boxCount(beforeR.length + 1);
  const unpaired = await added(beforeR);
  check(unpaired.tool === 'browse' && await readsOf(unpaired.id) === `${join.id}:right`, `a Browse of the Join's R: ${JSON.stringify(unpaired)}, ${await readsOf(unpaired.id)}`);
  await settled();
  check(await resultRows() === all - fewer, `the Input's rows no Filter row pairs with: ${all} - ${fewer}`);
  // set from its settings to the Join's L: each of the Filter's trades has its match
  await page.locator('.dc-flow-input select').first().selectOption(`grid:${join.id}:left`);
  await page.waitForFunction(([id, j]) => {
    const doc = window.__dataPage.document('x');
    const view = doc.views.find((v) => v.id === id);
    const input = doc.cubes.find((c) => c.id === view.cube).cube.source.input;
    return input.grid === j && input.port === 'left';
  }, [unpaired.id, join.id], { timeout: 20_000 });
  await settled();
  check(await resultRows() === 0, 'its L: none of the Filter\'s trades without a match');
  await page.screenshot({ path: outPath('flow-join-ports.png') });

  // TIDY UP (M7): a box pushed out of place, then every box a column after the boxes it reads -- one step; then a box's
  // menu, Duplicate
  const pushed = page.locator(`.dc-flow-box[data-tile="${formula.id}"]`);
  await pushed.focus();
  for (let i = 0; i < 6; i += 1) await pushed.press('Shift+ArrowDown');
  await page.locator('.dc-flow-tidy').click();
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Tidy Up', 'one step: Tidy Up');
  const tidied = await page.evaluate(() => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes);
  const xOf = (id) => tidied.find((b) => b.tile === id).x;
  check(xOf(input.id) < xOf(filter.id) && xOf(filter.id) < xOf(join.id) && xOf(filter.id) < xOf(formula.id),
    `each box after the boxes it reads: ${JSON.stringify(tidied.map((b) => [b.tile, b.x, b.y]))}`);
  await page.screenshot({ path: outPath('flow-tidied.png') });
  const beforeCopy = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).click({ button: 'right' });
  const duplicate = page.locator('.dc-menu .dc-menu-item', { hasText: 'Duplicate' }).first();
  await duplicate.waitFor({ timeout: 5_000 });
  check(await page.locator('.dc-menu .dc-menu-item', { hasText: 'Freeze its Input' }).count() === 1, 'its menu offers to freeze its input');
  await duplicate.click();
  await boxCount(beforeCopy.length + 1);
  const copy = await added(beforeCopy);
  check(copy.tool === 'formula' && copy.says === 'fee', `a copy of the Formula: ${JSON.stringify(copy)}`);
  check(await readsOf(copy.id) === filter.id, 'reading what it reads');
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Duplicate Box', 'one step: Duplicate Box');

  // RUN (M5, M6): each box shown in turn, its rows counted beside its output; an edit upstream drops the counts after it
  await page.locator('.dc-flow-run').click();
  await page.waitForFunction(() => /Run$/.test(document.querySelector('.dc-flow-run')?.textContent ?? ''), undefined, { timeout: 120_000 });
  const counts = await page.locator('.dc-flow-box').evaluateAll((els) => Object.fromEntries(els.map((e) => [e.dataset.tile, e.querySelector('.dc-flow-count')?.textContent ?? ''])));
  check(counts[input.id] === '5,000' && counts[filter.id] === fewer.toLocaleString() && counts[union.id] === (fewer + all).toLocaleString(),
    `each box counted: ${JSON.stringify(counts)}`);
  // its log (M6): every box, its rows and its time
  const logged = await page.locator('.dc-flow-log .dc-flow-log-item').count();
  check(await page.locator('.dc-flow-log').isVisible() && logged === Object.keys(counts).length,
    `the run's log, a line a box: ${logged} of ${Object.keys(counts).length}`);
  check(/^Ran \d+ boxes in /.test(await page.locator('.dc-flow-log-title').textContent() ?? ''), 'it says what it ran, and how long it took');
  await page.screenshot({ path: outPath('flow-run.png') });
  await page.locator('.dc-flow-log-close').click();
  check(!(await page.locator('.dc-flow-log').isVisible()), 'closed by its ×');

  // A SAMPLE (M3), dragged onto the Filter: the first five rows by trade_id, in its window docked
  const beforeSample = (await boxes()).map((b) => b.id);
  await drag('sample', filter.id);
  await boxCount(beforeSample.length + 1);
  const sample = await added(beforeSample);
  await docked('Sample');
  await page.locator('.dc-flow-dock .dc-sample-n').fill('5');
  await page.locator('.dc-flow-dock .dc-sample-by').selectOption('trade_id');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const sampled = (await boxes()).find((b) => b.id === sample.id);
  check(sampled?.tool === 'sample' && sampled.says === 'first 5 by trade_id', `a Sample box: ${JSON.stringify(sampled)}`);
  check(await resultRows() === 5, 'its first five rows');
  // set again from its settings: the first of each region by trade_id, then one of every 100
  const resample = async (set) => {
    await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
    await docked('Sample');
    await set(page.locator('.dc-flow-dock .dc-sample'));
    await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
    await settled();
    return (await boxes()).find((b) => b.id === sample.id);
  };
  const perRegion = await resample(async (w) => {
    await w.locator('.dc-sample-n').fill('1');
    await w.locator('.dc-sample-grouped input').check();
    await w.locator('.dc-sample-groups input[value="region"]').check();
  });
  check(perRegion?.says === 'first 1 of each region by trade_id', `a Sample of each group: ${JSON.stringify(perRegion)}`);
  check(await resultRows() === 3, 'a row of each of the three regions');
  check(await page.locator('.dc-flow-results .dc-th[data-column="SampleRow"]').count() === 0, 'its row number hidden');
  const everyHundred = await resample(async (w) => {
    await w.locator('.dc-sample-which').selectOption('every');
    await w.locator('.dc-sample-n').fill('100');
    await w.locator('.dc-sample-grouped input').uncheck();
  });
  check(everyHundred?.says === 'one of every 100 by trade_id', `a Sample of one of every 100: ${JSON.stringify(everyHundred)}`);
  check(await resultRows() === Math.ceil(fewer / 100), `one of every 100 of the Filter's ${fewer}: ${Math.ceil(fewer / 100)}`);
  // Select Records: the rows at some places -- the first three and the tenth
  const places = await resample(async (w) => {
    await w.locator('.dc-sample-which').selectOption('records');
    check(await w.locator('.dc-sample-n').isHidden() && await w.locator('.dc-sample-ranges').isVisible(), 'its places asked, not a number');
    await w.locator('.dc-sample-ranges').fill('1-3, 10');
  });
  check(places?.says === 'rows 1-3, 10 by trade_id', `a Sample of some places: ${JSON.stringify(places)}`);
  check(await resultRows() === 4, 'the rows at them: four');
  // a Random % Sample: about a tenth of them, the same tenth for its seed
  const atRandom = await resample(async (w) => {
    await w.locator('.dc-sample-which').selectOption('random');
    check(await w.locator('.dc-sample-by').isHidden() && await w.locator('.dc-sample-seed').isVisible(), 'its percent and its seed asked, no order');
    await w.locator('.dc-sample-n').fill('10');
    await w.locator('.dc-sample-seed').fill('7');
  });
  check(atRandom?.says === '10% at random, seed 7', `a random Sample: ${JSON.stringify(atRandom)}`);
  const tenth = await resultRows();
  check(tenth > fewer * 0.04 && tenth < fewer * 0.2, `about a tenth of the Filter's ${fewer}: ${tenth}`);
  // CREATE SAMPLES (M3): a Sample a filter makes has two outputs, its rows (S) and the rest (R) -- Alteryx's estimation
  // and holdout; a line from its R let go on the canvas, a Browse of the rest
  check(JSON.stringify(await page.locator(`.dc-flow-box[data-tile="${sample.id}"] > .dc-flow-port-out:visible`).allTextContents()) === '["S","R"]',
    `a Sample's S and R: ${await page.locator(`.dc-flow-box[data-tile="${sample.id}"] > .dc-flow-port-out:visible`).allTextContents()}`);
  const beforeRest = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${sample.id}"]`).scrollIntoViewIfNeeded();
  const restPort = await page.locator(`.dc-flow-box[data-tile="${sample.id}"] .dc-flow-port-false`).boundingBox();
  const restSpot = await emptyCanvasPoint();
  await page.mouse.move(restPort.x + restPort.width / 2, restPort.y + restPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(restSpot.x, restSpot.y, { steps: 12 });
  await page.mouse.up();
  await boxCount(beforeRest.length + 1);
  const theRest = await added(beforeRest);
  check(theRest.tool === 'browse' && await readsOf(theRest.id) === `${sample.id}:false`, `a Browse of the rest: ${JSON.stringify(theRest)}, ${await readsOf(theRest.id)}`);
  await settled();
  check(await resultRows() === fewer - tenth, `the rest: ${fewer} - ${tenth}`);
  await page.screenshot({ path: outPath('flow-sample-rest.png') });

  // A DATE FILTER (M3), dragged onto the Filter: its trade dates between two days, in its window docked; a Filter's T
  // and F; a range relative to today said in its words
  const beforeDated = (await boxes()).map((b) => b.id);
  await drag('dateFilter', filter.id);
  await boxCount(beforeDated.length + 1);
  const dateFiltered = await added(beforeDated);
  await docked('Date Filter');
  await page.locator('.dc-flow-dock .dc-date-filter-column').selectOption('trade_date');
  check(await page.locator('.dc-flow-dock .dc-date-filter-days').isHidden(), 'a range asks no days');
  await page.locator('.dc-flow-dock .dc-date-filter-way').selectOption('between');
  check(await page.locator('.dc-flow-dock .dc-date-filter-days').isVisible(), 'its two days asked');
  await page.locator('.dc-flow-dock .dc-date-filter-from').fill('2023-01-01');
  await page.locator('.dc-flow-dock .dc-date-filter-to').fill('2023-12-31');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const dateBox = (await boxes()).find((b) => b.id === dateFiltered.id);
  check(dateBox?.tool === 'dateFilter' && dateBox.says === 'trade_date from 2023-01-01 to 2023-12-31', `a Date Filter box: ${JSON.stringify(dateBox)}`);
  const in2023 = await resultRows();
  check(in2023 > 0 && in2023 < fewer, `the Filter's trades of 2023: ${in2023} of ${fewer}`);
  check(JSON.stringify(await page.locator(`.dc-flow-box[data-tile="${dateFiltered.id}"] > .dc-flow-port-out:visible`).allTextContents()) === '["T","F"]',
    'its T and F, as a Filter\'s');
  await page.screenshot({ path: outPath('flow-date-filter.png') });
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Date Filter');
  check(await page.locator('.dc-flow-dock .dc-date-filter-from').inputValue() === '2023-01-01', 'set again: its days as they are');
  await page.locator('.dc-flow-dock .dc-date-filter-way').selectOption('last12Months');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  check((await boxes()).find((b) => b.id === dateFiltered.id)?.says === 'trade_date: Last 12 Months', 'set again: a range relative to today');

  // A CUSTOM FILTER (FL12), dragged onto the Filter: its condition in Pure, checked by the compiler -- one that is no
  // Boolean refused in its words -- a column a click away; its T and F; set again, its condition as the compiler prints it
  const beforeCustom = (await boxes()).map((b) => b.id);
  await drag('customFilter', filter.id);
  await boxCount(beforeCustom.length + 1);
  const customed = await added(beforeCustom);
  await docked('Custom Filter');
  const conditionBox = page.locator('.dc-flow-dock .dc-custom-filter-condition');
  await conditionBox.fill('$x.notional->toOne() * 2');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await page.waitForFunction(() => /a Boolean: this one is Float/.test(document.querySelector('.dc-flow-dock .dc-sample-status')?.textContent ?? ''), undefined, { timeout: 30_000 });
  await conditionBox.fill('');
  await page.locator('.dc-flow-dock .dc-custom-filter-column:text-is("region")').click();
  check(await conditionBox.inputValue() === '$x.region', `a column put in: ${await conditionBox.inputValue()}`);
  await conditionBox.press('End');
  await conditionBox.type(" == 'EMEA'");
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const customBox = (await boxes()).find((b) => b.id === customed.id);
  check(customBox?.tool === 'customFilter' && customBox.says === 'a condition on region', `a Custom Filter box: ${JSON.stringify(customBox)}`);
  const inEmea = await resultRows();
  check(inEmea > 0 && inEmea < fewer, `the Filter's EMEA trades: ${inEmea} of ${fewer}`);
  check(JSON.stringify(await page.locator(`.dc-flow-box[data-tile="${customed.id}"] > .dc-flow-port-out:visible`).allTextContents()) === '["T","F"]',
    'its T and F, as a Filter\'s');
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Custom Filter');
  const printed = await page.locator('.dc-flow-dock .dc-custom-filter-condition').inputValue();
  check(/region/.test(printed) && /EMEA/.test(printed), `set again: its condition as the compiler prints it: ${printed}`);
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-quiet').click();

  // A PARAMETER OF THE PAGE (M8, an analytic app's input): the palette's Parameter opens the parameters bar's form -- a
  // pick-list of regions -- and a Custom Filter reads it as $region_of; its value picked again, the box's rows are read so
  await page.locator('.dc-flow-tool[data-tool="parameter"]').click();
  const paramForm = page.locator('.dc-page-parameter-form');
  await paramForm.waitFor({ timeout: 20_000 });
  await paramForm.locator('.dc-page-parameter-name').fill('region_of');
  await paramForm.locator('.dc-page-parameter-type').selectOption('String');
  await paramForm.locator('.dc-page-parameter-first').fill('EMEA');
  await paramForm.locator('.dc-page-parameter-choices').fill('EMEA, AMER');
  await paramForm.locator('.dc-page-filter-apply').click();
  await page.locator('.dc-page-parameter[data-parameter="region_of"] select').waitFor({ timeout: 20_000 });
  const paramsSaved = await page.evaluate(() => window.__dataPage.document('x')?.parameters);
  check(JSON.stringify(paramsSaved) === JSON.stringify([{ name: 'region_of', type: 'String', value: 'EMEA', choices: ['EMEA', 'AMER'] }]),
    `the page writes it: ${JSON.stringify(paramsSaved)}`);
  const beforeParam = (await boxes()).map((b) => b.id);
  await drag('customFilter', filter.id);
  await boxCount(beforeParam.length + 1);
  const paramBox = await added(beforeParam);
  await docked('Custom Filter');
  await page.locator('.dc-flow-dock .dc-custom-filter-condition').fill('$x.region == $region_of');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  check(await resultRows() === inEmea, `read as EMEA, its value: ${await resultRows()} of ${inEmea}`);
  await page.locator('.dc-page-parameter[data-parameter="region_of"] select').selectOption({ label: 'AMER' });
  await page.waitForFunction(() => window.__dataPage.document('x')?.parameters?.[0]?.value === 'AMER', undefined, { timeout: 30_000 });
  await settled();
  const inAmer = await resultRows();
  check(inAmer > 0 && inAmer !== inEmea, `its value picked again, read so: ${inAmer} AMER trades`);
  // a box after it reads its rows with the value as it is; picked again, the box shown is opened again over them
  const beforeParamAfter = (await boxes()).map((b) => b.id);
  await drag('browse', paramBox.id);
  await boxCount(beforeParamAfter.length + 1);
  await added(beforeParamAfter);
  await settled();
  check(await resultRows() === inAmer, `the box after it: its ${inAmer} AMER trades`);
  await page.locator('.dc-page-parameter[data-parameter="region_of"] select').selectOption({ label: 'EMEA' });
  await page.waitForFunction((n) => {
    const t = document.querySelector('.dc-flow-results')?.textContent ?? '';
    const m = /of ([\d,]+) rows/.exec(t) ?? /([\d,]+) rows ×/.exec(t);
    return m !== null && Number(m[1].replace(/,/g, '')) === n;
  }, inEmea, { timeout: 60_000 }).catch(() => undefined);
  check(await resultRows() === inEmea, `picked again: the box after it opened again over EMEA's ${inEmea}: ${await resultRows()}`);

  // A STANDARD MACRO (M8): the parameter's Custom Filter saved as a macro from its menu -- its steps one box -- then the
  // palette's Macro clicked and picked: a Pure box of its function after the box shown, named after it, its rows the region's
  await page.locator(`.dc-flow-box[data-tile="${paramBox.id}"]`).click({ button: 'right' });
  await page.locator('.dc-menu .dc-menu-item', { hasText: 'Save as Macro' }).first().click();
  const macroForm = page.locator('.dc-flow-macro-form');
  await macroForm.waitFor({ timeout: 10_000 });
  await macroForm.locator('.dc-flow-macro-name').fill('Region trades');
  await macroForm.locator('button[type="submit"]').click();
  await macroForm.waitFor({ state: 'detached', timeout: 60_000 });
  const macrosSaved = await page.evaluate(() => window.__dataPage.document('x')?.macros?.map((m) => m.name));
  check(JSON.stringify(macrosSaved) === '["Region trades"]', `the page writes it: ${JSON.stringify(macrosSaved)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Save Macro', 'one step: Save Macro');
  const beforeMacro = (await boxes()).map((b) => b.id);
  // clicked: after the box shown (the Custom Filter, shown again once it was read)
  await page.locator('.dc-flow-tool[data-tool="macro"]').scrollIntoViewIfNeeded();
  await page.locator('.dc-flow-tool[data-tool="macro"]').click();
  await page.locator('.dc-menu .dc-menu-item', { hasText: 'Region trades' }).first().click();
  await boxCount(beforeMacro.length + 1);
  const macroBox = await added(beforeMacro);
  await settled();
  const macroTitle = await page.locator(`.dc-flow-box[data-tile="${macroBox.id}"]`).textContent();
  check(macroBox.tool === 'pure' && /Region trades/.test(macroTitle ?? ''), `a Pure box named after it: ${JSON.stringify(macroBox)} ${macroTitle}`);
  check(await resultRows() === inEmea, `its rows the region's: ${await resultRows()} of ${inEmea}`);

  // A PURE BOX (FL12), dragged onto the Filter: its rows a function of the Filter's, in Pure over $input -- a step put in
  // by its button, one that is no relation refused in words; set, its box says its functions and its rows are theirs;
  // set again, its Pure as the compiler prints it; taken off in one step, undone
  const beforePure = (await boxes()).map((b) => b.id);
  await drag('pure', filter.id);
  await boxCount(beforePure.length + 1);
  const pured = await added(beforePure);
  await docked('Pure');
  const pureText = page.locator('.dc-flow-dock .dc-pure-text');
  check(await pureText.inputValue() === '$input', 'it starts at its input');
  await pureText.press('End');
  await page.locator('.dc-flow-dock .dc-pure-step[data-step="limit"]').click();
  check(await pureText.inputValue() === '$input->limit(10)', `a step put in: ${await pureText.inputValue()}`);
  await pureText.fill('$input->count()');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await page.waitForFunction(() => (document.querySelector('.dc-flow-dock .dc-sample-status')?.textContent ?? '') !== '', undefined, { timeout: 30_000 });
  const valueSaid = await page.locator('.dc-flow-dock .dc-sample-status').textContent() ?? '';
  check(/its Pure gives a value, not rows/.test(valueSaid), `a value, no rows: refused in words: ${valueSaid}`);
  await pureText.fill("$input->filter(x|$x.region == 'EMEA')->limit(10)");
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Set Pure', undefined, { timeout: 30_000 });
  await settled();
  const pureBox = (await boxes()).find((b) => b.id === pured.id);
  check(pureBox?.tool === 'pure' && pureBox.says === 'filter → limit', `a Pure box: ${JSON.stringify(pureBox)}`);
  const pureRows = await resultRows();
  check(pureRows === Math.min(10, inEmea), `its rows, the Filter's EMEA trades, ten at most: ${pureRows}`);
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Pure');
  const printedPure = await page.locator('.dc-flow-dock .dc-pure-text').inputValue();
  check(/^\$input/.test(printedPure) && /limit\(10\)/.test(printedPure) && /EMEA/.test(printedPure), `set again: its Pure as the compiler prints it: ${printedPure}`);
  await page.locator('.dc-flow-dock .dc-pure-remove').click();
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Take Pure Off', undefined, { timeout: 30_000 });
  await settled();
  check(await resultRows() === fewer, `taken off: the Filter's rows again: ${await resultRows()}`);
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  await page.keyboard.press('Control+z');
  await page.waitForFunction((id) => document.querySelector(`.dc-flow-box[data-tile="${id}"]`)?.textContent?.includes('filter → limit'), pured.id, { timeout: 30_000 });
  check((await boxes()).find((b) => b.id === pured.id)?.says === 'filter → limit', 'undone: its Pure back');
  // IMPORT PURE (FL12): its Pure taken apart by Import as Boxes -- this box its filter, a box after it of its limit
  await page.locator(`.dc-flow-box[data-tile="${pured.id}"]`).click();
  await settled();
  // its window docked already (it stays after Take Its Pure Off and its undo), or opened from its settings
  const openPure = page.locator('.dc-flow-settings button:visible', { hasText: 'Open its settings' });
  if (await openPure.count() > 0) await openPure.click();
  await docked('Pure');
  const beforeImport = (await boxes()).map((b) => b.id);
  await page.locator('.dc-flow-dock .dc-pure-import').click();
  await boxCount(beforeImport.length + 1);
  const imported = await added(beforeImport);
  await page.waitForFunction((id) => document.querySelector(`.dc-flow-box[data-tile="${id}"]`)?.textContent?.includes('filter'), pured.id, { timeout: 30_000 });
  check((await boxes()).find((b) => b.id === pured.id)?.says === 'filter' && imported.tool === 'pure' && imported.says === 'limit',
    `imported as boxes: a filter, then a limit after it: ${JSON.stringify([(await boxes()).find((b) => b.id === pured.id), imported])}`);
  const importedInput = await page.evaluate((id) => window.__dataPage.document('x')?.cubes.find((c) => c.id === id)?.cube.source.input, imported.id);
  check(JSON.stringify(importedInput) === JSON.stringify({ grid: pured.id }), `the limit reads the filter: ${JSON.stringify(importedInput)}`);

  // NEW BOX AFTER (FL9): a cell of the Filter's rows right-clicked, its region filtered by -- in a box of its own after
  // the Filter, the Filter left doing its one thing
  await page.locator(`.dc-flow-box[data-tile="${filter.id}"]`).click();
  await settled();
  const filterSays = (await boxes()).find((b) => b.id === filter.id)?.says;
  const regionCell = page.locator('.dc-flow-results .dc-row .dc-cell[data-column="region"]').first();
  const region = (await regionCell.textContent() ?? '').trim();
  const beforeAfter = (await boxes()).map((b) => b.id);
  await regionCell.click({ button: 'right' });
  await item('New Box After').hover();
  // its own entry, by its label (the parent's text holds every entry of its submenu)
  await item(`Filter: region = '${region}'`).click();
  await boxCount(beforeAfter.length + 1);
  const afterBox = await added(beforeAfter);
  await settled();
  check(afterBox.tool === 'filter' && await readsOf(afterBox.id) === filter.id, `a Filter box after the Filter: ${JSON.stringify(afterBox)}, ${await readsOf(afterBox.id)}`);
  await page.waitForFunction((t) => /region/.test(document.querySelector(`.dc-flow-box[data-tile="${t}"] .dc-flow-says`)?.textContent ?? ''), afterBox.id, { timeout: 30_000 });
  check((await boxes()).find((b) => b.id === filter.id)?.says === filterSays, 'the Filter as it was: its one thing');
  const regionRows = await resultRows();
  check(regionRows > 0 && regionRows < fewer, `its rows: the Filter's of ${region}, ${regionRows} of ${fewer}`);

  // A BINNING (M3, Alteryx's Multi-Field Binning), dragged onto the Filter: its notionals into five bins of equal width,
  // the database placing each; the bin beside its column, its helpers hidden
  const beforeBins = (await boxes()).map((b) => b.id);
  await drag('binning', filter.id);
  await boxCount(beforeBins.length + 1);
  const binnedBox = await added(beforeBins);
  await docked('Binning');
  await page.locator('.dc-flow-dock .dc-text-tool-check', { hasText: /^notional$/ }).locator('input').check();
  await page.locator('.dc-flow-dock .dc-binning-bins').fill('5');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const binBox = (await boxes()).find((b) => b.id === binnedBox.id);
  check(binBox?.tool === 'binning' && binBox.says === 'notional in 5 bins', `a Binning box: ${JSON.stringify(binBox)}`);
  const binHeads = await page.locator('.dc-flow-results .dc-th[data-column]').evaluateAll((els) => els.map((e) => e.dataset.column));
  check(binHeads.includes('notional_bins') && !binHeads.some((c) => /__(min|max|upper)$/.test(c)), `its bin beside notional, its helpers hidden: ${JSON.stringify(binHeads)}`);
  check(await resultRows() === fewer, "its input's rows, each binned");

  // A GENERATE ROWS (M3), dragged onto the Filter: each of its rows repeated for each number from 1 to 3, the database
  // making every row
  const beforeGen = (await boxes()).map((b) => b.id);
  await drag('generateRows', filter.id);
  await boxCount(beforeGen.length + 1);
  const genBox = await added(beforeGen);
  await docked('Generate Rows');
  await page.locator('.dc-flow-dock .dc-generate-to').fill('3');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const generated = (await boxes()).find((b) => b.id === genBox.id);
  check(generated?.tool === 'generateRows' && generated.says === 'N from 1 to 3', `a Generate Rows box: ${JSON.stringify(generated)}`);
  const tripled = await resultRows();
  check(tripled === fewer * 3, `each of the Filter's ${fewer} rows three times: ${tripled}`);

  // A TEST (FL8), dragged onto the Filter: its trade ids repeat none, its notionals within bounds -- each check's failing
  // rows counted by the database, its box marked by what its row says; set again, passing
  const beforeTest = (await boxes()).map((b) => b.id);
  await drag('test', filter.id);
  await boxCount(beforeTest.length + 1);
  const tested = await added(beforeTest);
  await docked('Test');
  const firstCheck = page.locator('.dc-flow-dock .dc-test-check').first();
  await firstCheck.locator('.dc-test-column').selectOption('trade_id');
  await firstCheck.locator('.dc-test-kind').selectOption('unique');
  await page.locator('.dc-flow-dock .dc-test-add').click();
  const secondCheck = page.locator('.dc-flow-dock .dc-test-check').nth(1);
  await secondCheck.locator('.dc-test-kind').selectOption('between');
  await secondCheck.locator('.dc-test-column').selectOption('notional');
  await secondCheck.locator('.dc-test-min').fill('0');
  await secondCheck.locator('.dc-test-max').fill('950000');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  check(await resultRows() === 1, 'its one row: its counts');
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.classList.contains('dc-flow-failing'), tested.id, { timeout: 10_000 });
  const testSays = await page.locator(`.dc-flow-box[data-tile="${tested.id}"] .dc-flow-says`).textContent();
  check(testSays === '1 of 2 checks fail', `its result said: ${testSays}`);
  await page.screenshot({ path: outPath('flow-test.png') });
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Test');
  check(await page.locator('.dc-flow-dock .dc-test-check').count() === 2, 'set again: its two checks as they are');
  // its second a count of rows (of no column): at most 1, its many rows failing it -- counted after grouping
  const countCheck = page.locator('.dc-flow-dock .dc-test-check').nth(1);
  await countCheck.locator('.dc-test-kind').selectOption('rows');
  check(await countCheck.locator('.dc-test-column').isHidden(), 'a count of rows names no column');
  await countCheck.locator('.dc-test-min').fill('');
  await countCheck.locator('.dc-test-max').fill('1');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"] .dc-flow-says`)?.getAttribute('title')?.includes('at most 1 row: it has'), tested.id, { timeout: 10_000 });
  const counting = await page.locator(`.dc-flow-box[data-tile="${tested.id}"] .dc-flow-says`).getAttribute('title');
  check(/trade_id repeats no value, at most 1 row\n.*at most 1 row: it has \d+ rows/.test(counting ?? ''), `its count of rows failing, its rows said: ${counting}`);
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Test');
  check(await page.locator('.dc-flow-dock .dc-test-check').nth(1).locator('.dc-test-kind').inputValue() === 'rows', 'read back: a count of rows');
  // its second made a list of values: every region one of the three -- passing, both
  const listCheck = page.locator('.dc-flow-dock .dc-test-check').nth(1);
  await listCheck.locator('.dc-test-kind').selectOption('oneOf');
  await listCheck.locator('.dc-test-column').selectOption('region');
  await listCheck.locator('.dc-test-text').fill('EMEA, APAC, AMER');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"] .dc-flow-says`)?.textContent === 'all 2 checks pass', tested.id, { timeout: 30_000 });
  check(/region is one of EMEA, APAC, AMER/.test(await page.locator(`.dc-flow-box[data-tile="${tested.id}"] .dc-flow-says`).getAttribute('title') ?? ''), 'a list of values, said');
  await page.locator('.dc-flow-settings button', { hasText: 'Open its settings' }).click();
  await docked('Test');
  check(await page.locator('.dc-flow-dock .dc-test-check').nth(1).locator('.dc-test-text').inputValue() === 'EMEA, APAC, AMER', 'read back: its values');
  await page.locator('.dc-flow-dock .dc-test-check').nth(1).locator('.dc-test-off').click();
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.classList.contains('dc-flow-passing'), tested.id, { timeout: 10_000 });
  check(await page.locator(`.dc-flow-box[data-tile="${tested.id}"] .dc-flow-says`).textContent() === 'its check passes', 'its one check passes');

  // A DATETIME (M3), dragged onto the Filter: its trade dates taken to their month, in its window docked
  const beforeDate = (await boxes()).map((b) => b.id);
  await drag('datetime', filter.id);
  await boxCount(beforeDate.length + 1);
  const dated = await added(beforeDate);
  await docked('Date Period');
  check(await page.locator('.dc-flow-dock .dc-date-column option').count() > 0, 'its window offers the date columns');
  await page.locator('.dc-flow-dock .dc-date-column').selectOption('trade_date');
  await page.locator('.dc-flow-dock .dc-date-grain').selectOption('month');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const monthly = (await boxes()).find((b) => b.id === dated.id);
  check(monthly?.tool === 'datetime' && monthly.says === 'trade_date by month', `a DateTime box: ${JSON.stringify(monthly)}`);
  check(await page.locator('.dc-flow-results .dc-th[data-column="trade_date_month"]').count() > 0, 'its column in its rows: trade_date_month');
  check(await resultRows() === fewer, 'its input\'s rows, each kept');

  // A DATETIME'S PART (M3): its year of the trade date, the same window's second way
  const beforePart = (await boxes()).map((b) => b.id);
  await drag('datetime', filter.id);
  await boxCount(beforePart.length + 1);
  const parted = await added(beforePart);
  await docked('Date Period');
  await page.locator('.dc-flow-dock .dc-date-mode').selectOption('part');
  await page.locator('.dc-flow-dock .dc-date-part').selectOption('year');
  await page.locator('.dc-flow-dock .dc-date-part-column').selectOption('trade_date');
  check(await page.locator('.dc-flow-dock .dc-date-name').inputValue() === 'trade_date_year', 'named after its column and its part');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const partBox = (await boxes()).find((b) => b.id === parted.id);
  check(partBox?.tool === 'datetime' && partBox.says === 'year of trade_date', `a DateTime's part: ${JSON.stringify(partBox)}`);
  check(await page.locator('.dc-flow-results .dc-th[data-column="trade_date_year"]').count() > 0, 'its column in its rows: trade_date_year');

  // THE TEXT TOOLS (M3), each dragged onto the Filter, set in its window docked, its columns planned and run
  const textStep = async (tool, window, set, expect) => {
    const before = (await boxes()).map((b) => b.id);
    await drag(tool, filter.id);
    await boxCount(before.length + 1);
    const made = await added(before);
    await docked(window);
    await set(page.locator('.dc-flow-dock'));
    await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
    await settled();
    const box = (await boxes()).find((b) => b.id === made.id);
    const refused = await page.locator('.dc-flow-dock .dc-sample-status').textContent().catch(() => '');
    check(box?.tool === tool && box.says === expect.says, `a ${window} box: ${JSON.stringify(box)}${refused ? `, refused: ${refused}` : ''}`);
    for (const c of expect.columns) {
      check(await page.locator(`.dc-flow-results .dc-th[data-column="${c}"]`).count() > 0, `its column ${c} in its rows`);
    }
    // into rows: each row once a part, more rows than its input's
    const n = await resultRows();
    check(expect.more ? n > fewer : n === fewer, `${window}: ${expect.more ? `a row a part, more than its input's ${fewer}` : "its input's rows, each kept"}: ${n}`);
  };
  await textStep('textToColumns', 'Text To Columns', async (w) => {
    await w.locator('.dc-text-tool-column').selectOption('book');
    await w.locator('.dc-text-tool-delimiter').fill(' ');
  }, { says: "book split on ' '", columns: ['book_1', 'book_2'] });
  await textStep('textToColumns', 'Text To Columns', async (w) => {
    await w.locator('.dc-text-tool-column').selectOption('book');
    await w.locator('.dc-text-tool-delimiter').fill(' ');
    await w.locator('.dc-text-tool-into').selectOption('rows');
    await w.locator('.dc-text-tool-name').fill('word');
  }, { says: "book split on ' ' to rows", columns: ['word'], more: true });
  await textStep('regex', 'RegEx', async (w) => {
    await w.locator('.dc-text-tool-column').selectOption('desk');
    await w.locator('.dc-text-tool-pattern').fill('^[A-Z][a-z]+');
    await w.locator('.dc-text-tool-mode').selectOption('extract');
  }, { says: '/^[A-Z][a-z]+/ of desk', columns: ['desk_extracted'] });
  await textStep('cleanse', 'Data Cleansing', async (w) => {
    await w.locator('.dc-text-tool-check', { hasText: 'region' }).locator('input').check();
    await w.locator('.dc-text-tool-case').selectOption('lower');
  }, { says: 'region cleansed in place', columns: ['region_clean'] });

  // IMPUTATION (M3), dragged onto the Filter: notional's empties filled with its average across every row
  const beforeImpute = (await boxes()).map((b) => b.id);
  await drag('impute', filter.id);
  await boxCount(beforeImpute.length + 1);
  const filling = await added(beforeImpute);
  await docked('Imputation');
  await page.locator('.dc-flow-dock .dc-impute input[value="notional"]').check();
  await page.locator('.dc-flow-dock .dc-impute-with').selectOption('average');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const filledBox = (await boxes()).find((b) => b.id === filling.id);
  const imputeRefused = await page.locator('.dc-flow-dock .dc-sample-status').textContent().catch(() => '');
  check(filledBox?.tool === 'impute' && filledBox.says === "notional's empties → its average in place", `an Imputation box: ${JSON.stringify(filledBox)}${imputeRefused ? `, refused: ${imputeRefused}` : ''}`);
  check(await page.locator('.dc-flow-results .dc-th[data-column="notional_filled"]').count() > 0
    && await page.locator('.dc-flow-results .dc-th[data-column="notional__average"]').count() === 0, 'its column in its rows, its average hidden');
  check(await resultRows() === fewer, "Imputation: its input's rows, each kept");

  // SELECT (M1), dragged onto the Filter: region renamed area, book left out; the box after it reads area
  const beforeSelect = (await boxes()).map((b) => b.id);
  await drag('select', filter.id);
  await boxCount(beforeSelect.length + 1);
  const chose = await added(beforeSelect);
  await docked('Select');
  const sw = page.locator('.dc-flow-dock .dc-select-form');
  // many renamed at once (Dynamic Rename): a prefix given to each kept column, then their own names again
  await sw.locator('.dc-select-prefix').fill('x_');
  await sw.locator('.dc-select-affix').click();
  check(await sw.locator('.dc-select-row[data-column="desk"] .dc-select-rename').inputValue() === 'x_desk', 'each kept one prefixed');
  await sw.locator('.dc-select-unname').click();
  check(await sw.locator('.dc-select-row[data-column="desk"] .dc-select-rename').inputValue() === '', 'and its own name again');
  await sw.locator('.dc-select-row[data-column="region"] .dc-select-rename').fill('area');
  await sw.locator('.dc-select-row[data-column="book"] .dc-select-keep').uncheck();
  // and a number made text: a conversion of it, handed on by its name, in its place
  await sw.locator('.dc-select-row[data-column="quantity"] .dc-select-retype').selectOption('String');
  // and desk moved up, above region (Alteryx's fields moved up and down)
  await sw.locator('.dc-select-row[data-column="desk"] .dc-select-up').click();
  const windowOrder = await sw.locator('.dc-select-list .dc-select-row').evaluateAll((els) => els.map((e) => e.dataset.column));
  check(windowOrder.indexOf('desk') === windowOrder.indexOf('region') - 1, `desk moved above region: ${windowOrder}`);
  await sw.locator('.dc-sample-foot .dc-primary').click();
  await settled();
  const choseBox = (await boxes()).find((b) => b.id === chose.id);
  check(choseBox?.tool === 'select' && /^region → area; quantity as String; 1 of \d+ columns left out; columns reordered$/.test(choseBox.says), `a Select box: ${JSON.stringify(choseBox)}`);
  // a Browse after it, from the palette: its rows as it reads them
  const beforeReader = (await boxes()).map((b) => b.id);
  await drag('browse', chose.id);
  await boxCount(beforeReader.length + 1);
  const reader = await added(beforeReader);
  check(reader.tool === 'browse' && reader.says === 'every row', `a Browse from the palette: ${JSON.stringify(reader)}`);
  await show(reader.id);
  const readHeads = await page.locator('.dc-flow-results .dc-th[data-column]').evaluateAll((els) => els.map((e) => e.dataset.column));
  check(readHeads.includes('area') && !readHeads.includes('region') && !readHeads.includes('book') && readHeads.includes('quantity')
    && !readHeads.some((h) => h.includes('__')), `the box after it reads area, no region, no book, quantity by its name: ${readHeads}`);
  check(readHeads.indexOf('desk') < readHeads.indexOf('area') && readHeads.indexOf('quantity') === readHeads.indexOf('pnl') + 1,
    `in its order: desk before area, quantity where it was: ${readHeads}`);
  // its quantity text now: the Profile says its type
  await page.locator('.dc-flow-mode[data-mode="profile"]:visible').click();
  await page.locator('.dc-flow-profile-table:visible tr[data-column="quantity"]').waitFor({ timeout: 60_000 });
  const quantityType = await page.locator('.dc-flow-profile-table:visible tr[data-column="quantity"] .dc-flow-profile-type').textContent();
  check(quantityType === 'String', `quantity read as text after it: ${quantityType}`);
  await page.locator('.dc-flow-mode[data-mode="rows"]:visible').click();

  // TRANSPOSE (M4), dragged onto the Filter: notional and pnl made rows beside each trade's id, the rest left out
  const beforeTranspose = (await boxes()).map((b) => b.id);
  await drag('transpose', filter.id);
  await boxCount(beforeTranspose.length + 1);
  const turned = await added(beforeTranspose);
  await docked('Transpose');
  const tw = page.locator('.dc-flow-dock .dc-transpose');
  for (const r of await tw.locator('.dc-transpose-row[data-column]').all()) {
    const c = await r.getAttribute('data-column');
    await r.locator('.dc-transpose-key').setChecked(c === 'trade_id');
    await r.locator('.dc-transpose-data').setChecked(c === 'notional' || c === 'pnl');
  }
  await tw.locator('.dc-sample-foot .dc-primary').click();
  await settled();
  const turnedBox = (await boxes()).find((b) => b.id === turned.id);
  const turnRefused = await tw.locator('.dc-sample-status').textContent().catch(() => '');
  check(turnedBox?.tool === 'transpose' && turnedBox.says === 'notional, pnl to rows', `a Transpose box: ${JSON.stringify(turnedBox)}${turnRefused ? `, refused: ${turnRefused}` : ''}`);
  check(await resultRows() === fewer * 2, `each of the Filter's ${fewer} rows twice: ${fewer * 2}`);
  const turnedHeads = await page.locator('.dc-flow-results .dc-th[data-column]').evaluateAll((els) => els.map((e) => e.dataset.column));
  check(JSON.stringify(turnedHeads) === JSON.stringify(['trade_id', 'Name', 'Value']), `its key, its Name and its Value alone: ${turnedHeads}`);
  await page.screenshot({ path: outPath('flow-transpose.png') });
  // what opening a box before it left out is said on that box's own status bar, never on the page's line under another
  check(!/no longer in its input/.test(await page.locator('.dc-app-status').first().textContent().catch(() => '') ?? ''),
    'no other box\'s note on the page\'s status line');

  // THE MINIMAP (M7), asked for by its button: a flow taller than the canvas drawn small, the part in view framed; a
  // press on it moves there; asked away again, it covers nothing
  const minimap = page.locator('.dc-flow-minimap');
  check(!(await minimap.isVisible()), 'no minimap unasked');
  await page.locator('.dc-flow-map').click();
  check(await minimap.isVisible(), 'a minimap, asked for, the flow no longer fitting');
  const mapBox = await minimap.boundingBox();
  const scrolledBefore = await page.locator('.dc-flow-canvas').evaluate((c) => c.scrollTop);
  await page.mouse.click(mapBox.x + mapBox.width / 2, mapBox.y + 4);
  const scrolledTop = await page.locator('.dc-flow-canvas').evaluate((c) => c.scrollTop);
  await page.mouse.click(mapBox.x + mapBox.width / 2, mapBox.y + mapBox.height - 4);
  const scrolledBottom = await page.locator('.dc-flow-canvas').evaluate((c) => c.scrollTop);
  check(scrolledBottom > scrolledTop, `a press on it moves the view there: ${scrolledBefore} -> ${scrolledTop} -> ${scrolledBottom}`);
  check(await minimap.locator('.dc-flow-minimap-view').count() === 1 && await minimap.locator('.dc-flow-minimap-shown').count() === 1,
    'the part in view framed, the box shown marked');
  await page.screenshot({ path: outPath('flow-minimap.png') });
  await page.locator('.dc-flow-map').click();
  check(!(await minimap.isVisible()), 'asked away again');

  // MULTI-FIELD FORMULA (M3), dragged onto the Filter: one expression over two text columns, a column of each; then one
  // the compiler refuses (trim of a number), said in the window
  const beforeMulti = (await boxes()).map((b) => b.id);
  await drag('multiField', filter.id);
  await boxCount(beforeMulti.length + 1);
  const multi = await added(beforeMulti);
  await docked('Multi-Field Formula');
  const mf = page.locator('.dc-flow-dock .dc-multi-field');
  await mf.locator('.dc-multi-field-kind').selectOption('numeric');
  await mf.locator('input[value="notional"]').check();
  await mf.locator('.dc-multi-field-expression').fill('$v->toOne()->trim()');
  await mf.locator('.dc-sample-foot .dc-primary').click();
  await page.waitForFunction(() => (document.querySelector('.dc-flow-dock .dc-multi-field .dc-sample-status')?.textContent ?? '') !== '', null, { timeout: 60_000 });
  const multiRefusal = await mf.locator('.dc-sample-status').textContent();
  check(/trim/.test(multiRefusal ?? ''), `a Multi-Field the compiler refuses, said in its window: ${multiRefusal}`);
  await mf.locator('.dc-multi-field-kind').selectOption('text');
  for (const c of ['region', 'desk']) await mf.locator(`input[value="${c}"]`).check();
  await mf.locator('.dc-multi-field-expression').fill('$v->toOne()->length()');
  await mf.locator('.dc-multi-field-suffix').fill('_len');
  await mf.locator('.dc-sample-foot .dc-primary').click();
  await settled();
  const multiBox = (await boxes()).find((b) => b.id === multi.id);
  check(multiBox?.tool === 'multiField' && multiBox.says === 'over region, desk', `a Multi-Field Formula box: ${JSON.stringify(multiBox)}`);
  for (const c of ['region_len', 'desk_len']) {
    check(await page.locator(`.dc-flow-results .dc-th[data-column="${c}"]`).count() > 0, `its column ${c} in its rows`);
  }
  check(await page.locator('.dc-flow-results .dc-th[data-column="notional_new"]').count() === 0, 'the refused one added nothing');
  check(await resultRows() === fewer, "Multi-Field Formula: its input's rows, each kept");

  // UNIQUE (M3), dragged onto the Input: a row of each region, its U output; a Browse off its D, the duplicates
  const beforeUnique = (await boxes()).map((b) => b.id);
  await drag('unique', input.id);
  await boxCount(beforeUnique.length + 1);
  const uniq = await added(beforeUnique);
  await docked('Unique');
  await page.locator('.dc-flow-dock .dc-unique input[value="region"]').check();
  await page.locator('.dc-flow-dock .dc-unique-by').selectOption('notional');
  await page.locator('.dc-flow-dock .dc-unique-direction').selectOption('desc');
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const uniqueBox = (await boxes()).find((b) => b.id === uniq.id);
  check(uniqueBox?.tool === 'unique' && uniqueBox.says === 'by region', `a Unique box: ${JSON.stringify(uniqueBox)}`);
  check(await resultRows() === 3, 'a row of each region: 3');
  check(await page.locator('.dc-flow-results .dc-th[data-column="UniqueRow"]').count() === 0, 'its row number hidden');
  const ports = await page.locator(`.dc-flow-box[data-tile="${uniq.id}"] .dc-flow-port-out:visible`).allTextContents();
  check(JSON.stringify(ports) === '["U","D"]', `its outputs U and D: ${ports}`);
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const beforeDupes = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${uniq.id}"]`).scrollIntoViewIfNeeded();
  const dPort = await page.locator(`.dc-flow-box[data-tile="${uniq.id}"] .dc-flow-port-false`).boundingBox();
  const dupeSpot = await emptyCanvasPoint();
  await page.mouse.move(dPort.x + dPort.width / 2, dPort.y + dPort.height / 2);
  await page.mouse.down();
  await page.mouse.move(dupeSpot.x, dupeSpot.y, { steps: 12 });
  await page.mouse.up();
  await boxCount(beforeDupes.length + 1);
  const dupes = await added(beforeDupes);
  check(await readsOf(dupes.id) === `${uniq.id}:false`, `a Browse of its Duplicates: ${await readsOf(dupes.id)}`);
  await settled();
  check(await resultRows() === all - 3, `its duplicates, every row after a region's first: ${all} - 3`);

  // ZOOM (M7): a level up, a box dragged as far across the window moves less on the canvas; back to its own size
  await page.locator('.dc-flow-zoom', { hasText: '+' }).click();
  check(await page.locator('.dc-flow-zoom-level').textContent() === '110%', 'zoomed in a level: 110%');
  await page.locator(`.dc-flow-box[data-tile="${input.id}"]`).scrollIntoViewIfNeeded();
  const zoomedBox = await page.locator(`.dc-flow-box[data-tile="${input.id}"]`).boundingBox();
  check(Math.abs(zoomedBox.width - 184 * 1.1) < 2, `a box drawn at its zoom: ${zoomedBox.width}px`);
  const beforeZoomMove = await page.evaluate((id) => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes.find((b) => b.tile === id), input.id);
  await page.mouse.move(zoomedBox.x + 30, zoomedBox.y + 20);
  await page.mouse.down();
  await page.mouse.move(zoomedBox.x + 30, zoomedBox.y + 20 + 110, { steps: 8 });
  await page.mouse.up();
  const afterZoomMove = await page.evaluate((id) => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes.find((b) => b.tile === id), input.id);
  check(Math.abs(afterZoomMove.y - beforeZoomMove.y - 100) <= 1, `moved 110px of the window, 100 of the canvas: ${beforeZoomMove.y} to ${afterZoomMove.y}`);
  await page.locator('.dc-flow-zoom-level').click();
  check(await page.locator('.dc-flow-zoom-level').textContent() === '100%', 'back to its own size');

  // A COMMENT (M7): the palette's Comment dropped on the canvas, its words typed in place -- a note where it was let go
  const notes = () => page.locator('.dc-flow-note').count();
  const spot = await emptyCanvasPoint();
  await page.locator('.dc-flow-tool[data-tool="comment"]').scrollIntoViewIfNeeded();
  const commentTool = await page.locator('.dc-flow-tool[data-tool="comment"]').boundingBox();
  await page.mouse.move(commentTool.x + commentTool.width / 2, commentTool.y + commentTool.height / 2);
  await page.mouse.down();
  await page.mouse.move(spot.x, spot.y, { steps: 12 });
  await page.mouse.up();
  await page.locator('.dc-flow-note textarea').waitFor({ timeout: 10_000 });
  check(await notes() === 1, 'a note on the canvas, its editor open');
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Comment', 'one step: Add Comment');
  await page.locator('.dc-flow-note textarea').fill('Big trades only: notional over 900,000');
  await page.locator('.dc-flow-note .dc-text-bar button', { hasText: 'OK' }).click();
  await page.waitForFunction(() => /Big trades only/.test(document.querySelector('.dc-flow-note-body')?.textContent ?? ''), undefined, { timeout: 10_000 });
  const noteId = await page.locator('.dc-flow-note').getAttribute('data-tile');
  const written = await page.evaluate((id) => window.__dataPage.document('x').views.find((v) => v.id === id), noteId);
  check(written?.kind === 'text' && /Big trades only/.test(written.text), `a text tile of the page: ${JSON.stringify(written)}`);
  await page.screenshot({ path: outPath('flow-comment.png') });

  // COPY AND PASTE (M7): the Formula copied, pasted where the pointer is -- a box of its cube, reading what it reads
  const beforePaste = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).focus();
  await page.keyboard.press('Control+c');
  const pasteAt = await emptyCanvasPoint();
  await page.mouse.move(pasteAt.x, pasteAt.y);
  await page.locator('.dc-flow-canvas').focus();
  await page.keyboard.press('Control+v');
  await boxCount(beforePaste.length + 1);
  const pasted = await added(beforePaste);
  check(pasted.tool === 'formula' && pasted.says === 'fee', `a copy of the Formula: ${JSON.stringify(pasted)}`);
  check(await readsOf(pasted.id) === await readsOf(formula.id), 'reading what it reads');
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Paste Box', 'one step: Paste Box');
  const pastedAt = await page.evaluate((id) => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes.find((b) => b.tile === id), pasted.id);
  check(Math.abs(pastedAt.x + 92 - pasteAt.canvasX) <= 2, `placed where the pointer was: its middle at ${pastedAt.x + 92}, the pointer at ${pasteAt.canvasX}`);

  // UNDO AND REDO on the canvas's toolbar, each saying its step
  check(await page.locator('.dc-flow-undo').getAttribute('title') === 'Undo Paste Box (Ctrl+Z)', 'Undo says what it undoes');
  await page.locator('.dc-flow-undo').click();
  await boxCount(beforePaste.length);
  check(await page.locator('.dc-flow-redo').getAttribute('title') === 'Redo Paste Box (Ctrl+Shift+Z)', 'undone by its button; Redo says it');
  await page.locator('.dc-flow-redo').click();
  await boxCount(beforePaste.length + 1);
  check(await page.locator('.dc-flow-redo').isDisabled(), 'redone; nothing left to redo');

  // MANY BOXES AT ONCE (M7): the Formula and the Summarize picked together (a click, then Shift and a click), dragged
  // as one -- one step, Move Boxes; copied and pasted together, the pasted Summarize reading the pasted Formula; the
  // two taken off together, one step; every box picked by Ctrl+A, let go by Escape
  const layoutOf = () => page.evaluate(() => Object.fromEntries(window.__dataPage.document('x').sheets
    .find((s) => s.layout.kind === 'flow').layout.boxes.map((b) => [b.tile, [b.x, b.y]])));
  const picked = () => page.locator('.dc-flow-box.dc-flow-picked').evaluateAll((els) => els.map((e) => e.dataset.tile).sort());
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const sumBox = (await boxes()).find((b) => b.tool === 'summarize');
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).scrollIntoViewIfNeeded();
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"] .dc-flow-name`).click();
  await page.locator(`.dc-flow-box[data-tile="${sumBox.id}"] .dc-flow-name`).click({ modifiers: ['Shift'] });
  check(JSON.stringify(await picked()) === JSON.stringify([formula.id, sumBox.id].sort()), `two boxes picked: ${await picked()}`);
  const placesBefore = await layoutOf();
  const fBox = await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).boundingBox();
  await page.mouse.move(fBox.x + 40, fBox.y + 12);
  await page.mouse.down();
  await page.mouse.move(fBox.x + 40, fBox.y + 12 + 60, { steps: 8 });
  await page.mouse.up();
  const placesAfter = await layoutOf();
  check(placesAfter[formula.id][1] - placesBefore[formula.id][1] === 60 && placesAfter[sumBox.id][1] - placesBefore[sumBox.id][1] === 60,
    `both moved down 60: ${placesBefore[formula.id]} -> ${placesAfter[formula.id]}, ${placesBefore[sumBox.id]} -> ${placesAfter[sumBox.id]}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Move Boxes', 'one step: Move Boxes');
  // copied together, pasted where the pointer is: the pasted Summarize reads the pasted Formula, it the Filter
  const beforeMany = (await boxes()).map((b) => b.id);
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).focus();
  await page.keyboard.press('Control+c');
  const manySpot = await emptyCanvasPoint();
  await page.mouse.move(manySpot.x, manySpot.y);
  await page.locator('.dc-flow-canvas').focus();
  await page.keyboard.press('Control+v');
  await boxCount(beforeMany.length + 2);
  const pastedTwo = (await boxes()).filter((b) => !beforeMany.includes(b.id));
  const pf = pastedTwo.find((b) => b.tool === 'formula');
  const ps = pastedTwo.find((b) => b.tool === 'summarize');
  check(pf !== undefined && ps !== undefined, `a Formula and a Summarize pasted: ${JSON.stringify(pastedTwo)}`);
  check(await readsOf(ps.id) === pf.id && await readsOf(pf.id) === await readsOf(formula.id),
    `the pasted Summarize reads the pasted Formula, it what the Formula reads: ${await readsOf(ps.id)}, ${await readsOf(pf.id)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Paste 2 Boxes', 'one step: Paste 2 Boxes');
  const pastedPlaces = await layoutOf();
  check(pastedPlaces[ps.id][0] - pastedPlaces[pf.id][0] === placesAfter[sumBox.id][0] - placesAfter[formula.id][0],
    'placed as they were placed together');
  // what was pasted, picked together: taken off together, one step
  check(JSON.stringify(await picked()) === JSON.stringify([pf.id, ps.id].sort()), `the pasted boxes picked: ${await picked()}`);
  await page.locator(`.dc-flow-box[data-tile="${ps.id}"]`).focus();
  await page.keyboard.press('Delete');
  await boxCount(beforeMany.length);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Remove 2 Boxes', 'one step: Remove 2 Boxes');
  // Ctrl+A on the canvas picks every box; Escape lets them go
  await page.locator('.dc-flow-canvas').focus();
  await page.keyboard.press('Control+a');
  check((await picked()).length === beforeMany.length, `every box picked: ${(await picked()).length}`);
  await page.keyboard.press('Escape');
  check((await picked()).length === 0, 'Escape lets them go');
  // a band dragged from the canvas's empty corner round the Input picks it, and no box past the band
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const planeAt = await page.locator('.dc-flow-plane').boundingBox();
  const inputAt = await page.locator(`.dc-flow-box[data-tile="${input.id}"]`).boundingBox();
  await page.mouse.move(planeAt.x + 8, planeAt.y + 8);
  await page.mouse.down();
  await page.mouse.move(inputAt.x + 30, inputAt.y + 20, { steps: 10 });
  check(await page.locator('.dc-flow-band').count() === 1, 'its band drawn while it is dragged');
  await page.mouse.up();
  check((await picked()).includes(input.id) && !(await picked()).includes(formula.id), `a band round the Input picks it: ${await picked()}`);
  check(await page.locator('.dc-flow-band').count() === 0, 'its band gone, let go');
  await page.keyboard.press('Escape');
  check((await picked()).length === 0, 'Escape lets it go');

  // A TEXT INPUT (M7's In tools): dropped on the canvas, a table of its own pasted in from a sheet -- an Input over it
  const beforeText = (await boxes()).map((b) => b.id);
  const textSpot = await emptyCanvasPoint();
  await page.locator('.dc-flow-tool[data-tool="textInput"]').scrollIntoViewIfNeeded();
  const textTool = await page.locator('.dc-flow-tool[data-tool="textInput"]').boundingBox();
  await page.mouse.move(textTool.x + textTool.width / 2, textTool.y + textTool.height / 2);
  await page.mouse.down();
  await page.mouse.move(textSpot.x, textSpot.y, { steps: 12 });
  await page.mouse.up();
  const typedIn = page.locator('.dc-text-input');
  await typedIn.waitFor({ timeout: 10_000 });
  await typedIn.locator('.dc-text-input-name input').fill('fx rates');
  await typedIn.locator('.dc-text-input-head').first().evaluate((field) => {
    const data = new DataTransfer();
    data.setData('text/plain', 'ccy\trate\r\nEUR\t1.08\r\nGBP\t1.27\r\nJPY\t0.0067\r\n');
    field.dispatchEvent(new ClipboardEvent('paste', { clipboardData: data, bubbles: true, cancelable: true }));
  });
  const heads = await typedIn.locator('.dc-text-input-head').evaluateAll((els) => els.map((e) => e.value));
  check(JSON.stringify(heads) === '["ccy","rate"]', `the pasted block fills the table from its head row: ${heads}`);
  check(await typedIn.locator('tbody tr').count() === 3, 'a row a line pasted, the empty rows filled');
  await typedIn.locator('.dc-save-foot .dc-primary').click();
  await boxCount(beforeText.length + 1);
  const rates = await added(beforeText);
  check(rates.tool === 'input' && rates.says === 'fx rates.csv' && rates.shown, `an Input over it: ${JSON.stringify(rates)}`);
  check(await resultRows() === 3, 'its three rows');
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Text Input', 'one step: Add Text Input');
  const ratesAt = await page.evaluate((id) => window.__dataPage.document('x').sheets.find((s) => s.layout.kind === 'flow').layout.boxes.find((b) => b.tile === id), rates.id);
  check(Math.abs(ratesAt.x + 92 - textSpot.canvasX) <= 2, `made where it was let go: its middle at ${ratesAt.x + 92}`);

  // DYNAMIC REPLACE (§4, Alteryx's): its rules a Text Input's rows -- a column, a condition in Pure, a value each -- the
  // rules box picked in its window and read (that box shown, then this one again, its window opened on its rows), loaded:
  // region replaced in place by the rule's value where its condition holds
  const beforeRules = (await boxes()).map((b) => b.id);
  const rulesSpot = await emptyCanvasPoint();
  const rulesTool = await page.locator('.dc-flow-tool[data-tool="textInput"]').boundingBox();
  await page.mouse.move(rulesTool.x + rulesTool.width / 2, rulesTool.y + rulesTool.height / 2);
  await page.mouse.down();
  await page.mouse.move(rulesSpot.x, rulesSpot.y, { steps: 12 });
  await page.mouse.up();
  const rulesIn = page.locator('.dc-text-input');
  await rulesIn.waitFor({ timeout: 10_000 });
  await rulesIn.locator('.dc-text-input-name input').fill('region rules');
  await rulesIn.locator('.dc-text-input-head').first().evaluate((field) => {
    const data = new DataTransfer();
    data.setData('text/plain', "Column\tCondition\tValue\r\nregion\t$x.region == 'EMEA'\tEurope\r\n");
    field.dispatchEvent(new ClipboardEvent('paste', { clipboardData: data, bubbles: true, cancelable: true }));
  });
  await rulesIn.locator('.dc-save-foot .dc-primary').click();
  await boxCount(beforeRules.length + 1);
  const rulesBox = await added(beforeRules);
  const beforeReplace = (await boxes()).map((b) => b.id);
  await drag('dynamicReplace', filter.id);
  await boxCount(beforeReplace.length + 1);
  const replaceBox = await added(beforeReplace);
  await docked('Dynamic Replace');
  await page.locator('.dc-flow-dock .dc-replace-from').selectOption(rulesBox.id);
  await page.waitForFunction(() => document.querySelector('.dc-flow-dock .dc-replace-read')?.textContent === '1 rule, replacing region',
    undefined, { timeout: 60_000 });
  check(await page.locator('.dc-flow-dock .dc-replace-condition').inputValue() === 'Condition', "read: its parts picked in its columns' order");
  await page.locator('.dc-flow-dock .dc-sample-foot .dc-primary').click();
  await settled();
  const replacedBox = (await boxes()).find((b) => b.id === replaceBox.id);
  const replaceRefused = await page.locator('.dc-flow-dock .dc-sample-status').last().textContent().catch(() => '');
  check(replacedBox?.tool === 'dynamicReplace' && replacedBox.says === 'region by 1 rule in place',
    `a Dynamic Replace box: ${JSON.stringify(replacedBox)}${replaceRefused ? `, refused: ${replaceRefused}` : ''}`);
  check(await page.locator('.dc-flow-results .dc-th[data-column="region_replaced"]').count() > 0, 'its column in its rows, handed on as region');

  // A BOX'S PROFILE (M5): the Input shown (Enter on it: other boxes cover it by now), its Profile -- each column
  // summarised by the database; its Rows again
  await page.locator(`.dc-flow-box[data-tile="${input.id}"]`).focus();
  await page.keyboard.press('Enter');
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.classList.contains('dc-flow-shown'), input.id);
  await page.locator('.dc-flow-mode[data-mode="profile"]').click();
  await page.locator('.dc-flow-profile-table').waitFor({ timeout: 60_000 });
  const profiled = await page.locator('.dc-flow-profile-table tbody tr').evaluateAll((trs) => Object.fromEntries(trs.map((tr) => [
    tr.dataset.column, [...tr.querySelectorAll('td')].map((td) => td.textContent),
  ])));
  check(/^5,000 rows, \d+ columns$/.test(await page.locator('.dc-flow-profile-sum').textContent() ?? ''), 'its rows and columns counted');
  check(profiled.region?.[2] === '5,000 (100%)' && profiled.region?.[3] === '3', `region: every row filled, 3 different values: ${profiled.region}`);
  check(!(await page.locator('.dc-flow-results-body > :first-child').isVisible()), 'its grid out of sight behind its profile');
  check(profiled.notional?.[1] === 'Float' && profiled.notional?.[4] !== '' && profiled.notional?.[5] !== '', `notional: a Float, its smallest and largest: ${profiled.notional}`);
  // its number columns' correlations beneath (Alteryx's Pearson Correlation): each with itself 1 (none where the database
  // says it does not vary: profile.ts), each pair from -1 to 1
  await page.locator('.dc-flow-corr-table').waitFor({ timeout: 60_000 });
  const corr = await page.locator('.dc-flow-corr-table tbody tr').evaluateAll((trs) => Object.fromEntries(trs.map((tr) => [
    tr.dataset.column, [...tr.querySelectorAll('td')].map((td) => td.dataset.r),
  ])));
  const names = Object.keys(corr);
  check(names.includes('notional') && names.includes('pnl') && corr.notional[names.indexOf('notional')] === '1'
    && names.every((n, i) => corr[n][i] === '1' || corr[n][i] === '')
    && names.every((n) => corr[n].every((r) => r === '' || (Number(r) >= -1 && Number(r) <= 1))), `its correlations: ${JSON.stringify(corr)}`);
  // a column opened: its most frequent values, counted by the database (profile.ts topValuesLambda) -- region's three,
  // the most first, their counts its 5,000 rows; closed again
  const regionOpen = page.locator('.dc-flow-profile-table tr[data-column="region"] .dc-flow-profile-open');
  await regionOpen.click();
  await page.locator('.dc-flow-top-list .dc-flow-top-line').first().waitFor({ timeout: 60_000 });
  const tops = await page.locator('.dc-flow-top-line').evaluateAll((lines) => lines.map((l) => [
    l.querySelector('.dc-flow-top-value')?.textContent, Number((l.querySelector('.dc-flow-top-count')?.textContent ?? '').replace(/\D/g, '')),
  ]));
  check(tops.length === 3 && tops.reduce((sum, [, n]) => sum + n, 0) === 5000 && tops.every(([, n], i) => i === 0 || n <= tops[i - 1][1])
    && await regionOpen.getAttribute('aria-expanded') === 'true', `region's top values: ${JSON.stringify(tops)}`);
  await page.screenshot({ path: outPath('flow-profile-top-values.png') });
  await regionOpen.click();
  check(await page.locator('.dc-flow-top-list').count() === 0, 'closed again');
  await page.screenshot({ path: outPath('flow-profile.png') });
  await page.locator('.dc-flow-corr').scrollIntoViewIfNeeded();
  await page.screenshot({ path: outPath('flow-correlations.png') });
  await page.locator('.dc-flow-mode[data-mode="rows"]').click();
  check(await page.locator('.dc-flow-profile').count() === 0 && await resultRows() === all, 'its rows again');
  check(await page.locator('.dc-flow-results-body > :first-child').isVisible(), 'its grid in sight again');

  // A LINE SAYS WHAT GOES ALONG IT, where the pointer rests on it; the toolbar's ? says the canvas's keys
  const lineSays = await page.locator(`.dc-flow-hit[data-tile="${formula.id}"] title`).first().textContent();
  check(/→ Formula/.test(lineSays) && /\d+ columns: trade_id, /.test(lineSays), `a line's words: ${JSON.stringify(lineSays)}`);
  await page.locator('.dc-flow-keys').click();
  check(await page.locator('.dc-flow-keys-panel').isVisible()
    && (await page.locator('.dc-flow-keys-list dt').allTextContents()).includes('Ctrl+C, X, V'), 'the ? says the keys');
  await page.locator('.dc-flow-keys-close').click();
  check(!(await page.locator('.dc-flow-keys-panel').isVisible()), 'closed by its ×');

  // A CONTAINER (M7, Alteryx's Tool Container): Ctrl+G round the Formula and the Summarize picked; dragged by its head
  // with the boxes in it (one step), folded (its boxes out of sight, their lines to its head), unfolded
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  const sumAgain = (await boxes()).find((b) => b.tool === 'summarize');
  await page.locator(`.dc-flow-box[data-tile="${formula.id}"]`).focus();
  await page.keyboard.press('Enter');
  await page.locator(`.dc-flow-box[data-tile="${sumAgain.id}"]`).scrollIntoViewIfNeeded();
  await page.locator(`.dc-flow-box[data-tile="${sumAgain.id}"] .dc-flow-name`).click({ modifiers: ['Shift'], force: true });
  check((await picked()).includes(formula.id) && (await picked()).includes(sumAgain.id), `the two picked: ${await picked()}`);
  await page.locator('.dc-flow-canvas').focus();
  await page.keyboard.press('Control+g');
  const frameBox = page.locator('.dc-flow-container');
  await frameBox.waitFor({ timeout: 10_000 });
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Container', 'one step: Add Container');
  const frameId = await frameBox.getAttribute('data-tile');
  const held = Number((await frameBox.locator('.dc-flow-frame-count').textContent()).split(' ')[0]);
  check(held >= 2, `the container holds the two: ${held} boxes`);
  const framePlace = await layoutOf();
  const fHead = await frameBox.locator('.dc-flow-note-head').boundingBox();
  await page.mouse.move(fHead.x + fHead.width - 90, fHead.y + fHead.height / 2);
  await page.mouse.down();
  await page.mouse.move(fHead.x + fHead.width - 90, fHead.y + fHead.height / 2 + 40, { steps: 8 });
  await page.mouse.up();
  const frameMoved = await layoutOf();
  check(frameMoved[frameId][1] - framePlace[frameId][1] === 40 && frameMoved[formula.id][1] - framePlace[formula.id][1] === 40
    && frameMoved[sumAgain.id][1] - framePlace[sumAgain.id][1] === 40, 'moved with the boxes in it: 40 down, each');
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Move Container', 'one step: Move Container');
  await frameBox.locator('.dc-flow-frame-fold').click();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.hidden === true, formula.id);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Fold Container', 'folded: its boxes out of sight, one step');
  check(await page.locator(`.dc-flow-hit[data-tile="${sumAgain.id}"]`).count() === 0
    && await page.locator(`.dc-flow-hit[data-tile="${formula.id}"]`).count() === 1
    && await page.locator('.dc-flow-container.dc-flow-folded').count() === 1,
  'a line inside it not drawn; one from outside drawn to its head');
  await page.screenshot({ path: outPath('flow-container.png') });
  await frameBox.locator('.dc-flow-frame-fold').click();
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"]`)?.hidden === false, formula.id);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Unfold Container', 'unfolded: its boxes again');

  // AN OUTPUT (M6), dragged onto the Filter: it writes a CSV named after what it reads; named again; Write now hands over
  // every row it gives (the file read back); Run writes it too, said in its log
  const beforeOut = (await boxes()).map((b) => b.id);
  await page.locator('.dc-flow-canvas').evaluate((c) => { c.scrollLeft = 0; c.scrollTop = 0; });
  await drag('output', filter.id);
  await boxCount(beforeOut.length + 1);
  const outBox = await added(beforeOut);
  check(outBox.tool === 'output' && /^→ .+ out\.csv$/.test(outBox.says), `an Output box: ${JSON.stringify(outBox)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Output', 'one step: Add Output');
  await page.locator(`.dc-flow-box[data-tile="${outBox.id}"]`).focus();
  await page.keyboard.press('Enter');
  const outName = page.locator('.dc-flow-output-name');
  await outName.waitFor({ timeout: 10_000 });
  await outName.fill('big trades.csv');
  await outName.press('Enter');
  await page.waitForFunction((t) => document.querySelector(`.dc-flow-box[data-tile="${t}"] .dc-flow-says`)?.textContent === '→ big trades.csv', outBox.id);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Set Output', 'named again, one step: Set Output');
  const [outFile] = await Promise.all([page.waitForEvent('download', { timeout: 60_000 }), page.locator('.dc-flow-output-write').click()]);
  const csvText = (await import('node:fs')).readFileSync(await outFile.path(), "utf8");
  const csvLines = csvText.replace(/^\uFEFF/, '').trimEnd().split(/\r\n/);
  check(outFile.suggestedFilename() === "big trades.csv", `its file: ${outFile.suggestedFilename()}`);
  check(csvLines[0].startsWith('trade_id,trade_date,') && csvLines.length === fewer + 1, `every row it gives, its head first: ${csvLines.length - 1} rows, ${csvLines[0].slice(0, 40)}`);
  await page.locator('.dc-flow-output-said', { hasText: 'Wrote big trades.csv' }).waitFor({ timeout: 10_000 });
  // Run from the keys: Ctrl+Enter on a box of the flow
  const [ranOut] = await Promise.all([page.waitForEvent('download', { timeout: 180_000 }), (async () => {
    await page.locator(`.dc-flow-box[data-tile="${outBox.id}"]`).focus();
    await page.keyboard.press('Control+Enter');
  })()]);
  check(ranOut.suggestedFilename() === 'big trades.csv', 'Run (Ctrl+Enter) writes it too');
  await page.waitForFunction(() => /Run$/.test(document.querySelector('.dc-flow-run')?.textContent ?? ''), undefined, { timeout: 180_000 });
  check(/wrote big trades\.csv/.test(await page.locator('.dc-flow-log').textContent() ?? ''), 'its log says it wrote it');
  await page.locator('.dc-flow-log-close').click();

  // A MESSAGE AND BLOCK UNTIL DONE (M8), AND THE RUN HISTORY: the palette's Message clicked after the Output -- a box whose
  // words a run's log says, its rows counted; set to an error, a run stopped, nothing written; the Output set to block (its
  // settings' checkbox), a run in two stages, its Output written before the Message stops what is after it; the log's
  // history of runs; each undone
  const runDone = () => page.waitForFunction(() => /Run$/.test(document.querySelector('.dc-flow-run')?.textContent ?? ''), undefined, { timeout: 180_000 });
  const beforeMessage = (await boxes()).map((b) => b.id);
  await show(outBox.id);
  await page.locator('.dc-flow-tool[data-tool="message"]').scrollIntoViewIfNeeded();
  await page.locator('.dc-flow-tool[data-tool="message"]').click();
  await boxCount(beforeMessage.length + 1);
  const messageBox = await added(beforeMessage);
  check(messageBox.tool === 'message' && messageBox.says === '“{rows} rows”', `a Message box: ${JSON.stringify(messageBox)}`);
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Add Message', 'one step: Add Message');
  const [ranMessage] = await Promise.all([page.waitForEvent('download', { timeout: 180_000 }), page.locator('.dc-flow-run').click()]);
  check(ranMessage.suggestedFilename() === 'big trades.csv', 'a Message that informs: the Output written');
  await runDone();
  const saidRows = await page.locator('.dc-flow-log .dc-flow-log-said').allTextContents();
  check(JSON.stringify(saidRows) === JSON.stringify([`${fewer.toLocaleString('en-US')} rows`]), `its words in the log, its rows counted: ${saidRows}`);
  await show(messageBox.id);
  await page.locator('.dc-flow-message-level').selectOption('error');
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Set Message', undefined, { timeout: 20_000 });
  await page.locator('.dc-flow-run').click();
  await runDone();
  const messageStopped = await page.locator('.dc-flow-log-title').textContent() ?? '';
  check(/stopped by Message: nothing written/.test(messageStopped), `an error stops the run: ${messageStopped}`);
  check(/not written: .*said/.test(await page.locator('.dc-flow-log').textContent() ?? ''), 'its Output not written, saying why');
  // the run history: every run this session, an earlier one read again
  const runs = await page.locator('.dc-flow-log-runs option').count();
  check(runs >= 4, `the runs listed: ${runs}`);
  await page.locator('.dc-flow-log-runs').selectOption(String(runs - 2));
  check(!/stopped/.test(await page.locator('.dc-flow-log-title').textContent() ?? ''), 'the run before it, read again: it ran through');
  await page.locator('.dc-flow-log-runs').selectOption(String(runs - 1));
  // the Output blocks: written in its stage before the Message's stops the rest
  await show(outBox.id);
  await page.locator('.dc-flow-block-check input').check();
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Block Until Done', undefined, { timeout: 20_000 });
  check((await boxes()).find((b) => b.id === outBox.id)?.tool === 'output', 'an Output still, that blocks');
  const [ranStaged] = await Promise.all([page.waitForEvent('download', { timeout: 180_000 }), page.locator('.dc-flow-run').click()]);
  check(ranStaged.suggestedFilename() === 'big trades.csv', 'its stage written before the Message is judged');
  await runDone();
  const stagedTitle = await page.locator('.dc-flow-log-title').textContent() ?? '';
  check(/in 2 stages/.test(stagedTitle) && /nothing after it written/.test(stagedTitle), `a run in two stages: ${stagedTitle}`);
  await page.screenshot({ path: outPath('flow-message-block.png') });
  await page.locator('.dc-flow-log-close').click();
  await page.locator('.dc-flow-canvas').click({ position: { x: 5, y: 5 } });
  for (let i = 0; i < 3; i++) await page.keyboard.press('Control+z');
  await boxCount(beforeMessage.length);
  check(await page.evaluate((t) => window.__dataPage.document('x').sheets.flatMap((sh) => sh.layout.kind === 'flow' ? sh.layout.boxes : [])
    .find((b) => b.tile === t)?.blocks === undefined, outBox.id), 'undone: it blocks no more');

  // THE PALETTE'S FIND: its tools narrowed to what is typed
  await page.fill('.dc-flow-find', 'join');
  const visible = await page.locator('.dc-flow-tool:visible').evaluateAll((els) => els.map((e) => e.dataset.tool));
  // its name or what it does: Join, and Fuzzy Match (two inputs joined where their text is alike)
  check(JSON.stringify(visible) === JSON.stringify(['join', 'fuzzyMatch']), `found: ${visible}`);
  await page.fill('.dc-flow-find', '');

  // THE FLOW EXAMPLE, opened from Open…'s examples: its boxes on its canvas, each the tool it is, in its containers; Run
  // counts every box -- the continents, the countries' top orders -- and writes its Output
  await page.click('.dc-titlebar-menu');
  await item('Open…').click();
  await page.locator('#cubeswin .dc-open-example[data-example="prep-flow"]').click();
  // its first sheet's boxes (its Reshape sheet's are on the page too, out of sight)
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-box')].filter((e) => e.offsetParent !== null).length === 8, undefined, { timeout: 120_000 });
  const exampleTools = await page.locator('.dc-flow-box:visible').evaluateAll((els) => els.map((e) => e.dataset.tool).sort());
  check(JSON.stringify(exampleTools) === JSON.stringify(['filter', 'formula', 'input', 'input', 'join', 'output', 'summarize', 'unique']),
    `the example's boxes: ${exampleTools}`);
  check(await page.locator('.dc-flow-container:visible').count() === 3 && await page.locator('.dc-flow-note:not(.dc-flow-container):visible').count() === 1,
    'its three containers and its comment');
  const [exampleFile] = await Promise.all([page.waitForEvent('download', { timeout: 240_000 }), page.locator('.dc-flow-run:visible').click()]);
  check(exampleFile.suggestedFilename() === 'revenue by continent.csv', `Run writes its Output: ${exampleFile.suggestedFilename()}`);
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
  const exampleCounts = await page.locator('.dc-flow-box:visible').evaluateAll((els) => Object.fromEntries(els.map((e) => [e.dataset.tile, e.querySelector('.dc-flow-count')?.textContent ?? ''])));
  check(Object.values(exampleCounts).every((c) => /^[\d,]+$/.test(c)), `every box counted: ${JSON.stringify(exampleCounts)}`);
  const continents = Number(exampleCounts['grid-6'].replaceAll(',', ''));
  check(continents >= 5 && continents <= 6, `a row a continent (and the orders with no country): ${continents}`);
  await page.locator('.dc-flow-log-close:visible').click();
  check(!/no longer in its input/.test(await page.locator('.dc-app-status').first().textContent().catch(() => '') ?? ''),
    'nothing of the page before it said on its status line, nor anything gone');
  await page.screenshot({ path: outPath('flow-example.png') });

  // ITS RESHAPE SHEET: the top two orders of each region, chosen and renamed, their money made rows and summed by measure
  await page.locator('.dc-sheet-tab', { hasText: 'Reshape' }).click();
  await page.waitForFunction(() => document.querySelector('.dc-flow-box[data-tile="grid-12"]')?.offsetParent != null, undefined, { timeout: 60_000 });
  await page.locator('.dc-flow-run:visible').click();
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
  const reshapeCounts = await page.locator('.dc-flow-box:visible').evaluateAll((els) => Object.fromEntries(els.map((e) => [e.dataset.tile, Number((e.querySelector('.dc-flow-count')?.textContent ?? '').replaceAll(',', ''))])));
  const topOrders = reshapeCounts['grid-9'];
  check(topOrders > 0 && topOrders % 2 === 0 && reshapeCounts['grid-10'] === topOrders && reshapeCounts['grid-11'] === topOrders * 3 && reshapeCounts['grid-12'] === 3,
    `Reshape: two of each region, chosen, three rows each, summed by measure: ${JSON.stringify(reshapeCounts)}`);
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-11"]').click();
  // the box clicked shown (Run leaves its last box's), its rows in
  await page.waitForFunction(() => {
    const shown = [...document.querySelectorAll('.dc-flow-results')].find((r) => r.offsetParent != null);
    return shown?.querySelector('.dc-flow-results-title')?.textContent === 'Money to rows' && /of [\d,]+ rows|[\d,]+ rows ×/.test(shown.textContent ?? '');
  }, undefined, { timeout: 60_000 });
  const turnedOut = await page.locator('.dc-flow-results:visible .dc-th[data-column]').evaluateAll((els) => els.map((e) => e.dataset.column));
  check(JSON.stringify(turnedOut) === JSON.stringify(['order_id', 'region', 'measure', 'amount']), `its Transpose reads the Select's sales: ${turnedOut}`);
  await page.screenshot({ path: outPath('flow-example-reshape.png') });

  // ITS MATCH SHEET: what the join leaves on each side (its L and R), how many of the orders it leaves (Count Records),
  // and every order's country as the countries name it (Find Replace) -- run
  await page.locator('.dc-sheet-tab', { hasText: 'Match' }).click();
  await page.waitForFunction(() => document.querySelector('.dc-flow-box[data-tile="grid-16"]')?.offsetParent != null, undefined, { timeout: 60_000 });
  await page.locator('.dc-flow-run:visible').click();
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
  const matchCounts = await page.locator('.dc-flow-box:visible').evaluateAll((els) => Object.fromEntries(els.map((e) => [e.dataset.tile, e.querySelector('.dc-flow-count')?.textContent ?? ''])));
  const unmatchedOrders = matchCounts['grid-13'];
  check(/^[\d,]+$/.test(unmatchedOrders) && unmatchedOrders !== '0' && matchCounts['grid-14'] === '1' && /^[\d,]+$/.test(matchCounts['grid-15']) && matchCounts['grid-16'] === '6,000',
    `Match: the orders with no country, counted, the countries with none, every order named: ${JSON.stringify(matchCounts)}`);
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-14"]').click();
  await page.waitForFunction(() => {
    const shown = [...document.querySelectorAll('.dc-flow-results')].find((r) => r.offsetParent != null);
    return shown?.querySelector('.dc-flow-results-title')?.textContent === 'How many' && /of [\d,]+ rows|[\d,]+ rows ×/.test(shown.textContent ?? '');
  }, undefined, { timeout: 60_000 });
  check((await page.locator('.dc-flow-results:visible').textContent() ?? '').includes(unmatchedOrders), `its count, the orders the join left: ${unmatchedOrders}`);
  await page.screenshot({ path: outPath('flow-example-match.png') });

  // ITS CHECKS SHEET: the orders of each region unioned again (a Union of three), those of 2024 (a Date Filter), each
  // order checked (a Test) -- run: every order back, 2024's, one row of counts, one check failing and said so
  await page.locator('.dc-sheet-tab', { hasText: 'Checks' }).click();
  await page.waitForFunction(() => document.querySelector('.dc-flow-box[data-tile="grid-22"]')?.offsetParent != null, undefined, { timeout: 60_000 });
  await page.locator('.dc-flow-run:visible').click();
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
  const checkCounts = await page.locator('.dc-flow-box:visible').evaluateAll((els) => Object.fromEntries(els.map((e) => [e.dataset.tile, e.querySelector('.dc-flow-count')?.textContent ?? ''])));
  const in2024 = Number((checkCounts['grid-21'] ?? '').replace(/,/g, ''));
  check(checkCounts['grid-20'] === '6,000' && in2024 > 2_000 && in2024 < 4_000 && checkCounts['grid-22'] === '1',
    `Checks: every order back, 2024's, one row of counts: ${JSON.stringify(checkCounts)}`);
  check(await page.locator('.dc-flow-box[data-tile="grid-22"].dc-flow-failing').count() === 1
    && await page.locator('.dc-flow-box[data-tile="grid-22"] .dc-flow-says').textContent() === '1 of 3 checks fail', 'its discount check fails, said on its box');
  check(/1 test failing/.test(await page.locator('.dc-flow-log:visible .dc-flow-log-title').textContent() ?? ''), 'and in the run\'s log');
  // its Cross Tab, its columns fixed to two of the three regions (FL2): the third said, on its box and in the log (FL3)
  check(await page.locator('.dc-flow-box[data-tile="grid-23"].dc-flow-failing').count() === 1
    && await page.locator('.dc-flow-box[data-tile="grid-23"] .dc-flow-says').textContent() === '1 value has no column', 'its Cross Tab: a region with no column, said on its box');
  check(/1 Cross Tab missing values/.test(await page.locator('.dc-flow-log:visible .dc-flow-log-title').textContent() ?? ''), 'and in the run\'s log, apart from the Test');
  const logRow = async (name) => page.locator('.dc-flow-log:visible .dc-flow-log-item', { hasText: name }).locator('.dc-flow-log-rows').textContent();
  check(/wrote orders of 2024\.csv/.test(await logRow('Orders of 2024') ?? ''), 'a Test that warns: 2024\'s orders written');
  await page.screenshot({ path: outPath('flow-example-checks.png') });
  // A TEST THAT STOPS A RUN (FL8's refuse): set so from its settings, run again -- red, and no Output written
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-22"]').click();
  await page.locator('.dc-flow-settings:visible .dc-flow-stops .dc-flow-stops-check input').check();
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Stop Run On Failure', 'one step: Stop Run On Failure');
  await page.locator('.dc-flow-run:visible').click();
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
  const stoppedTitle = await page.locator('.dc-flow-log:visible .dc-flow-log-title').textContent() ?? '';
  check(/stopped by Orders checked: nothing written/.test(stoppedTitle), `its log says what stopped it: ${stoppedTitle}`);
  check(/not written: Orders checked failed/.test(await logRow('Orders of 2024') ?? ''), '2024\'s orders not written, said why');
  check(await page.locator('.dc-flow-box[data-tile="grid-22"].dc-flow-failing.dc-flow-stops').count() === 1, 'the Test red: it stops a run, failing');
  await page.screenshot({ path: outPath('flow-example-stopped.png') });
  // ITS CROSS TAB'S MISSING VALUE (FL3): named in its settings, and Load values again gives it a column -- passing
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-23"]').click();
  await settled();
  const lackingSaid = await page.locator('.dc-flow-settings:visible .dc-flow-pin-lacking').textContent();
  check(lackingSaid === 'region APAC has no column of its own: add it to its columns, or Load values again', `its settings name it: ${lackingSaid}`);
  check(/^Its columns: AMER, EMEA\.$/.test(await page.locator('.dc-flow-settings:visible .dc-flow-pin-words').textContent() ?? ''), 'its columns: AMER, EMEA');
  // the fix offered (FL8's "Add to the cast"): the value it lacks given a column after its own, one step
  await page.locator('.dc-flow-settings:visible .dc-flow-pin-add').click();
  await page.waitForFunction(() => document.querySelector('.dc-flow-box[data-tile="grid-23"] .dc-flow-says')?.textContent === 'every value has a column', undefined, { timeout: 60_000 });
  check(await page.locator('.dc-flow-box[data-tile="grid-23"].dc-flow-passing').count() === 1, 'added: every region a column, passing');
  check(/^Its columns: AMER, EMEA, APAC\.$/.test(await page.locator('.dc-flow-settings:visible .dc-flow-pin-words').textContent() ?? ''), 'its columns: its two, then APAC after them');
  check(await page.locator('.dc-flow-settings:visible .dc-flow-pin-add').count() === 0, 'nothing more to add: the fix gone');
  // A DRY RUN (M6): every box counted and judged, nothing written -- its stopping Test failing, 2024's orders would not
  // be; set to warn, they would be, and still nothing is written
  const dryRun = async () => {
    await page.locator('.dc-flow-dry:visible').click();
    await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-run')].every((b) => /Run$/.test(b.textContent ?? '')), undefined, { timeout: 240_000 });
    return await page.locator('.dc-flow-log:visible .dc-flow-log-title').textContent() ?? '';
  };
  const dryStopped = await dryRun();
  check(/^Dry run: ran \d+ boxes/.test(dryStopped) && /stopped by Orders checked: nothing would be written/.test(dryStopped), `a dry run, stopped: ${dryStopped}`);
  check(/would not be written: Orders checked failed/.test(await logRow('Orders of 2024') ?? ''), 'its Output would not be written, said why');
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-22"]').click();
  // the Test's own mode, once it is the box shown (a Cross Tab's says a value, not a check)
  await page.waitForFunction(() => [...document.querySelectorAll('.dc-flow-settings .dc-flow-stops')]
    .some((e) => e.offsetParent !== null && /when a check fails/.test(e.textContent ?? '')), undefined, { timeout: 30_000 });
  await page.locator('.dc-flow-settings:visible .dc-flow-stops .dc-flow-stops-check input').uncheck();
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Warn On Failure', undefined, { timeout: 30_000 });
  const dryWarned = await dryRun();
  check(/; nothing written$/.test(dryWarned), `a dry run writes nothing: ${dryWarned}`);
  check(/would write orders of 2024\.csv/.test(await logRow('Orders of 2024') ?? ''), 'its Output would be written, and is not');
  // A CACHE (FL10): the Union set to cache its rows, one step -- its rows are this tab's own already (files read into
  // it: a held copy, whose plane cannot move), so nothing is copied, and its settings and its box say so; ticked off,
  // no longer cached. (A copy made and read by the box after it: Python's host, test_page.py's cached box.)
  await page.locator('.dc-flow-log-close:visible').click();
  await page.locator('.dc-flow-box[data-tile="grid-20"]').click();
  await settled();
  await page.locator('.dc-flow-settings:visible .dc-flow-cache-check input').check();
  check(await page.evaluate(() => window.__dataPage.undoLabel) === 'Cache Rows', 'one step: Cache Rows');
  await page.waitForFunction(() => /^Nothing to copy: its rows are in this page's database already/.test(
    [...document.querySelectorAll('.dc-flow-settings .dc-flow-cache-said')].find((e) => e.offsetParent !== null)?.textContent ?? ''), undefined, { timeout: 60_000 });
  check(await page.locator('.dc-flow-box[data-tile="grid-20"].dc-flow-cached[data-cached="here"]').count() === 1, 'its box marked cached, its rows here');
  const cacheSaved = await page.evaluate(() => window.__dataPage.document('x')?.sheets.flatMap((s) => s.layout.boxes ?? []).find((b) => b.tile === 'grid-20')?.cache);
  check(cacheSaved === true, `the page writes it so: ${cacheSaved}`);
  await page.locator('.dc-flow-settings:visible .dc-flow-cache-check input').uncheck();
  await page.waitForFunction(() => window.__dataPage.undoLabel === 'Stop Caching', undefined, { timeout: 30_000 });
  check(await page.locator('.dc-flow-box[data-tile="grid-20"].dc-flow-cached').count() === 0, 'no longer cached');
  await page.screenshot({ path: outPath('flow-example-cache.png') });
  // AN OUTPUT INTO A TABLE (M6, FL13): the Date Filter's Output made a table of the tab's database -- its name carried
  // over -- written now in one transaction; written again as a new table, refused in the database's words; appended
  await page.locator('.dc-flow-box[data-tile="grid-21"]').click();
  await settled();
  await page.locator('.dc-flow-settings:visible .dc-flow-output-kind').selectOption('table');
  await page.waitForFunction(() => window.__dataPage.document('x')?.sheets.flatMap((s) => s.layout.boxes ?? [])
    .find((b) => b.tile === 'grid-21')?.output?.format === 'table', undefined, { timeout: 30_000 });
  const tableOut = await page.evaluate(() => window.__dataPage.document('x').sheets.flatMap((s) => s.layout.boxes ?? []).find((b) => b.tile === 'grid-21').output);
  check(JSON.stringify(tableOut) === JSON.stringify({ format: 'table', name: 'orders_of_2024', mode: 'create' }), `a table, its name carried over: ${JSON.stringify(tableOut)}`);
  const writeSaid = async () => {
    await page.locator('.dc-flow-settings:visible .dc-flow-output-write').click();
    await page.waitForFunction(() => {
      const said = [...document.querySelectorAll('.dc-flow-output-said')].find((e) => e.offsetParent !== null)?.textContent ?? '';
      return said !== '' && said !== 'Writing…';
    }, undefined, { timeout: 120_000 });
    return page.locator('.dc-flow-settings:visible .dc-flow-output-said').textContent();
  };
  check(await writeSaid() === 'Wrote table orders_of_2024', 'written into its table now');
  check(/already exists/i.test(await writeSaid() ?? ''), 'a new table again: refused, the database\'s words');
  await page.locator('.dc-flow-settings:visible .dc-flow-output-mode').selectOption('append');
  await page.waitForFunction(() => window.__dataPage.document('x')?.sheets.flatMap((s) => s.layout.boxes ?? [])
    .find((b) => b.tile === 'grid-21')?.output?.mode === 'append', undefined, { timeout: 30_000 });
  check(await writeSaid() === 'Wrote table orders_of_2024', 'appended to it');
  await page.screenshot({ path: outPath('flow-example-table-output.png') });
} catch (e) {
  bad(`threw: ${e.stack ?? e}`);
  // the page as it was when it threw, beside the test's other outputs
  await page.screenshot({ path: outPath('flow-threw.png') }).catch(() => {});
} finally {
  check(errors.length === 0, `no page errors${errors.length ? `: ${errors.join(' | ')}` : ''}`);
  check(alerts.length === 0, `no alerts${alerts.length ? `: ${alerts.join(' | ')}` : ''}`);
  await browser.close();
  await close();
}
process.exit(failed ? 1 : 0);
