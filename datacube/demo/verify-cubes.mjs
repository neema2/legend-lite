// SAVED CUBES, driven the way a person drives them (docs/DATACUBE_SAVE_SHARE_2026_09_28.md,
// milestone 1 step 1), in a real browser with its real IndexedDB:
//
//   1. open a file, shape a cube, save it from the Cubes window;
//   2. RELOAD the page (nothing survives but the browser's database), open the cube: the page
//      asks for the file (no handle: Playwright hands files to an <input>, which keeps none),
//      and the cube comes back with the SAME typed values;
//   3. a cube over a SAMPLE reopens with no question at all (rebuilt from its seed);
//   4. delete, and the list says so.
//
//   bazel run //datacube:verify_cubes

// first: points Playwright at the Chromium Bazel fetched (as a browser_test; a no-op under bazel run)
import '../../tools/browser/pinned-chromium.mjs';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, join } from 'node:path';
import { chromium } from 'playwright';

import { readView, sameTyped, stamp } from './typed-view.mjs';
import { frames, serve, siteRoot, tmpDir } from './harness.mjs';

const ROOT = siteRoot();
const { port, close: closeServer } = await serve(ROOT);
const URL_BASE = `http://127.0.0.1:${port}`;

// the CSVs it picks, in the test's own temp directory (harness.tmpDir)
const dir = await tmpDir('dc-cubes-');
const csv = join(dir, 'trades.csv');
const lossy = join(dir, 'trades-no-notional.csv');
// a LARGE file picked first and a small one straight after (P2-330): the small one must win
const big = join(dir, 'big-trades.csv');
await writeFile(big, 'region,desk,notional,qty\n'
  + Array.from({ length: 400_000 }, (_, i) => `R${i % 97},D${i % 13},${i},${i % 7}`).join('\n') + '\n');
const small = join(dir, 'small-trades.csv');
await writeFile(small, 'region,desk,notional,qty\nEMEA,Rates,1,1\nAMER,FX,2,2\n');
await writeFile(lossy, 'region,desk,qty\n'
  + Array.from({ length: 60 }, (_, i) => `${['EMEA', 'AMER', 'APAC'][i % 3]},${['Rates', 'Credit', 'FX'][i % 5 % 3]},${i}`).join('\n') + '\n');
await writeFile(csv, 'region,desk,notional,qty\n'
  + Array.from({ length: 60 }, (_, i) =>
    `${['EMEA', 'AMER', 'APAC'][i % 3]},${['Rates', 'Credit', 'FX'][i % 5 % 3]},${(i * 12.5).toFixed(2)},${i}`).join('\n') + '\n');

const browser = await chromium.launch();
// the clipboard: Copy Share Link writes the link there, and the check reads it back
const context = await browser.newContext({ viewport: { width: 1400, height: 900 },
  permissions: ['clipboard-read', 'clipboard-write'] });
const page = await context.newPage();
const pageErrors = [];
page.on('pageerror', (e) => pageErrors.push(e.message));

const results = [];
async function check(name, fn) {
  try {
    const detail = await fn();
    results.push({ name, ok: true });
    console.log(`  ok   ${name}${detail ? ` — ${detail}` : ''}`);
  } catch (e) {
    results.push({ name, ok: false });
    console.log(`  BAD  ${name} — ${String(e.message ?? e).split('\n').slice(0, 4).join(' | ')}`);
  }
}

/** The status line's text, to wait for it to change. */
const statusNow = () => page.evaluate(() => document.querySelector('.dc-status-timing')?.textContent ?? '');
async function landed(before) {
  await page.waitForFunction((was) => {
    const line = document.querySelector('.dc-status-timing')?.textContent ?? '';
    return line !== was && /rows/.test(line) && document.querySelectorAll('.dc-row').length > 0;
  }, before, { timeout: 60_000 });
  await frames(page);
}
async function load() {
  await page.goto(`${URL_BASE}/demo/index.html`);
  await page.waitForSelector('.dc-row', { timeout: 90_000 });
}
/**
 * Pick a hamburger entry by its own label, through its submenus: an entry under View or Insert is
 * shown only once its parent is hovered (and a parent's text holds its children's labels too).
 */
async function pickEntry(label) {
  const item = page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(label)}))`).first();
  if (!(await item.isVisible())) {
    const parents = await item.evaluate((el) => {
      const out = [];
      for (let p = el.parentElement?.closest('.dc-menu-item'); p; p = p.parentElement?.closest('.dc-menu-item')) {
        out.unshift(p.querySelector(':scope > .dc-menu-label')?.textContent ?? '');
      }
      return out;
    });
    for (const parent of parents) {
      await page.locator(`.dc-menu-item:has(> .dc-menu-label:text-is(${JSON.stringify(parent)}))`).first().hover();
    }
  }
  await item.click();
}
async function burger(label) {
  await page.click('.dc-titlebar-menu');
  await pickEntry(label);
}
/** A host window shown (its menu entry toggles it), the other ones closed. */
async function showWin(id, label) {
  for (const other of ['cubeswin', 'querywin', 'sharewin']) {
    if (other !== id && await page.locator(`#${other}`).isVisible()) {
      // the host windows' close, or Open…'s (a window of its own, as the picker is)
      await page.click(`#${other} .hostwin-close, #${other} .dc-picker-close`);
    }
  }
  if (!(await page.locator(`#${id}`).isVisible())) await burger(label);
  await page.locator(`#${id}`).waitFor({ state: 'visible' });
}
/**
 * The source picker (src/ui/source-picker.ts), to open IN PLACE of the page, as a person does:
 * New ▸ Blank Page, then "Add a data source".
 */
async function openPicker() {
  for (const other of ['cubeswin', 'querywin', 'sharewin']) {
    if (await page.locator(`#${other}`).isVisible()) await page.click(`#${other} .hostwin-close, #${other} .dc-picker-close`);
  }
  await burger('Blank Page');
  await page.locator('.dc-blank').waitFor({ state: 'visible' });
  await page.click('.dc-blank .dc-primary');
  await page.locator('.dc-picker').waitFor({ state: 'visible' });
}
/** A file, through the picker's Files section. */
async function pickFile(file) {
  await openPicker();
  await page.locator('.dc-picker-tab[data-section="files"]').click();
  await page.setInputFiles('.dc-picker-file', file);
}
/** A sample at `rows` rows, through the picker's Examples section. */
async function pickSample(id, rows) {
  await openPicker();
  await page.locator('.dc-picker-tab[data-section="examples"]').click();
  await page.locator(`.dc-picker-card[data-example="${id}"]`).click();
  await page.fill('.dc-picker-rows', String(rows));
  await page.click('.dc-picker-choice .dc-primary');
}
const libMessage = () => page.locator('#cubelib .dc-lib-message').textContent();
async function waitMessage(re) {
  await page.waitForFunction((src) => new RegExp(src).test(
    document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''), re.source, { timeout: 60_000 });
  return libMessage();
}
/** Shape the cube: grouped by region, notional summed. Setup only -- what is tested is the save. */
async function shape() {
  const before = await statusNow();
  await page.evaluate(async () => {
    const app = window.__dataCube;
    await app.change((s) => ({ ...s, snapshot: { ...s.snapshot, rows: ['region'], measures: [{ name: 'notional', column: 'notional', fn: 'sum' }] } }));
  });
  await landed(before);
}
/** The menu's Save As: its own window (src/ui/save-dialog.ts), the name, Save -- closed once it is saved. */
async function saveAs(name) {
  await burger('Save As\u2026');
  await page.locator('.dc-save').waitFor({ timeout: 10_000 });
  await page.fill('.dc-save-input', name);
  await page.locator('.dc-save-foot .dc-primary').click();
  await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
}
async function openSaved(name) {
  await showWin('cubeswin', 'Open\u2026');
  const row = page.locator('#cubelib .dc-lib-row', { hasText: name });
  await row.waitFor({ timeout: 10_000 });
  const before = await statusNow();
  await row.locator('.dc-lib-button', { hasText: 'Open' }).click();
  return before;
}
const typed = () => readView(page);

try {
  await check('a cube over a file is saved, and the list shows it', async () => {
    await load();
    const before = await statusNow();
    await pickFile(csv);
    await landed(before);
    await shape();
    await saveAs('Trades by region');
    // the saved cubes are listed where they are opened from: Open…
    await showWin('cubeswin', 'Open\u2026');
    await page.locator('#cubelib .dc-lib-row').first().waitFor({ timeout: 10_000 });
    const rows = await page.locator('#cubelib .dc-lib-row').allTextContents();
    if (!rows.some((r) => r.includes('Trades by region'))) throw new Error(`list: ${rows.join(' | ')}`);
    return rows.length + ' saved';
  });

  const savedView = await typed();

  await check('after a reload, opening it asks for the file, then shows the same typed values', async () => {
    await load();
    const before = await openSaved('Trades by region');
    await page.waitForSelector('#cubelib .dc-lib-ask:not([hidden])', { timeout: 10_000 });
    const ask = await page.locator('#cubelib .dc-lib-ask').textContent();
    if (!/trades\.csv/.test(ask ?? '')) throw new Error(`the ask does not name the file: ${ask}`);
    await page.setInputFiles('#cubelib .dc-lib-choose', csv);
    await landed(before);
    const message = await waitMessage(/opened/);
    const now = await typed();
    const differs = savedView.map((c, i) => {
      const n = now[i];
      if (!n || n.name !== c.name || n.type !== c.type) return `${c.name}:${c.type} vs ${n?.name}:${n?.type}`;
      const bad = c.values.findIndex((v, r) => !sameTyped(v, n.values[r] ?? null, c.type));
      return bad < 0 && c.values.length === n.values.length ? null : `${c.name} row ${bad}`;
    }).filter(Boolean);
    if (differs.length) throw new Error(`differs: ${differs.join('; ')}`);
    return `${message.trim()} — ${now.length} columns, ${now[0]?.values.length} rows, identical`;
  });

  await check('changed since saved: the title marks it, and saving clears the mark', async () => {
    const title = () => page.title();
    if ((await title()).startsWith('\u2022')) throw new Error(`marked before any change: ${await title()}`);
    const out = await page.evaluate(async () => {
      const app = window.__dataCube;
      const was = app.snapshot.rows.join(',');
      const o = await app.change((s) => ({ ...s, snapshot: { ...s.snapshot, rows: ['desk'] } }));
      return `${o.kind} (rows ${was} -> ${app.snapshot.rows.join(',')})`;
    });
    // `change` resolves once the view has LANDED: no need to watch the status line (3 regions
    // regrouped as 3 desks can read exactly as before, and the wait never ended)
    if (!/^applied/.test(out)) throw new Error(`the change did not land: ${out}`);
    await page.waitForFunction(() => document.title.startsWith('\u2022'), undefined, { timeout: 10_000 })
      .catch(async () => { throw new Error(`not marked after a change (${out}): ${await title()}`); });
    // Save, over the copy it was opened from: the window says so, and closes once it is saved
    await burger('Save');
    await page.locator('.dc-save').waitFor({ timeout: 10_000 });
    const where = (await page.locator('.dc-save-where').textContent()) ?? '';
    if (!/Saves over/.test(where)) throw new Error(`the Save window says "${where}"`);
    await page.locator('.dc-save-foot .dc-primary').click();
    await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 30_000 });
    if ((await title()).startsWith('\u2022')) throw new Error(`still marked after saving: ${await title()}`);
    // A PRESENTATION change runs no query (Leg B): it is a change all the same
    const pinned = await page.evaluate(async () => (await window.__dataCube.change((s) => ({ ...s,
      configuration: { ...s.configuration, columns: { ...s.configuration.columns, desk: { pinned: 'left' } } } }))).kind);
    if (pinned !== 'applied') throw new Error(`the pin did not apply: ${pinned}`);
    await page.waitForFunction(() => document.title.startsWith('\u2022'), undefined, { timeout: 10_000 })
      .catch(async () => { throw new Error(`a pin (no query) did not mark it: ${await title()}`); });
    return 'marked, saved and clear, marked again by a pin that ran no query';
  });

  await check('opened over a file that lost a column, Save says what it would drop', async () => {
    await load();
    const before = await openSaved('Trades by region');
    await page.waitForSelector('#cubelib .dc-lib-ask:not([hidden])', { timeout: 10_000 });
    await page.setInputFiles('#cubelib .dc-lib-choose', lossy);
    await landed(before);
    const message = await waitMessage(/changes since it was saved/);
    if (!/notional/.test(message)) throw new Error(`the changes do not name notional: ${message}`);
    if (!(await page.title()).startsWith('\u2022')) throw new Error('not marked as changed');
    await page.click('#cubeswin .dc-picker-close');
    // Save, over the copy it was opened from: the window says what that drops BEFORE writing
    await burger('Save');
    await page.locator('.dc-save').waitFor({ timeout: 10_000 });
    await page.locator('.dc-save-foot .dc-primary').click();
    await page.locator('.dc-save-warning:not([hidden])').waitFor({ timeout: 5_000 });
    const warning = await page.locator('.dc-save-warning').textContent();
    if (!/cannot show/.test(warning ?? '')) throw new Error(`no warning: ${warning}`);
    if ((await page.locator('.dc-save-foot .dc-primary').textContent()) !== 'Save over it anyway') {
      throw new Error('the window does not ask again before saving over');
    }
    await page.locator('.dc-save-foot button', { hasText: 'Cancel' }).click();
    await page.locator('.dc-save').waitFor({ state: 'detached', timeout: 5_000 });
    return 'warned before saving over it';
  });

  await check('a cube over a SAMPLE reopens with no question', async () => {
    let before = await statusNow();
    // the cube on screen has unsaved changes (the check above cancelled its save): opening a
    // sample over it asks first -- answered yes here, and the question checked
    let asked = '';
    page.once('dialog', (d) => { asked = d.message(); void d.accept(); });
    await pickSample('trades', 500);
    await landed(before);
    if (!/unsaved changes/.test(asked)) throw new Error(`it did not ask before replacing: "${asked}"`);
    await shape();
    await saveAs('Sample trades');
    const want = await typed();
    await load();
    before = await openSaved('Sample trades');
    await landed(before);
    const message = await waitMessage(/opened/);
    if (!(await page.locator('#cubelib .dc-lib-ask').isHidden())) throw new Error('it asked for a file');
    const got = await typed();
    if (stamp(got) !== stamp(want)) throw new Error('the rebuilt sample shows different values');
    return message.trim();
  });

  await check('delete removes it from the list', async () => {
    await showWin('cubeswin', 'Open\u2026');
    const row = page.locator('#cubelib .dc-lib-row', { hasText: 'Sample trades' });
    await row.locator('.dc-lib-button', { hasText: 'Delete' }).click();
    await page.locator('#cubelib .dc-lib-confirm .dc-lib-button', { hasText: 'Delete' }).click();
    await waitMessage(/deleted/);
    const rows = await page.locator('#cubelib .dc-lib-row').allTextContents();
    if (rows.some((r) => r.includes('Sample trades'))) throw new Error('still listed');
    return `${rows.length} left`;
  });

  // THE SHARE LINK (milestone 1b): the page's settings in the address, never its data.
  /** Copy the page's share link through the menu, and read it off the clipboard. */
  async function copyLink() {
    // Share... shows the link in a window of its own and copies it; the window says what it holds
    await burger('Share\u2026');
    await page.waitForFunction(() => /^Copied/.test(document.querySelector('#sharenote')?.textContent ?? ''), null, { timeout: 10_000 });
    const said = await page.locator('#sharenote').textContent();
    const url = await page.evaluate(() => navigator.clipboard.readText());
    if (url !== await page.locator('#sharelink').inputValue()) throw new Error('the window and the clipboard disagree');
    await page.click('#sharewin .hostwin-close');
    if (!/#p1\./.test(url)) throw new Error(`not a share link: ${url.slice(0, 80)}`);
    return { url, said };
  }
  /** Wait until the library in `tab` says the page opened: its answer has landed. */
  async function openedIn(tab) {
    try {
      await tab.waitForFunction(() => /opened/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''),
        undefined, { timeout: 90_000 });
    } catch {
      const said = await tab.evaluate(() => ({ message: document.querySelector('#cubelib .dc-lib-message')?.textContent ?? '(none)',
        status: document.querySelector('.dc-status-timing')?.textContent ?? '', rows: window.__dataCube?.snapshot.rows,
        url: location.href }));
      throw new Error(`it never said opened: ${JSON.stringify(said)}`);
    }
  }
  /** Where two typed views part, said briefly. */
  const differs = (a, b) => {
    const names = (v) => v.map((c) => `${c.name}:${c.type}[${c.values.length}]`).join(',');
    if (names(a) !== names(b)) return `columns ${names(a)} vs ${names(b)}`;
    const i = a.findIndex((c, k) => stamp(c) !== stamp(b[k]));
    return `column ${a[i].name}: ${stamp(a[i].values.slice(0, 5))} vs ${stamp(b[i].values.slice(0, 5))}`;
  };
  /** A fresh tab at `url`, its errors kept. */
  async function openTab(url) {
    const tab = await context.newPage();
    tab.on('pageerror', (e) => pageErrors.push(`(shared tab) ${e.message}`));
    await tab.goto(url);
    return tab;
  }

  await check('a SAMPLE page\'s link opens the same page in a fresh tab, with no question', async () => {
    await load();
    let before = await statusNow();
    page.once('dialog', (d) => { void d.accept(); });
    await pickSample('trades', 500);
    await landed(before);
    await shape();
    const want = await typed();
    const { url, said } = await copyLink();
    if (!/not its data/.test(said)) throw new Error(`it does not say the link holds no data: ${said}`);
    const tab = await openTab(url);
    try {
      await openedIn(tab);
      const got = await readView(tab);
      if (stamp(got) !== stamp(want)) throw new Error(`the shared page shows different values: ${differs(want, got)}`);
      if (/#p1\./.test(tab.url())) throw new Error('the link stayed in the address: a reload would reopen it over later changes');
      return `${url.length} characters; the same typed values in the fresh tab`;
    } finally {
      await tab.close();
    }
  });

  await check('a FILE page\'s link asks the opener for the file, then shows the same values', async () => {
    await load();
    const before = await statusNow();
    await pickFile(csv);
    await landed(before);
    await shape();
    const want = await typed();
    const { url, said } = await copyLink();
    if (!/needs trades\.csv/.test(said)) throw new Error(`it does not say the file is needed: ${said}`);
    const tab = await openTab(url);
    try {
      await tab.waitForSelector('#cubelib .dc-lib-ask:not([hidden])', { timeout: 90_000 });
      await tab.setInputFiles('#cubelib .dc-lib-choose', csv);
      await openedIn(tab);
      const got = await readView(tab);
      if (stamp(got) !== stamp(want)) throw new Error(`the shared page shows different values: ${differs(want, got)}`);
      return 'asked for trades.csv, then the same typed values';
    } finally {
      await tab.close();
    }
  });

  await check('a damaged link says so, and opens nothing', async () => {
    const tab = await openTab(`${URL_BASE}/demo/index.html#p1.AAAAthisisnotapage`);
    try {
      await tab.waitForFunction(() => /damaged or incomplete/.test(document.querySelector('#cubelib .dc-lib-message')?.textContent ?? ''),
        undefined, { timeout: 90_000 });
      return 'said: damaged or incomplete';
    } finally {
      await tab.close();
    }
  });

  await check('a second file picked while the first is still reading is not taken: the window says it is busy (P2-330)', async () => {
    // The source picker reads a choice INSIDE its window and takes no other until that one lands,
    // so two opens cannot race (the race P2-330 fixed came through the old file bar).
    await load();
    const answer = (d) => { void d.accept(); };
    page.on('dialog', answer); // "open anyway?" -- yes
    try {
      await pickFile(big);
      const said = (await page.locator('.dc-picker-status').textContent()) ?? '';
      if (!/Reading big-trades/.test(said)) throw new Error(`while reading, the window says "${said}"`);
      await page.setInputFiles('.dc-picker-file', small);
      await page.locator('.dc-picker').waitFor({ state: 'detached', timeout: 120_000 });
      // the file's cube on screen: rows and a status line saying so
      await page.waitForFunction(() => document.querySelectorAll('.dc-row').length > 0
        && /rows/.test(document.querySelector('.dc-status-timing')?.textContent ?? ''), null, { timeout: 60_000 });
      await frames(page, 4);
    } finally {
      page.off('dialog', answer);
    }
    const title = await page.evaluate(() => window.__dataCube.configuration.reportTitle ?? '');
    if (!/big-trades/.test(title)) throw new Error(`the cube on screen is "${title}", not the file whose read was under way`);
    return `on screen: ${title}; the second pick was not taken`;
  });

  await check('choosing another plane with a file open asks first; No stays (P2-337)', async () => {
    const here = page.url();
    let asked = '';
    // the question, awaited (G-11): it comes or the wait ends, never a fixed sleep
    const question = page.waitForEvent('dialog', { timeout: 10_000 })
      .then(async (d) => { asked = d.message(); await d.dismiss(); }).catch(() => {});
    // where the planner runs is chosen from the status bar's readout
    await page.click('.dc-status-host-pick');
    await page.locator('.dc-menu .dc-menu-item', { hasText: 'Plan remote' }).first().click();
    await question;
    await frames(page);
    if (!asked) throw new Error('it navigated away without asking: the opened file would be lost');
    if (page.url() !== here) throw new Error(`it left for ${page.url()} after No`);
    return `asked: "${asked.slice(0, 60)}…", stayed`;
  });

  await check('no page errors', async () => {
    if (pageErrors.length) throw new Error(pageErrors.slice(0, 2).join(' | '));
  });
} finally {
  await browser.close();
  closeServer();
}

const bad = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - bad}/${results.length} saved-cube checks work`);
process.exit(bad ? 1 : 0);
