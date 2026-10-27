// The sample picker, driven the way a person drives it.
//
// The previous check asserted the button was VISIBLE and stopped
// there, so it passed while the dropdown was empty, the row box sat
// at its minimum and the button did nothing — a stale cached bundle,
// invisible to a test that never looked inside the control. Presence
// is not function.
//
//   bazel run //datacube:verify_picker      (needs bazel run //datacube:serve on :8000)

import { chromium } from 'playwright';

const URL_ = process.env.URL ?? 'http://localhost:8000/demo/index.html';

const browser = await chromium.launch();
// Run under a DARK preference: that is where the report came from,
// and a page that only half-declares its colours fails only there.
const ctx = await browser.newContext({
  acceptDownloads: true,
  colorScheme: process.env.SCHEME === 'light' ? 'light' : 'dark',
});
const page = await ctx.newPage();
const errs = [];
page.on('pageerror', (e) => errs.push(`pageerror: ${e.message}`));

let failed = false;
const bad = (m) => { console.log(`FAIL: ${m}`); failed = true; };

try {
  // The BARE origin too. Serving the page at `/` instead of
  // redirecting broke every relative URL in it, so the shell
  // rendered with no script at all -- a page that looks right and
  // does nothing. Checking only the full path missed it entirely.
  for (const entry of ['http://localhost:8000', 'http://localhost:8000/demo']) {
    const probe = await ctx.newPage();
    await probe.goto(entry, { waitUntil: 'load', timeout: 60_000 });
    await probe.waitForFunction(
      () => document.querySelectorAll('.dc-row').length > 0,
      undefined, { timeout: 60_000 },
    ).catch(() => bad(`${entry} renders a shell with no working script`));
    await probe.close();
  }
  console.log('bare origin and /demo both reach a working page');

  await page.goto(URL_, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0, { timeout: 120_000 });

  // OPEN THE SOURCE PICKER (src/ui/source-picker.ts). The page is nothing but the grid, and
  // the picker is a window the title bar menu opens -- so the first thing a person does to
  // reach it is the first thing this does too.
  const openPicker = async () => {
    await page.click('.dc-titlebar-menu');
    await page.waitForSelector('.dc-menu', { timeout: 10_000 });
    // in place of the page: New Page, then "Add a data source"
  await page.locator('.dc-menu .dc-menu-item', { has: page.locator(':scope > .dc-menu-label:text-is("New Page")') }).click();
  await page.locator('.dc-blank').waitFor({ timeout: 10_000 });
  await page.click('.dc-blank .dc-primary');
    await page.waitForSelector('.dc-picker', { timeout: 10_000 }).catch(() => bad('the source picker did not open from the title bar menu'));
    await page.locator('.dc-picker-tab[data-section="examples"]').click();
  };
  await openPicker();

  // The examples must be THERE, each with a name and a word about it.
  const cards = await page.$$eval('.dc-picker-card', (els) => els.map((e) => ({
    id: e.dataset.example ?? '',
    name: e.querySelector('.dc-picker-card-name')?.textContent ?? '',
    text: e.querySelector('.dc-picker-card-text')?.textContent ?? '',
  })));
  console.log(`examples: ${cards.length}`);
  if (cards.length < 10) bad(`only ${cards.length} examples`);
  if (cards.some((c) => !c.name.trim() || !c.text.trim())) bad('an example has no name or no description');

  // Choosing one shows it at the foot, with its own row count to start.
  await page.click('.dc-picker-card[data-example="wide"]');
  const rows1 = await page.inputValue('.dc-picker-rows');
  const chosen1 = await page.textContent('.dc-picker-choice-name');
  console.log(`after choosing 'wide': "${chosen1}", rows=${rows1}`);
  if (Number(rows1) < 2) bad(`row count is ${rows1}: the choice did not seed it`);

  // The download must still produce a file.
  const [dl] = await Promise.all([
    page.waitForEvent('download', { timeout: 60_000 }),
    page.click('.dc-picker-choice .dc-quiet'),
  ]);
  console.log(`downloaded: ${dl.suggestedFilename()}`);
  if (!/\.csv$/.test(dl.suggestedFilename())) bad('not a csv');

  // Readability, while the window is up: its text needs real contrast, since a page that only
  // half-declares its colours renders dark-on-dark under a forced theme.
  const contrast = await page.evaluate(() => {
    const lum = (c) => {
      const [r, g, b] = (c.match(/\d+/g) ?? ['0', '0', '0']).map(Number);
      const f = (v) => {
        const x = v / 255;
        return x <= 0.03928 ? x / 12.92 : ((x + 0.055) / 1.055) ** 2.4;
      };
      return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
    };
    const onto = (el) => {
      let n = el;
      while (n) {
        const bg = getComputedStyle(n).backgroundColor;
        if (bg && !/rgba\(0, 0, 0, 0\)|transparent/.test(bg)) return bg;
        n = n.parentElement;
      }
      return 'rgb(255,255,255)';
    };
    const out = {};
    for (const sel of ['.dc-picker-title', '.dc-picker-tab-label', '.dc-picker-card-name', '.dc-picker-card-text',
      '.dc-picker-rows', '.dc-picker-choice .dc-primary']) {
      const el = document.querySelector(sel);
      if (!el) continue;
      const a = lum(getComputedStyle(el).color);
      const b = lum(onto(el));
      out[sel] = Math.round(((Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05)) * 10) / 10;
    }
    return out;
  });
  console.log(`contrast ratios: ${JSON.stringify(contrast)}`);
  for (const [sel, ratio] of Object.entries(contrast)) {
    // 4.5:1 is the ordinary readable-text bar.
    if (ratio < 4.5) bad(`${sel} contrast is ${ratio}:1, under 4.5:1`);
  }

  // And Open must load it straight into the cube, no file step.
  await page.click('.dc-picker-card[data-example="orders-json"]');
  await page.fill('.dc-picker-rows', '300');
  await page.click('.dc-picker-choice .dc-primary');
  await page.waitForFunction(() => /^300 rows \u00d7 8 cols/.test(
    document.querySelector('.dc-status-timing')?.textContent ?? ''),
  null, { timeout: 60_000 });
  console.log(`opened: ${await page.textContent('.dc-status-timing')}`);

  // THE PAGE MUST STILL SAY WHERE PLANNING HAPPENS.
  //
  // It used to say so in a banner, which once claimed "Planning
  // through legend-lite on :8080" long after this page stopped
  // needing a server -- the page telling the user something untrue
  // about itself. The banner is gone, so the requirement moved to
  // the control that replaced it: a picker in the title bar that
  // both states the plane and changes it.
  // FROM THE STATUS BAR'S READOUT, where the plane is read (the user, 2026-09-30): clicking it
  // offers the planes.
  await page.click('.dc-status-host-pick');
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  const planes = await page.evaluate(() =>
    [...document.querySelectorAll('.dc-menu-item')]
      .map((e) => ({
        label: e.querySelector('.dc-menu-label')?.textContent?.trim() ?? '',
        off: e.classList.contains('dc-disabled'),
      }))
      // "Run on the engine" is a plane too, and a filter of
      // /^Plan / silently dropped it -- the check passed while the
      // third entry went unexamined.
      .filter((m) => /^(Plan |Run on)/.test(m.label)));
  console.log(`plane entries: ${planes.map((p) =>
    `${p.label}${p.off ? ' [current]' : ''}`).join(' / ')}`);
  // BY THE PLANE'S OWN WORD -- local, remote, engine -- which is
  // what the status bar shows and what the menu entries name.
  const here = planes.find((p) => /local/i.test(p.label));
  const server = planes.find((p) => /remote/i.test(p.label));
  const engine = planes.find((p) => /engine/i.test(p.label));
  if (!here) bad('the menu does not offer planning in this tab');
  if (!server) bad('the menu does not offer planning on the server');
  if (!engine) bad('the menu does not offer running on the engine');
  if (planes.length !== 3) {
    bad(`the menu offers ${planes.length} planes, not the three there are`);
  }
  // THE CURRENT PLANE IS THE ONE THAT IS DISABLED, which is how the
  // menu still says where planning happens -- the job the banner
  // used to do, and once did untruthfully.
  if (here && !here.off) {
    bad('this page plans in the tab, but the menu does not say so');
  }
  if (server && server.off) {
    bad('the remote entry is marked as current on the local page');
  }
  if (engine && engine.off) {
    bad('the engine entry is marked as current on the local page');
  }
  await page.keyboard.press('Escape');
  await page.waitForTimeout(150);

  // EACH PLANNER THE PICKER OFFERS MUST GIVE A PAGE: the one page (index.html) with its
  // `?planner=` (the user, 2026-09-30). A planner whose server is absent is SUPPOSED to refuse --
  // in words, naming what was wanted and where, never by planning somewhere else -- so what is
  // checked is that it either ran or refused, on a page that rendered either way.
  for (const plane of [
    { page: 'index.html?planner=remote', refusal: 'plannermissing', what: 'server' },
    { page: 'index.html?planner=engine', refusal: 'plannermissing', what: 'engine' },
  ]) {
    const other = await ctx.newPage();
    const errors = [];
    other.on('pageerror', (e) => errors.push(e.message.split('\n')[0]));
    await other.goto(URL_.replace('index.html', plane.page),
      { waitUntil: 'load', timeout: 120_000 });
    // Either it produced rows or it said why not. A blank page is
    // the failure, and it is the failure this block exists to catch.
    await other.waitForFunction(
      (id) => document.querySelectorAll('.dc-row').length > 0
        || document.getElementById(id)?.hidden === false,
      plane.refusal, { timeout: 60_000 },
    ).catch(() => {});
    const state = await other.evaluate((id) => ({
      rows: document.querySelectorAll('.dc-row').length,
      refusal: document.getElementById(id)?.hidden === false
        ? (document.getElementById(id)?.textContent ?? '')
          .replace(/\s+/g, ' ').trim().slice(0, 60)
        : '',
      offstage: document.getElementById('offstage') !== null,
      // The plane's own word, as the status bar says it -- the thing
      // the old banner got wrong.
      word: document.querySelector('.dc-status-host')?.textContent?.trim()
        ?? '',
    }), plane.refusal);
    console.log(`${plane.what} page: ${state.rows} rows, `
      + `backend="${state.word}", refusal="${state.refusal}"`);
    if (!state.offstage) {
      bad(`the ${plane.what} page is missing #offstage, which \`boot\` `
        + `requires`);
    }
    if (state.rows === 0 && !state.refusal) {
      bad(`the ${plane.what} page neither ran nor said why: `
        + `${errors.join(' | ') || 'nothing said at all'}`);
    }
    // A page that RAN must name its own plane, not inherit local's.
    if (state.rows > 0 && !new RegExp(plane.what === 'server'
      ? 'remote' : 'engine').test(state.word)) {
      bad(`the ${plane.what} page ran but its status bar says `
        + `"${state.word}"`);
    }
    const missed = errors.filter((e) => /missing #/.test(e));
    if (missed.length) {
      bad(`the ${plane.what} page asked for: ${missed.join(' | ')}`);
    }
    await other.close();
  }

} catch (e) {
  bad(e.message.split('\n')[0]);
} finally {
  if (errs.length) { console.log(`page errors: ${errs.join(' | ')}`); failed = true; }
  await browser.close();
}

console.log(failed ? '\n!!! the picker is not usable !!!'
  : '\n*** picker populated, choosable, downloads, and readable ***');
process.exit(failed ? 1 : 0);
