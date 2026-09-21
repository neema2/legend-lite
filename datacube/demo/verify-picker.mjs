// The sample picker, driven the way a person drives it.
//
// The previous check asserted the button was VISIBLE and stopped
// there, so it passed while the dropdown was empty, the row box sat
// at its minimum and the button did nothing — a stale cached bundle,
// invisible to a test that never looked inside the control. Presence
// is not function.
//
//   node demo/verify-picker.mjs      (needs npm start on :8000)

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
      () => document.querySelectorAll('#samplepick option').length > 0,
      undefined, { timeout: 60_000 },
    ).catch(() => bad(`${entry} renders a shell with no working script`));
    await probe.close();
  }
  console.log('bare origin and /demo both reach a working page');

  await page.goto(URL_, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0, { timeout: 120_000 });

  // OPEN THE DATA PANEL. The page is nothing but the grid now, and
  // the picker lives in a window the title bar menu opens -- so the
  // first thing a person does to reach it is the first thing this
  // does too.
  await page.click('.dc-titlebar-menu');
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  await page.locator('.dc-menu-item', { hasText: 'Data' }).first().click();
  await page.waitForTimeout(400);
  if (await page.evaluate(() => document.getElementById('datawin')?.hidden)) {
    bad('the Data panel did not open from the title bar menu');
  }

  // The dropdown must have OPTIONS, not merely exist.
  const options = await page.$$eval('#samplepick option',
    (els) => els.map((e) => e.textContent ?? ''));
  console.log(`options: ${options.length}`);
  if (options.length < 10) bad(`only ${options.length} options`);
  if (options.some((o) => !o.trim())) bad('an option has no label');

  // The row box must be seeded from the choice, not sit at its min.
  const rows0 = await page.inputValue('#samplerows');
  console.log(`default rows: ${rows0}`);
  if (Number(rows0) < 2) bad(`row count is ${rows0} — showPick did not run`);

  // Changing the choice must change both the rows and the note.
  await page.selectOption('#samplepick', 'wide');
  const rows1 = await page.inputValue('#samplerows');
  const note1 = await page.textContent('#uploadnote');
  console.log(`after choosing 'wide': rows=${rows1}, note="${
    (note1 ?? '').slice(0, 50)}…"`);
  if (rows1 === rows0 && note1 === '') bad('choosing did nothing');

  // And the button must actually produce a file.
  const [dl] = await Promise.all([
    page.waitForEvent('download', { timeout: 60_000 }),
    page.click('#samplecsv'),
  ]);
  console.log(`downloaded: ${dl.suggestedFilename()}`);
  if (!/\.csv$/.test(dl.suggestedFilename())) bad('not a csv');

  // THE PAGE MUST STILL SAY WHERE PLANNING HAPPENS.
  //
  // It used to say so in a banner, which once claimed "Planning
  // through legend-lite on :8080" long after this page stopped
  // needing a server -- the page telling the user something untrue
  // about itself. The banner is gone, so the requirement moved to
  // the control that replaced it: a picker in the title bar that
  // both states the plane and changes it.
  // In the title bar MENU: the bar is for what you watch, the menu
  // for what you do occasionally.
  await page.click('.dc-titlebar-menu');
  await page.waitForSelector('.dc-menu', { timeout: 10_000 });
  const planes = await page.evaluate(() =>
    [...document.querySelectorAll('.dc-menu-item')]
      .map((e) => ({
        label: e.querySelector('.dc-menu-label')?.textContent?.trim() ?? '',
        off: e.classList.contains('dc-disabled'),
      }))
      .filter((m) => /^Plan /.test(m.label)));
  console.log(`plane entries: ${planes.map((p) =>
    `${p.label}${p.off ? ' [current]' : ''}`).join(' / ')}`);
  const here = planes.find((p) => /wasm/i.test(p.label));
  const server = planes.find((p) => /:8080|server/i.test(p.label));
  if (!here) bad('the menu does not offer planning in this tab');
  if (!server) bad('the menu does not offer planning on the server');
  // THE CURRENT PLANE IS THE ONE THAT IS DISABLED, which is how the
  // menu still says where planning happens -- the job the banner
  // used to do, and once did untruthfully.
  if (here && !here.off) {
    bad('this page plans in the tab, but the menu does not say so');
  }
  if (server && server.off) {
    bad('the server entry is marked as the current plane on the wasm page');
  }
  await page.keyboard.press('Escape');
  await page.waitForTimeout(150);

  // Readability: every control needs real contrast, since a page
  // that only half-declares its colours renders dark-on-dark under a
  // forced theme.
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
    for (const id of ['samplepick', 'samplerows', 'uploadnote', 'samplecsv']) {
      const el = document.getElementById(id);
      if (!el) continue;
      const a = lum(getComputedStyle(el).color);
      const b = lum(onto(el));
      out[id] = Math.round(((Math.max(a, b) + 0.05)
        / (Math.min(a, b) + 0.05)) * 10) / 10;
    }
    return out;
  });
  console.log(`contrast ratios: ${JSON.stringify(contrast)}`);
  for (const [id, ratio] of Object.entries(contrast)) {
    // 4.5:1 is the ordinary readable-text bar.
    if (ratio < 4.5) bad(`#${id} contrast is ${ratio}:1, under 4.5:1`);
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
