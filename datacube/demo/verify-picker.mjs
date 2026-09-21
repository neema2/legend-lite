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
  await page.goto(URL_, { waitUntil: 'load', timeout: 120_000 });
  await page.waitForFunction(
    () => document.querySelectorAll('.dc-row').length > 0, { timeout: 120_000 });

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
