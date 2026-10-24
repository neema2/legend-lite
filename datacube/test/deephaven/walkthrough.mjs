// THE SINGLE-USER WALK-THROUGH (`bazel test //datacube:deephaven_walkthrough --test_output=streamed`, manual;
// docs/DEEPHAVEN_PLAN_2026_10_22.md A1): what a person does on the two demo pages, as a person does it -- a chevron
// clicked, a header clicked, a right-click, Ctrl+Z, the page's menu -- over the demo's own world, each step timed and
// screenshotted, every error the page says or logs collected. It fails on a page error, a step that does not land, or
// an error said on the page; Deephaven's JS library's own log lines are counted, not failed on. Its screenshots and
// `walkthrough.json` are its outputs.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
/** Deephaven's JS library logs a stream it closed itself as an error: counted, not a fault of the page's. */
const LIBRARY_NOISE = /Stream canceled on the server|IdeConnection on .* Failed to connect|net::ERR_CONNECTION_REFUSED/;

const server = startDeephaven({ quiet: true });
const config = await server.next('on starting');
const site = await serveDemo(config);
const browser = await chromium.launch();
const log = [];
let failed = false;
let shot = 0;
try {
  for (const [name, path, global] of [['cube', '/', '__cube'], ['board', '/board.html', '__board']]) {
    const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1600, height: 1000 } });
    await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: site.url });
    const page = await context.newPage();
    const errors = [];
    let noise = 0;
    page.on('pageerror', (e) => errors.push(`page error: ${e.message}`));
    page.on('console', (m) => {
      if (m.type() !== 'error') return;
      if (LIBRARY_NOISE.test(m.text())) noise++;
      else errors.push(`console: ${m.text().slice(0, 300)}`);
    });
    // before the page is opened it has none: an empty state
    const state = () => page.evaluate((g) => (window[g] === undefined ? {} : JSON.parse(JSON.stringify(window[g]))), global);
    const until = async (test, what, ms = 60_000) => {
      const end = Date.now() + ms;
      while (Date.now() < end) {
        const s = await state();
        if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
        if (test(s)) return s;
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error(`not ${what} in ${ms / 1000} s`);
    };
    /** The page's header as it says it now (its status pill). */
    const said = () => page.locator('#status').textContent();
    /** One step: `act`, then `settle` (what says it landed), timed; a screenshot; the errors it brought. */
    const step = async (what, act, settle = async () => {}) => {
      const from = errors.length;
      const before = await state();
      const t0 = Date.now();
      try {
        await act(before);
        await settle(before);
      } catch (e) {
        errors.push(`${what}: ${String(e?.message ?? e)}`);
      }
      const ms = Date.now() - t0;
      await page.waitForTimeout(400);
      const file = `${String(++shot).padStart(2, '0')}-${name}-${what.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}.png`;
      await page.screenshot({ path: join(outputs, file) });
      const status = (await said())?.trim() ?? '';
      const brought = errors.slice(from);
      const row = { page: name, step: what, ms, status, errors: brought, screenshot: file };
      log.push(row);
      console.log(`${brought.length === 0 ? 'ok  ' : 'FAIL'} ${name}: ${what} -- ${ms} ms; the page says "${status}"`
        + (brought.length > 0 ? `\n       ${brought.join('\n       ')}` : ''));
      if (brought.length > 0) failed = true;
    };
    /** Landed: the cube's next view followed (cube page), or `grid`'s next view (board). */
    const landed = (grid) => async (before) => {
      if (grid === undefined) await until((s) => s.followed > before.followed, 'the view followed');
      else await until((s) => (s.grids[grid]?.views ?? 0) > (before.grids[grid]?.views ?? 0), `${grid}'s view landed`);
    };

    await step('opened', async () => {
      await page.goto(`${site.url}${path}`);
      await until((s) => s.status === 'live', 'live', 180_000);
    });
    if (name === 'cube') {
      await step('a desk opened by its chevron', async () => {
        await page.locator('.dc-row', { hasText: 'FX' }).first().locator('.dc-chevron').click();
      }, landed());
      await step('sorted by a header click', async () => {
        await page.locator('.dc-head-row').getByText('mv', { exact: true }).first().click();
      }, landed());
      await step('pivoted by region', async () => { await page.evaluate(() => window.__cube.change('pivot')); }, landed());
      await step('regrouped by region and currency', async () => { await page.evaluate(() => window.__cube.change('regroup')); }, landed());
      await step('filtered to FX', async () => { await page.evaluate(() => window.__cube.change('filter')); }, landed());
      await step('a calculated column the server cannot run: refused, said, the view kept', async (before) => {
        const said = await page.evaluate(() => window.__cube.change('calc:x|$x.mv * 1.5d'));
        const after = await state();
        const overlay = await page.locator('.dc-app-overlay').allTextContents();
        if (after.columns.join() !== before.columns.join()) throw new Error(`the view changed: ${after.columns.join()}`);
        if (!overlay.some((t) => /decimal literal/.test(t))) throw new Error(`no window said why: ${said}; ${overlay.join(' | ').slice(0, 200)}`);
        console.log(`       the change answered "${said}"; the window says: ${overlay.join(' | ').replace(/\s+/g, ' ').slice(0, 240)}`);
      });
      await step('still ticking after the refusal', async (before) => {
        await page.locator('.dc-app-overlay .dc-overlay-close').first().click();
        await until((s) => s.refreshes >= before.refreshes + 5, 'five ticks', 10_000);
      });
      await step('a calculated column added', async () => { await page.evaluate(() => window.__cube.change('calc')); }, landed());
      await step('undone (Ctrl+Z)', async () => {
        await page.locator('.dc-row .dc-cell').first().click();
        await page.keyboard.press('Control+z');
      }, landed());
      await step('a cell right-clicked: its menu', async () => {
        await page.locator('.dc-row .dc-cell').nth(5).click({ button: 'right' });
      });
      await page.keyboard.press('Escape');
      await step('copied (Ctrl+C)', async () => {
        await page.locator('.dc-row .dc-cell').nth(5).click();
        await page.keyboard.press('Control+c');
      });
      await step('the Properties window', async () => { await page.locator('.dc-status-properties').first().click(); });
      await page.keyboard.press('Escape');
      await step('flat: every position', async () => { await page.evaluate(() => window.__cube.change('flat')); }, landed());
      await step('ticking for five seconds', async () => { await page.waitForTimeout(5000); });
      await step('back to the opening view', async () => { await page.evaluate(() => window.__cube.change('reset')); }, landed());
      await step('the server forgot the session (a laptop asleep): signed in again by itself', async (before) => {
        // the page's own fetch answers as a server that no longer knows the session: UNAUTHENTICATED on its reads
        await page.evaluate(() => {
          const real = window.fetch;
          window.__realFetch = real;
          window.fetch = (input, init) => (/TableService\/Batch|FlightService\/DoGet/.test(String(input instanceof Request ? input.url : input))
            ? Promise.resolve(new Response(new Uint8Array(0), { status: 200, headers: { 'grpc-status': '16', 'grpc-message': 'session%20expired' } }))
            : real(input, init));
        });
        await page.evaluate(() => window.__cube.change('regroup'));
        await until((s) => s.status === 'reconnecting', 'reconnecting', 30_000);
        await page.evaluate(() => { window.fetch = window.__realFetch; });
        await until((s) => s.status === 'live' && s.reconnects > before.reconnects, 'live again', 60_000);
        await page.locator('.dc-app-overlay .dc-overlay-close').first().click().catch(() => {});
      });
      await step('the server restarted, back by itself', async (before) => {
        server.restart();
        await until((s) => s.status === 'reconnecting', 'reconnecting', 60_000);
        await server.next('on restarting');
        await until((s) => s.status === 'live' && s.reconnects > before.reconnects, 'live again', 180_000);
      });
    } else {
      await step('Desks pivoted by currency instead', async () => {
        const r = await page.evaluate(() => window.__board.edit('Desks', [{ kind: 'pivot', columns: ['ccy'] }]));
        if (r !== 'applied') throw new Error(r);
      }, landed('Desks'));
      await step('Sectors grouped by region and sector', async () => {
        const r = await page.evaluate(() => window.__board.edit('Sectors', [{ kind: 'group', rows: ['region', 'sector'] }]));
        if (r !== 'applied') throw new Error(r);
      }, landed('Sectors'));
      await step('ticking for five seconds', async () => { await page.waitForTimeout(5000); });
      await step('saved from the page menu', async () => {
        await page.locator('.dc-page-bar .dc-titlebar-menu').click();
        await page.getByRole('menuitem', { name: 'Save', exact: true }).click();
        await page.waitForFunction(() => document.querySelector('#status')?.textContent?.includes('saved'), null, { timeout: 10_000 });
      });
      await step('opened from the page menu', async (before) => {
        await page.locator('.dc-page-bar .dc-titlebar-menu').click();
        await page.getByRole('menuitem', { name: 'Open', exact: true }).click();
        await until((s) => s.opened > before.opened && s.status === 'live', 'opened');
      });
      await step('the server restarted, back by itself', async () => {
        server.restart();
        await until((s) => s.status === 'reconnecting', 'reconnecting', 60_000);
        await server.next('on restarting');
        await until((s) => s.status === 'live' && s.reconnects >= 1, 'live again', 180_000);
      });
    }
    console.log(`     ${name}: Deephaven's JS library logged ${noise} errors of its own (streams it closed, a server away)`);
    log.push({ page: name, libraryNoise: noise });
    await context.close();
  }
} catch (e) {
  failed = true;
  console.log(`FAIL ${String(e?.stack ?? e).slice(0, 800)}`);
} finally {
  await browser.close();
  site.close();
  await server.stop();
  writeFileSync(join(outputs, 'walkthrough.json'), JSON.stringify(log, null, 1));
}
process.exit(failed ? 1 : 0);
