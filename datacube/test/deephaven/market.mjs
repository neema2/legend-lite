// THE MARKET WORLD, CHECKED (//datacube:deephaven_market_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH28), run
// in the pinned Chromium. Node here only drives: it starts the pinned Deephaven with the market world as its
// application (demo/deephaven/market/), a small world and a feed address that refuses, serves the check page from an
// origin that is not the server's, and reports what the page found (test/deephaven/market-page.ts).

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';

// a small world, quick to check: 50 names, 2,000 quotes and 400 trades a second; the feed's address refuses at once
const NAMES = 50;
const TRADES = 400;
const deephaven = startDeephaven({
  quiet: true,
  fixture: 'MARKET_FIXTURE',
  env: {
    MARKET_NAMES: String(NAMES),
    MARKET_QUOTES: '2000',
    MARKET_TRADES: String(TRADES),
    MARKET_FEED: 'coinbase',
    MARKET_FEED_URL: 'wss://127.0.0.1:9/',
  },
});
const config = await deephaven.next('starting');

const files = {
  '/market.html': ['text/html', '<!doctype html><meta charset="utf-8"><title>The market world</title>'
    + '<script type="module" src="market.js"></script>'],
  '/market.js': ['text/javascript', readFileSync(runfileFromEnv('MARKET_PAGE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
  // the one trading model (//scripts/corpus:trading_model), which the page holds
  '/trading-model.pure': ['text/plain; charset=utf-8', readFileSync(runfileFromEnv('TRADING_MODEL'))],
  '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk, names: NAMES, trades: TRADES })],
};
// the page's own origin, not the server's, served for real on the loopback (test/deephaven/deephaven.mjs says why)
const site = createServer((req, res) => {
  const f = files[new URL(req.url, 'http://page').pathname];
  if (!f) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, { 'content-type': f[0] });
  res.end(f[1]);
});
const sitePort = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));

const CHECKS = 19;
let failed = false;
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  await page.goto(`http://127.0.0.1:${sitePort}/market.html`);
  await page.waitForFunction(() => window.__market?.done === true, undefined, { timeout: 240_000 });
  const verdict = await page.evaluate(() => window.__market);
  for (const o of verdict.outcomes) {
    console.log(`${o.ok ? 'ok  ' : 'FAIL'} ${o.name}${o.ok ? '' : `\n${o.message}`}`);
    if (!o.ok) failed = true;
  }
  if (verdict.setupError) {
    console.log(`FAIL the setup: ${verdict.setupError}`);
    failed = true;
  }
  if (verdict.outcomes.length !== CHECKS) {
    console.log(`FAIL ${verdict.outcomes.length} checks ran, not ${CHECKS}`);
    failed = true;
  }
} finally {
  await browser?.close();
  site.close();
  await deephaven.stop();
}
process.exit(failed ? 1 : 0);
