// THE MARKET WORLD'S FEED, MISBEHAVING (//datacube:deephaven_feed_test; docs/DEEPHAVEN_START_HERE.md, the desk under
// load, 5), run in the pinned Chromium. Node drives: a fake feed on the loopback (fake-feed.mjs) that sends each message
// in parts, a message that is not JSON, and a price that is not a number; the pinned Deephaven running the market world
// with its feed pointed there; and the market lane's way of reading the world (feed-page.ts). The world reads the feed
// live, skipping and counting what it cannot read or place, its equities ticking through it. Then the feed goes quiet in
// the middle of a message, as a laptop's sleep leaves it: the world asks again on a new socket -- the quiet one let go
// first, one socket at a time -- and reads it whole, nothing spliced.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { chromium } from 'playwright';
import { runfileFromEnv, runfileNamed } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';
import { environmentsAt } from '../../demo/deephaven/market/environments.mjs';
import { fakeFeed } from './fake-feed.mjs';

const feed = await fakeFeed();
const deephaven = startDeephaven({
  quiet: true,
  fixture: 'MARKET_FIXTURE',
  env: {
    // a small world, its morning short
    MARKET_NAMES: '20',
    MARKET_QUOTES: '400',
    MARKET_TRADES: '80',
    MARKET_MORNING: '60',
    MARKET_MORNING_PRINTS: '2000',
    MARKET_FEED: 'coinbase',
    MARKET_FEED_URL: feed.url,
  },
});
const config = await deephaven.next('starting');

const files = {
  '/feed.html': ['text/html', '<!doctype html><meta charset="utf-8"><title>The feed</title><script type="module" src="feed.js"></script>'],
  '/feed.js': ['text/javascript', readFileSync(runfileFromEnv('FEED_PAGE'))],
  '/wasm-gc-module-runtime.js': ['text/javascript', readFileSync(runfileNamed('PLANNER', 'wasm-gc-module-runtime.js'))],
  '/classes.wasm': ['application/wasm', readFileSync(runfileNamed('PLANNER', 'classes.wasm'))],
  '/trading-model.pure': ['text/plain; charset=utf-8', readFileSync(runfileFromEnv('TRADING_MODEL'))],
  '/market.pure': ['text/plain; charset=utf-8', readFileSync(runfileFromEnv('MARKET_PURE'))],
  '/environments.pure': ['text/plain; charset=utf-8',
    environmentsAt(readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'), new URL(config.url))],
  '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })],
};
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

let failed = false;
const fail = (why) => { console.log(`FAIL ${why}`); failed = true; };
const ok = (what) => console.log(`ok   ${what}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let browser;
try {
  browser = await chromium.launch();
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
  page.on('pageerror', (e) => fail(`page error: ${e.message}`));
  page.on('console', (m) => { if (m.type() === 'error') console.log(`page console: ${m.text()}`); });
  await page.goto(`http://127.0.0.1:${sitePort}/feed.html`);
  await page.waitForFunction(() => window.__feed?.ready === true || window.__feed?.setupError !== undefined, undefined, { timeout: 240_000 });
  const setup = await page.evaluate(() => window.__feed.setupError);
  if (setup !== undefined) throw new Error(`the page's setup: ${setup}`);
  const crypto = () => page.evaluate(() => window.__feed.crypto());
  const prints = () => page.evaluate(() => window.__feed.prints());
  /** `test`, asked each second until it holds (its last answer), or null after `ms`. */
  const until = async (test, ms) => {
    const end = Date.now() + ms;
    for (;;) {
      const got = await test();
      if (got) return got;
      if (Date.now() > end) return null;
      await sleep(1000);
    }
  };
  const skippedOf = (said) => Number(/\((\d+) messages skipped\)/.exec(said)?.[1] ?? 0);

  // LIVE FROM THE FEED, its misreads skipped and counted: the message that is not JSON and the bid that is not a number
  const live = await until(async () => { const c = await crypto(); return c[0] === 'live' ? c : null; }, 60_000);
  const first = await prints();
  await sleep(3000);
  const then = await prints();
  const skipped = live === null ? 0 : skippedOf(live[1]);
  if (live !== null && skipped >= 2 && then.venues.CBX > first.venues.CBX && then.equities > first.equities) {
    ok(`live from the feed, each message in parts: "${live[1]}"; ${then.venues.CBX - first.venues.CBX} coin prints from it and ${then.equities - first.equities} equity prints in 3 s`);
  } else {
    fail(`live: the world says ${JSON.stringify(live ?? await crypto())}; prints ${JSON.stringify(first)} then ${JSON.stringify(then)}`);
  }

  // QUIET IN THE MIDDLE OF A MESSAGE: fifteen seconds quiet and it is no longer live (simulated for the moment it asks
  // again: under a second, too short to be read here), and asks again on a new socket -- the quiet one let go first, so
  // never two open -- whose messages are read whole: nothing more skipped, none spliced onto the quiet one's part
  feed.stall();
  const stalled = Date.now();
  const again = await until(async () => feed.opened === 2 && (await crypto())[0] === 'live' ? crypto() : null, 90_000);
  const backAt = ((Date.now() - stalled) / 1000).toFixed(0);
  const before = await prints();
  await sleep(3000);
  const after = await prints();
  if (again !== null && skippedOf(again[1]) === skipped && after.venues.CBX > before.venues.CBX
    && feed.opened === 2 && feed.most === 1 && feed.sockets() === 1) {
    ok(`quiet mid-message: live again on a second socket ${backAt} s after, the first let go before it opened (at most ${feed.most} open), nothing spliced ("${again[1]}"; ${after.venues.CBX - before.venues.CBX} coin prints in 3 s)`);
  } else {
    fail(`quiet mid-message: the world says ${JSON.stringify(again ?? await crypto())}; sockets opened ${feed.opened}, at most ${feed.most} at once, ${feed.sockets()} open now; coin prints ${before.venues.CBX} then ${after.venues.CBX}`);
  }
} catch (e) {
  fail(String(e?.stack ?? e));
} finally {
  await browser?.close();
  site.close();
  feed.close();
  await deephaven.stop();
}
console.log(failed ? '\nFAIL the feed' : '\nok   the feed');
process.exit(failed ? 1 : 0);
