// THE NETWORK BENCHMARK (README.md; `bazel test //datacube:deephaven_network_bench --test_output=streamed`, manual;
// docs/DEEPHAVEN_PLAN_2026_10_22.md N1): the cube page (demo/deephaven/cube.ts) over a link with a round trip of its
// own, as a person on another machine meets it. For each round trip it starts the pinned Deephaven with the demo's
// application, puts a delaying TCP proxy between the browser and the server -- each chunk held half the round trip in
// each direction, so TLS, HTTP/2 and every request meet the delay as on a real link -- serves the page, and says: the
// page's start to its first view (Deephaven's JS library fetched over the link too), each gesture's time to the view,
// its time to the view followed (live: it ticks from then on; the next gesture waits for it, as a person's comes after
// it) and the requests it sent -- the first time (its tables made) and warm (the median of the rounds after) -- and a tick's
// time on screen after its rows arrived (the link adds its one-way delay before that, never a request: a tick is
// pushed). Its JSON is an output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { writeFileSync } from 'node:fs';
import { createServer, connect } from 'node:net';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
/** The round trips, ms (`--test_env=BENCH_RTTS=0,40`). */
const RTTS = (process.env.BENCH_RTTS ?? '0,20,80').split(',').map(Number);
const GESTURES = ['expand', 'collapse', 'pivot', 'regroup', 'filter'];
/** Each gesture's round of the sequence: the first, which makes its tables, and the warm ones after
 *  (`--test_env=BENCH_ROUNDS=3`). */
const ROUNDS = Number(process.env.BENCH_ROUNDS ?? 3);
/** Set, each gesture's requests are printed: when each started and ended after the gesture did, and its method
 *  (`--test_env=BENCH_TRACE=1`). */
const TRACE = process.env.BENCH_TRACE === '1';

/** A TCP proxy on the loopback to `port`, each chunk held `oneWayMs` in each direction, in order. */
function delayProxy(port, oneWayMs) {
  const held = (from, to) => {
    from.on('data', (chunk) => { setTimeout(() => { if (!to.destroyed) to.write(chunk); }, oneWayMs); });
    from.on('end', () => setTimeout(() => to.end(), oneWayMs));
    from.on('error', () => to.destroy());
  };
  const proxy = createServer((client) => {
    const server = connect(port, '127.0.0.1');
    held(client, server);
    held(server, client);
  });
  return new Promise((done) => proxy.listen(0, '127.0.0.1', () => done({ port: proxy.address().port, close: () => proxy.close() })));
}

const f0 = (x) => (Number.isFinite(x) ? x.toFixed(0) : '-');
const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const results = [];
let failed = false;
const browser = await chromium.launch();
try {
  for (const rtt of RTTS) {
    const server = startDeephaven({ quiet: true });
    const config = await server.next('on starting');
    const proxy = rtt > 0 ? await delayProxy(Number(new URL(config.url).port), rtt / 2) : null;
    const url = proxy ? `https://127.0.0.1:${proxy.port}` : config.url;
    const site = await serveDemo({ ...config, url });
    const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1280, height: 860 } });
    const page = await context.newPage();
    let requests = 0;
    const sent = new Map();
    let trace = [];
    page.on('request', (r) => {
      if (!r.url().startsWith(url)) return;
      requests++;
      sent.set(r, Date.now());
    });
    const ended = (r) => {
      const at = sent.get(r);
      if (at === undefined) return;
      sent.delete(r);
      trace.push({ method: r.url().slice(r.url().lastIndexOf('/') + 1), at, end: Date.now() });
    };
    page.on('requestfinished', ended);
    page.on('requestfailed', ended);
    const state = () => page.evaluate(() => ({ ...window.__cube, change: undefined, scroll: undefined,
      serverOps: undefined, serverRun: undefined }));
    const until = async (test, what, ms = 180_000) => {
      const end = Date.now() + ms;
      while (Date.now() < end) {
        const s = await state();
        if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
        if (test(s)) return s;
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error(`not ${what} in ${ms / 1000} s`);
    };
    try {
      const started = Date.now();
      await page.goto(`${site.url}/`);
      await until((s) => s.status === 'live' && s.views >= 1, 'live');
      const live = Date.now() - started;
      const startRequests = requests;
      await page.evaluate(() => { window.__cube.paused = true; });
      // the first gesture waits for the page to settle, as a person's does
      await new Promise((r) => setTimeout(r, 2000));
      const gestures = Object.fromEntries(GESTURES.map((g) => [g, { ms: [], live: [], requests: [] }]));
      for (let round = 0; round < ROUNDS; round++) {
        for (const g of GESTURES) {
          const before = requests;
          trace = [];
          const t0 = Date.now();
          const kind = await page.evaluate((w) => window.__cube.change(w), g);
          if (kind !== 'applied') throw new Error(`the ${g} was ${kind}`);
          const ms = Date.now() - t0;
          gestures[g].ms.push(ms);
          gestures[g].requests.push(requests - before);
          // the view followed -- live from then on -- before the next gesture, as a person's comes after it
          const { epoch } = await state();
          await until((s) => s.followed >= epoch, `the ${g}'s view followed`);
          gestures[g].live.push(Date.now() - t0);
          if (TRACE) {
            // what the gesture's own time held, and what it started after (its view's following)
            await new Promise((r) => setTimeout(r, rtt + 100));
            console.log(`  ${rtt} ms, round ${round}, ${g} in ${ms} ms: ` + [...trace, ...[...sent].map(([r, at]) => ({
              method: `${r.url().slice(r.url().lastIndexOf('/') + 1)}(open)`, at, end: NaN }))]
              .sort((a, b) => a.at - b.at)
              .map((t) => `${t.method} ${t.at - t0}..${Number.isFinite(t.end) ? t.end - t0 : ''}`).join(', '));
          }
        }
        // each round from the page's own view, followed
        if ((await page.evaluate(() => window.__cube.change('reset'))) !== 'applied') throw new Error('no reset');
        const { epoch } = await state();
        await until((s) => s.followed >= epoch, 'the reset view followed');
      }
      const t0 = await state();
      await page.evaluate(() => { window.__cube.paused = false; });
      await new Promise((r) => setTimeout(r, 5000));
      const t1 = await state();
      const paints = t1.tickToPaint.slice(-(t1.refreshes - t0.refreshes));
      const row = { rtt, live, startRequests, gestures, ticks: t1.refreshes - t0.refreshes, tickP50: median(paints),
        tickSent: t1.counts.sent - t0.counts.sent };
      results.push(row);
      console.log(`round trip ${rtt} ms: live in ${live} ms (${startRequests} requests); `
        + GESTURES.map((g) => `${g} ${f0(gestures[g].ms[0])}/${f0(median(gestures[g].ms.slice(1)))} ms, live `
          + `${f0(gestures[g].live[0])}/${f0(median(gestures[g].live.slice(1)))} `
          + `(${gestures[g].requests[0]}/${median(gestures[g].requests.slice(1))} req)`).join(', ')
        + `; ${row.ticks} ticks in 5 s, on screen ${f0(row.tickP50)} ms after their rows, ${row.tickSent} operations sent`);
    } catch (e) {
      failed = true;
      console.log(`round trip ${rtt} ms: failed: ${String(e?.stack ?? e).slice(0, 600)}`);
      results.push({ rtt, error: String(e?.message ?? e) });
    } finally {
      await context.close();
      site.close();
      proxy?.close();
      await server.stop();
    }
  }
} finally {
  await browser.close();
  writeFileSync(join(outputs, 'network-bench.json'), JSON.stringify(results, null, 1));
}
process.exit(failed ? 1 : 0);
