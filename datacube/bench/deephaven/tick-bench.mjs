// THE TICK BENCHMARK (README.md; `bazel test //datacube:deephaven_tick_bench --test_output=streamed`, manual): how fast
// a tick on a Deephaven server reaches a page, by transport. For each world (its size, how its prices tick, the server's
// update cycle) it starts the pinned Deephaven with the benchmark's application (tick.groovy), serves the page
// (tick-page.mjs) on the loopback, runs each transport on each result for a few seconds in the pinned Chromium, and
// prints, per run: updates a second, new quotes seen a second, a tick's latency (its quote's stamp to the page seeing
// it: p50, p95, max), the rows each update carried, and the page's time to read them (p50, p95). Its JSON is an output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const SECONDS = Number(process.env.BENCH_SECONDS ?? 6);
/** Which worlds to run, by their place in WORLDS (all unless given): `--test_env=BENCH_WORLDS=0,2`. */
const ONLY = process.env.BENCH_WORLDS?.split(',').map(Number);

/** The worlds: a small book and a big one, a quiet feed and a storm, the server's cycle from 100 ms down to 20. */
const WORLDS = [
  { name: 'small, one quote a tick', positions: 400, instruments: 40, tickMs: 100, storm: false },
  { name: 'small, storm', positions: 400, instruments: 40, tickMs: 100, storm: true },
  { name: 'big, storm', positions: 100_000, instruments: 2_000, tickMs: 100, storm: true },
  { name: 'big, storm, 50 ms cycle', positions: 100_000, instruments: 2_000, tickMs: 50, storm: true },
  { name: 'big, storm, 20 ms cycle', positions: 100_000, instruments: 2_000, tickMs: 20, storm: true },
];

/** The runs in each world: a transport, a result, how many rows it shows, how often it asks for updates -- each at 10
 *  ms (as soon as the server has one), and the viewport also at the cycle (what asking at the cycle costs). */
const runs = (world) => [
  { transport: 'viewport', table: 'agg', rows: 16, intervalMs: world.tickMs },
  { transport: 'viewport', table: 'agg', rows: 16, intervalMs: 10 },
  { transport: 'viewport', table: 'flat', rows: 50, intervalMs: 10 },
  { transport: 'full', table: 'agg', intervalMs: 10 },
  { transport: 'setViewport', table: 'agg', rows: 16, intervalMs: 10 },
  // every row of a big world each tick is no grid's read: a full subscription of 100,000 rows in the storm fell 15 s
  // behind (2 updates a second, 193 ms to read each) and took the server down with it (measured 2026-10-23)
  ...(world.positions > 10_000 ? [] : [{ transport: 'full', table: 'flat', intervalMs: 10 }]),
];

const quantile = (xs, q) => {
  if (xs.length === 0) return NaN;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))];
};

/** A run's events, summarized: a tick's latency is counted on the update that first carried its stamp. */
function summarize(events, seconds) {
  const latencies = [];
  let newest = 0;
  for (const [at, stamp] of events) {
    if (stamp > newest) {
      newest = stamp;
      latencies.push(at - stamp);
    }
  }
  const readMs = events.map((e) => e[3]);
  const rows = events.map((e) => e[2]);
  return {
    updatesPerS: events.length / seconds,
    quotesPerS: latencies.length / seconds,
    latencyP50: quantile(latencies, 0.5),
    latencyP95: quantile(latencies, 0.95),
    latencyMax: latencies.length ? Math.max(...latencies) : NaN,
    rows: quantile(rows, 0.5),
    readP50: quantile(readMs, 0.5),
    readP95: quantile(readMs, 0.95),
  };
}

const files = {
  '/': ['text/html', '<!doctype html><meta charset="utf-8"><title>tick bench</title><script type="module" src="tick-page.mjs"></script>'],
  '/tick-page.mjs': ['text/javascript', readFileSync(runfileFromEnv('TICK_PAGE'))],
};

const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : '-');
const results = [];
let failed = false;
const browser = await chromium.launch();
try {
  for (const world of WORLDS.filter((_, i) => ONLY === undefined || ONLY.includes(i))) {
    const server = startDeephaven({
      quiet: true,
      fixture: 'TICK_FIXTURE',
      args: [`--jvm_flag=-Dembedded.deephaven.cycleMs=${world.tickMs}`],
      env: {
        BENCH_POSITIONS: String(world.positions),
        BENCH_INSTRUMENTS: String(world.instruments),
        BENCH_TICK_MS: String(world.tickMs),
        BENCH_STORM: world.storm ? '1' : '0',
      },
    });
    const config = await server.next('on starting');
    const served = { ...files, '/config.json': ['application/json', JSON.stringify({ url: config.url, psk: config.psk })] };
    const site = createServer((req, res) => {
      const f = served[new URL(req.url, 'http://page').pathname]; // portable: an HTTP route, never a file's path
      res.writeHead(f ? 200 : 404, f ? { 'content-type': f[0] } : {});
      res.end(f ? f[1] : undefined);
    });
    const port = await new Promise((done) => site.listen(0, '127.0.0.1', () => done(site.address().port)));
    const page = await (await browser.newContext({ ignoreHTTPSErrors: true })).newPage();
    page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
    page.on('console', (m) => { if (m.type() === 'error') console.log(`  [page error] ${m.text().slice(-400)}`); });
    try {
      await page.goto(`http://127.0.0.1:${port}/`);
      await page.waitForFunction(() => window.bench?.ready === true, null, { timeout: 120_000 });
      console.log(`\n${world.name}: ${world.positions} positions, ${world.instruments} instruments, a ${world.tickMs} ms cycle`);
      console.log('  transport    result  ask ms  upd/s  quotes/s  lat p50  lat p95  lat max   rows  read p50  read p95');
      for (const run of runs(world)) {
        try {
          // each run on a fresh page: its own connection, nothing a run before it left behind
          await page.goto(`http://127.0.0.1:${port}/`);
          await page.waitForFunction(() => window.bench?.ready === true, null, { timeout: 120_000 });
          // a run that never ends is a failure, said, not a hung benchmark
          const events = await Promise.race([
            page.evaluate((r) => window.bench.run(r), { ...run, seconds: SECONDS }),
            new Promise((_, fail) => setTimeout(() => fail(new Error('no answer in time')), (SECONDS + 40) * 1000)),
          ]);
          const s = summarize(events, SECONDS);
          results.push({ world, run, ...s });
          console.log(`  ${run.transport.padEnd(12)} ${run.table.padEnd(6)} ${String(run.intervalMs).padStart(6)} `
            + `${f1(s.updatesPerS).padStart(6)} ${f1(s.quotesPerS).padStart(9)} ${f1(s.latencyP50).padStart(8)} `
            + `${f1(s.latencyP95).padStart(8)} ${f1(s.latencyMax).padStart(8)} ${String(s.rows).padStart(6)} `
            + `${f1(s.readP50).padStart(9)} ${f1(s.readP95).padStart(9)}`);
        } catch (e) {
          console.log(`  ${run.transport.padEnd(12)} ${run.table.padEnd(6)} failed: ${String(e?.message ?? e).slice(0, 200)}`);
          results.push({ world, run, error: String(e?.message ?? e) });
        }
      }
    } finally {
      await page.context().close();
      site.close();
      await server.stop();
    }
  }
} catch (e) {
  console.log(`FAIL ${e?.stack ?? e}`);
  failed = true;
} finally {
  await browser.close();
  writeFileSync(join(outputs, 'tick-bench.json'), JSON.stringify(results, null, 1));
}
process.exit(failed ? 1 : 0);
