// THE TREE BENCHMARK (README.md; `bazel test //datacube:deephaven_tree_bench --test_output=streamed`, manual;
// docs/DEEPHAVEN_PLAN_2026_10_22.md R1): DataCube's tree on a ticking server, one aggregation a level against
// Deephaven's rollup. For each world (its size; every instrument re-quoted every 100 ms cycle) and each way -- `none`
// (the feed and the base alone, the floor), `levels`, `rollup` -- it starts the pinned Deephaven with the tick world
// (tick.groovy; its state log every second), serves the page (tree-page.mjs), and prints a tick's latency (from the
// cycle that made it to the page: p50, p95, and the slowest result's p50), updates a second, rows an update, the page's
// read, and the server's update graph busy share; for the rollup its making, first rows and each group's opening, and
// the probes. Its JSON is an output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
const SECONDS = Number(process.env.BENCH_SECONDS ?? 8);
const ONLY = process.env.BENCH_WORLDS?.split(',').map(Number);

const WORLDS = [
  { name: 'small, storm', positions: 400, instruments: 40 },
  { name: 'big, storm', positions: 100_000, instruments: 2_000 },
];
const MODES = ['none', 'levels', 'rollup'];

const quantile = (xs, q) => {
  if (xs.length === 0) return NaN;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(q * s.length))];
};
const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : '-');

/** Each result's latencies (an update that first carried a newer stamp), and the run's other figures. */
function summarize(out, seconds) {
  const byResult = new Map();
  for (const [at, stamp, , , i] of out.events) {
    const r = byResult.get(i) ?? { newest: 0, latencies: [] };
    if (stamp > r.newest) {
      r.newest = stamp;
      r.latencies.push(at - stamp);
    }
    byResult.set(i, r);
  }
  const all = [...byResult.values()].flatMap((r) => r.latencies);
  return {
    updatesPerS: out.events.length / seconds,
    latencyP50: quantile(all, 0.5),
    latencyP95: quantile(all, 0.95),
    slowestP50: Math.max(...[...byResult.values()].map((r) => quantile(r.latencies, 0.5)).filter(Number.isFinite)),
    rows: quantile(out.events.map((e) => e[2]), 0.5),
    readP50: quantile(out.events.map((e) => e[3]), 0.5),
    busy: out.busy?.share,
    cycleP50: out.busy?.cycleP50,
    cycleP95: out.busy?.cycleP95,
  };
}

const files = {
  '/': ['text/html', '<!doctype html><meta charset="utf-8"><title>tree bench</title><script type="module" src="tree-page.mjs"></script>'],
  '/tree-page.mjs': ['text/javascript', readFileSync(runfileFromEnv('TREE_PAGE'))],
};

const results = [];
let failed = false;
const browser = await chromium.launch();
try {
  for (const world of WORLDS.filter((_, i) => ONLY === undefined || ONLY.includes(i))) {
    console.log(`\n${world.name}: ${world.positions} positions, ${world.instruments} instruments, a 100 ms cycle`);
    console.log('  way      upd/s  lat p50  lat p95  slowest p50  rows  read p50  server busy  cycle p50  cycle p95');
    for (const mode of MODES) {
      const server = startDeephaven({
        quiet: true,
        fixture: 'TICK_FIXTURE',
        args: ['--jvm_flag=-Dembedded.deephaven.cycleMs=100',
          '--jvm_flag=-Dembedded.deephaven.serverFlags=-DServerStateTracker.reportIntervalMillis=1000'],
        env: {
          BENCH_POSITIONS: String(world.positions),
          BENCH_INSTRUMENTS: String(world.instruments),
          BENCH_TICK_MS: '100',
          BENCH_STORM: '1',
          BENCH_TREE: mode === 'levels' ? 'levels' : '',
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
      // Deephaven's library says some refusals as page errors (the aggregates probe's): said, never the run's failure
      page.on('pageerror', (e) => console.log(`  [page error] ${e.message.slice(0, 200)}`));
      try {
        await page.goto(`http://127.0.0.1:${port}/`);
        await page.waitForFunction(() => window.bench?.ready === true, null, { timeout: 120_000 });
        // the base made and ticking before anything is measured
        await new Promise((r) => setTimeout(r, 3000));
        const run = (m) => Promise.race([
          page.evaluate(([w, s]) => window.bench.run(w, s), [m, SECONDS]),
          new Promise((_, fail) => setTimeout(() => fail(new Error('no answer in time')), (SECONDS + 120) * 1000)),
        ]);
        const out = await run(mode);
        if (out.error) throw new Error(out.error);
        const s = summarize(out, SECONDS);
        results.push({ world, mode, ...s, ...(mode === 'rollup' ? { makeMs: out.makeMs, firstMs: out.firstMs,
          openMs: out.openMs, opened: out.opened, shape: out.shape, probes: out.probes } : {}), busyColumns: out.busy?.columns });
        console.log(`  ${mode.padEnd(8)} ${f1(s.updatesPerS).padStart(5)} ${f1(s.latencyP50).padStart(8)} `
          + `${f1(s.latencyP95).padStart(8)} ${f1(s.slowestP50).padStart(12)} ${String(s.rows).padStart(5)} `
          + `${f1(s.readP50).padStart(9)} ${(Number.isFinite(s.busy) ? `${(100 * s.busy).toFixed(1)}%` : '-').padStart(12)}`
          + ` ${f1(s.cycleP50).padStart(10)} ${f1(s.cycleP95).padStart(10)}`);
        if (mode === 'rollup') {
          console.log(`           made in ${f1(out.makeMs)} ms, first rows ${f1(out.firstMs)} ms, groups opened in `
            + `${out.openMs.map(f1).join(', ')} ms (${out.opened} rows on screen); first rows: ${out.shape.join(' ')}`);
          for (const [k, v] of Object.entries(out.probes)) console.log(`           ${k}: ${v}`);
          if (world.positions <= 1000) {
            const aggregates = await run('aggregates');
            results.push({ world, mode: 'aggregates', aggregates });
            for (const [k, v] of Object.entries(aggregates)) console.log(`           rollup ${k}: ${String(v).slice(0, 160)}`);
          }
        }
        if (!Number.isFinite(s.busy)) console.log(`           the server state log: ${JSON.stringify(out.busy).slice(0, 600)}`);
      } catch (e) {
        console.log(`  ${mode.padEnd(8)} failed: ${String(e?.message ?? e).slice(0, 400)}`);
        results.push({ world, mode, error: String(e?.message ?? e) });
        failed = true;
      } finally {
        await page.context().close();
        site.close();
        await server.stop();
      }
    }
  }
} catch (e) {
  console.log(`FAIL ${e?.stack ?? e}`);
  failed = true;
} finally {
  await browser.close();
  writeFileSync(join(outputs, 'tree-bench.json'), JSON.stringify(results, null, 1));
}
process.exit(failed ? 1 : 0);
