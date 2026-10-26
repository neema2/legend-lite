// THE LIVE DUCKDB MEASURED (docs/DUCKDB_LIVE_2026_10_25.md §2, stage A): the measuring page (measure.ts) served beside
// DataCube's own site -- its DuckDB build, its planner module and worker, as the site ships them -- and run in the
// pinned Chromium once for each tape size, a fresh page each; each run's report judged against the budgets stated
// before any number came in, printed, and written to the test's outputs (market-measure.json).
//
//   MEASURE_TAPES    the tape sizes, prints kept (25000,100000,250000)
//   MEASURE_SECONDS  each tab's measured seconds (20), MEASURE_WARM its warm-up (5), MEASURE_TABS the tabs (all)

import '../../../../tools/browser/pinned-chromium.mjs';

import { readFileSync, writeFileSync } from 'node:fs';
import { cpus, totalmem } from 'node:os';
import { chromium } from 'playwright';

import { runfileFromEnv } from '../../../../tools/js/runfiles.mts';
import { outPath, serve, siteRoot } from '../../harness.mjs';

/** THE BUDGETS, fixed before measuring: each a tab's figure, its limit, and what it says. */
const BUDGETS = [
  ['emit p95', (t) => t.emit.p95, 100, 'a refresh of the shown tab: 40% of the 250 ms emit'],
  ['write p95', (t) => t.flush.p95, 30, 'a flush of the world\'s rows'],
  ['click p95', (t) => t.probe.p95, 150, 'a planned query\'s wait behind what is queued'],
  ['tick p95', (t) => t.tick.p95, 500, 'a write landed to its grid told'],
  ['beat p95', (t) => t.beat.p95, 4, 'the world\'s step on the page\'s thread, of a 50 ms beat'],
];
const STARTUP_BUDGET_MS = 5000;

const tapes = (process.env.MEASURE_TAPES ?? '25000,100000,250000').split(',').map(Number);
const seconds = Number(process.env.MEASURE_SECONDS ?? 20);
const warm = Number(process.env.MEASURE_WARM ?? 5);
const only = process.env.MEASURE_TABS;
// the fixes switched off for a comparison (the page's whole=0, direct=0, cycle=0: the baseline's configuration)
const extra = new URLSearchParams(process.env.MEASURE_PARAMS ?? '');

const bundle = readFileSync(runfileFromEnv('MEASURE_BUNDLE'));
const html = readFileSync(runfileFromEnv('MEASURE_PAGE'));
const model = readFileSync(runfileFromEnv('TRADING_MODEL'));
const site = await serve(siteRoot(), {
  headers: { 'Cache-Control': 'no-cache' },
  route: (req, res, url) => {
    const answer = (type, body) => {
      res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache' });
      res.end(body);
      return true;
    };
    if (url.pathname === '/demo/market-measure.html') return answer('text/html; charset=utf-8', html); // portable: an HTTP route, never a file's path
    if (url.pathname === '/demo/market-measure.js') return answer('text/javascript', bundle); // portable: an HTTP route, never a file's path
    if (url.pathname === '/demo/trading-model.pure') return answer('text/plain; charset=utf-8', model); // portable: an HTTP route, never a file's path
    return false;
  },
});

const browser = await chromium.launch();
const environment = {
  cpu: cpus()[0]?.model, cores: cpus().length, memoryGb: Math.round(totalmem() / 2 ** 30), node: process.version,
  chromium: browser.version(),
};
console.log(`environment: ${JSON.stringify(environment)}`);

const runs = [];
let failed = false;
for (const tape of tapes) {
  const page = await browser.newPage();
  page.on('console', (m) => { if (m.text().startsWith('[measure]')) console.log(`  ${m.text()}`); });
  page.on('pageerror', (e) => console.log(`  page error: ${e.message}`));
  const query = new URLSearchParams({
    tape: String(tape), seconds: String(seconds), warm: String(warm), ...(only ? { tabs: only } : {}), ...Object.fromEntries(extra),
  });
  console.log(`\n== a tape of ${tape.toLocaleString('en-US')} prints${extra.size > 0 ? ` (${extra})` : ''}`);
  await page.goto(`${site.origin}/demo/market-measure.html?${query}`);
  await page.waitForFunction(() => globalThis.__measure?.done === true, null, { timeout: 0, polling: 1000 });
  const result = await page.evaluate(() => globalThis.__measure);
  await page.close();
  if (result.failed) {
    console.log(`  FAILED: ${result.failed}`);
    failed = true;
    runs.push({ tape, failed: result.failed });
    continue;
  }
  runs.push({ tape, ...result.report });
  report(result.report);
}
await browser.close();
site.close();
const file = outPath('market-measure.json');
writeFileSync(file, JSON.stringify({ environment, budgets: BUDGETS.map(([n, , l, w]) => ({ name: n, limitMs: l, what: w })), runs }, null, 2));
console.log(`\nthe report: ${file}`);
process.exit(failed ? 1 : 0);

function report(r) {
  const s = r.startup;
  console.log(`  startup ${s.totalMs} ms (planner ${s.plannerMs}, DuckDB ${s.duckdbMs}, tables' DDL written ${s.ddlMs} and run ${s.tablesMs}, `
    + `world ${s.worldMs} of which the morning ${s.morningMs} for ${s.morningPrints} prints, desk ${s.deskBuildMs}, plans ${s.planMs})`
    + ` -- the market's own start (tables and world) ${verdict(s.ddlMs + s.tablesMs + s.worldMs, STARTUP_BUDGET_MS)}`);
  if (r.refused.length > 0) {
    console.log(`  ${r.refused.length} tiles not planned:`);
    for (const x of r.refused) console.log(`    ${x.tile}: ${x.why.split('\n')[0].slice(0, 240)}`);
  }
  const cols = ['tab', 'tiles', 'q', 'open', 'emit p50/p95', 'write p95', 'click p50/p95', 'tick p50/p95', 'beat p95',
    'in use %', 'alone: write ms/trips', 'alone: refresh ms/trips', 'served/ran', 'reruns (same)', 'DuckDB MB', 'verdict'];
  const rows = r.tabs.map((t) => {
    const fails = BUDGETS.filter(([, of, limit]) => !(of(t) <= limit)).map(([n]) => n);
    const mb = (v) => (v === null || Number.isNaN(v) ? '?' : (v / 2 ** 20).toFixed(0));
    return [t.tab, t.tiles, t.queries, t.openMs, `${t.emit.p50}/${t.emit.p95}`, t.flush.p95, `${t.probe.p50}/${t.probe.p95}`,
      `${t.tick.p50}/${t.tick.p95}`, t.beat.p95, t.duckdbInUsePct, `${t.alone.write.p50}/${t.alone.writeTrips.p50}`,
      `${t.alone.refresh.p50}/${t.alone.refreshTrips.p50}`, `${t.served}/${t.ran}`, `${t.reruns} (${t.unchanged})`,
      `${mb(t.memory.duckdbBefore)}->${mb(t.memory.duckdbAfter)}`, fails.length === 0 ? 'within' : `OVER: ${fails.join(', ')}`];
  });
  const widths = cols.map((c, i) => Math.max(c.length, ...rows.map((row) => String(row[i]).length)));
  const line = (cells) => `  ${cells.map((c, i) => String(c).padEnd(widths[i])).join('  ')}`;
  console.log(line(cols));
  for (const row of rows) console.log(line(row));
  // each tab's queries alone, the slowest first: where a heavy tab's time goes
  for (const t of r.tabs) {
    if (t.slowest === undefined) continue;
    console.log(`  ${t.tab}: its queries alone ${t.allQueriesMs} ms; slowest ${t.slowest.map((q) => `${q.tile} "${q.title}" ${q.kind} ${q.ms} ms (${q.rows} rows)`).join('; ')}`);
  }
  // the round trips by kind, over every tab's measured span: how many a second, and each one's mean
  const trips = {};
  for (const t of r.tabs) {
    for (const [k, v] of Object.entries(t.trips)) {
      const s = (trips[k] ??= { n: 0, total: 0 });
      s.n += v.n;
      s.total += v.n * v.meanMs;
    }
  }
  const span = r.tabs.length * r.settings.measureS;
  console.log(`  round trips: ${Object.entries(trips).map(([k, v]) => `${k} ${(v.n / span).toFixed(0)}/s at ${(v.total / v.n).toFixed(2)} ms`).join('; ')}`);
}

function verdict(v, limit) {
  return v <= limit ? `within its ${limit} ms` : `OVER its ${limit} ms`;
}
