// A LONG SESSION'S COST ON THE SERVER (`bazel test //datacube:deephaven_session_bench --test_output=streamed`, manual;
// docs/DEEPHAVEN_PLAN_2026_10_22.md A1): the cube page over the storm world -- 100,000 positions over 2,000
// instruments, every instrument re-priced ten times a second -- and what the session's kept tables cost the server as
// a person works. The session keeps alive the tables each gesture made (deephaven.ts `keep`, 128 idle ones) so a view
// gone back to is not made again; the server keeps every one of them current at every update cycle. It measures the
// server's CPU (its process and every one under it, from /proc) and the page's ticks -- how many a second, each one's
// time on screen after its rows -- with the page open on its first view, then after many distinct gestures (each its
// own tables: filters to one instrument, regroupings by every pair of columns), back on the first view. Its JSON is
// an output.

// first: points Playwright at the Chromium Bazel fetched
import '../../../tools/browser/pinned-chromium.mjs';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? process.env.TEST_TMPDIR ?? '.';
/** How many distinct filters, and how long each measure runs (`--test_env=BENCH_FILTERS=40`, `BENCH_SECONDS=10`). */
const FILTERS = Number(process.env.BENCH_FILTERS ?? 40);
const SECONDS = Number(process.env.BENCH_SECONDS ?? 10);
const DIMENSIONS = ['desk', 'sector', 'region', 'ccy', 'inst_id'];
const CLOCK = 100;

/** The CPU seconds `pid` and every process under it have used, and their resident memory (MB), from /proc. */
function usage(pid) {
  const procs = new Map();
  for (const d of readdirSync('/proc')) {
    if (!/^\d+$/.test(d)) continue;
    try {
      const stat = readFileSync(`/proc/${d}/stat`, 'utf8');
      const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
      // after the name: state, ppid ... utime (14th field, here 11), stime (12), rss pages (22)
      procs.set(Number(d), { ppid: Number(fields[1]), cpu: (Number(fields[11]) + Number(fields[12])) / CLOCK,
        rss: Number(fields[21]) * 4096 / 1e6 });
    } catch {
      // gone meanwhile
    }
  }
  const tree = new Set([pid]);
  for (let grew = true; grew;) {
    grew = false;
    for (const [p, s] of procs) if (!tree.has(p) && tree.has(s.ppid)) { tree.add(p); grew = true; }
  }
  let cpu = 0;
  let rss = 0;
  for (const p of tree) { cpu += procs.get(p)?.cpu ?? 0; rss += procs.get(p)?.rss ?? 0; }
  return { cpu, rss };
}

const median = (xs) => (xs.length === 0 ? NaN : [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]);
const f1 = (x) => (Number.isFinite(x) ? x.toFixed(1) : '-');
const server = startDeephaven({ quiet: true, env: { DEMO_POSITIONS: '100000', DEMO_INSTRUMENTS: '2000', DEMO_STORM: '1' } });
const config = await server.next('on starting');
const site = await serveDemo(config);
// the page's heap read exactly, after a collection (Chromium's own flags)
const browser = await chromium.launch({ args: ['--enable-precise-memory-info', '--js-flags=--expose-gc'] });
const results = [];
let failed = false;
try {
  const page = await (await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1280, height: 860 } })).newPage();
  page.on('pageerror', (e) => { console.log(`page error: ${e.message}`); failed = true; });
  page.on('console', (m) => { if (m.type() === 'error' || m.type() === 'warning') console.log(`[page ${m.type()}] ${m.text().slice(0, 300)}`); });
  await page.goto(`${site.url}/`);
  const state = () => page.evaluate(() => ({ ...window.__cube, change: undefined, edit: undefined, scroll: undefined,
    serverOps: undefined, serverRun: undefined }));
  const until = async (test, what, ms = 240_000) => {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const s = await state();
      if (s.status === 'failed') throw new Error(`the page failed: ${s.error}`);
      if (test(s)) return s;
      await new Promise((r) => setTimeout(r, 100));
    }
    const s = await state();
    throw new Error(`not ${what} in ${ms / 1000} s: ${JSON.stringify({ status: s.status, error: s.error, epoch: s.epoch,
      followed: s.followed, views: s.views, columns: s.columns, rows: s.rows?.length, counts: s.counts })}`);
  };
  await until((s) => s.status === 'live' && s.views >= 1, 'live');
  /** The view `commands` make, landed and followed. */
  const edit = async (commands, what) => {
    // a tick's run has an epoch of its own: the view the change landed is the next one followed
    const { followed } = await state();
    const said = await page.evaluate((c) => window.__cube.edit(c), commands);
    if (said !== 'applied') throw new Error(`${what}: ${said}`);
    await until((s) => s.followed > followed, `${what} followed`);
  };
  /** The page's JavaScript heap after a collection, MB. */
  const heap = () => page.evaluate(() => {
    window.gc?.();
    return (performance.memory?.usedJSHeapSize ?? NaN) / 1e6;
  });
  /** `SECONDS` of the page as it is: the server's cores busy and memory, the page's ticks and their time on screen. */
  const measure = async (label) => {
    await new Promise((r) => setTimeout(r, 3000));
    const a = await state();
    const u0 = usage(server.pid);
    const t0 = Date.now();
    await new Promise((r) => setTimeout(r, SECONDS * 1000));
    const b = await state();
    const u1 = usage(server.pid);
    const wall = (Date.now() - t0) / 1000;
    const ticks = b.refreshes - a.refreshes;
    const row = {
      label, cores: (u1.cpu - u0.cpu) / wall, rssMB: u1.rss, ticksPerSecond: ticks / wall,
      tickP50: median(b.tickToPaint.slice(-ticks)), kept: b.counts.kept, following: b.counts.following,
      sent: b.counts.sent - a.counts.sent, heapMB: await heap(),
    };
    results.push(row);
    console.log(`${label}: the server ${f1(row.cores)} cores busy, ${row.rssMB.toFixed(0)} MB; the page ${f1(row.ticksPerSecond)} `
      + `ticks a second, on screen ${f1(row.tickP50)} ms after their rows; the session keeps ${row.kept} tables, follows `
      + `${row.following}; ${row.sent} operations sent while measured; the page's heap ${f1(row.heapMB)} MB`);
    return row;
  };

  const first = await measure('the first view');
  // MANY DISTINCT VIEWS: each its own tables on the server, kept alive by the session once the view moves on
  const started = Date.now();
  let views = 0;
  for (let i = 0; i < FILTERS; i++) {
    await edit([{ kind: 'filter', filter: { kind: 'condition', column: 'inst_id', operator: 'equal', value: `I${i * 37}` } }],
      `a filter to I${i * 37}`);
    views++;
  }
  await edit([{ kind: 'filter', filter: null }], 'no filter');
  for (const a of DIMENSIONS) {
    for (const b of DIMENSIONS) {
      // the first view's own grouping is no change: it lands no view
      if (a === b || (a === 'desk' && b === 'sector')) continue;
      await edit([{ kind: 'group', rows: [a, b] }], `grouped by ${a}, ${b}`);
      views++;
    }
  }
  console.log(`${views} distinct views in ${((Date.now() - started) / 1000).toFixed(0)} s`);
  await edit([{ kind: 'group', rows: ['desk', 'sector'] }], 'the first view again');
  const after = await measure(`back on the first view after ${views} others`);
  // A MINUTE OF TICKS on that view: the page's heap after it, against before it
  await new Promise((r) => setTimeout(r, 60_000));
  const soaked = await measure('after a minute more of ticks');
  console.log(`the page's heap over a minute of ${f1(soaked.ticksPerSecond * 60)} ticks: ${f1(after.heapMB)} -> ${f1(soaked.heapMB)} MB`);
  // the same first view on a session that keeps nothing idle: what the kept tables cost -- the reloaded page's old
  // session ended as the page went (pagehide), its tables with it
  await page.reload();
  await until((s) => s.status === 'live' && s.views >= 1, 'live again');
  const fresh = await measure('the first view, a fresh session');
  console.log(`the kept tables cost the server ${f1(after.cores - fresh.cores)} cores (${f1(first.cores)} on the first view, `
    + `${f1(after.cores)} after the session's ${views} views, ${f1(fresh.cores)} fresh)`);
} catch (e) {
  failed = true;
  console.log(`failed: ${String(e?.stack ?? e).slice(0, 800)}`);
} finally {
  await browser.close();
  site.close();
  await server.stop();
  writeFileSync(join(outputs, 'session-bench.json'), JSON.stringify(results, null, 1));
}
process.exit(failed ? 1 : 0);
