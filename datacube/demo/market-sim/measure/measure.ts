// MEASURING THE LIVE DUCKDB (docs/DUCKDB_LIVE_2026_10_25.md §2, stage A): can the tab's DuckDB-WASM -- one thread, in a
// worker, as a static site runs it -- run a desk tab's live queries again every emit while the market world writes into
// it, with room left for a person's click? The real stack: the site's DuckDB build, the planner on its worker, the live
// engine, the world. The real workload: the saved trading desk (market/screens.ts DESK) as the app opens it -- each
// cube's saved query opened by the app's rule (saved-queries.ts savedQueryOpened, cube-document.ts openCube), each
// tile's first view's query by the app's own builders (plan.ts levelWithValues for a grid, chart-spec.ts chartQuery for
// a chart), planned by the planner -- one reader a cube, as the app has one a grid. One tab shown at a time (the app runs
// no hidden tab's queries): a warm-up, then the measured seconds, the world beating every 50 ms, writing every 250, the
// engine emitting every 250, and a probe every 100 ms -- a planned query run as a click's would be, its wait the queue's.
//
// Not measured here (stage B's, the app wired): a group expanded, a total, a drill; drawing. The page reports; its
// driver (measure.mjs) judges against the budgets stated before it ran.

import { startDuckDbInTab } from '../../../../engine-client/src/duckdb-tab.ts';
import type { DuckDbLiveReader } from '../../../../engine-client/src/duckdb-live.ts';
import type { Plan } from '../../../../engine-client/src/relation-type.ts';
import type { Lambda } from '../../../../pure-protocol/src/index.ts';
import { chartQuery } from '../../../src/chart-spec.ts';
import { openCube, type CubeDocument } from '../../../src/cube-document.ts';
import { LitePlanner } from '../../../src/lite-planner.ts';
import { layoutTiles, pageToJson, readPage, type ChartView, type GridView, type PageDocument, type PageView } from '../../../src/page-document.ts';
import { levelWithValues } from '../../../src/plan.ts';
import { savedQueryOpened } from '../../../src/saved-queries.ts';
import { DESK_MODEL, IN_TAB } from '../../deephaven/market/model.ts';
import { DESK } from '../../deephaven/market/screens.ts';
import { openLiveMarket, type LiveMarket } from '../live-market.ts';

/** A run's settings, from the page's address: the tape kept, the seconds warmed and measured a tab, the tabs. */
const params = new URLSearchParams(location.search);
const TAPE = Number(params.get('tape') ?? 100_000);
const WARM_S = Number(params.get('warm') ?? 5);
const MEASURE_S = Number(params.get('seconds') ?? 20);
const ONLY = params.get('tabs')?.split(',');
/** The fixes the baseline's round trips called for (docs/DUCKDB_LIVE_2026_10_25.md §2), each on unless switched off:
 *  results read whole (one round trip a statement), appends straight in, and one cycle (written, then refreshed). */
const WHOLE = params.get('whole') !== '0';
const DIRECT = params.get('direct') !== '0';
const CYCLE = params.get('cycle') !== '0';

/** Durations (ms), summarised. */
class Series {
  readonly #values: number[] = [];

  push(v: number): void {
    this.#values.push(v);
  }

  get summary(): { n: number; p50: number; p95: number; max: number; mean: number } {
    const s = [...this.#values].sort((a, b) => a - b);
    const at = (q: number): number => (s.length === 0 ? Number.NaN : s[Math.min(s.length - 1, Math.floor(q * s.length))]!);
    const mean = s.length === 0 ? Number.NaN : s.reduce((a, b) => a + b, 0) / s.length;
    const r = (v: number): number => Math.round(v * 10) / 10;
    return { n: s.length, p50: r(at(0.5)), p95: r(at(0.95)), max: r(s.length === 0 ? Number.NaN : s[s.length - 1]!), mean: r(mean) };
  }
}

interface Report {
  progress: string;
  done: boolean;
  failed?: string;
  report?: unknown;
}
const out: Report = { progress: 'starting', done: false };
(globalThis as unknown as { __measure: Report }).__measure = out;
const say = (what: string): void => {
  out.progress = what;
  console.log(`[measure] ${what}`);
};

/** DuckDB's own memory, bytes (duckdb_memory()): what the measurement reads of the database itself. */
const MEMORY_SQL = 'SELECT sum(memory_usage_bytes) AS bytes FROM duckdb_memory()';

/**
 * EVERY REQUEST TO DUCKDB'S WORKER, logged: DuckDB-WASM posts each (a query run, a pending query started, polled, its
 * results fetched, Arrow put in) through its database's `postTask`, one at a time in the worker. A request's interval is
 * from its posting to its answer, so the union of the intervals is the time the worker had work -- an upper bound on its
 * being busy (it includes each message's passage) -- and their count is the round trips an operation costs.
 */
interface Task { readonly type: string; readonly start: number; end: number }
const TASKS: Task[] = [];

function logTasks(db: unknown): void {
  const d = db as { postTask: (task: { type: unknown }, transfer?: unknown) => Promise<unknown> };
  const post = d.postTask.bind(d);
  d.postTask = (task, transfer) => {
    const t: Task = { type: String(task.type), start: performance.now(), end: Number.NaN };
    TASKS.push(t);
    return post(task, transfer).finally(() => { t.end = performance.now(); });
  };
}

/** The share of [from, to] in which the worker had a request: the union of the requests' intervals, clipped. */
function inUse(from: number, to: number): number {
  const spans = TASKS.filter((t) => t.start < to && (Number.isNaN(t.end) ? to : t.end) > from)
    .map((t) => [Math.max(from, t.start), Math.min(to, Number.isNaN(t.end) ? to : t.end)] as const)
    .sort((a, b) => a[0] - b[0]);
  let busy = 0;
  let at = from;
  for (const [s, e] of spans) {
    if (e <= at) continue;
    busy += e - Math.max(s, at);
    at = e;
  }
  return Math.round((busy / (to - from)) * 1000) / 10;
}

/** The requests posted in [from, to], by type: how many, and their mean round trip (ms). */
function tasksIn(from: number, to: number): Record<string, { n: number; meanMs: number }> {
  const by: Record<string, { n: number; total: number }> = {};
  for (const t of TASKS) {
    if (t.start < from || t.start >= to || Number.isNaN(t.end)) continue;
    const s = (by[t.type] ??= { n: 0, total: 0 });
    s.n++;
    s.total += t.end - t.start;
  }
  return Object.fromEntries(Object.entries(by).map(([k, v]) => [k, { n: v.n, meanMs: Math.round((v.total / v.n) * 100) / 100 }]));
}

async function main(): Promise<void> {
  const t0 = performance.now();
  const [corpus, desk, environments] = await Promise.all(['./releases/trader-screens/trading-model.pure', './releases/trader-screens/market.pure', './releases/trader-screens/environments.pure']
    .map(async (f) => (await fetch(f)).text()));
  const model = DESK_MODEL(corpus!, desk!, environments!);
  const planner = new LitePlanner({
    assetBaseUrl: new URL('./vendor/', location.href).href, model, runtime: IN_TAB.runtime,
    workerUrl: new URL('./planner-worker.js', location.href).href,
  });
  say('the planner warming up');
  await planner.warmUp();
  const plannerMs = performance.now() - t0;
  say('DuckDB starting');
  const tab = await startDuckDbInTab('./vendor/', planner);
  logTasks(tab.db);
  const duckdbMs = performance.now() - t0 - plannerMs;
  say(`the market's tables and world (a tape of ${TAPE})`);
  const market: LiveMarket = await openLiveMarket({
    connect: () => tab.connect({ whole: WHOLE }), planner, model, tape: TAPE, timer: false, directAppends: DIRECT,
    settings: { feed: 'off', timer: false, morningPrints: TAPE },
  });
  const { live, world } = market;

  // THE DESK, built as the app builds it: its sources' columns typed by the planner, its first values read from the market
  say('the desk built');
  const tBuild = performance.now();
  const scratch = await live.reader();
  const page: PageDocument = readPage(pageToJson(await DESK.build(
    async (text) => (await planner.relationTypeText(text)).map((c) => ({ name: c.name, type: c.type })),
    async (text) => {
      const column = (await scratch.execute(await planner.planText(text), 1)).columns[0];
      return column === undefined ? [] : [...column.values].filter((v) => v !== null && v !== undefined).map(String);
    })));
  const buildMs = performance.now() - tBuild;

  // EACH TILE'S QUERY: its cube opened by the app's rule, its first view's query by the app's builders, planned
  say('each tile planned');
  const tPlan = performance.now();
  // the model's elements read once, as the page's services read them (src/services/browser.ts `tabEngines`)
  const elements = planner.modelElements(model);
  const reading = { elements: async (t: string) => (t === model ? elements : planner.modelElements(t)), parse: (t: string) => planner.parse(t) };
  const cubes = new Map<string, { doc: CubeDocument; planner: LitePlanner; snapshot: ReturnType<typeof openCube>['snapshot'] }>();
  const refused: { tile: string; why: string }[] = [];
  for (const { id, cube } of page.cubes) {
    try {
      const source = cube.source;
      if (source._type !== 'savedQuery') throw new Error(`a ${source._type} source`);
      const opened = await savedQueryOpened(source.query, model, reading, (runtime, how) => planner.withModel(model, runtime, how));
      const own = planner.withModel(model, opened.runtime, opened.how);
      cubes.set(id, { doc: cube, planner: own, snapshot: openCube(cube, { query: opened.source }, source.columns).snapshot });
    } catch (e) {
      refused.push({ tile: `cube ${id}`, why: e instanceof Error ? e.message : String(e) });
    }
  }
  const views = new Map<string, PageView>(page.views.map((v) => [v.id, v]));
  const plans = new Map<string, Plan>();
  for (const v of page.views) {
    if (v.kind !== 'grid' && v.kind !== 'chart') continue;
    const cube = cubes.get(v.cube);
    if (cube === undefined) continue;
    try {
      const query: Lambda = v.kind === 'chart' ? chartQuery(cube.snapshot, v.spec).query
        : await levelWithValues(cube.snapshot, undefined, async (q) => scratch.execute(await cube.planner.plan(q), 1));
      plans.set(v.id, await cube.planner.plan(query));
    } catch (e) {
      refused.push({ tile: v.id, why: e instanceof Error ? e.message : String(e) });
    }
  }
  const planMs = performance.now() - tPlan;
  await scratch.close();
  // a click's query: planned once, run as the app runs a new one (no kept result answers it)
  const probePlan = await planner.planText("products::Equity.all()->project(~[t:x|$x.ticker, cap:x|$x.marketCap])->sort(~cap->descending())->limit(20)");
  const probe = await live.reader();
  const memory = async (): Promise<number | null> => {
    try {
      return Number((await probe.run(MEMORY_SQL, 0)).columns[0]?.values[0] ?? Number.NaN);
    } catch {
      return null;
    }
  };

  const startup = {
    plannerMs: Math.round(plannerMs), duckdbMs: Math.round(duckdbMs), ddlMs: Math.round(market.started.ddl),
    tablesMs: Math.round(market.started.tables),
    worldMs: Math.round(market.started.world), morningPrints: world.counts.morningPrints, morningMs: Math.round(world.counts.morningMs),
    deskBuildMs: Math.round(buildMs), planMs: Math.round(planMs), totalMs: Math.round(performance.now() - t0),
  };
  say(`started: ${JSON.stringify(startup)}`);

  const tabs: unknown[] = [];
  for (const sheet of page.sheets) {
    const name = sheet.name ?? sheet.id;
    if (ONLY !== undefined && !ONLY.includes(name)) continue;
    say(`tab ${name}`);
    tabs.push(await runTab(name, layoutTiles(sheet.layout)));
  }
  out.report = {
    settings: {
      tape: TAPE, warmS: WARM_S, measureS: MEASURE_S, names: world.equities.length, emitMs: 250, flushMs: 250, beatMs: 50,
      whole: WHOLE, direct: DIRECT, cycle: CYCLE,
    },
    environment: { userAgent: navigator.userAgent, hardwareConcurrency: navigator.hardwareConcurrency },
    startup, refused, tabs, written: world.counts.rows, trims: live.counts.trims,
  };
  out.done = true;
  say('done');

  /** One tab shown: its cubes' readers, its tiles' first views, then the clock for the warm-up and the measured span. */
  async function runTab(name: string, tiles: string[]): Promise<unknown> {
    const shown = tiles.map((id) => views.get(id)).filter((v): v is GridView | ChartView => v !== undefined && v.kind !== 'text');
    const byCube = new Map<string, (GridView | ChartView)[]>();
    for (const v of shown) byCube.set(v.cube, [...(byCube.get(v.cube) ?? []), v]);
    const readers: { reader: DuckDbLiveReader; plans: Plan[]; epoch: number }[] = [];
    const tOpen = performance.now();
    for (const vs of byCube.values()) {
      const reader = await live.reader();
      const ps = vs.map((v) => plans.get(v.id)).filter((p): p is Plan => p !== undefined);
      for (const p of ps) await reader.execute(p, 1);
      await reader.follow(1);
      readers.push({ reader, plans: ps, epoch: 1 });
    }
    const openMs = performance.now() - tOpen;

    let measuring = false;
    const emit = new Series();
    const flush = new Series();
    const probeWait = new Series();
    const tick = new Series();
    const beat = new Series();
    const tickRun = new Series();
    let ticks = 0;
    let served = 0;
    let ran = 0;
    let failures = 0;
    for (const r of readers) {
      r.reader.onLive((e) => {
        if (e.failed !== undefined) {
          failures++;
          return;
        }
        if (measuring) tick.push(performance.now() - e.since);
        // the grid's run at its tick: its queries again, a new epoch, then its view followed
        void (async () => {
          const t = performance.now();
          r.epoch++;
          for (const p of r.plans) {
            const result = await r.reader.execute(p, r.epoch);
            if (measuring) {
              if (result.elapsedMs === 0) served++;
              else ran++;
            }
          }
          await r.reader.follow(r.epoch);
          if (measuring) {
            ticks++;
            tickRun.push(performance.now() - t);
          }
        })();
      });
    }
    let flushing = false;
    let emitting = false;
    let probing = false;
    let probeEpoch = 1;
    const timers = [
      setInterval(() => {
        const t = performance.now();
        world.beat();
        if (measuring) beat.push(performance.now() - t);
      }, 50),
      ...(CYCLE ? [
        // one cycle (live-market.ts): written, then refreshed at once
        setInterval(() => {
          if (flushing) return;
          flushing = true;
          const t = performance.now();
          void world.flush().then(() => {
            const e = performance.now();
            if (measuring) flush.push(e - t);
            return live.emit().then(() => { if (measuring) emit.push(performance.now() - e); });
          }).finally(() => { flushing = false; });
        }, 250),
      ] : [
        setInterval(() => {
          if (flushing) return;
          flushing = true;
          const t = performance.now();
          void world.flush().then(() => { if (measuring) flush.push(performance.now() - t); }).finally(() => { flushing = false; });
        }, 250),
        setInterval(() => {
          if (emitting) return;
          emitting = true;
          const t = performance.now();
          void live.emit().then(() => { if (measuring) emit.push(performance.now() - t); }).finally(() => { emitting = false; });
        }, 250),
      ]),
      setInterval(() => {
        if (probing) return;
        probing = true;
        const t = performance.now();
        void probe.run(probePlan.sql, probeEpoch++).then(() => { if (measuring) probeWait.push(performance.now() - t); })
          .finally(() => { probing = false; });
      }, 100),
    ];
    await new Promise((r) => setTimeout(r, WARM_S * 1000));
    const before = { ...live.counts };
    const heap0 = (performance as unknown as { memory?: { usedJSHeapSize: number } }).memory?.usedJSHeapSize ?? null;
    const duck0 = await memory();
    measuring = true;
    const from = performance.now();
    await new Promise((r) => setTimeout(r, MEASURE_S * 1000));
    const to = performance.now();
    measuring = false;
    for (const t of timers) clearInterval(t);
    // let what is in flight land before the readers go
    await world.flush();
    await live.emit();
    const after = { ...live.counts };
    const heap1 = (performance as unknown as { memory?: { usedJSHeapSize: number } }).memory?.usedJSHeapSize ?? null;
    const duck1 = await memory();

    // ISOLATED, the clock stopped: a quarter-second's rows written alone, then a refresh alone -- each one's time and its
    // round trips to the worker, nothing else in flight
    const alone = { write: new Series(), writeTrips: new Series(), refresh: new Series(), refreshTrips: new Series() };
    for (let i = 0; i < 8; i++) {
      for (let b = 0; b < 5; b++) {
        await new Promise((r) => setTimeout(r, 50));
        world.beat();
      }
      let k = TASKS.length;
      let t = performance.now();
      await world.flush();
      alone.write.push(performance.now() - t);
      alone.writeTrips.push(TASKS.length - k);
      k = TASKS.length;
      t = performance.now();
      await live.emit();
      alone.refresh.push(performance.now() - t);
      alone.refreshTrips.push(TASKS.length - k);
    }
    // EACH TILE'S QUERY ALONE: its run's median of three, on a connection of its own, no kept result answering it -- what
    // a refresh pays for it at this tape
    const perQuery: { tile: string; title: string; kind: string; ms: number; rows: number }[] = [];
    for (const v of shown) {
      const p = plans.get(v.id);
      if (p === undefined) continue;
      const times: number[] = [];
      let rows = 0;
      for (let i = 0; i < 3; i++) {
        const t = performance.now();
        rows = (await probe.run(p.sql, 0)).rowCount;
        times.push(performance.now() - t);
      }
      times.sort((a, b) => a - b);
      perQuery.push({ tile: v.id, title: v.title ?? '', kind: v.kind, ms: Math.round(times[1]! * 10) / 10, rows });
    }
    perQuery.sort((a, b) => b.ms - a.ms);
    for (const r of readers) await r.reader.close();
    const result = {
      slowest: perQuery.slice(0, 5), allQueriesMs: Math.round(perQuery.reduce((s, q) => s + q.ms, 0)),
      tab: name, tiles: shown.length, readers: readers.length, queries: readers.reduce((s, r) => s + r.plans.length, 0),
      openMs: Math.round(openMs), emit: emit.summary, flush: flush.summary, probe: probeWait.summary, tick: tick.summary,
      tickRun: tickRun.summary, beat: beat.summary, ticks, served, ran, failures,
      reruns: after.reruns - before.reruns, unchanged: after.unchanged - before.unchanged, told: after.told - before.told,
      duckdbInUsePct: inUse(from, to), trips: tasksIn(from, to),
      alone: { write: alone.write.summary, writeTrips: alone.writeTrips.summary, refresh: alone.refresh.summary, refreshTrips: alone.refreshTrips.summary },
      memory: { jsHeapBefore: heap0, jsHeapAfter: heap1, duckdbBefore: duck0, duckdbAfter: duck1 },
    };
    TASKS.length = 0;
    return result;
  }
}

main().catch((e: unknown) => {
  out.failed = e instanceof Error ? `${e.message}\n${e.stack ?? ''}` : String(e);
  out.done = true;
  console.error('[measure] failed', e);
});
