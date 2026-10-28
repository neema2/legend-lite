// THE DATACUBE ON DEEPHAVEN (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23, DH24): DataCube's own app -- its grid, zones, menus,
// filters, pivots and totals, unchanged -- planning in this tab with legend-lite's WebAssembly planner on a Deephaven
// runtime, and running on the Deephaven server itself (engine-client's DeephavenEngine): no server of legend-lite's in
// between. Every query the cube sends over the risk source builds on the source's tables the session keeps alive, so a
// regroup, a repivot or a filter sends only what differs.
//
// IT TICKS: every view the grid shows is followed (`engine.follow`) -- each result it read kept live in the page, its
// rows pushed by Deephaven's JS library at every change -- and when one changes the cube runs again at once, every
// query answered from those live rows: nothing sent, nothing fetched. The page shows how long a
// tick takes from its rows arriving to the grid painted. When the server goes away (a restart: Deephaven keeps no
// session across one), the page says so, signs in again once the server answers, and runs the cube again.

import { CubeApp } from '../../src/app.ts';
import type { CubeView } from '../../src/cube.ts';
import { sourceWithDerived } from '../../src/query.ts';
import { ServerBases } from '../../src/server-base.ts';
import type { GridCommand } from '../../src/grid-commands.ts';
import type { DeephavenCounts } from '../../../engine-client/src/deephaven-engine.ts';
import { CONNECTION, STORES, openRisk, type RiskConfig } from './risk.ts';
import { openServerLog, type ServerOp } from './server-log.ts';

/** DataCube's row height (app.ts DATACUBE_ROW_HEIGHT): a row's place times it is where the grid scrolls to show it. */
const ROW_HEIGHT = 20;

/** What the page shows, for a person and for its test. */
interface Shown {
  status: 'starting' | 'live' | 'reconnecting' | 'failed';
  error: string;
  views: number;
  reconnects: number;
  /** Ticks (a followed result changed: at most one a frame) and the runs they made. */
  ticks: number;
  refreshes: number;
  /** Each tick's time from its rows arriving to the grid painted, and the cube's run in it, ms (the last 200). */
  tickToPaint: number[];
  runMs: number[];
  /** Each tick's time in parts, ms: waiting for the run before it, the run, and the frame after it (the last 200). */
  tickParts: [number, number, number][];
  counts: DeephavenCounts | null;
  /** The view on screen: its epoch, its columns, and its rows' values; and the epoch of the last view followed. */
  epoch: number;
  followed: number;
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  /** A flat view read by a window (V1): where its rows are in the result, and how many the result has; else null. */
  window: { offset: number; total: number } | null;
  /** The grid's base on the server (B1): its table, and the queries read from it, read as written, and bases made. */
  base: { table: string | null; based: number; asWritten: number; made: number } | null;
  /** Set, a tick runs nothing: a test measures a change's own queries. */
  paused: boolean;
  /** The gestures the test makes, as the app's own changes (the zones' and the filter editor's). */
  change?: (what: string) => Promise<string>;
  /** Any change, as grid commands (a benchmark's many views): the refusal, or 'applied'. */
  edit?: (commands: readonly GridCommand[]) => Promise<string>;
  /** Scrolls the grid to show row `row` at its top, as a person does: ms until the grid holds that row's values. */
  scroll?: (row: number) => Promise<number>;
  /** What the server made since `ms` (its clock, ms since the epoch), by its own operation log: a test's diagnostic. */
  serverOps?: (ms: number) => Promise<ServerOp[]>;
  /** Runs `code` in the server's console: a test's change to the demo's data. */
  serverRun?: (code: string) => Promise<void>;
}
const shown: Shown = {
  status: 'starting', error: '', views: 0, reconnects: 0, ticks: 0, refreshes: 0, tickToPaint: [], runMs: [], tickParts: [], counts: null,
  epoch: 0, followed: 0, columns: [], rows: [], window: null, base: null, paused: false,
};
(window as unknown as { __cube: Shown }).__cube = shown;

const byId = (id: string): HTMLElement => {
  const e = document.getElementById(id);
  if (!e) throw new Error(`the page has no #${id}`);
  return e;
};

/** What the page says in its header: the page's state ('live', 'reconnecting'), or a PASSING message -- a grid's
 *  refusal, a save -- said, then the page's state again after eight seconds (A1: an error kept saying itself long
 *  after the page had gone on). */
let passing: ReturnType<typeof setTimeout> | undefined;
function said(text: string, kind: 'ok' | 'warn' | 'error' = 'ok', passes = false): void {
  const e = byId('status');
  e.textContent = text;
  e.className = kind;
  clearTimeout(passing);
  if (passes) passing = setTimeout(() => { if (shown.status === 'live') said('live'); }, 8000);
}

const frame = (): Promise<void> => new Promise((r) => { requestAnimationFrame(() => r()); });
const median = (xs: readonly number[]): number => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] ?? NaN;

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as RiskConfig;
  const { model, planner, engine, cube } = await openRisk(config);
  // the tab going (closed, reloaded, navigated away): its session ended as it goes, never left ticking on the server
  addEventListener('pagehide', () => { engine.close().catch(() => { /* the page is gone */ }); });
  const snapshot = cube(['desk', 'sector']);
  // the server's own account of each gesture, for the test: opened when first asked
  let serverLog: ReturnType<typeof openServerLog> | null = null;
  shown.serverOps = async (ms) => (await (serverLog ??= openServerLog(config.url, config.psk))).since(ms);
  shown.serverRun = async (code) => (await (serverLog ??= openServerLog(config.url, config.psk))).run(code);

  // every query the grid sends reads its base from one table the server keeps (B1)
  const runner = new ServerBases(planner, engine, { model, connection: CONNECTION, stores: STORES, baseOf: sourceWithDerived }).runner();
  // the counts kept at once (the test reads them); said in the header at most twice a second -- the header's text laid
  // out again at every tick laid the grid out with it
  let saidAt = 0;
  let sayLater: ReturnType<typeof setTimeout> | undefined;
  const say = (): void => {
    sayLater = undefined;
    saidAt = performance.now();
    const c = engine.counts;
    const paint = shown.tickToPaint.length > 0
      ? `; a tick on screen ${median(shown.tickToPaint).toFixed(1)} ms after its rows arrived`
      : '';
    byId('counts').textContent = `${c.following} results live, ${c.fetched} fetched, ${c.live} answered live; `
      + `${c.planned} operations planned, ${c.sent} sent${paint}`;
  };
  const countedSoon = (): void => {
    shown.counts = engine.counts;
    shown.base = { table: runner.baseTable, ...runner.counts };
    sayLater ??= setTimeout(say, Math.max(0, 500 - (performance.now() - saidAt)));
  };
  const showRows = (view: CubeView): void => {
    shown.epoch = view.snapshot.epoch;
    shown.columns = view.rows.columns.map((c) => c.name);
    shown.rows = Array.from({ length: view.rows.rowCount }, (_, r) => view.rows.columns.map((c) => {
      const v = c.values[r];
      return typeof v === 'bigint' ? v.toString() : v;
    }));
    shown.window = view.window ? { ...view.window } : null;
  };
  const app = new CubeApp(byId('cube'), snapshot, {
    runner,
    showColumnZone: true,
    onStatus: (text, kind) => { if (kind !== 'ok') said(text, kind, true); },
    onView: (view: CubeView) => {
      shown.views++;
      showRows(view);
      // the view on screen followed: its results kept live, the ones no view reads let go
      engine.follow(view.snapshot.epoch).then(() => {
        shown.followed = Math.max(shown.followed, view.snapshot.epoch);
        countedSoon();
      }, (e: unknown) => {
        shown.error = String(e instanceof Error ? e.message : e);
        said(`could not follow the view: ${shown.error}`, 'error');
      });
      countedSoon();
    },
    // a tick's view: shown, nothing followed anew (what the view reads, and what it reads since, is followed already)
    onTicked: (view: CubeView) => {
      showRows(view);
      countedSoon();
    },
  });
  await app.open();
  shown.status = 'live';
  said('live');

  // A TICK: run the cube again, one run at a time -- a tick that lands during a run is run after it
  let running = false;
  let pending: number | null = null;
  const tick = async (since: number): Promise<void> => {
    if (running || app.busy || shown.paused || shown.status !== 'live') {
      pending = Math.min(pending ?? since, since);
      return;
    }
    running = true;
    shown.refreshes++;
    const started = performance.now();
    await app.tick();
    const ran = performance.now();
    shown.runMs.push(ran - started);
    await frame();
    shown.tickToPaint.push(performance.now() - since);
    shown.tickParts.push([started - since, ran - started, performance.now() - ran]);
    if (shown.tickParts.length > 200) shown.tickParts.shift();
    if (shown.tickToPaint.length > 200) shown.tickToPaint.shift();
    if (shown.runMs.length > 200) shown.runMs.shift();
    running = false;
    countedSoon();
    if (pending !== null && !shown.paused) {
      const next = pending;
      pending = null;
      void tick(next);
    }
  };
  // THE SERVER WENT AWAY: said, and once it answers again, a new session and the cube run on it
  const reconnect = async (why: string): Promise<void> => {
    shown.status = 'reconnecting';
    shown.error = why;
    said('reconnecting: Deephaven is not answering', 'warn');
    for (;;) {
      await new Promise((r) => setTimeout(r, 1000));
      try {
        await engine.reconnect();
        break;
      } catch {
        // not back yet
      }
    }
    shown.reconnects++;
    // the cube run again before the page says it is live: a tick meanwhile waits, as the person does
    await app.refresh();
    shown.status = 'live';
    said('live');
  };
  engine.onLive((e) => {
    if (e.ended !== null) {
      if (shown.status !== 'reconnecting') void reconnect(e.ended);
      return;
    }
    if (e.failed !== undefined) {
      shown.error = e.failed;
      said(`a result will not tick: ${e.failed}`, 'error');
      return;
    }
    shown.ticks++;
    void tick(e.since);
  });

  // the test's gestures, as the commands the screen's chevrons, zones and filter editor make (grid-commands.ts)
  const gestures: Readonly<Record<string, readonly GridCommand[]>> = {
    expand: [{ kind: 'open', path: ['FX'], open: true }],
    collapse: [{ kind: 'open', path: ['FX'], open: false }],
    pivot: [{ kind: 'group', rows: ['desk'] }, { kind: 'pivot', columns: ['region'] }],
    regroup: [{ kind: 'group', rows: ['region', 'ccy'] }, { kind: 'pivot', columns: [] }],
    filter: [{ kind: 'filter', filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' } }],
    // the view the page opens with again: by desk and sector, no pivot, no filter (a benchmark's rounds start from it)
    reset: [{ kind: 'filter', filter: null }, { kind: 'group', rows: ['desk', 'sector'] }, { kind: 'pivot', columns: [] }],
    // every position, grouped by nothing, the largest market value first: a flat grid of the whole book (V1)
    flat: [{ kind: 'group', rows: [] }, { kind: 'pivot', columns: [] }, { kind: 'sorts', sorts: [{ column: 'mv', direction: 'desc' }] }],
  };
  // a leaf-level calculated column, as the column editor adds one: the person's expression, parsed
  /** A calculated column, `mvk`: the market value in thousands, or what `text` says. */
  const calc = async (text = 'x|$x.mv / 1000.0'): Promise<readonly GridCommand[]> => [{
    kind: 'calculate', level: 'row',
    column: { name: 'mvk', lambda: await planner.parse(text), kind: 'measure', aggregate: 'sum' },
  }];
  shown.scroll = async (row: number): Promise<number> => {
    const scroller = document.querySelector<HTMLElement>('#cube .dc-scroller');
    if (!scroller) throw new Error('the grid has no scroller');
    const started = performance.now();
    scroller.scrollTop = row * ROW_HEIGHT;
    for (;;) {
      await frame();
      const w = shown.window;
      if (w !== null && row >= w.offset && row < w.offset + shown.rows.length) return performance.now() - started;
      if (performance.now() - started > 10_000) throw new Error(`row ${row} not shown in 10 s: ${JSON.stringify(w)}`);
    }
  };
  shown.edit = async (commands) => (await app.edit(commands, 'benchmark')) ?? 'applied';
  shown.change = async (what: string): Promise<string> => {
    // `calc:<lambda>`: a calculated column of the walk-through's own text
    const commands = what === 'calc' ? await calc() : what.startsWith('calc:') ? await calc(what.slice(5)) : gestures[what];
    if (commands === undefined) return `no gesture '${what}'`;
    return (await app.edit(commands, what)) ?? 'applied';
  };
}

main().catch((e) => {
  shown.status = 'failed';
  shown.error = String(e instanceof Error ? e.stack ?? e.message : e);
  said(`failed to start: ${e instanceof Error ? e.message : String(e)}`, 'error');
});
