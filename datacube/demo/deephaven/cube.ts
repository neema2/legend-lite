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
import { LitePlanner } from '../../src/lite-planner.ts';
import type { GridCommand } from '../../src/grid-commands.ts';
import type { ColumnSpec, CubeSnapshot } from '../../src/snapshot.ts';
import { DeephavenEngine, type DeephavenCounts } from '../../../engine-client/src/deephaven-engine.ts';
import { MODEL } from './model.ts';

const RUNTIME = 'risk::LiveRuntime';
const JOIN = 'meta::pure::functions::relation::JoinKind.LEFT';
/** The cube's source: every position, with its book, instrument, live price and fx rate, valued in dollars. */
const SOURCE = `#>{risk::Store.position}#`
  + `->join(#>{risk::Store.book}#, ${JOIN}, {p, b | $p.book_id == $b.bid})`
  + `->join(#>{risk::Store.instrument}#, ${JOIN}, {p, i | $p.inst_id == $i.iid})`
  + `->join(#>{risk::Store.price}#, ${JOIN}, {p, q | $p.inst_id == $q.px_iid})`
  + `->join(#>{risk::Store.fx}#, ${JOIN}, {p, f | $p.ccy == $f.fx_ccy})`
  + `->extend(~[mv:x|$x.qty * $x.px * $x.rate])`;

/** The columns the cube shows, and how each aggregates. */
const SHOWN: Readonly<Record<string, ColumnSpec['aggregate'] | null>> = {
  desk: null, sector: null, region: null, ccy: null, inst_id: null, pid: 'count', qty: 'sum', px: 'average', mv: 'sum',
};

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
  counts: DeephavenCounts | null;
  /** The view on screen: its epoch, its columns, and its rows' values. */
  epoch: number;
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  /** Set, a tick runs nothing: a test measures a change's own queries. */
  paused: boolean;
  /** The gestures the test makes, as the app's own changes (the zones' and the filter editor's). */
  change?: (what: string) => Promise<string>;
}
const shown: Shown = {
  status: 'starting', error: '', views: 0, reconnects: 0, ticks: 0, refreshes: 0, tickToPaint: [], runMs: [], counts: null,
  epoch: 0, columns: [], rows: [], paused: false,
};
(window as unknown as { __cube: Shown }).__cube = shown;

const byId = (id: string): HTMLElement => {
  const e = document.getElementById(id);
  if (!e) throw new Error(`the page has no #${id}`);
  return e;
};

function said(text: string, kind: 'ok' | 'warn' | 'error' = 'ok'): void {
  const e = byId('status');
  e.textContent = text;
  e.className = kind;
}

const frame = (): Promise<void> => new Promise((r) => { requestAnimationFrame(() => r()); });
const median = (xs: readonly number[]): number => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)] ?? NaN;

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const at = new URL(config.url);
  const planner = new LitePlanner({ model: MODEL(at.hostname, Number(at.port)), runtime: RUNTIME, assetBaseUrl: './' });
  await planner.warmUp();
  const engine = await DeephavenEngine.open({
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  }, (op, args) => planner.deephaven(op, args));
  const source = await planner.parse(SOURCE);
  const query = source.body[0];
  if (query === undefined) throw new Error('the risk source parsed to no expression');
  const types = new Map((await planner.relationType(source)).map((c) => [c.name, c.type]));
  const columns: ColumnSpec[] = Object.entries(SHOWN).map(([name, aggregate]) => {
    const type = types.get(name);
    if (type === undefined) throw new Error(`the risk source has no column '${name}'`);
    return aggregate ? { name, type, aggregate } : { name, type };
  });
  const snapshot: CubeSnapshot = {
    source: { query },
    columns,
    derived: [],
    rows: ['desk', 'sector'],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };

  const counted = (): void => {
    const c = engine.counts;
    shown.counts = c;
    const paint = shown.tickToPaint.length > 0
      ? `; a tick on screen ${median(shown.tickToPaint).toFixed(1)} ms after its rows arrived`
      : '';
    byId('counts').textContent = `${c.following} results live, ${c.fetched} fetched, ${c.live} answered live; `
      + `${c.planned} operations planned, ${c.sent} sent${paint}`;
  };
  const app = new CubeApp(byId('cube'), snapshot, {
    engine,
    planner,
    showColumnZone: true,
    onStatus: (text, kind) => { if (kind !== 'ok') said(text, kind); },
    onView: (view: CubeView) => {
      shown.views++;
      shown.epoch = view.snapshot.epoch;
      shown.columns = view.rows.columns.map((c) => c.name);
      shown.rows = Array.from({ length: view.rows.rowCount }, (_, r) => view.rows.columns.map((c) => {
        const v = c.values[r];
        return typeof v === 'bigint' ? v.toString() : v;
      }));
      // the view on screen followed: its results kept live, the ones no view reads let go
      engine.follow(view.snapshot.epoch).then(counted, (e: unknown) => {
        shown.error = String(e instanceof Error ? e.message : e);
        said(`could not follow the view: ${shown.error}`, 'error');
      });
      counted();
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
    shown.runMs.push(performance.now() - started);
    await frame();
    shown.tickToPaint.push(performance.now() - since);
    if (shown.tickToPaint.length > 200) shown.tickToPaint.shift();
    if (shown.runMs.length > 200) shown.runMs.shift();
    running = false;
    counted();
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
    shown.status = 'live';
    await app.refresh();
    said('live');
  };
  engine.onLive((e) => {
    if (e.ended !== null) {
      if (shown.status !== 'reconnecting') void reconnect(e.ended);
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
  };
  shown.change = async (what: string): Promise<string> => {
    const commands = gestures[what];
    if (commands === undefined) return `no gesture '${what}'`;
    return (await app.edit(commands, what)) ?? 'applied';
  };
}

main().catch((e) => {
  shown.status = 'failed';
  shown.error = String(e instanceof Error ? e.stack ?? e.message : e);
  said(`failed to start: ${e instanceof Error ? e.message : String(e)}`, 'error');
});
