// THE DATACUBE ON DEEPHAVEN (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23): DataCube's own app -- its grid, zones, menus,
// filters, pivots and totals, unchanged -- planning in this tab with legend-lite's WebAssembly planner on a Deephaven
// runtime, and running on the Deephaven server itself (engine-client's DeephavenEngine): no server of legend-lite's in
// between. Every query the cube sends over the risk source reads the source's tables the session made for the first
// one, so a regroup, a repivot or a filter sends only what differs. When a result the grid shows ticks on the server,
// the page re-runs the cube (`refresh`): the plans are the same, nothing is sent, and the rows are read again -- a
// stand-in for DataCube's own ticking mode (L4), which will match ticked rows into the grid by their group key. When
// the server goes away (a restart: Deephaven keeps no session across one), the page says so, signs in again once the
// server answers, and re-runs the cube, which makes the source's tables again on the new session.

import { CubeApp } from '../../src/app.ts';
import type { CubeView } from '../../src/cube.ts';
import { LitePlanner } from '../../src/lite-planner.ts';
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

/** How often, at most, a tick re-runs the cube (the server's cycle is 100 ms). */
const TICK_MS = 250;

/** What the page shows, for a person and for its test. */
interface Shown {
  status: 'starting' | 'live' | 'reconnecting' | 'failed';
  error: string;
  views: number;
  reconnects: number;
  /** Ticks the server sent for results the engine read, those of the view on screen, and the re-runs they made. */
  ticks: number;
  onScreen: number;
  refreshes: number;
  counts: DeephavenCounts | null;
  /** The view on screen: its epoch, its columns, and its rows' values. */
  epoch: number;
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  /** Set, a tick re-runs nothing: a test measures a change's own queries. */
  paused: boolean;
  /** The gestures the test makes, as the app's own changes (the zones' and the filter editor's). */
  change?: (what: string) => Promise<string>;
}
const shown: Shown = {
  status: 'starting', error: '', views: 0, reconnects: 0, ticks: 0, onScreen: 0, refreshes: 0, counts: null, epoch: 0, columns: [], rows: [],
  paused: false,
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
      shown.counts = engine.counts;
      const c = engine.counts;
      byId('counts').textContent = `${c.queries} queries, ${c.planned} operations planned, ${c.sent} sent`
        + ` (${c.planned - c.sent} read from what the session held)`;
    },
  });
  await app.open();
  shown.status = 'live';
  said('live');

  // A TICK: a result on screen changed on the server -- the cube runs again, reading what the session holds
  let last = 0;
  let waiting: ReturnType<typeof setTimeout> | null = null;
  const rerun = (): void => {
    waiting = null;
    if (app.busy || shown.paused || shown.status !== 'live') return;
    last = performance.now();
    shown.refreshes++;
    void app.state.refresh();
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
    const out = await app.state.refresh();
    if (out.kind === 'refused') {
      void reconnect(String(out.error));
      return;
    }
    shown.status = 'live';
    said('live');
  };
  engine.onTick((ticked) => {
    if (ticked.ended !== null) {
      if (shown.status !== 'reconnecting') void reconnect(ticked.ended);
      return;
    }
    if (ticked.failures.length > 0) {
      shown.status = 'failed';
      shown.error = ticked.failures.join('; ');
      said(`the server says: ${shown.error}`, 'error');
      return;
    }
    shown.ticks++;
    if (!ticked.epochs.has(shown.epoch)) return;
    shown.onScreen++;
    if (waiting !== null) return;
    waiting = setTimeout(rerun, Math.max(0, TICK_MS - (performance.now() - last)));
  });

  // the test's gestures: what dragging a column to a zone, or applying a filter, changes
  shown.change = async (what: string): Promise<string> => {
    const out = await app.change((s) => {
      const snap = s.snapshot;
      const next: CubeSnapshot = what === 'regroup' ? { ...snap, rows: ['region', 'ccy'], pivotOn: [] }
        : what === 'pivot' ? { ...snap, rows: ['desk'], pivotOn: ['region'] }
        : what === 'filter' ? { ...snap, filter: { kind: 'condition', column: 'desk', operator: 'equal', value: 'FX' } }
        : snap;
      return { ...s, snapshot: next };
    });
    return out.kind;
  };
}

main().catch((e) => {
  shown.status = 'failed';
  shown.error = String(e instanceof Error ? e.stack ?? e.message : e);
  said(`failed to start: ${e instanceof Error ? e.message : String(e)}`, 'error');
});
