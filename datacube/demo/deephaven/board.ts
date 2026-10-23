// A DATACUBE PAGE OF SEVERAL GRIDS AND CHARTS ON DEEPHAVEN (docs/DEEPHAVEN_PLAN_2026_10_22.md V2, V3, DH27): DataCube's
// own page -- its bar, its tiles, its grids and charts -- over the demo's Deephaven server, every grid reading the risk
// source through one engine: one session, one live reader, one base on the server for the three grids. The book flat,
// the largest market value first (read by a window: V1); the desks pivoted by region; the sectors by currency; and a
// chart following each pivoted grid. Each grid has its own reader (`DeephavenEngine.reader()`): it follows what its
// views and charts read, and a change to any of it runs that grid again from the live rows and draws its charts again
// -- nothing sent, nothing fetched. The page says each grid's and each chart's time from a tick's rows to the screen.

import { getInstanceByDom } from 'echarts/core';
import { CubeApp } from '../../src/app.ts';
import type { CubeView } from '../../src/cube.ts';
import { DEFAULT_CONFIGURATION } from '../../src/config.ts';
import { defaultChart } from '../../src/chart-spec.ts';
import { PageApp, type GridMaker } from '../../src/page/page-app.ts';
import { ServerBases } from '../../src/server-base.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import type { DeephavenCounts } from '../../../engine-client/src/deephaven-engine.ts';
import { CONNECTION, STORES, openRisk, type RiskConfig } from './risk.ts';

/** One grid as the page shows it, for a person and for its test. */
interface ShownGrid {
  views: number;
  ticks: number;
  /** Each tick's time from its rows arriving to the grid painted, and the cube's run in it, ms (the last 200). */
  tickToPaint: number[];
  runMs: number[];
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  window: { offset: number; total: number } | null;
}

/** One chart as the page shows it: how many times it drew after a tick, each one's time from that tick's rows, and the
 *  values its first series draws now. */
interface ShownChart {
  grid: string;
  draws: number;
  tickToDraw: number[];
  values: readonly unknown[];
}

interface Shown {
  status: 'starting' | 'live' | 'failed';
  error: string;
  grids: Record<string, ShownGrid>;
  charts: Record<string, ShownChart>;
  counts: DeephavenCounts | null;
  /** The bases the page's grids read, kept on the server. */
  bases: readonly string[];
  /** Set, a tick runs nothing. */
  paused: boolean;
}
const shown: Shown = { status: 'starting', error: '', grids: {}, charts: {}, counts: null, bases: [], paused: false };
(window as unknown as { __board: Shown }).__board = shown;

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
const keep = (xs: number[], x: number): void => {
  xs.push(x);
  if (xs.length > 200) xs.shift();
};

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as RiskConfig;
  const { model, planner, engine, cube } = await openRisk(config);
  // the page's bases: one per source and its calculated columns, whichever grids read it (V2)
  const bases = new ServerBases(planner, engine, { model, connection: CONNECTION, stores: STORES });
  const counted = (): void => {
    shown.counts = engine.counts;
    shown.bases = bases.tables;
  };
  /** When each grid's last tick's rows arrived: what its charts' draws are timed from. */
  const tickedAt = new Map<string, number>();

  /** A grid on the page: its own reader, its view followed when it lands, its ticks run one at a time. */
  const grid = (name: string, snapshot: CubeSnapshot): GridMaker => (host, spawned, start) => {
    const reader = engine.reader();
    const g: ShownGrid = { views: 0, ticks: 0, tickToPaint: [], runMs: [], columns: [], rows: [], window: null };
    shown.grids[name] = g;
    const show = (view: CubeView): void => {
      g.columns = view.rows.columns.map((c) => c.name);
      g.rows = Array.from({ length: view.rows.rowCount }, (_, r) => view.rows.columns.map((c) => {
        const v = c.values[r];
        return typeof v === 'bigint' ? v.toString() : v;
      }));
      g.window = view.window ? { ...view.window } : null;
    };
    const app = new CubeApp(host, start?.snapshot ?? snapshot, {
      runner: bases.runner(reader),
      compact: true,
      configuration: start?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
      ...spawned,
      onView: (view) => {
        g.views++;
        show(view);
        // the view that landed followed: what it read, and what its charts read from it
        reader.follow(view.snapshot.epoch).then(counted, (e: unknown) => {
          shown.error = String(e instanceof Error ? e.message : e);
          said(`${name} could not follow its view: ${shown.error}`, 'error');
        });
        counted();
      },
      onTicked: show,
    });
    let running = false;
    let pending: number | null = null;
    const tick = async (since: number): Promise<void> => {
      if (running || app.busy || shown.paused || shown.status !== 'live') {
        pending = Math.min(pending ?? since, since);
        return;
      }
      running = true;
      g.ticks++;
      tickedAt.set(name, since);
      const started = performance.now();
      await app.tick();
      keep(g.runMs, performance.now() - started);
      await frame();
      keep(g.tickToPaint, performance.now() - since);
      running = false;
      if (pending !== null && !shown.paused) {
        const next = pending;
        pending = null;
        void tick(next);
      }
    };
    reader.onLive((e) => {
      if (e.ended !== null) {
        shown.status = 'failed';
        shown.error = e.ended;
        said(`the server went away: ${e.ended}`, 'error');
        return;
      }
      if (e.failed !== undefined) {
        shown.error = e.failed;
        said(`${name}: a result will not tick: ${e.failed}`, 'error');
        return;
      }
      void tick(e.since);
    });
    return app;
  };

  const flat: CubeSnapshot = { ...cube([]), sorts: [{ column: 'mv', direction: 'desc' }] };
  const desks = cube(['desk'], ['region']);
  const sectors = cube(['sector'], ['ccy']);
  const page = new PageApp({
    host: byId('cube'),
    title: 'Risk',
    empty: (slot) => { slot.textContent = 'No grids'; },
  });
  const ids = {
    Book: page.addGrid(grid('Book', flat), { title: 'Book' }),
    Desks: page.addGrid(grid('Desks', desks), { title: 'Desks' }),
    Sectors: page.addGrid(grid('Sectors', sectors), { title: 'Sectors' }),
  };
  await page.ready();

  // A CHART FOLLOWING EACH PIVOTED GRID: its market value by the grid's rows, drawn again at each of the grid's ticks
  for (const [n, [name, snapshot]] of ([['Desks', desks], ['Sectors', sectors]] as const).entries()) {
    const spec = defaultChart(snapshot);
    if (spec === null) throw new Error(`no chart of ${name}`);
    // a chart's id is the page's kind: chart-N
    const id = `chart-${n + 1}`;
    await page.follow([{
      kind: 'addChart', id, near: ids[name], spec: { ...spec, y: [{ column: 'mv', fn: 'sum' }] },
      title: `${name}: market value`, columns: null,
    }], new Map());
    shown.charts[id] = { grid: name, draws: 0, tickToDraw: [], values: [] };
  }
  // each chart's drawing watched: its values, and its time from its grid's tick
  const watch = (): boolean => {
    let all = true;
    for (const [id, c] of Object.entries(shown.charts)) {
      const el = document.querySelector<HTMLElement>(`[data-tile="${id}"] .dc-chartpanel-chart`);
      const chart = el ? getInstanceByDom(el) : undefined;
      if (!chart) {
        all = false;
        continue;
      }
      if ((chart as unknown as { __watched?: boolean }).__watched) continue;
      (chart as unknown as { __watched?: boolean }).__watched = true;
      chart.on('finished', () => {
        const series = (chart.getOption() as { series?: { data?: unknown[] }[] }).series ?? [];
        c.values = (series[0]?.data ?? []).map((d) => (Array.isArray(d) ? d[d.length - 1] : (d as { value?: unknown })?.value ?? d));
        const since = tickedAt.get(c.grid);
        if (since === undefined) return;
        c.draws++;
        keep(c.tickToDraw, performance.now() - since);
      });
    }
    return all;
  };
  for (let i = 0; i < 100 && !watch(); i++) await new Promise((r) => setTimeout(r, 100));
  counted();
  shown.status = 'live';
  said('live');
  setInterval(counted, 500);
}

main().catch((e) => {
  shown.status = 'failed';
  shown.error = String(e instanceof Error ? e.stack ?? e.message : e);
  said(`failed to start: ${e instanceof Error ? e.message : String(e)}`, 'error');
});
