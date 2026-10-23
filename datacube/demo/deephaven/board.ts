// A DATACUBE PAGE OF SEVERAL GRIDS AND CHARTS ON DEEPHAVEN (docs/DEEPHAVEN_PLAN_2026_10_22.md V2, V3, DH27): DataCube's
// own page -- its bar, its tiles, its grids and charts -- over the demo's Deephaven server, every grid reading the risk
// source through one engine: one session, one live reader, one base on the server for the three grids. The book flat,
// the largest market value first (read by a window: V1); the desks pivoted by region; the sectors by currency; and a
// chart following each pivoted grid. Each grid has its own reader (`DeephavenEngine.reader()`): it follows what its
// views and charts read, and a change to any of it runs that grid again from the live rows and draws its charts again
// -- nothing sent, nothing fetched. The page says each grid's and each chart's time from a tick's rows to the screen.
//
// THE PAGE, WHOLE (B2): saved and opened again -- its menu's Save writes it down in this browser, each grid's source
// the risk query by its text, and Open puts it back, each grid made again over the source as it is now -- and when the
// server restarts, every grid and chart comes back by itself: a new session, each grid run again, its base made again.
// A grid that goes lets its reader and its base go. Each chart's value axis is held while it ticks (C2): the page says
// the extent it drew it at, each draw.

import { getInstanceByDom } from 'echarts/core';
import { CubeApp } from '../../src/app.ts';
import type { CubeView } from '../../src/cube.ts';
import { openCube } from '../../src/cube-document.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../../src/config.ts';
import { defaultChart } from '../../src/chart-spec.ts';
import { pageToJson, readPage } from '../../src/page-document.ts';
import { PageApp, type GridMaker } from '../../src/page/page-app.ts';
import { ServerBases } from '../../src/server-base.ts';
import type { CubeSnapshot } from '../../src/snapshot.ts';
import type { TreeState } from '../../src/tree.ts';
import type { DeephavenCounts } from '../../../engine-client/src/deephaven-engine.ts';
import { CONNECTION, STORES, isRiskSource, openRisk, type RiskConfig } from './risk.ts';

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

/** One chart as the page shows it: how many times it drew after a tick, each one's time from that tick's rows, the
 *  values its first series draws now, and the extent its value axis was drawn at, each draw (the last 200). */
interface ShownChart {
  grid: string;
  draws: number;
  tickToDraw: number[];
  values: readonly unknown[];
  axes: [number, number][];
}

interface Shown {
  status: 'starting' | 'live' | 'reconnecting' | 'failed';
  error: string;
  /** Sessions signed in again after the server went away, and pages opened from a saved one. */
  reconnects: number;
  opened: number;
  grids: Record<string, ShownGrid>;
  charts: Record<string, ShownChart>;
  counts: DeephavenCounts | null;
  /** The bases the page's grids read, kept on the server. */
  bases: readonly string[];
  /** Set, a tick runs nothing. */
  paused: boolean;
  /** The page written down, as its Save writes it; and a page so written opened in its place, as its Open does. */
  save?: () => string;
  open?: (saved: string) => Promise<void>;
}
const shown: Shown = {
  status: 'starting', error: '', reconnects: 0, opened: 0, grids: {}, charts: {}, counts: null, bases: [], paused: false,
};
/** Where Save keeps the page: this browser's storage. */
const SAVED = 'legend-lite.deephaven.board';
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
const keep2 = (xs: [number, number][], x: [number, number]): void => {
  xs.push(x);
  if (xs.length > 200) xs.shift();
};

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as RiskConfig;
  const { model, planner, engine, cube, query, columns, source } = await openRisk(config);
  // the page's bases: one per source and its calculated columns, whichever grids read it (V2)
  const bases = new ServerBases(planner, engine, { model, connection: CONNECTION, stores: STORES });
  const counted = (): void => {
    shown.counts = engine.counts;
    shown.bases = bases.tables;
  };
  /** When each grid's last tick's rows arrived: what its charts' draws are timed from. */
  const tickedAt = new Map<string, number>();

  /** Each grid's app on the page now, by its name: what a reconnect runs again. */
  const apps = new Map<string, CubeApp>();

  /** A grid on the page: its own reader, its view followed when it lands, its ticks run one at a time; let go, its
   *  reader and its base go with it. `opened`: where a saved grid starts, its configuration and its open groups. */
  const grid = (name: string, snapshot: CubeSnapshot,
    opened?: { readonly configuration: CubeConfiguration; readonly tree: TreeState }): GridMaker => (host, spawned, start) => {
    const reader = engine.reader();
    const runner = bases.runner(reader);
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
    const tree = start?.tree ?? opened?.tree;
    const app = new CubeApp(host, start?.snapshot ?? snapshot, {
      runner,
      compact: true,
      // the grid writes its source down when the page is saved: the risk query, by its text
      cubeSource: source,
      configuration: start?.configuration ?? opened?.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
      ...(tree ? { tree } : {}),
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
      onDispose: () => {
        if (apps.get(name) === app) apps.delete(name);
        runner.close();
        void reader.close();
      },
    });
    apps.set(name, app);
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
        if (shown.status !== 'reconnecting') void reconnect(e.ended);
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

  // THE SERVER WENT AWAY: said, and once it answers again, a new session and every grid run on it -- its base made
  // again, its view followed again, its charts drawn from it
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
    // every grid run again before the page says it is live: a tick meanwhile waits, as the person does
    await Promise.all([...apps.values()].map((app) => app.refresh()));
    counted();
    shown.status = 'live';
    said('live');
  };

  const flat: CubeSnapshot = { ...cube([]), sorts: [{ column: 'mv', direction: 'desc' }] };
  const desks = cube(['desk'], ['region']);
  const sectors = cube(['sector'], ['ccy']);
  const page: PageApp = new PageApp({
    host: byId('cube'),
    title: 'Risk',
    empty: (slot) => { slot.textContent = 'No grids'; },
    // the page kept in this browser: Save writes it down, Open puts it back
    hostMenu: () => [
      { id: 'host.save' as const, label: 'Save', section: 'file' as const },
      { id: 'host.open' as const, label: 'Open', section: 'file' as const },
    ],
    onHostMenu: (item) => {
      if (item.id === 'host.save') {
        try {
          localStorage.setItem(SAVED, save());
          said('saved in this browser');
        } catch (e) {
          said(`could not save: ${e instanceof Error ? e.message : String(e)}`, 'error');
        }
      } else if (item.id === 'host.open') {
        const saved = localStorage.getItem(SAVED);
        if (saved === null) said('nothing saved in this browser', 'warn');
        else open(saved).catch((e: unknown) => said(`could not open: ${e instanceof Error ? e.message : String(e)}`, 'error'));
      }
    },
  });

  /** The page written down: its grids (each its source, its query, its configuration), its charts and its layout. */
  const save = (): string => {
    const refused = page.saveRefusal();
    if (refused !== undefined) throw new Error(refused);
    const doc = page.document(page.title);
    if (doc === undefined) throw new Error('the page could not write itself down');
    return pageToJson(doc);
  };

  /** Each chart on the page watched as it draws: its values, its value axis, and its time from its grid's tick. */
  const watchCharts = async (): Promise<void> => {
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
          const axis = (chart as unknown as { getModel(): { getComponent(t: string, i: number): { axis?: { scale?: { getExtent(): number[] } } } } })
            .getModel().getComponent('yAxis', 0)?.axis?.scale?.getExtent();
          if (axis && axis.length === 2) keep2(c.axes, [axis[0]!, axis[1]!]);
          const since = tickedAt.get(c.grid);
          if (since === undefined) return;
          c.draws++;
          keep(c.tickToDraw, performance.now() - since);
        });
      }
      return all;
    };
    for (let i = 0; i < 100 && !watch(); i++) await new Promise((r) => setTimeout(r, 100));
  };

  /** A saved page opened in this one's place: each grid of the risk source made again over it as it is now, under its
   *  saved id and title, its charts and layout as saved; a grid of another source is left out, and said. */
  const open = async (saved: string): Promise<void> => {
    const doc = readPage(saved);
    const makers = new Map<string, GridMaker>();
    const titles = new Map(doc.views.flatMap((v) => (v.kind === 'grid' && v.title ? [[v.cube, v.title] as const] : [])));
    const notes: string[] = [];
    for (const { id, cube: kept } of doc.cubes) {
      if (!isRiskSource(kept.source)) {
        notes.push(`${kept.source.name} is not this page's source: left out`);
        continue;
      }
      const opened = openCube(kept, { query }, columns);
      notes.push(...opened.notes);
      makers.set(id, grid(titles.get(id) ?? opened.configuration.reportTitle ?? id, opened.snapshot, opened));
    }
    shown.status = 'starting';
    shown.grids = {};
    shown.charts = {};
    tickedAt.clear();
    page.restore(doc, makers);
    await page.ready();
    for (const v of doc.views) {
      if (v.kind !== 'chart') continue;
      shown.charts[v.id] = { grid: titles.get(v.cube) ?? v.cube, draws: 0, tickToDraw: [], values: [], axes: [] };
    }
    await watchCharts();
    shown.opened++;
    counted();
    shown.status = 'live';
    said(notes.length > 0 ? `opened: ${notes.join('; ')}` : 'opened', notes.length > 0 ? 'warn' : 'ok');
  };
  shown.save = save;
  shown.open = open;

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
    shown.charts[id] = { grid: name, draws: 0, tickToDraw: [], values: [], axes: [] };
  }
  await watchCharts();
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
