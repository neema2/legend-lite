// THE TRADER SCREENS, OPENED IN DATACUBE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13): DataCube's own page --
// its bar, its sheets (tabs), its grids and charts -- over the market world's Deephaven, each screen a saved page
// (screens.ts) opened from the page's menu (Trader screens ▸) as any saved page is opened: each grid made again over its
// source as it is now -- a saved query over the one trading model's classes, typed by the tab's planner -- and its
// charts and sheets as saved. Every grid has its own reader (`DeephavenEngine.reader()`): it follows what its views and
// charts read, and a tick runs that grid again from the live rows and draws its charts again. Save and Open keep the
// page as it is now in this browser: a screen moved about, a grid regrouped, a chart added, all kept.
//
// THE GRIDS READ THEIR SOURCES AS WRITTEN (the plan's ledger, §3): no base kept on the server (src/server-base.ts B1).
// A base re-declares the planner's model, and the trading model takes 4.4 s to hold (scripts/corpus/trading_model.py):
// a page of five grids would hold it six times before it showed. The fix is the planner's (a base's Database held over
// the held model, not in its text); until then each query joins its own way to the classes it reads.

import { getInstanceByDom } from 'echarts/core';
import { CubeApp } from '../../../src/app.ts';
import type { CubeView } from '../../../src/cube.ts';
import { tiles } from '../../../src/layout/bands.ts';
import { openCube } from '../../../src/cube-document.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../../../src/config.ts';
import { LitePlanner } from '../../../src/lite-planner.ts';
import { pageToJson, readPage, type PageDocument } from '../../../src/page-document.ts';
import { PageApp, type GridMaker } from '../../../src/page/page-app.ts';
import { PlanThenRun } from '../../../src/runner.ts';
import type { ColumnSpec, CubeSnapshot } from '../../../src/snapshot.ts';
import type { TreeState } from '../../../src/tree.ts';
import type { MenuItem } from '../../../src/ui/menu.ts';
import type { ValueSpecification } from '../../../../pure-protocol/src/index.ts';
import { DeephavenEngine, type DeephavenCounts } from '../../../../engine-client/src/deephaven-engine.ts';
import { MARKET_MODEL, MARKET_RUNTIME } from './model.ts';
import { TRADER_PAGES, isMarketSource, type TraderPage } from './screens.ts';

/** One grid as the page shows it, for a person and for its test. */
interface ShownGrid {
  views: number;
  ticks: number;
  /** Each tick's time from its rows arriving to the grid painted (the last 200), ms. */
  tickToPaint: number[];
  columns: readonly string[];
  rows: readonly (readonly unknown[])[];
  window: { offset: number; total: number } | null;
  /** Its first view's time from the page's opening, ms. */
  firstViewMs: number | null;
  /** The queries whose rows were read from the server after the grid's first view, not from rows it follows: what a
   *  tick should never do (the last 20, each its plan's first 300 characters). */
  fetched: string[];
}

/** One chart as the page shows it: its draws after a tick, and what it draws now -- its first series' values, or a
 *  KPI's figure. */
interface ShownChart {
  grid: string;
  kind: string;
  draws: number;
  tickToDraw: number[];
  values: readonly unknown[];
}

interface Shown {
  status: 'starting' | 'opening' | 'live' | 'reconnecting' | 'failed';
  error: string;
  /** The screen on the page now, by its id (a page opened from this browser: 'saved'). */
  screen: string;
  /** The screens the menu opens. */
  screens: readonly { readonly id: string; readonly label: string }[];
  reconnects: number;
  opened: number;
  /** How long the screen last opened took, from its menu entry to its last chart drawn, ms; and by step. */
  openMs: number;
  openSteps: Record<string, number>;
  /** The page's start, by step, ms: the model fetched, the planner's module loaded, the server signed in to, the first
   *  source typed (the planner holding the trading model), the first screen written and opened. */
  startup: Record<string, number>;
  /** The planner's calls, by kind: how many, and their time from asked to answered (overlapping calls each counted). */
  planner: Record<string, { calls: number; ms: number }>;
  grids: Record<string, ShownGrid>;
  charts: Record<string, ShownChart>;
  counts: DeephavenCounts | null;
  paused: boolean;
  /** The screen `id` opened, as its menu entry opens it. */
  openScreen?: (id: string) => Promise<void>;
  /** The page written down, as Save writes it; and a page so written opened, as Open does. */
  save?: () => string;
  open?: (saved: string) => Promise<void>;
  /** The page's sheets (its tabs) by name, and the one shown: a sheet shown by name. */
  sheets?: () => { readonly names: readonly string[]; readonly shown: string };
  showSheet?: (name: string) => void;
}
const shown: Shown = {
  status: 'starting', error: '', screen: '', screens: TRADER_PAGES.map((p) => ({ id: p.id, label: p.label })),
  reconnects: 0, opened: 0, openMs: 0, openSteps: {}, startup: {}, planner: {}, grids: {}, charts: {}, counts: null, paused: false,
};
(window as unknown as { __traders: Shown }).__traders = shown;
/** Where Save keeps the page: this browser's storage. */
const SAVED = 'legend-lite.deephaven.traders';

const byId = (id: string): HTMLElement => {
  const e = document.getElementById(id);
  if (!e) throw new Error(`the page has no #${id}`);
  return e;
};
let passing: ReturnType<typeof setTimeout> | undefined;
/** What the header says: the page's state, or a passing message said for eight seconds. */
function said(text: string, kind: 'ok' | 'warn' | 'error' = 'ok', passes = false): void {
  const e = byId('status');
  e.textContent = text;
  e.className = kind;
  clearTimeout(passing);
  if (passes) passing = setTimeout(() => { if (shown.status === 'live') said('live'); }, 8000);
}
const frame = (): Promise<void> => new Promise((r) => { requestAnimationFrame(() => r()); });
const keep = (xs: number[], x: number): void => {
  xs.push(x);
  if (xs.length > 200) xs.shift();
};

/** The market world's server as the page reaches it: its address and its (test) key, from the page's config.json. */
interface MarketConfig {
  readonly url: string;
  readonly psk: string;
}

async function main(): Promise<void> {
  let step = performance.now();
  const took = (what: string): void => {
    const now = performance.now();
    shown.startup[what] = Math.round(now - step);
    step = now;
  };
  const config = await (await fetch('config.json')).json() as MarketConfig;
  const corpus = await (await fetch('trading-model.pure')).text();
  took('model fetched');
  const at = new URL(config.url);
  const model = MARKET_MODEL(corpus, at.hostname, Number(at.port));
  const planner = new LitePlanner({ model, runtime: MARKET_RUNTIME, assetBaseUrl: './' });
  // each of the planner's calls timed, by kind: where a screen's opening goes
  for (const kind of ['plan', 'planText', 'relationType', 'relationTypeText', 'nullable', 'parse', 'deephaven', 'print'] as const) {
    const call = (planner[kind] as (...args: unknown[]) => Promise<unknown>).bind(planner);
    (planner as unknown as Record<string, unknown>)[kind] = async (...args: unknown[]) => {
      const t0 = performance.now();
      try {
        return await call(...args);
      } finally {
        const e = shown.planner[kind] ??= { calls: 0, ms: 0 };
        e.calls++;
        e.ms = Math.round(e.ms + performance.now() - t0);
      }
    };
  }
  await planner.warmUp();
  took('planner loaded');
  // the server signed in to while the planner holds the trading model: neither waits for the other
  const opening = DeephavenEngine.open({
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  }, (op, args) => planner.deephaven(op, args));
  // the trading model held now, by typing a small query over it: every source's typing after it is quick
  const holding = planner.relationTypeText('marketdata::MarketQuote.all()->project(~[q:x|$x.quoteId])');
  const engine = await opening;
  took('signed in');
  await holding;
  took('model held');
  addEventListener('pagehide', () => { engine.close().catch(() => { /* the page is gone */ }); });
  // the engine's counts as they are when read: a test's two readings bracket exactly what happened between them
  Object.defineProperty(shown, 'counts', { get: () => engine.counts, enumerable: true });

  /** A source's query and its columns as the compiler types them, by its text: asked once a text. */
  const typed = new Map<string, Promise<{ readonly query: ValueSpecification; readonly columns: readonly ColumnSpec[] }>>();
  const typeOf = (text: string): Promise<{ readonly query: ValueSpecification; readonly columns: readonly ColumnSpec[] }> => {
    let t = typed.get(text);
    if (t === undefined) {
      t = (async () => {
        const lambda = await planner.parse(text);
        const query = lambda.body[0];
        if (query === undefined) throw new Error(`a screen's source parsed to no expression: ${text}`);
        // typed over the model the planner holds (relationType's pure/v1 route compiles the model's text again: 2.7 s
        // a source for the trading model, the task's to fix in the planner)
        const columns = (await planner.relationTypeText(text)).map((c) => ({ name: c.name, type: c.type }));
        return { query, columns };
      })();
      typed.set(text, t);
      t.catch(() => { typed.delete(text); });
    }
    return t;
  };

  const tickedAt = new Map<string, number>();
  const apps = new Map<string, CubeApp>();
  /** When the screen on the page began opening: each grid's first view is timed from it. */
  let openedAt = performance.now();

  /** A grid on the page: its own reader, its queries planned in the tab and run on it, its view followed when it
   *  lands, its ticks run one at a time; let go, its reader goes with it. */
  const grid = (name: string, snapshot: CubeSnapshot, source: Parameters<typeof isMarketSource>[0],
    opened: { readonly configuration: CubeConfiguration; readonly tree: TreeState }): GridMaker => (host, spawned, start) => {
    const reader = engine.reader();
    const g: ShownGrid = { views: 0, ticks: 0, tickToPaint: [], columns: [], rows: [], window: null, firstViewMs: null, fetched: [] };
    // a result answered from the rows a reader follows took no time on the wire (deephaven-engine.ts execute): one that
    // did was read from the server
    const execute = reader.execute.bind(reader);
    reader.execute = async (plan, epoch, signal) => {
      const result = await execute(plan, epoch, signal);
      if (g.views > 0 && result.elapsedMs > 0) {
        g.fetched.push(plan.sql.slice(0, 300));
        if (g.fetched.length > 20) g.fetched.shift();
      }
      return result;
    };
    const runner = new PlanThenRun(planner, reader);
    shown.grids[name] = g;
    const show = (view: CubeView): void => {
      g.columns = view.rows.columns.map((c) => c.name);
      g.rows = Array.from({ length: Math.min(view.rows.rowCount, 50) }, (_, r) => view.rows.columns.map((c) => {
        const v = c.values[r];
        return typeof v === 'bigint' ? v.toString() : v;
      }));
      g.window = view.window ? { ...view.window } : null;
    };
    const tree = start?.tree ?? opened.tree;
    const app = new CubeApp(host, start?.snapshot ?? snapshot, {
      runner,
      compact: true,
      // the grid writes its source down when the page is saved: the screen's query, by its text
      cubeSource: source,
      configuration: start?.configuration ?? opened.configuration ?? { ...DEFAULT_CONFIGURATION, reportTitle: name },
      tree,
      ...spawned,
      onStatus: (text, kind) => { if (kind !== 'ok') said(`${name}: ${text}`, kind, true); },
      onView: (view) => {
        g.views++;
        g.firstViewMs ??= Math.round(performance.now() - openedAt);
        show(view);
        reader.follow(view.snapshot.epoch).catch((e: unknown) => {
          shown.error = String(e instanceof Error ? e.message : e);
          said(`${name} could not follow its view: ${shown.error}`, 'error');
        });
      },
      onTicked: show,
      onDispose: () => {
        if (apps.get(name) === app) apps.delete(name);
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
      await app.tick();
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
    await Promise.all([...apps.values()].map((app) => app.refresh()));
    shown.status = 'live';
    said('live');
  };

  const screenItems = (): MenuItem[] => TRADER_PAGES.map((p) => ({
    id: `host.screen.${p.id}` as const, label: p.label, ...(shown.screen === p.id ? { checked: true } : {}),
  }));
  const page: PageApp = new PageApp({
    host: byId('cube'),
    title: 'Trader screens',
    empty: (slot) => { slot.textContent = 'No grids: open a screen from the menu (Trader screens ▸)'; },
    hostMenu: () => [
      { label: 'Trader screens', section: 'file' as const, submenu: screenItems() },
      { id: 'host.save' as const, label: 'Save in this browser', section: 'file' as const },
      { id: 'host.open' as const, label: 'Open from this browser', section: 'file' as const },
    ],
    onHostMenu: (item) => {
      const id = item.id ?? '';
      if (id.startsWith('host.screen.')) {
        openScreen(id.slice('host.screen.'.length)).catch((e: unknown) => failedToOpen(e));
      } else if (id === 'host.save') {
        try {
          localStorage.setItem(SAVED, save());
          said('saved in this browser', 'ok', true);
        } catch (e) {
          said(`could not save: ${e instanceof Error ? e.message : String(e)}`, 'error');
        }
      } else if (id === 'host.open') {
        const saved = localStorage.getItem(SAVED);
        if (saved === null) said('nothing saved in this browser', 'warn');
        else open(saved, 'saved').catch((e: unknown) => failedToOpen(e));
      }
    },
  });
  const failedToOpen = (e: unknown): void => {
    shown.status = 'live';
    shown.error = String(e instanceof Error ? e.message : e);
    said(`could not open: ${shown.error}`, 'error');
  };

  const save = (): string => {
    const refused = page.saveRefusal();
    if (refused !== undefined) throw new Error(refused);
    const doc = page.document(page.title);
    if (doc === undefined) throw new Error('the page could not write itself down');
    return pageToJson(doc);
  };

  /** The charts `ids` watched as they draw: their values (a KPI's figure), and their time from their grid's tick. */
  const watchCharts = async (ids: readonly string[]): Promise<void> => {
    const drew = (c: ShownChart, values: readonly unknown[]): void => {
      c.values = values;
      const since = tickedAt.get(c.grid);
      if (since === undefined) return;
      c.draws++;
      keep(c.tickToDraw, performance.now() - since);
    };
    const watch = (): boolean => {
      let all = true;
      for (const id of ids) {
        const c = shown.charts[id];
        if (c === undefined) continue;
        const tileEl = document.querySelector<HTMLElement>(`[data-tile="${id}"]`);
        if (c.kind === 'kpi') {
          const figure = tileEl?.querySelector<HTMLElement>('.dc-kpi-value');
          if (!figure) {
            all = false;
            continue;
          }
          if ((figure as unknown as { __watched?: boolean }).__watched) continue;
          (figure as unknown as { __watched?: boolean }).__watched = true;
          c.values = [figure.textContent];
          // the tile is drawn again on each tick: its box made anew
          new MutationObserver(() => {
            const now = tileEl?.querySelector<HTMLElement>('.dc-kpi-value');
            if (now) drew(c, [now.textContent]);
          }).observe(tileEl!, { subtree: true, childList: true, characterData: true });
          continue;
        }
        const el = tileEl?.querySelector<HTMLElement>('.dc-chartpanel-chart');
        const chart = el ? getInstanceByDom(el) : undefined;
        if (!chart) {
          all = false;
          continue;
        }
        if ((chart as unknown as { __watched?: boolean }).__watched) continue;
        (chart as unknown as { __watched?: boolean }).__watched = true;
        const values = (): unknown[] => {
          const series = (chart.getOption() as { series?: { data?: unknown[] }[] }).series ?? [];
          return (series[0]?.data ?? []).map((d) => (Array.isArray(d) ? d[d.length - 1] : (d as { value?: unknown })?.value ?? d));
        };
        c.values = values();
        chart.on('finished', () => drew(c, values()));
      }
      return all;
    };
    for (let i = 0; i < 200 && !watch(); i++) await new Promise((r) => setTimeout(r, 100));
  };

  /** A saved page opened in this one's place: each grid of a screen's source made again over the source as it is now,
   *  under its saved id and title, its charts and sheets as saved; a grid of another source is left out, and said. */
  const open = async (savedText: string | PageDocument, screen: string): Promise<void> => {
    const started = performance.now();
    openedAt = started;
    let step = started;
    const steps: Record<string, number> = {};
    const took = (what: string): void => {
      const now = performance.now();
      steps[what] = Math.round(now - step);
      step = now;
    };
    shown.status = 'opening';
    said('opening…', 'warn');
    const doc = typeof savedText === 'string' ? readPage(savedText) : savedText;
    const titles = new Map(doc.views.flatMap((v) => (v.kind === 'grid' && v.title ? [[v.cube, v.title] as const] : [])));
    const notes: string[] = [];
    const makers = new Map<string, GridMaker>();
    await Promise.all(doc.cubes.map(async ({ id, cube: kept }) => {
      if (!isMarketSource(kept.source)) {
        notes.push(`${kept.source.name} is not a source of the market: left out`);
        return;
      }
      const { query, columns } = await typeOf(kept.source.query.content);
      const opened = openCube(kept, { query }, columns);
      notes.push(...opened.notes);
      makers.set(id, grid(titles.get(id) ?? opened.configuration.reportTitle ?? id, opened.snapshot, kept.source, opened));
    }));
    took('sources typed');
    shown.grids = {};
    shown.charts = {};
    tickedAt.clear();
    page.restore(doc, makers);
    page.setTitle(doc.name);
    await page.ready();
    took('grids shown');
    for (const v of doc.views) {
      if (v.kind !== 'chart') continue;
      shown.charts[v.id] = { grid: titles.get(v.cube) ?? v.cube, kind: v.spec.mark, draws: 0, tickToDraw: [], values: [] };
    }
    shown.screen = screen;
    // a sheet not shown draws its charts when it is shown: each sheet shown once, so every chart is drawn and watched
    for (const s of doc.sheets) {
      page.showSheet(s.id);
      await page.ready();
      await watchCharts(tiles(s.layout).filter((t) => shown.charts[t] !== undefined));
      took(`sheet ${s.name ?? s.id} drawn`);
    }
    if (doc.sheets[0] !== undefined) page.showSheet(doc.sheets[0].id);
    shown.opened++;
    shown.openMs = performance.now() - started;
    shown.openSteps = steps;
    shown.status = 'live';
    history.replaceState(null, '', `#${screen}`);
    said(notes.length > 0 ? `opened: ${notes.join('; ')}` : `${doc.name}: live`, notes.length > 0 ? 'warn' : 'ok', true);
  };

  /** The screen `id`, written afresh from its definition and opened. */
  const openScreen = async (id: string): Promise<void> => {
    const screen: TraderPage | undefined = TRADER_PAGES.find((p) => p.id === id);
    if (screen === undefined) throw new Error(`no trader screen '${id}'`);
    const doc = await screen.build(async (text) => (await typeOf(text)).columns);
    await open(doc, id);
  };

  shown.openScreen = openScreen;
  shown.save = save;
  shown.open = (text) => open(text, 'saved');
  shown.sheets = () => ({ names: page.sheets.map((id) => page.sheetLabel(id)), shown: page.sheetLabel(page.shownSheet) });
  shown.showSheet = (name) => {
    const id = page.sheets.find((s) => page.sheetLabel(s) === name);
    if (id === undefined) throw new Error(`no sheet '${name}'`);
    page.showSheet(id);
  };

  shown.status = 'live';
  const first = location.hash.slice(1);
  await openScreen(TRADER_PAGES.some((p) => p.id === first) ? first : TRADER_PAGES[0]!.id);
  took('first screen open');
}

main().catch((e) => {
  shown.status = 'failed';
  shown.error = String(e instanceof Error ? e.stack ?? e.message : e);
  said(`failed to start: ${e instanceof Error ? e.message : String(e)}`, 'error');
});
