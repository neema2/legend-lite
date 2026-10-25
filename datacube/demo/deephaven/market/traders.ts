// THE TRADER SCREENS, OPENED IN DATACUBE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K4, K8-K13): DataCube's own page --
// its bar, its sheets (tabs), its grids and charts -- over the market world's Deephaven, each screen a saved page
// (screens.ts) opened from the page's menu (Trader screens ▸) as any saved page is opened: each grid made again over its
// source as it is now -- a saved query over the one trading model's classes, typed by the tab's planner -- and its
// charts and sheets as saved. Every grid has its own reader (`DeephavenEngine.reader()`): it follows what its views and
// charts read, and a tick runs that grid again from the live rows and draws its charts again.
//
// OPEN… AND SAVE ARE DATACUBE'S (src/app/datacube-app.ts): Open… lists the pages saved in this browser -- searched,
// sorted, opened, deleted, or a page file opened -- and, under them as its examples are, the trader screens, each
// written afresh from its definition; Save, Save As… and Export ▸ Page File keep the page as it is now, in the tab's
// own store of saved pages and by its rules (src/services/browser-pages.ts): a screen moved about, a grid regrouped,
// a chart added, all kept, and opened again over the market as it is then.
//
// EACH GRID READS A BASE ON THE SERVER (src/server-base.ts B1): its source -- the joins to the classes it reads, its
// calculated columns (an option's Black and Scholes) -- computed once into a table the server keeps, every query of the
// grid and its charts reading that table's plain columns. The bases are declared in a model of their own (the market's
// connection and their Database, `connectionOf`), so the page's planner holds the trading model once.

import { getInstanceByDom } from 'echarts/core';
import { CubeApp } from '../../../src/app.ts';
import type { CubeView } from '../../../src/cube.ts';
import { places } from '../../../src/layout/bands.ts';
import { openCube } from '../../../src/cube-document.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../../../src/config.ts';
import { LitePlanner } from '../../../src/lite-planner.ts';
import { NOTHING_TO_SAVE, pageSaving, pageToJson, readPage, readSaved, savedPage, type PageDocument } from '../../../src/page-document.ts';
import { PageApp, type GridMaker } from '../../../src/page/page-app.ts';
import { ServerBases } from '../../../src/server-base.ts';
import { browserBasics } from '../../../src/services/browser-basics.ts';
import { browserPages } from '../../../src/services/browser-pages.ts';
import { CubeLibrary } from '../../../src/ui/cube-library.ts';
import { saveDialog } from '../../../src/ui/save-dialog.ts';
import type { ColumnSpec, CubeSnapshot } from '../../../src/snapshot.ts';
import type { TreeState } from '../../../src/tree.ts';
import type { MenuItem } from '../../../src/ui/menu.ts';
import type { ValueSpecification } from '../../../../pure-protocol/src/index.ts';
import { loadQueriesModule } from '../../../../query-store/src/wasm-server.ts';
import { DeephavenEngine, type DeephavenCounts } from '../../../../engine-client/src/deephaven-engine.ts';
import { MARKET_CONNECTION, MARKET_MODEL, MARKET_RUNTIME } from './model.ts';
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
  /** The table its base is kept in on the server (none: its queries read its source as written), and how many of its
   *  queries read the base, read the source, and bases it moved to. */
  base: { table: string | null; based: number; asWritten: number; made: number } | null;
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
  /** The screen on the page now, by its id (a page saved in this browser, or a page file: 'saved'). */
  screen: string;
  /** The screens Open… and the menu open. */
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
  planner: Record<string, { calls: number; ms: number; slowest: number }>;
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
  /** A chart's window as ECharts holds it now (its first: start and end values), and its latest point's time and value. */
  chartNow?: (id: string) => { readonly zoom: { readonly startValue?: number; readonly endValue?: number } | null;
    readonly last: readonly unknown[] | null } | null;
}
const shown: Shown = {
  status: 'starting', error: '', screen: '', screens: TRADER_PAGES.map((p) => ({ id: p.id, label: p.label })),
  reconnects: 0, opened: 0, openMs: 0, openSteps: {}, startup: {}, planner: {}, grids: {}, charts: {}, counts: null, paused: false,
};
(window as unknown as { __traders: Shown }).__traders = shown;

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
        const e = shown.planner[kind] ??= { calls: 0, ms: 0, slowest: 0 };
        const took = performance.now() - t0;
        e.calls++;
        e.ms = Math.round(e.ms + took);
        e.slowest = Math.max(e.slowest, Math.round(took));
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
  // the page's bases: one per source and its calculated columns, whichever grids read it, in a model of their own
  const bases = new ServerBases(planner, engine, {
    model, connection: 'market::Live', stores: ['store::DB'], connectionOf: MARKET_CONNECTION(at.hostname, Number(at.port)),
  });
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
        // typed as Pure text over the model the planner holds
        const columns = (await planner.relationTypeText(text)).map((c) => ({ name: c.name, type: c.type }));
        return { query, columns };
      })();
      typed.set(text, t);
      t.catch(() => { typed.delete(text); });
    }
    return t;
  };

  /** The one value a query answers now (a screen's first symbol): planned in the tab, read once from the server. */
  const firstValue = async (text: string): Promise<string> => {
    const plan = await planner.planText(text);
    const reader = engine.reader();
    try {
      const result = await reader.execute(plan, 1);
      const v = result.columns[0]?.values[0];
      if (v === undefined || v === null) throw new Error(`the market answers nothing for ${text}`);
      return String(v);
    } finally {
      void reader.close();
    }
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
    const g: ShownGrid = { views: 0, ticks: 0, tickToPaint: [], columns: [], rows: [], window: null, firstViewMs: null, base: null, fetched: [] };
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
    const runner = bases.runner(reader);
    shown.grids[name] = g;
    const show = (view: CubeView): void => {
      g.base = { table: runner.baseTable, ...runner.counts };
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

  // THE SAVED PAGES: the tab's store and its rules (src/services/browser-pages.ts), in this browser for this page's
  // address; the rules' module (query-store's, WebAssembly) fetched at the store's first call
  const pages = await browserPages(() => loadQueriesModule(new URL('queries/', location.href).href));
  let showOpen = (): void => undefined;
  let saveAs = (_asNew: boolean): void => undefined;
  const screenItems = (): MenuItem[] => TRADER_PAGES.map((p) => ({
    id: `host.screen.${p.id}` as const, label: p.label, ...(shown.screen === p.id ? { checked: true } : {}),
  }));
  const page: PageApp = new PageApp({
    host: byId('cube'),
    title: 'Trader screens',
    empty: (slot) => { slot.textContent = 'No grids: open a screen from the menu (Open…, or Trader screens ▸)'; },
    hostMenu: () => [
      // the app's own entries (src/app/datacube-app.ts), the screens a submenu beside them
      { id: 'host.save' as const, label: 'Save', section: 'file' as const },
      { id: 'host.saveAs' as const, label: 'Save As\u2026', section: 'file' as const },
      { id: 'host.open' as const, label: 'Open\u2026', section: 'file' as const },
      { label: 'Trader screens', section: 'file' as const, submenu: screenItems() },
    ],
    onHostMenu: (item) => {
      const id = item.id ?? '';
      if (id.startsWith('host.screen.')) openScreen(id.slice('host.screen.'.length)).catch((e: unknown) => failedToOpen(e));
      else if (id === 'host.save') saveAs(false);
      else if (id === 'host.saveAs') saveAs(true);
      else if (id === 'host.open') showOpen();
    },
    // Export ▸ Page File (JSON): the page as Save writes it, for Open…'s "Open a cube file…" here or anywhere
    download: (name, mime, content) => browserBasics.download(name, mime, content),
    refuse: (text) => said(text, 'error'),
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

  /** The page saved in the store as `name`: over the saved page it is, or, Save As (`asNew`), a new one -- the record
   *  by DataCube's one rule (pageSaving), as the tab's Save writes it. */
  const saveTo = async (name: string, asNew: boolean): Promise<void> => {
    const refused = page.saveRefusal();
    if (refused !== undefined) throw new Error(refused);
    const doc = page.document(name);
    if (doc === undefined) throw new Error('This page cannot be saved yet: a grid on it does not know its source.');
    const saving = pageSaving(name, doc, crypto.randomUUID(), asNew);
    if ('refused' in saving) throw new Error(saving.refused);
    const { record } = saving;
    if (saving.over) await pages.store.update(record.id, record);
    else await pages.store.create(record);
    page.savedAs(record.id);
    page.rename(record.name);
    await pages.persist();
    library.sync();
    said(`saved “${record.name}” ${pages.where}`, 'ok', true);
  };
  // SAVE AND SAVE AS: the app's window (src/ui/save-dialog.ts) -- the name, where it is kept, what is kept and what not
  saveAs = (asNew) => {
    if (page.empty) {
      said(NOTHING_TO_SAVE, 'warn', true);
      return;
    }
    void (async () => {
      const record = page.record;
      const over = record !== undefined ? await pages.store.get(record).catch(() => undefined) : undefined;
      const views = page.views().views;
      const charts = views.filter((v) => v.kind === 'chart').length;
      const grids = views.filter((v) => v.kind === 'grid').length;
      await saveDialog(document, {
        purpose: asNew || over === undefined ? 'saveAs' : 'save',
        name: over?.name ?? page.suggestedName,
        ...(over !== undefined ? { over: { name: over.name, ...(over.lastUpdatedAt !== undefined ? { savedAt: over.lastUpdatedAt } : {}) } } : {}),
        where: pages.where,
        keeps: [
          `${grids} grid${grids === 1 ? '' : 's'}, each grouped, filtered and calculated as it is now`,
          `${charts} visualization${charts === 1 ? '' : 's'}, the page's ${page.sheets.length} sheet${page.sheets.length === 1 ? '' : 's'} and their layouts`,
          "Each grid's formats, widths, colours and settings",
        ],
        leaves: 'Not the rows: opening it reads each grid\'s source from the market on Deephaven again, live.',
        save: (name, saveAsNew) => saveTo(name, saveAsNew),
      });
    })().catch((e: unknown) => said(`could not save: ${e instanceof Error ? e.message : String(e)}`, 'error'));
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
        // mounted is not drawn: a chart is watched from its first drawing (ECharts has no option before it)
        if (!chart || chart.getOption() === undefined) {
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
    // the charts a sheet shows: a tile's, and a stack's in front (one behind its tab draws when it is shown)
    const inFront = new Set(doc.sheets.flatMap((s) => places(s.layout)));
    for (const v of doc.views) {
      if (v.kind !== 'chart' || !inFront.has(v.id)) continue;
      shown.charts[v.id] = { grid: titles.get(v.cube) ?? v.cube, kind: v.spec.mark, draws: 0, tickToDraw: [], values: [] };
    }
    shown.screen = screen;
    // a sheet not shown draws its charts when it is shown: each sheet shown once, so every chart is drawn and watched
    for (const s of doc.sheets) {
      page.showSheet(s.id);
      await page.ready();
      await watchCharts(places(s.layout).filter((t) => shown.charts[t] !== undefined));
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
    const doc = await screen.build(async (text) => (await typeOf(text)).columns, firstValue);
    await open(doc, id);
  };

  // OPEN…: the app's window (src/app/datacube-app.ts) -- the saved pages, and the trader screens under them as its
  // examples are; it closes once a page opens, and what failed is said in it
  const libraryRoot = document.createElement('div');
  libraryRoot.id = 'cubelib';
  const library = new CubeLibrary(libraryRoot, pages.store, {
    currentId: () => page.record,
    open: async (id) => {
      await open(savedPage(readSaved((await pages.store.get(id)).content)), 'saved');
      closeOpen?.();
    },
    openText: async (text) => {
      await open(savedPage(readSaved(text)), 'saved');
      closeOpen?.();
    },
    forget: async (id) => { if (page.record === id) page.savedAs(null); },
  });
  let closeOpen: (() => void) | undefined;
  showOpen = () => {
    if (!closeOpen) {
      const backdrop = document.createElement('div');
      backdrop.id = 'cubeswin';
      backdrop.className = 'dc-picker-backdrop';
      const win = document.createElement('div');
      win.className = 'dc-picker dc-open dc-app-floating';
      win.setAttribute('role', 'dialog');
      win.setAttribute('aria-modal', 'true');
      win.setAttribute('aria-labelledby', 'dc-open-title');
      const head = document.createElement('div');
      head.className = 'dc-picker-head';
      const titles = document.createElement('div');
      titles.className = 'dc-picker-titles';
      const title = document.createElement('h2');
      title.className = 'dc-picker-title';
      title.id = 'dc-open-title';
      title.textContent = 'Open a saved page';
      const sub = document.createElement('p');
      sub.className = 'dc-picker-subtitle';
      sub.textContent = `Saved ${pages.where}. Each reopens over the market on Deephaven as it is then, live; or open a trader screen.`;
      titles.append(title, sub);
      const close = document.createElement('button');
      close.type = 'button';
      close.className = 'dc-picker-close';
      close.setAttribute('aria-label', 'Close');
      close.textContent = '\u00d7';
      head.append(titles, close);
      const body = document.createElement('div');
      body.className = 'dc-open-body';
      body.append(libraryRoot, screensList());
      win.append(head, body);
      backdrop.append(win);
      document.body.append(backdrop);
      const onKey = (e: KeyboardEvent): void => {
        if (e.key === 'Escape') { e.preventDefault(); closeOpen?.(); }
      };
      document.addEventListener('keydown', onKey, true);
      closeOpen = () => {
        document.removeEventListener('keydown', onKey, true);
        backdrop.remove();
        closeOpen = undefined;
      };
      close.addEventListener('click', () => closeOpen?.());
      backdrop.addEventListener('mousedown', (e) => { if (e.target === backdrop) closeOpen?.(); });
    }
    library.sync();
    void library.refresh();
    (libraryRoot.querySelector('.dc-lib-search') as HTMLElement | null)?.focus();
  };
  /** The trader screens, under the saved pages as the app's examples are: each written afresh when it is picked. */
  const screensList = (): HTMLElement => {
    const box = document.createElement('section');
    box.className = 'dc-open-examples';
    box.setAttribute('aria-label', 'Trader screens');
    const heading = document.createElement('h3');
    heading.className = 'dc-open-examples-title';
    heading.textContent = 'Trader screens';
    box.append(heading);
    for (const screen of TRADER_PAGES) {
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'dc-open-example';
      item.dataset['example'] = screen.id;
      const name = document.createElement('span');
      name.className = 'dc-open-example-name';
      name.textContent = screen.label;
      const about = document.createElement('span');
      about.className = 'dc-open-example-about';
      about.textContent = screen.about;
      item.append(name, about);
      item.addEventListener('click', () => {
        item.disabled = true;
        openScreen(screen.id).then(() => closeOpen?.(), (e: unknown) => {
          library.say(`Could not open ${screen.label}: ${e instanceof Error ? e.message : String(e)}`, 'warn');
        }).finally(() => { item.disabled = false; });
      });
      box.append(item);
    }
    return box;
  };

  shown.chartNow = (id) => {
    const el = document.querySelector<HTMLElement>(`[data-tile="${id}"] .dc-chartpanel-chart`);
    const chart = el ? getInstanceByDom(el) : undefined;
    if (!chart) return null;
    const option = chart.getOption() as { dataZoom?: { startValue?: number; endValue?: number }[]; series?: { data?: unknown[] }[] } | undefined;
    if (option === undefined) return null;
    const zoom = option.dataZoom?.[0];
    const data = option.series?.[0]?.data ?? [];
    const last = data[data.length - 1];
    return { zoom: zoom ? { ...(zoom.startValue !== undefined ? { startValue: zoom.startValue } : {}), ...(zoom.endValue !== undefined ? { endValue: zoom.endValue } : {}) } : null,
      last: Array.isArray(last) ? last : null };
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
