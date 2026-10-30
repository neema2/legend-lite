// THE TIMESERIES VIEWER ON A SCREEN (ui/timeseries-view.ts; docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §6): the
// viewer in a DOM over a grid of the markets sample read into a real DuckDB, its every query planned by legend-lite's
// planner -- only the drawing is stood in for (the renderer, which ECharts' canvas needs). What it shows is held to the
// rows themselves: the legend's latest value, the statistics' first and last, the line drawn; what the person does is
// held to the spec it tells (the page's setTimeseries), and to what it asks the database again (D4: a hidden line asks
// nothing; a range asks only what reads the first date shown, and the statistics).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { col, fn, lambda, lit, minus, toJson } from '../../pure-protocol/src/index.ts';
import type { RunOutcome } from '../src/runner.ts';
import type { EChartsOption } from 'echarts';
import { marketsCsv } from '../src/samples.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { MountedTimeseries } from '../src/timeseries-render.ts';
import { exprSource, parseExpr } from '../src/timeseries-expr.ts';
import { tileCode } from '../src/timeseries-code.ts';
import type { FileSource } from '../src/cube-document.ts';
import { DATACUBE_LIBRARY } from '../src/pure-library-text.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { ingestFile } from '../src/upload.ts';
import { TimeseriesView, type MountOn, type TsGrid, type TsTogether } from '../src/ui/timeseries-view.ts';
import { MARKET_SPECS } from '../src/examples/markets.ts';
import { plannerFor } from './catalog-builder.ts';

/** The file the viewer's grid of prices was opened over, as a page records it: what the code that builds a tile opens. */
const PRICES_FILE: FileSource = { _type: 'file', name: 'prices.csv', format: 'csv', size: 1, sha256: '00', columns: [] };

const CSV = marketsCsv(2000);
const ROWS = CSV.trim().split('\n').slice(1).map((l) => l.split(','));
/** The rows of one instrument, oldest first: its days and its closes. */
const of = (symbol: string): { days: string[]; closes: number[] } => {
  const mine = ROWS.filter((r) => r[0] === symbol);
  return { days: mine.map((r) => r[2]!), closes: mine.map((r) => Number(r[6])) };
};

let engine: DuckDbEngine;
let grid: TsGrid;
let plan: (query: Parameters<TsGrid['run']>[0]) => Promise<RunOutcome>;
let runs = 0;
/** The database the grid reads, and its table: a test changes the rows under the grid's query. */
let database: Awaited<ReturnType<typeof openDatabase>>;
let table = '';

before(async () => {
  const db = await openDatabase();
  database = db;
  engine = new DuckDbEngine(db.connection);
  const writer = plannerFor('', 'local::RT');
  const r = await ingestFile(engine, db.files, new File([CSV], 'sample-markets.csv'), writer);
  table = r.table;
  const planner = plannerFor(r.model, r.runtime);
  // a query planned and run: its rows, and the SQL its plan ran (the Query window's)
  plan = async (query) => {
    const planned = await planner.plan(query);
    return { rows: await engine.execute(planned, 0), sql: planned.sql };
  };
  const snapshot: CubeSnapshot = {
    source: { query: r.source }, columns: [...MARKET_SPECS], derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
  };
  grid = {
    cube: 'prices', title: 'Prices', snapshot, filter: undefined,
    run: async (query): Promise<RunOutcome> => {
      runs += 1;
      return plan(query);
    },
    format: (value) => (typeof value === 'number' ? value.toFixed(2) : String(value)),
    print: (query) => planner.print(query),
  };
});

after(async () => {
  await engine?.close();
});

/** A viewer over the grid in a fresh DOM: what it drew, and the specs it told. `refused`: the queries its grid's
 *  database refuses, by their text (a bounds query's, a statistics query's...), as a warehouse that is down refuses them;
 *  `over`: the grid as it is read (its columns), the markets sample's by default. */
/** A picture the viewer had made: what it drew, as what, at what size, on what. */
interface Exported { option: EChartsOption; kind: 'png' | 'svg'; width: number; height: number; background: string }

function viewer(spec: TimeseriesSpec, refused?: (query: string) => boolean, over: TsGrid = grid): {
  view: TimeseriesView; root: HTMLElement; windows: HTMLElement; drawn: EChartsOption[]; told: TimeseriesSpec[]; zooms: unknown[];
  exported: Exported[]; mounted: { on?: MountOn; measuring: boolean; drawing: boolean };
} {
  const g: TsGrid = refused === undefined ? over : {
    ...over,
    run: (query, snapshot, signal) => (refused(toJson(query)) ? Promise.reject(new Error('the warehouse is down')) : over.run(query, snapshot, signal)),
  };
  const dom = new JSDOM('<!doctype html><div id="t" style="height:600px"></div><div id="w"></div>');
  const root = dom.window.document.getElementById('t') as unknown as HTMLElement;
  // its editors' windows, among the page's (the page's windows' host): outside the tile
  const windows = dom.window.document.getElementById('w') as unknown as HTMLElement;
  const drawn: EChartsOption[] = [];
  const told: TimeseriesSpec[] = [];
  const zooms: unknown[] = [];
  const exported: Exported[] = [];
  const mounted: { on?: MountOn; measuring: boolean; drawing: boolean } = { measuring: false, drawing: false };
  const mount = (_el: HTMLElement, on: MountOn): MountedTimeseries => {
    mounted.on = on;
    return {
      measure: (measuring) => { mounted.measuring = measuring; },
      draw: (drawing) => { mounted.drawing = drawing; },
      show: (option) => { drawn.push(option); },
      zoomTo: (from, to) => { zooms.push({ from, to }); },
      zoomed: () => ({}),
      highlight: () => {},
      height: () => 480,
      picture: () => null,
      exportUrl: async (option, kind, width, height, background) => { exported.push({ option, kind, width, height, background }); return `data:image/${kind};base64,AA==`; },
      dispose: () => {},
    };
  };
  const view = new TimeseriesView(root, {
    grid: (cube) => (cube === 'prices' ? g : undefined),
    heldFilter: (cube) => (cube === 'prices' ? { filter: g.filter } : undefined),
    grids: () => [g],
    together: () => Promise.reject(new Error('one grid here')),
    setSpec: (s) => { told.push(s); },
    windowHost: windows,
    title: () => 'Markets',
    codeOf: () => ({ title: 'Markets', grids: { prices: { title: 'Prices', source: PRICES_FILE } } }),
    mount,
  }, spec);
  return { view, root, windows, drawn, told, zooms, exported, mounted };
}

/** Until `done` holds, the viewer's queries answered meanwhile (bounded). */
async function until(done: () => boolean, what: string): Promise<void> {
  for (let i = 0; i < 400 && !done(); i++) await new Promise((r) => setTimeout(r, 10));
  assert.ok(done(), `the viewer did not show ${what}`);
}

const SPEC: TimeseriesSpec = {
  sources: [{ alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }],
  series: [
    { id: 'A', expr: parseExpr('close[SPX]'), name: 'S&P 500' },
    { id: 'B', expr: parseExpr('volatility($A, 21)'), name: '21d vol', pane: 1 },
  ],
  view: { stats: true },
};

const chips = (root: HTMLElement): string[] => [...root.querySelectorAll('.dc-ts-chip')].map((c) => c.textContent ?? '');
const statRows = (root: HTMLElement): string[][] => [...root.querySelectorAll('.dc-ts-table tr')].slice(1)
  .map((tr) => [...tr.querySelectorAll('td')].map((td) => td.textContent ?? ''));

describe('the timeseries viewer over a grid of the markets sample', () => {
  it('draws each line the database answered, its latest value in the legend and its statistics in the table', async () => {
    const { root, drawn, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2 && drawn.length > 0, 'its lines and their statistics');
      const spx = of('SPX');
      const last = spx.closes.at(-1)!;
      // the legend: each line named, its latest value as the grid writes its column
      const legend = chips(root);
      assert.ok(legend[0]!.startsWith('S&P 500') && legend[0]!.includes(last.toFixed(2)), `the latest close in the legend: ${legend[0]}`);
      assert.ok(legend[1]!.startsWith('21d vol') && legend[1]!.includes('%'), `a volatility in percent: ${legend[1]}`);
      // the drawing: the line's every point, oldest first, a day each
      const series = (drawn.at(-1)!.series as { id: string; data: [string, number | null][] }[]);
      const a = series.find((x) => x.id === 'A')!;
      assert.deepEqual(a.data.map((p) => p[0].slice(0, 10)), spx.days);
      assert.deepEqual(a.data.map((p) => p[1]), spx.closes);
      // the statistics: the first and the last close, over every date (no range: Max)
      const [first, lastCell] = statRows(root)[0]!.slice(1, 3);
      assert.deepEqual([first, lastCell], [spx.closes[0]!.toFixed(2), last.toFixed(2)]);
      // the band of panes: the volatility in a pane of its own, under the main one
      assert.equal((drawn.at(-1)!.grid as unknown[]).length, 2);
    } finally {
      view.dispose();
    }
  });

  // a level's change in percent beside its value; a spread's start may be zero or below, so its legend says none
  it('the legend\'s change in percent: a level\'s, and none for a spread of the same unit', async () => {
    const spec: TimeseriesSpec = { ...SPEC, series: [SPEC.series[0]!, { id: 'C', expr: parseExpr('close[SPX] - 4000'), name: 'Over 4000' }] };
    const { root, view } = viewer(spec);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const [spx, spread] = chips(root);
      assert.match(spx!, /[▲▼] \d+\.\d\d%$/, `the S&P's change: ${spx}`);
      assert.doesNotMatch(spread!, /[▲▼]/, `no change in percent for a spread: ${spread}`);
    } finally {
      view.dispose();
    }
  });

  // the expert statistics (2026-10-26): the panel's Risk tab, a level's returns' risk; a volatility (no level) none
  it('the statistics\' Risk tab: a level\'s Sortino, Calmar, skew, kurtosis, value at risk; none of what is no level', async () => {
    const { root, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const tab = [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Risk')!;
      assert.equal(tab.getAttribute('aria-selected'), 'false');
      tab.click();
      const heads = [...root.querySelectorAll('.dc-ts-table th')].map((th) => th.textContent);
      assert.deepEqual(heads, ['Line', 'Vol (ann.)', 'Sharpe', 'Sortino', 'Calmar', 'Max DD', 'Skew', 'Kurtosis', 'Up periods', 'Best', 'Worst', 'VaR 95%', 'ES 95%']);
      const [spx, vol] = statRows(root);
      assert.ok(spx!.slice(1, 4).every((c) => c !== '—' && c !== ''), `the S&P's vol, Sharpe and Sortino: ${spx!.join(' | ')}`);
      assert.equal(spx![4], '—', 'no Calmar under a year');
      assert.ok(spx!.slice(6).every((c) => c !== '—'), `its skew to its shortfall: ${spx!.join(' | ')}`);
      assert.ok(vol!.slice(1).every((c) => c === '—'), `a volatility is no level: ${vol!.join(' | ')}`);
      assert.equal([...root.querySelectorAll('.dc-ts-stats-tab')].find((b) => b.textContent === 'Risk')!.getAttribute('aria-selected'), 'true');
    } finally {
      view.dispose();
    }
  });

  it('the statistics\' Drawdowns tab: each level\'s deepest, from peak to trough to recovery, asked when shown', async () => {
    const asked: string[] = [];
    const { root, view } = viewer(SPEC, (q) => { asked.push(q); return false; });
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      assert.ok(!asked.some((q) => q.includes('__ts_dd_rank')), 'no drawdowns asked before their tab is shown');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Drawdowns')!.click();
      await until(() => statRows(root).length > 0, 'its drawdowns');
      assert.deepEqual([...root.querySelectorAll('.dc-ts-table th')].map((th) => th.textContent), ['Line', '#', 'Peak', 'Trough', 'Recovered', 'Depth', 'Days down', 'Days back']);
      const rows = statRows(root);
      assert.ok(rows.length >= 1 && rows.length <= 5, `${rows.length} drawdowns`);
      assert.ok(rows.every((r) => r[0] === 'S&P 500'), 'the S&P alone: a volatility is no level');
      assert.deepEqual(rows.map((r) => r[1]), rows.map((_, i) => String(i + 1)), 'deepest first');
      assert.ok(rows.every((r) => /^-/.test(r[5]!)), `each below its peak: ${rows.map((r) => r[5]).join(', ')}`);
      assert.ok(asked.some((q) => q.includes('__ts_dd_rank')), 'asked once shown');
    } finally {
      view.dispose();
    }
  });

  // the E9 review's 3 and 8: a hidden line's tables asked with the rest (tableSeries, Python's rule): shown again, its rows
  // are there -- never left out, nothing asked again
  it('a hidden line shown again: its drawdowns and its correlations there, nothing asked again', async () => {
    const spec: TimeseriesSpec = { ...SPEC, series: [SPEC.series[0]!, SPEC.series[1]!, { id: 'C', expr: parseExpr('close[NDX]'), name: 'Nasdaq', hidden: true }] };
    const { root, view } = viewer(spec);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const tab = (name: string): void => [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === name)!.click();
      tab('Drawdowns');
      await until(() => statRows(root).length > 0, 'its drawdowns');
      assert.ok(statRows(root).every((r) => r[0] === 'S&P 500'), 'the hidden Nasdaq not shown');
      tab('Correlations');
      await until(() => statRows(root).length === 2 && !statRows(root)[0]!.includes('Asking'), 'its matrix');
      await new Promise((r) => setTimeout(r, 100));
      const asked = runs;
      ([...root.querySelectorAll<HTMLElement>('.dc-ts-chip')].find((c) => c.textContent!.startsWith('Nasdaq'))!).click();
      await until(() => statRows(root).length === 3, 'the Nasdaq in the matrix');
      const [spx, , ndx] = statRows(root);
      assert.ok(spx![3] !== '—' && spx![3] === ndx![1], `the S&P with the Nasdaq, the database's: ${spx!.join(' | ')}`);
      assert.equal(ndx![3], '1.00', 'the Nasdaq with itself');
      tab('Drawdowns');
      assert.ok(statRows(root).some((r) => r[0] === 'Nasdaq'), 'the Nasdaq\'s drawdowns');
      assert.equal(runs, asked, 'nothing asked again: hiding is the drawing\'s');
    } finally {
      view.dispose();
    }
  });

  it('the statistics\' Correlations tab: a matrix of the lines, each pair the database\'s, asked when shown', async () => {
    const asked: string[] = [];
    const { root, view } = viewer(SPEC, (q) => { asked.push(q); return false; });
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      assert.ok(!asked.some((q) => q.includes('__ts_corr')), 'no correlations asked before their tab is shown');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Correlations')!.click();
      await until(() => statRows(root).length === 2 && !statRows(root)[0]!.includes('—'), 'its matrix');
      assert.deepEqual([...root.querySelectorAll('.dc-ts-table th')].map((th) => th.textContent), ['Line', 'S&P 500', '21d vol']);
      const [spx, vol] = statRows(root);
      assert.equal(spx![1], '1.00');
      assert.equal(vol![2], '1.00');
      assert.equal(spx![2], vol![1], 'the matrix is symmetric');
      // the S&P's returns (a level) against the volatility as it is, over the dates both have
      const c = of('SPX').closes;
      const r = c.map((v, i) => (i === 0 ? null : v / c[i - 1]! - 1));
      const vols = r.map((_, i) => {
        if (i < 21) return null;
        const w = r.slice(i - 20, i + 1) as number[];
        const m = w.reduce((x, y) => x + y, 0) / w.length;
        return Math.sqrt(w.reduce((x, y) => x + (y - m) ** 2, 0) / (w.length - 1)) * Math.sqrt(252) * 100;
      });
      const at = r.map((_, i) => i).filter((i) => vols[i] !== null);
      const xs = at.map((i) => r[i]!);
      const ys = at.map((i) => vols[i]!);
      const mean = (v: number[]): number => v.reduce((x, y) => x + y, 0) / v.length;
      const [mx, my] = [mean(xs), mean(ys)];
      const corr = xs.reduce((acc, x, k) => acc + (x - mx) * (ys[k]! - my), 0)
        / Math.sqrt(xs.reduce((acc, x) => acc + (x - mx) ** 2, 0) * ys.reduce((acc, y) => acc + (y - my) ** 2, 0));
      assert.equal(spx![2], corr.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
    } finally {
      view.dispose();
    }
  });

  // the expert functions (2026-10-26): a condition, shaded where it holds across every pane -- the database's runs
  it('a condition shaded where it holds, across every pane; its chip says whether it holds; no statistics of it', async () => {
    const spec: TimeseriesSpec = {
      ...SPEC,
      series: [...SPEC.series, { id: 'C', expr: parseExpr('close[SPX] > mean(close[SPX], 21)'), name: 'Above its 21-day' }],
    };
    const { root, drawn, view } = viewer(spec);
    try {
      await until(() => statRows(root).length === 2 && drawn.length > 0, 'its lines and their statistics');
      const option = drawn.at(-1)!;
      const shading = (option.series as { id: string; markArea?: { data: unknown[] } }[]).filter((x) => x.id === 'C' || x.id.startsWith('C:'));
      assert.equal(shading.length, 2, 'across both panes');
      assert.ok(shading.every((x) => (x.markArea?.data.length ?? 0) >= 1), 'each its periods');
      // its periods: where the close is above its 21-day mean, each from its first date to the next, from which it no
      // longer holds (its own last at the line's end) -- a run of one date a day wide (the E9 review's 4)
      const spx = of('SPX');
      const mean = spx.closes.map((_, i) => (i < 20 ? null : spx.closes.slice(i - 20, i + 1).reduce((a, b) => a + b, 0) / 21));
      const holds = spx.closes.map((c, i) => mean[i] !== null && c > mean[i]!);
      const runs: [string, string][] = [];
      holds.forEach((h, i) => {
        if (!h) return;
        const end = spx.days[Math.min(i + 1, spx.days.length - 1)]!;
        if (i > 0 && holds[i - 1]) runs.at(-1)![1] = end;
        else runs.push([spx.days[i]!, end]);
      });
      assert.deepEqual(shading[0]!.markArea!.data.map((d) => (d as { xAxis: string }[]).map((e) => e.xAxis.slice(0, 10))), runs);
      const chip = chips(root).find((c) => c.startsWith('Above its 21-day'))!;
      assert.match(chip, holds.at(-1) ? /holds$/ : /does not$/);
      assert.ok(!statRows(root).some((r) => r[0] === 'Above its 21-day'), 'a condition has no statistics');
    } finally {
      view.dispose();
    }
  });

  // marks from a grid's rows (Phase 6's D4: the trades; Phase 5's events from a grid): asked with the chart, each row the
  // database's, drawn over the lines; refused, said; asked as the Query window lists it
  it('marks from the grid\'s rows: each SPX row a triangle at its close, oldest first; refused, said; listed with the queries', async () => {
    const spec: TimeseriesSpec = {
      ...SPEC, annotations: [{ kind: 'marks', source: 'px', where: [{ value: 'SPX' }], at: 'close', text: 'volume', shape: 'up', label: 'Closes' }],
    };
    const { root, windows, drawn, view } = viewer(spec);
    try {
      await until(() => drawn.some((o) => (o.series as { id: string }[]).some((x) => x.id === 'marks:0')), 'its marks');
      const marks = (drawn.at(-1)!.series as { id: string; type: string; data: [string, number, string][] }[]).find((x) => x.id === 'marks:0')!;
      const spx = of('SPX');
      assert.equal(marks.type, 'scatter');
      assert.deepEqual(marks.data.map(([d, v]) => [d.slice(0, 10), v]), spx.days.map((d, i) => [d, spx.closes[i]]), 'each SPX row at its close, oldest first');
      barButton(root, 'Query').click();
      const said = [...windows.querySelectorAll('.dc-ts-window .dc-ts-ask summary span:first-child')].map((x) => x.textContent ?? '');
      assert.ok(said.includes('4. Marks "Closes": source px\'s rows at their close'), said.join(' | '));
    } finally {
      view.dispose();
    }
    const refused = viewer(spec, (q) => q.includes('__ts_text'));
    try {
      await until(() => /Closes were refused: the warehouse is down/.test(refused.root.querySelector('.dc-ts-banner')?.textContent ?? ''), 'its refusal');
      assert.ok(refused.root.querySelector('.dc-ts-banner')!.classList.contains('dc-ts-bad'));
    } finally {
      refused.view.dispose();
    }
  });

  // Phase 5's measure (TradingView's ruler): a drag across the chart is a span, each line's statistics over it the
  // database's -- statsQuery over those dates, the rule Python's statistics(start, end) reads
  it('Measure: a span dragged, each line\'s first and last value, its change and %, its days and observations over it', async () => {
    const { root, drawn, view, mounted } = viewer(SPEC);
    try {
      await until(() => drawn.length > 0 && mounted.on !== undefined, 'the chart');
      barButton(root, 'Measure').click();
      assert.equal(mounted.measuring, true, 'a drag is a measure now, not a pan');
      assert.equal(barButton(root, 'Measure').getAttribute('aria-pressed'), 'true');
      const spx = of('SPX');
      const [from, to] = [spx.days[30]!, spx.days[90]!];
      mounted.on!.onSpan!(from, to, false);
      mounted.on!.onSpan!(from, to, true);
      const box = (): HTMLElement => root.querySelector('.dc-ts-measure') as HTMLElement;
      await until(() => box().querySelectorAll('.dc-ts-measure-row').length >= 1, 'the span measured');
      const said = [...box().querySelectorAll('.dc-ts-measure-row')].map((r) => r.textContent ?? '');
      // the S&P's: its closes on those days, the change between, the calendar days, the observations
      const [a, b] = [spx.closes[30]!, spx.closes[90]!];
      const days = (Date.parse(to) - Date.parse(from)) / 86_400_000;
      const sp = said.find((t) => t.startsWith('S&P 500'))!;
      assert.match(sp, new RegExp(`${days} days, 61 obs$`), sp);
      assert.ok(sp.includes(`(${b / a - 1 >= 0 ? '+' : ''}${((b / a - 1) * 100).toLocaleString('en-US', { maximumFractionDigits: 2 })}%)`), sp);
      assert.equal(box().querySelector('.dc-ts-measure-head')!.textContent, `${from} → ${to}`);
      // the span shaded on the chart, and no pan while measuring
      const option = drawn.at(-1)! as { series: { id: string; markArea?: { data: { xAxis: string }[][] } }[]; dataZoom: { type: string; moveOnMouseMove?: boolean }[] };
      assert.deepEqual(option.series.find((x) => x.id === 'measure')!.markArea!.data[0]!.map((e) => e.xAxis), [from, to]);
      assert.equal(option.dataZoom.find((z) => z.type === 'inside')!.moveOnMouseMove, false);
      // off again: the span put away
      barButton(root, 'Measure').click();
      assert.equal(box().hidden, true);
      assert.equal(mounted.measuring, false);
    } finally {
      view.dispose();
    }
  });

  // Phase 5's drawings (TradingView's): a click in a pane is a point -- a line's two ends, a note's place and its text --
  // each told as an annotation by the tile's one rule (Python's annotate), listed in Settings to take off
  it('Draw: two clicks a line, a click and its text a note, each an annotation; Settings lists them; Measure ends it', async () => {
    const { root, windows, drawn, told, view, mounted } = viewer(SPEC);
    const box = (): HTMLElement => root.querySelector('.dc-ts-draw') as HTMLElement;
    const ids = (): string[] => (drawn.at(-1)!.series as { id: string }[]).map((x) => x.id);
    try {
      await until(() => drawn.length > 0 && mounted.on !== undefined, 'the chart');
      barButton(root, 'Draw').click();
      assert.equal(mounted.drawing, true, 'a click is a point now, not a pan');
      assert.equal(barButton(root, 'Draw').getAttribute('aria-pressed'), 'true');
      assert.match(box().textContent ?? '', /Click where the line starts/);
      await until(() => ids().includes('drawings:0'), 'the panes ready for a click');
      const spx = of('SPX');
      // its ends in either order: the line from the earlier
      mounted.on!.onPoint!(0, spx.days[40]!, 3300);
      assert.match(box().textContent ?? '', new RegExp(`From ${spx.days[40]}: click where it ends`));
      assert.equal(told.length, 0, 'one end is no line yet');
      mounted.on!.onPoint!(0, spx.days[10]!, 3123.456789);
      assert.deepEqual(told.at(-1)!.annotations, [{ kind: 'line', from: { date: spx.days[10]!, value: 3123.46 }, to: { date: spx.days[40]!, value: 3300 } }],
        'from its earlier end to its later, each value to six figures');
      // a note: its place, then its text
      const Event = root.ownerDocument.defaultView!.Event;
      const kind = box().querySelector<HTMLSelectElement>('[aria-label="Draw"]')!;
      kind.value = 'note';
      kind.dispatchEvent(new Event('change'));
      assert.match(box().textContent ?? '', /Click where the note goes/);
      mounted.on!.onPoint!(0, spx.days[20]!, 3200);
      const text = box().querySelector<HTMLInputElement>('[aria-label="Note"]')!;
      text.value = 'Earnings beat';
      [...box().querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'Add')!.click();
      assert.deepEqual(told.at(-1)!.annotations!.at(-1), { kind: 'note', at: { date: spx.days[20]!, value: 3200 }, text: 'Earnings beat' });
      await until(() => ((drawn.at(-1)!.series as { id: string; markPoint?: { data: { value?: string }[] } }[]).find((x) => x.id === 'drawings:0')?.markPoint?.data ?? [])
        .some((d) => d.value === 'Earnings beat'), 'the note drawn');
      // Settings lists them, to take off
      barButton(root, 'Settings').click();
      const listed = windows.querySelector('.dc-ts-window')!.textContent ?? '';
      assert.ok(listed.includes(`Line ${spx.days[10]} at 3,123.46 → ${spx.days[40]} at 3,300`), listed);
      assert.ok(listed.includes(`Note ${spx.days[20]} at 3,200: Earnings beat`), listed);
      // measuring ends drawing
      barButton(root, 'Measure').click();
      assert.deepEqual([mounted.drawing, mounted.measuring, box().hidden], [false, true, true]);
    } finally {
      view.dispose();
    }
  });

  // the Phase 5 review's 4 and 5: Measure pressed before the chart is made measures once it is; a span measured is put
  // away in a view (no dates there), and a replay is stopped by a change of the tile
  it('Measure before the chart is made, the span put away in a view, a replay stopped by a change', async () => {
    const { root, drawn, zooms, view, mounted } = viewer(SPEC);
    try {
      barButton(root, 'Measure').click();
      await until(() => drawn.length > 0 && mounted.on !== undefined, 'the chart');
      assert.equal(mounted.measuring, true, 'told to the chart made after it was pressed');
      const spx = of('SPX');
      mounted.on!.onSpan!(spx.days[30]!, spx.days[90]!, true);
      const box = (): HTMLElement => root.querySelector('.dc-ts-measure') as HTMLElement;
      await until(() => box().querySelectorAll('.dc-ts-measure-row').length >= 1, 'the span measured');
      view.update({ ...SPEC, view: { ...SPEC.view, mode: 'histogram' } });
      assert.equal(box().hidden, true, 'a view has no dates to measure');
      mounted.on!.onSpan!('1970-01-01', '1970-01-01', true);
      assert.equal(box().hidden, true, 'no span measured in a view');
      view.update(SPEC);
      await until(() => drawn.length > 0 && (drawn.at(-1)!.series as { id: string }[]).some((x) => x.id === 'A'), 'along time again');
      barButton(root, 'Replay').click();
      const n = zooms.length;
      view.update({ ...SPEC, view: { ...SPEC.view, subtitle: 'Changed' } });
      assert.ok(barButton(root, 'Replay'), 'stopped');
      await new Promise((r) => setTimeout(r, 250));
      assert.equal(zooms.length, n, 'no step after the change');
    } finally {
      view.dispose();
    }
  });

  // Phase 5's replay (TradingView's): the window's end stepped through the dates shown, the chart's zoom alone; stopped
  // where it is, and done at the last date
  it('Replay: the zoom\'s end stepped forward through the dates shown from a quarter in, stopped where it is', async () => {
    const { root, drawn, zooms, view, mounted } = viewer(SPEC);
    try {
      await until(() => drawn.length > 0 && mounted.on !== undefined, 'the chart');
      const n = zooms.length;
      barButton(root, 'Replay').click();
      assert.equal(barButton(root, 'Stop').getAttribute('aria-pressed'), 'true');
      await until(() => zooms.length >= n + 4, 'a few steps');
      barButton(root, 'Stop').click();
      const steps = zooms.slice(n) as { from: string; to: string }[];
      const spx = of('SPX');
      assert.equal(steps[0]!.from, spx.days[0], 'from the first date shown');
      assert.equal(steps[0]!.to, spx.days[Math.floor(spx.days.length / 4)], 'a quarter in');
      assert.ok(steps.every((z, i) => i === 0 || z.to > steps[i - 1]!.to), `each step later: ${steps.map((z) => z.to).join(', ')}`);
      const stopped = zooms.length;
      await new Promise((r) => setTimeout(r, 250));
      assert.equal(zooms.length, stopped, 'stopped: no more steps');
      assert.ok(barButton(root, 'Replay'), 'Replay again');
    } finally {
      view.dispose();
    }
  });

  // the Phase 6 review's 1: marks added once the tile is drawn were never asked (the fetch's key left the annotations
  // out), and an annotation taken off before them broke every drawing after (the answers kept by their place)
  it('marks added after it is drawn are asked and drawn; an annotation before them taken off, drawn still', async () => {
    const { drawn, view } = viewer(SPEC);
    const marks = { kind: 'marks', source: 'px', where: [{ value: 'SPX' }], at: 'close', shape: 'up', label: 'Closes' } as const;
    const ids = (): string[] => (drawn.at(-1)!.series as { id: string }[]).map((x) => x.id);
    try {
      await until(() => drawn.length > 0, 'the chart');
      view.update({ ...SPEC, annotations: [{ kind: 'level', value: 3000 }, marks] });
      await until(() => ids().includes('marks:1'), 'the marks added');
      const n = drawn.length;
      view.update({ ...SPEC, annotations: [marks] });
      await until(() => drawn.length > n && ids().includes('marks:0'), 'the marks, now the first annotation');
      assert.ok(!ids().includes('marks:1'));
      view.update({ ...SPEC, annotations: [] });
      await until(() => !ids().some((id) => id.startsWith('marks:')), 'the marks taken off');
    } finally {
      view.dispose();
    }
  });

  it('a range told as the page\'s spec, the zoom moved to it; a line hidden is drawn again from the rows it has', async () => {
    const { root, drawn, told, zooms, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const year = [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-range')].find((b) => b.textContent === '1Y')!;
      year.click();
      assert.deepEqual(told.at(-1)!.range, { last: '1y' });
      assert.equal(zooms.length, 1, 'the zoom moved to the range');
      // the buttons painted again, the range's pressed
      const pressed = (): string[] => [...root.querySelectorAll('.dc-ts-range[aria-pressed="true"]')].map((b) => b.textContent ?? '');
      await until(() => pressed().join() === '1Y', 'its range pressed');
      // a line hidden by its chip: told, and drawn from the rows it has -- nothing asked again
      await new Promise((r) => setTimeout(r, 100));
      const asked = runs;
      const drew = drawn.length;
      (root.querySelector('.dc-ts-chip') as HTMLElement).click();
      assert.equal(told.at(-1)!.series[0]!.hidden, true);
      await until(() => drawn.length > drew, 'the line hidden');
      assert.equal(runs, asked, 'a line hidden asks the database nothing');
      const ids = (drawn.at(-1)!.series as { id: string }[]).map((x) => x.id);
      assert.ok(!ids.includes('A'), 'the hidden line is not drawn');
    } finally {
      view.dispose();
    }
  });

  // the review's P2 (docs/TIMESERIES_REVIEW_2026_10_25.md): rows that move under the same query (a frame served again, a
  // file read again) were kept, the tile answering from what it had -- a results cache, which the owner ruled out
  it('a grid\'s view landing asks again, the database\'s new rows drawn though the grid\'s query is the same', async () => {
    const { root, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const last = of('SPX').closes.at(-1)!;
      assert.ok(chips(root)[0]!.includes(last.toFixed(2)));
      await (database.connection as unknown as { query(sql: string): Promise<unknown> }).query(`UPDATE "${table}" SET close = close * 2`);
      try {
        view.refresh(0);
        await until(() => chips(root)[0]!.includes((last * 2).toFixed(2)), 'the rows as the database now has them');
      } finally {
        await (database.connection as unknown as { query(sql: string): Promise<unknown> }).query(`UPDATE "${table}" SET close = close / 2`);
      }
    } finally {
      view.dispose();
    }
  });

  it('the series editor reads an expression as it is typed, says where it cannot, and plots it', async () => {
    const { root, windows, told, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-bar button')].find((b) => b.textContent === '+ Series')!.click();
      // a window beside the tile, among the page's windows, titled by the tile (the user, 2026-10-26: PR #73's design)
      const pop = windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      assert.ok(pop, 'the editor is open, a window of its own');
      assert.equal(root.querySelector('.dc-ts-pop'), null, 'not over the tile');
      assert.match(windows.querySelector('.dc-ts-window-title')?.textContent ?? '', /^New series C — Markets$/);
      const expr = pop.querySelector('.dc-ts-expr') as HTMLInputElement;
      const typed = (text: string): string => {
        expr.value = text;
        expr.dispatchEvent(new expr.ownerDocument.defaultView!.Event('input'));
        return pop.querySelector('.dc-ts-error')?.textContent ?? '';
      };
      assert.match(typed('vola($A)'), /there is no function vola/);
      assert.match(typed('smooth_outliers($A)'), /an iterative procedure/);
      // gs-quant's recursions are built now (the expert functions, 2026-10-26): read as typed
      assert.equal(typed('macd($A)'), '');
      assert.equal(typed('mean($A, 50)'), '');
      [...pop.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'OK')!.click();
      const added = told.at(-1)!.series.at(-1)!;
      assert.equal(added.id, 'C');
      assert.deepEqual(added.expr, parseExpr('mean($A, 50)'));
      assert.equal(windows.querySelector('.dc-ts-window'), null, 'the editor closed, its window gone');
      // the third review's 1, 2: a change refused keeps its editor open, what was typed in it kept; Escape puts it away
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-bar button')].find((b) => b.textContent === '+ Series')!.click();
      const again = windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const field = again.querySelector('.dc-ts-expr') as HTMLInputElement;
      field.value = 'nosuchcolumn[SPX]';
      field.dispatchEvent(new field.ownerDocument.defaultView!.Event('input'));
      const before = told.length;
      [...again.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'OK')!.click();
      assert.equal(told.length, before, 'nothing told');
      assert.ok(windows.querySelector('.dc-ts-window'), 'its editor still open');
      assert.equal((windows.querySelector('.dc-ts-expr') as HTMLInputElement).value, 'nosuchcolumn[SPX]', 'what was typed kept');
      const win = windows.querySelector<HTMLElement>('.dc-ts-window')!;
      win.dispatchEvent(new win.ownerDocument.defaultView!.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
      assert.equal(windows.querySelector('.dc-ts-window'), null, 'Escape puts it away');
    } finally {
      view.dispose();
    }
  });
});

/** A button of the tile's toolbar, by its label. */
const barButton = (root: HTMLElement, label: string): HTMLButtonElement =>
  [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-bar button')].find((b) => b.textContent === label)!;
/** A button of the open window, by its label. */
const windowButton = (windows: HTMLElement, label: string): HTMLButtonElement =>
  [...windows.querySelectorAll<HTMLButtonElement>('.dc-ts-window button')].find((b) => b.textContent === label)!;
/** A series' editor opened from its chip (a double-click), by the chip's name: its window's content. */
function openSeries(root: HTMLElement, windows: HTMLElement, name: string): HTMLElement {
  const chip = [...root.querySelectorAll<HTMLElement>('.dc-ts-chip')].find((c) => c.textContent?.startsWith(name))!;
  chip.dispatchEvent(new chip.ownerDocument.defaultView!.MouseEvent('dblclick', { bubbles: true }));
  return windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
}
/** A slider moved to `value`, as a drag moves it. */
function drag(slider: HTMLInputElement, value: string): void {
  slider.value = value;
  slider.dispatchEvent(new slider.ownerDocument.defaultView!.Event('input'));
}
/** The page's clipboard, what is written onto it kept. */
function clipboardOf(root: HTMLElement): string[] {
  const written: string[] = [];
  Object.defineProperty(root.ownerDocument.defaultView!.navigator, 'clipboard', {
    configurable: true, value: { writeText: async (text: string) => { written.push(text); } },
  });
  return written;
}

// the next phases' 2 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): what makes a tile a day's tool
describe('the timeseries viewer, every day: its data, a swap, its queries, a picture, a parameter dragged', () => {
  it('the Data tab: every line side by side, latest first, each in its format; Copy every date, oldest first', async () => {
    const { root, view } = viewer(SPEC);
    const written = clipboardOf(root);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Data')!.click();
      assert.deepEqual([...root.querySelectorAll('.dc-ts-data th')].map((th) => th.textContent), ['Date', 'S&P 500', '21d vol']);
      const spx = of('SPX');
      const rows = statRows(root);
      assert.equal(rows.length, Math.min(500, spx.days.length), 'the latest 500 dates at most');
      assert.equal(rows[0]![1], spx.closes.at(-1)!.toFixed(2), 'the latest first, as the grid writes its close');
      assert.match(rows[0]![2]!, /%$/, 'a volatility in percent');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats button')].find((b) => b.textContent === 'Copy')!.click();
      await until(() => written.length === 1, 'the table copied');
      const lines = written[0]!.trim().split('\n');
      assert.equal(lines[0], 'date\tS&P 500\t21d vol');
      assert.equal(lines.length, spx.days.length + 1, 'every date, not the latest 500 alone');
      const [date, close] = lines[1]!.split('\t');
      assert.deepEqual([date!.slice(0, 10), close], [spx.days[0], String(spx.closes[0])], 'oldest first, the figure as answered');
    } finally {
      view.dispose();
    }
  });

  it('Swap: one instrument for another of its source\'s, the database\'s list of them, told as the tile\'s one rule', async () => {
    const { root, windows, told, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Swap').click();
      const pop = windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const [from, to] = [...pop.querySelectorAll('select')] as HTMLSelectElement[];
      assert.deepEqual([...from!.options].map((o) => o.textContent), ['SPX']);
      await until(() => [...to!.options].some((o) => o.value === 'NDX'), 'the instruments its source has');
      assert.ok(![...to!.options].some((o) => o.value === 'SPX'), 'not the one it reads');
      to!.value = 'NDX';
      windowButton(windows, 'Swap').click();
      assert.deepEqual(told.at(-1)!.series[0]!.expr, parseExpr('close[NDX]'));
      assert.deepEqual(told.at(-1)!.series[1]!.expr, parseExpr('volatility($A, 21)'), 'its reader follows by its reference');
      assert.equal(windows.querySelector('.dc-ts-window'), null, 'the window put away');
    } finally {
      view.dispose();
    }
  });

  // the Phase 2 review's 6: the list painted again before the database answered the first: both answers appended
  it('Swap\'s list painted again while the database answers: the last paint\'s answer alone', async () => {
    const two: TimeseriesSpec = { ...SPEC, series: [{ id: 'A', expr: parseExpr('close[SPX]') }, { id: 'B', expr: parseExpr('close[NDX]') }] };
    const { root, windows, view } = viewer(two);
    try {
      await until(() => chips(root).length === 2, 'its lines');
      barButton(root, 'Swap').click();
      const pop = windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const [from, to] = [...pop.querySelectorAll('select')] as HTMLSelectElement[];
      from!.value = '1';
      from!.dispatchEvent(new (from!.ownerDocument.defaultView!.Event)('change'));
      await until(() => [...to!.options].some((o) => o.value === 'SPX'), 'the instruments its source has');
      await new Promise((r) => setTimeout(r, 200));
      const values = [...to!.options].map((o) => o.value).filter((v) => v !== '');
      assert.deepEqual(values, [...new Set(values)], 'each once');
      assert.ok(!values.includes('NDX'), 'not the one it reads');
    } finally {
      view.dispose();
    }
  });

  // the user, 2026-10-26: "show all the pure queries used to create a timeseries chart"
  it('Query: every question the tile asks the database, its Pure as the compiler prints it and the SQL it ran, each copied and all', async () => {
    const { root, windows, view } = viewer(SPEC);
    const written = clipboardOf(root);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Query').click();
      const pop = (): HTMLElement => windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const asks = (): HTMLElement[] => [...pop().querySelectorAll<HTMLElement>('.dc-ts-ask')];
      const said = (a: HTMLElement): string => a.querySelector('summary span')!.textContent ?? '';
      const status = (a: HTMLElement): string => a.querySelector('.dc-ts-ask-status')!.textContent ?? '';
      const code = (a: HTMLElement): string[] => [...a.querySelectorAll('.dc-ts-code')].map((c) => c.textContent ?? '');
      // by the one rule Python's queries() reads (timeseries-query.ts tileAsks): the chart's, then the panel's
      assert.deepEqual(asks().map(said), [
        "1. Source px's dates: its latest and earliest, where each range button starts",
        '2. Series A (S&P 500): its points',
        '3. Series B (21d vol): its points',
        '4. Series A (S&P 500): its statistics over the dates shown',
        '5. Series B (21d vol): its statistics over the dates shown',
        '6. Series A (S&P 500): its deepest drawdowns over the dates shown',
        '7. Series A (S&P 500): its correlation with itself',
        '8. The correlation of A (S&P 500) and B (21d vol)',
        '9. Series B (21d vol): its correlation with itself',
        '10. Series A (S&P 500): its returns each month and each year',
      ]);
      // the queries' two, then each program's formulas with their Pure (the user's "A", 2026-10-28)
      assert.deepEqual([...pop().querySelectorAll('.dc-ts-heading')].map((h) => h.textContent),
        ['Drawing the chart', 'The statistics panel', 'Each formula\'s Pure', 'Each formula\'s Pure']);
      // what was asked to draw the chart and its statistics ran; the tabs not shown not yet
      assert.deepEqual(asks().map(status), ['ran', 'ran', 'ran', 'ran', 'ran', 'not asked yet', 'not asked yet', 'not asked yet', 'not asked yet', 'not asked yet']);
      assert.deepEqual(asks().map((a) => a.hasAttribute('open')), [true, true, true, false, false, false, false, false, false, false], "the chart's open");
      await until(() => asks().every((a) => code(a)[0] !== 'Writing…'), 'every query written');
      for (const a of asks()) assert.match(code(a)[0]!.trimStart(), /^\|/, `${said(a)}: a lambda, as the compiler prints one: ${code(a)[0]!.slice(0, 80)}`);
      const [pureA, sqlA] = code(asks()[1]!);
      assert.match(pureA!, /'SPX'/);
      assert.match(sqlA!, /select/i, 'the SQL its rows were read by');
      assert.match(code(asks()[5]!)[1]!, /^Not asked yet: the Drawdowns tab asks for it when shown\.$/);
      assert.match(code(asks()[5]!)[0]!, /'SPX'/, 'written as its tab would ask it');
      windowButton(asks()[1]!, 'Copy SQL').click();
      await until(() => written.length === 1, 'its SQL copied');
      assert.equal(written[0], sqlA);
      windowButton(pop(), 'Copy all').click();
      await until(() => written.length === 2, 'every query copied');
      for (const a of asks()) assert.ok(written[1]!.includes(`// ${said(a)} -- for `), `${said(a)} among them`);
      assert.ok(written[1]!.includes(pureA!) && written[1]!.includes(sqlA!), 'each its Pure and its SQL');
      // a tab shown asks its question: the window says it ran, as it was sent
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Drawdowns')!.click();
      await until(() => statRows(root).length > 0 && statRows(root)[0]!.length === 8, 'its drawdowns');
      barButton(root, 'Query').click();
      await until(() => asks().length === 10 && status(asks()[5]!) === 'ran', 'the drawdowns asked');
      await until(() => code(asks()[5]!)[0] !== 'Writing…', 'written');
      assert.match(code(asks()[5]!)[1]!, /select/i, 'the SQL it ran');
    } finally {
      view.dispose();
    }
  });

  // the user, 2026-10-26: "an option that shows how exactly to build that chart programmatically from TS and python"
  it('Query: the tile as the code that builds it, in Python and in TypeScript, each copied -- and the library its queries call', async () => {
    const { root, windows, view } = viewer(SPEC);
    const written = clipboardOf(root);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Query').click();
      const tabs = [...windows.querySelectorAll<HTMLButtonElement>('.dc-ts-window [role=tab]')];
      assert.deepEqual(tabs.map((t) => t.textContent), ['Queries', 'Library', 'Python', 'TypeScript']);
      const panels = [...windows.querySelectorAll<HTMLElement>('.dc-ts-window .dc-ts-code-panel')];
      assert.deepEqual(panels.map((p) => p.hidden), [false, true, true, true], 'the queries first');
      // the user, 2026-10-26 (pure-library.ts): the Pure functions its queries call by name, as the planner reads them
      tabs[1]!.click();
      assert.deepEqual(panels.map((p) => p.hidden), [true, false, true, true]);
      const program = (i: number): string => panels[i]!.querySelector('.dc-ts-program')!.textContent!;
      // its text a chunk of its own (pure-library-text.ts), read as the planner reads it
      await until(() => program(1) !== 'Reading…', 'the library read');
      assert.equal(program(1), DATACUBE_LIBRARY);
      assert.match(program(1), /function timeseries::volatility</);
      windowButton(windows, 'Copy Library').click();
      await until(() => written.length === 1, 'the library copied');
      assert.equal(written[0], DATACUBE_LIBRARY);
      written.length = 0;
      tabs[2]!.click();
      assert.deepEqual(panels.map((p) => p.hidden), [true, true, false, true]);
      assert.deepEqual(tabs.map((t) => t.getAttribute('aria-selected')), ['false', 'false', 'true', 'false']);
      // by the one rule Python's tile.code() reads (timeseries-code.ts tileCode), over the grids the page says it reads:
      // a page of its own, its grid opened as the page's was (the user, 2026-10-28: "a generic snippet")
      const of = { title: 'Markets', grids: { prices: { title: 'Prices', source: PRICES_FILE } } };
      await until(() => program(2) !== 'Writing…' && program(3) !== 'Writing…', 'its programs written');
      const python = program(2);
      assert.equal(python, tileCode(SPEC, of, 'python'));
      assert.match(python, /page = ll\.Page\(\)\nprices = page\.grid\(file='prices\.csv'\)/);
      assert.match(python, /tile = prices\.timeseries\(/);
      // and under it each formula with the Pure the tile writes for it, as the compiler prints it
      const pures = (i: number): string[] => [...panels[i]!.querySelectorAll<HTMLElement>('pre.dc-ts-code:not(.dc-ts-program)')].map((x) => x.textContent ?? '');
      await until(() => pures(2).length === SPEC.series.length && pures(2).every((x) => x !== 'Writing…'), 'each formula\'s Pure');
      assert.deepEqual([...panels[2]!.querySelectorAll('.dc-ts-field-label')].map((x) => x.textContent), SPEC.series.map((x) => `${x.id}  ${exprSource(x.expr)}`));
      assert.ok(pures(2).every((x) => x.includes('#>{') || x.includes('->')), pures(2).join('\n'));
      assert.match(python, /\{'expr': 'close\[SPX\]', 'name': 'S&P 500'\}/);
      windowButton(windows, 'Copy Python').click();
      await until(() => written.length === 1, 'its Python copied');
      assert.equal(written[0], python);
      tabs[3]!.click();
      const typescript = program(3);
      assert.equal(typescript, tileCode(SPEC, of, 'typescript'));
      assert.match(typescript, /kind: 'addTimeseries', sheet, near: null/);
      assert.match(typescript, /export const cubes = \{\n  px: 'prices', \/\/ the grid "Prices"/);
      windowButton(windows, 'Copy TypeScript').click();
      await until(() => written.length === 2, 'its TypeScript copied');
      assert.equal(written[1], typescript);
    } finally {
      view.dispose();
    }
  });

  it('a picture: drawn again with the tile\'s title, subtitle and note above it, taller by them, on the page\'s surface', async () => {
    const { root, exported, drawn, view } = viewer({ ...SPEC, view: { ...SPEC.view, subtitle: 'Daily', note: 'Source: the sample' } });
    const saved: string[] = [];
    const Anchor = root.ownerDocument.defaultView!.HTMLAnchorElement;
    Anchor.prototype.click = function click(this: HTMLAnchorElement) { saved.push(`${this.download} ${this.href.slice(0, this.href.indexOf(";"))}`); };
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const canvas = root.querySelector('.dc-ts-chart') as HTMLElement;
      // not drawn on the screen (no width): said, nothing made
      barButton(root, 'PNG').click();
      await until(() => /no chart drawn/.test(root.querySelector('.dc-ts-banner')?.textContent ?? ''), 'no picture of nothing');
      assert.equal(exported.length, 0);
      Object.defineProperty(canvas, 'clientWidth', { configurable: true, value: 800 });
      barButton(root, 'SVG').click();
      await until(() => saved.length === 1, 'the picture handed over');
      assert.deepEqual(saved, ['Markets.svg data:image/svg']);
      const [made] = exported;
      assert.equal(made!.kind, 'svg');
      // its panes as tall as on the screen (the Phase 2 review's 10: the screen's heading had been counted twice), its
      // heading and its lines' legend above them, no navigator under them (the review's 4)
      const panes = (o: EChartsOption): number[] => (o.grid as { height: number }[]).map((g) => g.height);
      assert.equal(made!.width, 800);
      assert.deepEqual(panes(made!.option), panes(drawn.at(-1)!), 'its panes as tall as on the screen');
      assert.deepEqual((made!.option.title as { text: string }[]).map((t) => t.text), ['Markets', 'Daily', 'Source: the sample']);
      assert.deepEqual((made!.option.legend as { data: { name: string }[] }).data.map((d) => d.name), ['S&P 500', '21d vol'], 'its lines named in it');
      assert.ok(!(made!.option.dataZoom as { type: string }[]).some((z) => z.type === 'slider'), 'no navigator in a picture');
      assert.ok(made!.background !== '' && made!.background !== 'transparent', `on a surface: ${made!.background}`);
    } finally {
      view.dispose();
    }
  });

  it('a parameter dragged: its expression moved, the series drawn again when it stops, kept by OK alone', async () => {
    const { root, windows, told, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const pop = openSeries(root, windows, '21d vol');
      const slider = pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="w"]')!;
      assert.ok(slider, 'its window\'s length a slider');
      assert.equal(slider.value, '21');
      drag(slider, '30');
      assert.equal((pop.querySelector('.dc-ts-expr') as HTMLInputElement).value, 'volatility($A, 30)');
      // drawn again when it stops, as a look of the person's own: the page told nothing (the Phase 2 review's 1)
      await until(() => JSON.stringify(view.spec.series[1]!.expr) === JSON.stringify(parseExpr('volatility($A, 30)')), 'the series drawn again when it stops');
      await new Promise((r) => setTimeout(r, 450));
      assert.equal(told.length, 0, 'nothing told while it is tried');
      assert.ok(windows.querySelector('.dc-ts-window'), 'its editor still open');
      windowButton(windows, 'OK').click();
      assert.equal(told.length, 1, 'kept by OK, once: one step to undo');
      assert.deepEqual(told[0]!.series[1]!.expr, parseExpr('volatility($A, 30)'));
      assert.equal(told[0]!.series[1]!.name, '21d vol', 'the rest of the series as it was');
      assert.equal(windows.querySelector('.dc-ts-window'), null);
    } finally {
      view.dispose();
    }
  });

  // the Phase 2 review's 9: a number of no least had its slider from 0 -- a filter's -0.02 could go no lower, a power no
  // negative -- and a filter's 0 to 1 on prices was blind to their scale
  it('a number of no least or most: its slider either side of 0, as far as its value goes', async () => {
    const spec: TimeseriesSpec = { ...SPEC, series: [...SPEC.series, { id: 'C', expr: parseExpr('filter(returns($A), less_than, -0.02)'), name: 'Bad days' }] };
    const { root, windows, view } = viewer(spec);
    try {
      await until(() => chips(root).length === 3, 'its lines');
      const pop = openSeries(root, windows, 'Bad days');
      const slider = pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="value"]')!;
      assert.ok(slider, 'its value a slider');
      assert.equal(Number(slider.value), -0.02);
      assert.ok(Number(slider.min) <= -0.08 && Number(slider.max) >= 0.08, `either side of 0: ${slider.min} to ${slider.max}`);
      assert.equal(Number(slider.min), -Number(slider.max), 'as far down as up');
    } finally {
      view.dispose();
    }
  });

  it('a parameter dragged, then Cancel, Escape or Remove: nothing of the drag kept, nothing told after (the Phase 2 review\'s 1)', async () => {
    const { root, windows, told, view } = viewer(SPEC);
    const was = JSON.stringify(SPEC.series[1]!.expr);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      // tried and drawn, then Cancel: as it was, drawn so, the page told nothing
      let pop = openSeries(root, windows, '21d vol');
      drag(pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="w"]')!, '40');
      await until(() => JSON.stringify(view.spec.series[1]!.expr) === JSON.stringify(parseExpr('volatility($A, 40)')), 'the drag drawn');
      windowButton(windows, 'Cancel').click();
      assert.equal(JSON.stringify(view.spec.series[1]!.expr), was, 'Cancel leaves it as it was');
      // dragged, then Escape before it stops: its stop never lands
      pop = openSeries(root, windows, '21d vol');
      drag(pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="w"]')!, '30');
      const win = windows.querySelector('.dc-ts-window') as HTMLElement;
      win.dispatchEvent(new (win.ownerDocument.defaultView!.KeyboardEvent)('keydown', { key: 'Escape', bubbles: true }));
      assert.equal(windows.querySelector('.dc-ts-window'), null, 'Escape put it away');
      await new Promise((r) => setTimeout(r, 450));
      assert.equal(JSON.stringify(view.spec.series[1]!.expr), was);
      assert.equal(told.length, 0, 'nothing told by a drag put away');
      // tried, then another change told (Log): made to the tile as the page has it, never with the try
      pop = openSeries(root, windows, '21d vol');
      drag(pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="w"]')!, '50');
      await until(() => JSON.stringify(view.spec.series[1]!.expr) === JSON.stringify(parseExpr('volatility($A, 50)')), 'the drag drawn');
      barButton(root, 'Log').click();
      assert.equal(told.at(-1)!.panes?.[0]?.log, true);
      assert.equal(JSON.stringify(told.at(-1)!.series[1]!.expr), was, 'the try not told with it');
      assert.equal(JSON.stringify(view.spec.series[1]!.expr), was, 'and drawn as the page has it');
      windowButton(windows, 'Cancel').click();
      told.length = 0;
      // dragged, then Remove: the series taken off, and its drag's stop says nothing after
      pop = openSeries(root, windows, '21d vol');
      drag(pop.querySelector<HTMLInputElement>('.dc-ts-slider[aria-label$="w"]')!, '30');
      windowButton(windows, 'Remove').click();
      assert.deepEqual(told.at(-1)!.series.map((s) => s.id), ['A']);
      await new Promise((r) => setTimeout(r, 450));
      assert.equal(told.length, 1, 'the Remove alone');
      assert.equal(told[0]!.panes?.[0]?.log, true, 'made to the page\'s tile');
      assert.doesNotMatch(root.querySelector('.dc-ts-banner')?.textContent ?? '', /there is no series/);
    } finally {
      view.dispose();
    }
  });

  // marks from a grid's rows and a column shaded, each added in Settings by the tile's one rule (Phase 5, Phase 6's D4)
  it('Settings: marks of the grid\'s rows added at a column, and a column shaded where it is not 0', async () => {
    const { root, windows, told, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Settings').click();
      const win = (): HTMLElement => windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const Event = win().ownerDocument.defaultView!.Event;
      const field = <T extends HTMLElement>(label: string): T => win().querySelector<T>(`[aria-label="${label}"]`)!;
      const set = (label: string, value: string): void => {
        const f = field<HTMLInputElement | HTMLSelectElement>(label);
        f.value = value;
        f.dispatchEvent(new Event('change'));
      };
      set('Rows marked', 'SPX');
      set('Drawn at', 'close');
      set('Shape', 'down');
      set('Says', 'volume');
      set('Marks label', 'SPX closes');
      [...win().querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'Add' && b.title === 'Mark the rows over the series')!.click();
      assert.deepEqual(told.at(-1)!.annotations, [{ kind: 'marks', source: 'px', where: [{ value: 'SPX' }], at: 'close', shape: 'down', text: 'volume', label: 'SPX closes' }]);
      set('Shade column', 'volume');
      set('Rows shaded', 'NDX');
      [...win().querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'Shade')!.click();
      assert.deepEqual(told.at(-1)!.series.at(-1), { id: 'C', expr: parseExpr('volume[NDX] != 0'), name: 'volume' });
    } finally {
      view.dispose();
    }
  });

  // a trend projected (Phase 5), added in Settings by the tile's one rule: asked of the database beside the series,
  // drawn through the S&P's last closes and on past them -- its figures the database's
  it('Settings: a projection of a series added, asked and drawn on past its last date; Settings lists it', async () => {
    const { root, windows, told, drawn, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Settings').click();
      const win = (): HTMLElement => windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const Event = win().ownerDocument.defaultView!.Event;
      const set = (label: string, value: string): void => {
        const f = win().querySelector<HTMLInputElement | HTMLSelectElement>(`[aria-label="${label}"]`)!;
        f.value = value;
        f.dispatchEvent(new Event('change'));
      };
      set('Projected series', 'A');
      set('Observations fit', '20');
      set('Projected ahead', '1m');
      set('Projection label', 'Trend');
      [...win().querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === 'Project')!.click();
      assert.deepEqual(told.at(-1)!.annotations, [{ kind: 'projection', series: 'A', window: 20, ahead: '1m', label: 'Trend' }]);
      type Drawn = { id: string; data: [string, number][] };
      const projected = (): Drawn | undefined => (drawn.at(-1)!.series as Drawn[]).find((x) => x.id === 'projection:0');
      await until(() => projected() !== undefined, 'the projection drawn');
      const spx = of('SPX');
      const [from, last, end] = projected()!.data;
      assert.deepEqual([from![0], last![0]], [spx.days.at(-20)!, spx.days.at(-1)!], 'through its last 20 closes');
      assert.ok(end![0] > last![0] && end![0].slice(0, 7) !== last![0].slice(0, 7), `a month on: ${end![0]}`);
      // the line through them: its fit at the last close near it, the database's
      assert.ok(Math.abs(last![1] - spx.closes.at(-1)!) < spx.closes.at(-1)! * 0.1, `${last![1]} near ${spx.closes.at(-1)}`);
      assert.ok(win().textContent!.includes('Projection of A: its last 20 observations, 1m on'), 'Settings lists it');
    } finally {
      view.dispose();
    }
  });

  it('Settings: a subtitle drawn under the chart\'s top, a pane turned upside down, its ends set', async () => {
    const { root, windows, told, drawn, view } = viewer(SPEC);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Settings').click();
      const pop = windows.querySelector('.dc-ts-window .dc-ts-pop') as HTMLElement;
      const Event = pop.ownerDocument.defaultView!.Event;
      const sub = pop.querySelector<HTMLInputElement>('input[aria-label="Subtitle"]')!;
      sub.value = 'Daily close';
      sub.dispatchEvent(new Event('change'));
      assert.equal(told.at(-1)!.view?.subtitle, 'Daily close');
      const upside = [...windows.querySelectorAll<HTMLLabelElement>('.dc-ts-window label')].find((l) => l.textContent === 'Upside down')!;
      const box = upside.querySelector('input')!;
      box.checked = true;
      box.dispatchEvent(new Event('change'));
      assert.equal(told.at(-1)!.panes?.[0]?.invert, true);
      const most = windows.querySelector<HTMLInputElement>('.dc-ts-window input[aria-label="Most"]')!;
      most.value = '9000';
      most.dispatchEvent(new Event('change'));
      assert.equal(told.at(-1)!.panes?.[0]?.max, 9000);
      // a number the box cannot read ("5,000"), or none it can draw (1e400): said, the end kept -- never taken off
      // without a word (the Phase 2 review's 8)
      const kept = told.length;
      const bad = windows.querySelector<HTMLInputElement>('.dc-ts-window input[aria-label="Most"]')!;
      Object.defineProperty(bad, 'validity', { configurable: true, value: { badInput: true } });
      bad.value = '';
      bad.dispatchEvent(new Event('change'));
      assert.equal(told.length, kept, 'nothing told');
      assert.match(root.querySelector('.dc-ts-banner')?.textContent ?? '', /Most: what was typed is not a number/);
      const huge = windows.querySelector<HTMLInputElement>('.dc-ts-window input[aria-label="Most"]')!;
      // read as typed (a DOM's number box keeps no 1e400 of its own), its validity none bad
      Object.defineProperty(huge, 'validity', { configurable: true, value: { badInput: false } });
      Object.defineProperty(huge, 'value', { configurable: true, writable: true, value: '1e400' });
      huge.dispatchEvent(new Event('change'));
      assert.equal(told.length, kept, 'nothing told');
      assert.match(root.querySelector('.dc-ts-banner')?.textContent ?? '', /Most: 1e400 is no number the axis can end at/);
      assert.equal(windows.querySelector<HTMLInputElement>('.dc-ts-window input[aria-label="Most"]')!.value, '9000', 'the box as the tile has it');
      await until(() => (drawn.at(-1)!.yAxis as { inverse?: boolean; max?: number }[])[0]?.max === 9000, 'drawn so');
      const left = (drawn.at(-1)!.yAxis as { inverse?: boolean; max?: number }[])[0]!;
      assert.equal(left.inverse, true);
      assert.deepEqual((drawn.at(-1)!.title as { text: string }[]).map((t) => t.text), ['Daily close'], 'the subtitle on the screen too; the title the header\'s');
    } finally {
      view.dispose();
    }
  });
});

/** A select of the tile's toolbar, by its label, set to `value` as the person picks it. */
function pick(root: HTMLElement, label: string, value: string): void {
  const s = root.querySelector<HTMLSelectElement>(`select[aria-label="${label}"]`)!;
  s.value = value;
  s.dispatchEvent(new (root.ownerDocument.defaultView!.Event)('change'));
}
/** A series of the last option drawn, by its id. */
const drawnSeries = (drawn: EChartsOption[], id: string): { data: unknown[]; name?: string } | undefined =>
  (drawn.at(-1)?.series as { id?: string; data: unknown[]; name?: string }[] | undefined)?.find((x) => x.id === id);

// the next phases' 3 (2026-10-26, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): a tile's other views, each the
// database's answer to its own query -- held here to the sample's rows themselves
describe('the timeseries viewer\'s other views: a scatter, a histogram, seasonality, a curve, the calendar', () => {
  const TWO: TimeseriesSpec = {
    sources: [{ alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }],
    series: [{ id: 'A', expr: parseExpr('close[SPX]'), name: 'S&P 500' }, { id: 'B', expr: parseExpr('close[NDX]'), name: 'Nasdaq 100' }],
    view: { stats: true },
  };

  it('View ▸ Scatter: a point a date, y against x, and the database\'s regression drawn from the least x to the most', async () => {
    const { root, windows, told, drawn, exported, view } = viewer(TWO);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      pick(root, 'View', 'scatter');
      assert.equal(told.at(-1)!.view?.mode, 'scatter');
      await until(() => drawnSeries(drawn, 'points') !== undefined, 'the scatter');
      const [x, y] = [of('SPX'), of('NDX')];
      assert.equal(drawnSeries(drawn, 'points')!.data.length, x.days.length, 'a point a date both have');
      // ordinary least squares of y on x, by the test's own arithmetic
      const n = x.closes.length;
      const mx = x.closes.reduce((a, b) => a + b, 0) / n;
      const my = y.closes.reduce((a, b) => a + b, 0) / n;
      const cov = x.closes.reduce((a, v, i) => a + (v - mx) * (y.closes[i]! - my), 0);
      const vx = x.closes.reduce((a, v) => a + (v - mx) ** 2, 0);
      const slope = cov / vx;
      const lo = Math.min(...x.closes);
      const fit = drawnSeries(drawn, 'fit')!.data as [number, number][];
      assert.ok(Math.abs(fit[0]![0] - lo) < 1e-9, 'from the least x');
      assert.ok(Math.abs(fit[0]![1] - (my - slope * mx + slope * lo)) < 1e-6 * Math.abs(fit[0]![1]), `the database's fit: ${fit[0]![1]}`);
      assert.match(drawnSeries(drawn, 'fit')!.name!, /^y = .*R² /, 'its equation and R² in its legend');
      // its query in the Query window; a picture of it
      barButton(root, 'Query').click();
      assert.ok([...windows.querySelectorAll('.dc-ts-ask summary span')].some((h) => /^\d+\. The scatter: /.test(h.textContent ?? '')), 'the scatter\'s own query');
      const canvas = root.querySelector('.dc-ts-chart') as HTMLElement;
      Object.defineProperty(canvas, 'clientWidth', { configurable: true, value: 800 });
      const Anchor = root.ownerDocument.defaultView!.HTMLAnchorElement;
      Anchor.prototype.click = function click() { /* handed over */ };
      barButton(root, 'SVG').click();
      await until(() => exported.length === 1, 'the picture made');
      assert.ok((exported[0]!.option.series as { id?: string }[]).some((x) => x.id === 'points'), 'the scatter drawn, not the lines');
      assert.equal((exported[0]!.option.title as { text: string }[])[0]!.text, 'Markets');
      // along time again: the lines, from the rows it has
      pick(root, 'View', '');
      await until(() => drawnSeries(drawn, 'points') === undefined && (drawn.at(-1)!.series as unknown[]).length > 0, 'the lines again');
    } finally {
      view.dispose();
    }
  });

  it('a histogram of the first series\' values, every one counted; seasonality by year; no curve of a series of one line', async () => {
    const { root, drawn, view } = viewer(TWO);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      pick(root, 'View', 'histogram');
      await until(() => drawnSeries(drawn, 'bins') !== undefined, 'the histogram');
      const bins = drawnSeries(drawn, 'bins')!.data as { value: [number, number] }[];
      assert.ok(bins.length <= 30, `at most its 30 bins: ${bins.length}`);
      assert.equal(bins.reduce((a, b) => a + b.value[1], 0), of('SPX').days.length, 'every value in a bin');
      pick(root, 'View', 'seasonality');
      await until(() => drawnSeries(drawn, 'mean') !== undefined, 'the seasonality');
      const years = [...new Set(of('SPX').days.map((d) => d.slice(0, 4)))];
      for (const y of years) assert.ok(drawnSeries(drawn, `y${y}`) !== undefined, `a line for ${y}`);
      pick(root, 'View', 'curve');
      await until(() => /A curve is of a series split by a column/.test(root.querySelector('.dc-ts-banner')?.textContent ?? ''), 'its refusal said');
    } finally {
      view.dispose();
    }
  });

  it('a curve: a split series\' lines across its key, each its latest value, as of the latest date', async () => {
    const spec: TimeseriesSpec = {
      sources: [{ alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }],
      series: [{ id: 'A', expr: parseExpr('close'), split: 'symbol' }],
      view: { mode: 'curve', dates: ['latest'] },
    };
    const { drawn, view } = viewer(spec);
    try {
      await until(() => drawnSeries(drawn, 'd0') !== undefined, 'the curve');
      const axis = (drawn.at(-1)!.xAxis as { data: string[] }).data;
      assert.deepEqual([...axis].sort(), ['AAPL', 'EURUSD', 'GLD', 'JPM', 'MSFT', 'NDX', 'NVDA', 'SPX', 'TLT', 'XOM']);
      const values = drawnSeries(drawn, 'd0')!.data as { value: number; day: string }[];
      axis.forEach((k, i) => assert.equal(values[i]!.value, of(k).closes.at(-1), `${k}'s latest close`));
      axis.forEach((k, i) => assert.equal(values[i]!.day, of(k).days.at(-1), `${k}'s own day`));
      assert.match(drawnSeries(drawn, 'd0')!.name!, /^latest \(\d{4}-\d{2}-\d{2}\)$/, 'the date the database found');
    } finally {
      view.dispose();
    }
  });

  it('the Calendar tab: a level\'s return each month and each year, the database\'s, asked when shown', async () => {
    const { root, view } = viewer(TWO);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Calendar')!.click();
      await until(() => root.querySelector('.dc-ts-cal') !== null, 'the calendar');
      const spx = of('SPX');
      // each month's last close, by the test's own reading of the rows
      const ends = new Map<string, number>();
      spx.days.forEach((d, i) => ends.set(d.slice(0, 7), spx.closes[i]!));
      const months = [...ends.keys()];
      const last = months.at(-1)!;
      const expected = ends.get(last)! / ends.get(months.at(-2)!)! - 1;
      const rows = statRows(root);
      assert.equal(rows[0]![0], last.slice(0, 4), 'the latest year first');
      const cell = rows[0]![Number(last.slice(5, 7))]!;
      assert.ok(Math.abs(parseFloat(cell) - expected * 100) < 0.006, `${last}'s return: ${cell} against ${expected}`);
      const first = rows.at(-1)!;
      assert.equal(first[Number(months[0]!.slice(5, 7))], '—', 'the first month has no level before it');
      // the other line picked: its own calendar
      const line = root.querySelector<HTMLSelectElement>('.dc-ts-stats select[aria-label="Line"]')!;
      assert.deepEqual([...line.options].map((o) => o.textContent), ['S&P 500', 'Nasdaq 100']);
    } finally {
      view.dispose();
    }
  });

  // the Phase 3 review's 12: a seasonality's and a curve's dates their own, the range buttons pressed to no effect
  it('a seasonality\'s range buttons said not its own, never pressed to no effect', async () => {
    const { root, view } = viewer({ ...TWO, view: { ...TWO.view, mode: 'seasonality' } });
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      const ranges = [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-range')];
      assert.ok(ranges.length > 0 && ranges.every((b) => b.disabled), 'each off');
      assert.match(ranges[0]!.title, /reads its own dates/);
      pick(root, 'View', 'histogram');
      assert.ok([...root.querySelectorAll<HTMLButtonElement>('.dc-ts-range')].every((b) => !b.disabled), 'a histogram\'s the dates shown');
    } finally {
      view.dispose();
    }
  });

  // the Phase 3 review's 5: an ask superseded left the chart busy; another's end cleared it while one still asked
  it('busy while it asks, whatever asks overlap: none once every answer is in', async () => {
    const slow: TsGrid = { ...grid, run: async (q, sn, sig) => { await new Promise((r) => setTimeout(r, 40)); return grid.run(q, sn, sig); } };
    const { root, view } = viewer({ ...TWO, view: { ...TWO.view, mode: 'histogram' } }, undefined, slow);
    const canvas = root.querySelector('.dc-ts-chart') as HTMLElement;
    try {
      await until(() => !canvas.hasAttribute('aria-busy') && statRows(root).length === 2, 'the first answers');
      // the view's own ask, then the window's over it before it lands
      view.update({ ...view.spec, view: { ...view.spec.view, bins: 12 } });
      assert.equal(canvas.getAttribute('aria-busy'), 'true');
      barButton(root, '1Y').click();
      await until(() => !canvas.hasAttribute('aria-busy'), 'every answer in, busy no longer');
      await new Promise((r) => setTimeout(r, 200));
      assert.ok(!canvas.hasAttribute('aria-busy'));
    } finally {
      view.dispose();
    }
  });

  // the Phase 3 review's 12: a scatter's first pick lost where the tile had no pair yet
  it('a scatter\'s pickers: the first pick kept till its pair is whole', async () => {
    const one: TimeseriesSpec = { ...TWO, series: [TWO.series[0]!], view: { mode: 'scatter' } };
    const { root, windows, told, view } = viewer(one);
    try {
      await until(() => chips(root).length === 1, 'its line');
      barButton(root, 'Settings').click();
      pick(windows, 'Across (x)', 'A');
      assert.equal(told.length, 0, 'half a pair told nothing');
      pick(windows, 'Up (y)', 'A');
      assert.deepEqual(told.at(-1)!.view?.of, ['A', 'A']);
    } finally {
      view.dispose();
    }
  });

  it('Settings: what it is drawn as, and a scatter\'s x and y picked, told as the tile\'s one rule', async () => {
    const { root, windows, told, view } = viewer(TWO);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      barButton(root, 'Settings').click();
      pick(windows, 'Drawn', 'scatter');
      assert.equal(told.at(-1)!.view?.mode, 'scatter');
      // the pair by the rule the view reads (A across, B up), each pick told with the other as it is now
      pick(windows, 'Across (x)', 'B');
      assert.deepEqual(told.at(-1)!.view?.of, ['B', 'B']);
      pick(windows, 'Up (y)', 'A');
      assert.deepEqual(told.at(-1)!.view?.of, ['B', 'A']);
      // a histogram's bins: a number the box cannot read is said, never taken for none
      pick(windows, 'Drawn', 'histogram');
      const bins = windows.querySelector<HTMLInputElement>('input[aria-label="Bins"]')!;
      assert.equal(bins.value, '30');
      bins.value = '12';
      bins.dispatchEvent(new (root.ownerDocument.defaultView!.Event)('change'));
      assert.equal(told.at(-1)!.view?.bins, 12);
      bins.value = '500';
      bins.dispatchEvent(new (root.ownerDocument.defaultView!.Event)('change'));
      assert.equal(told.at(-1)!.view?.bins, 12, 'refused by the tile\'s reader');
      assert.match(root.querySelector('.dc-ts-banner')?.textContent ?? '', /bins 500 is not a whole number of 5 to 100/);
    } finally {
      view.dispose();
    }
  });
});

// the review's P4 (docs/TIMESERIES_REVIEW_2026_10_25.md; AGENTS.md invariant 4): a refusal was shown as an absence -- a
// refused bounds query made 1Y show everything, refused statistics blanked every cell, refused instruments an empty
// picker, each with no word; and a function on a blank box guessed a column named close
describe('a refusal said where it lands, never shown as something missing', () => {
  const banner = (root: HTMLElement): string => root.querySelector('.dc-ts-banner')?.textContent ?? '';

  it("a range whose dates were refused: the banner says so, and what is shown instead", async () => {
    const { root, view } = viewer({ ...SPEC, range: { last: '1y' } }, (q) => q.includes('__ts_latest'));
    try {
      await until(() => /range is not known/.test(banner(root)), 'the range refused');
      assert.match(banner(root), /The 1y range is not known: px's dates were refused \(the warehouse is down\)\. Shown from the first date\./);
      assert.ok(root.querySelector('.dc-ts-banner.dc-ts-bad'), 'said as a problem');
    } finally {
      view.dispose();
    }
  });

  it("a series' statistics refused: its row says so, and the banner", async () => {
    const { root, view } = viewer(SPEC, (q) => q.includes('__ts_stat_'));
    try {
      await until(() => statRows(root).length === 2 && /statistics were refused/.test(banner(root)), 'the statistics refused');
      assert.deepEqual(statRows(root).map((r) => r.slice(1)), [['Refused: the warehouse is down'], ['Refused: the warehouse is down']]);
      assert.match(banner(root), /Series A's statistics were refused: the warehouse is down/);
    } finally {
      view.dispose();
    }
  });

  it("an instrument column's values refused: its picker says so", async () => {
    const { root, windows, view } = viewer(SPEC, (q) => q.includes('__ts_n'));
    try {
      [...root.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === '+ Series')!.click();
      const picker = (): HTMLSelectElement | null => windows.querySelector<HTMLSelectElement>('select[aria-label="Instrument"]');
      await until(() => [...(picker()?.options ?? [])].some((o) => /refused/.test(o.textContent ?? '')), 'the instruments refused');
      const said = [...picker()!.options].find((o) => /refused/.test(o.textContent ?? ''))!;
      assert.equal(said.textContent, 'Its instruments were refused: the warehouse is down');
      assert.ok(said.disabled, 'a word, not an instrument to pick');
    } finally {
      view.dispose();
    }
  });

  it("a function on a blank box: of its source's first value by DataCube's rule, or refused -- never a column guessed", async () => {
    // a grid of no close: its first number (no price-like name) is its open (timeseries-expr.ts firstValue)
    const opens: TsGrid = { ...grid, snapshot: { ...grid.snapshot, columns: grid.snapshot.columns.filter((c) => c.name !== 'close') } };
    const { root, windows, view } = viewer({ ...SPEC, series: [{ id: 'A', expr: parseExpr('open[SPX]') }] }, undefined, opens);
    try {
      [...root.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === '+ Series')!.click();
      const fn = windows.querySelector<HTMLSelectElement>('select[aria-label="Function"]')!;
      fn.value = 'mean';
      fn.dispatchEvent(new root.ownerDocument.defaultView!.Event('change'));
      assert.match(windows.querySelector<HTMLInputElement>('input[aria-label="Expression"]')!.value, /^mean\(open\b/);
    } finally {
      view.dispose();
    }
    const dates: TsGrid = { ...grid, snapshot: { ...grid.snapshot, columns: grid.snapshot.columns.filter((c) => c.type === 'String' || c.name === 'date') } };
    const none = viewer({ ...SPEC, series: [] }, undefined, dates);
    try {
      [...none.root.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === '+ Series')!.click();
      const fn = none.windows.querySelector<HTMLSelectElement>('select[aria-label="Function"]')!;
      fn.value = 'mean';
      fn.dispatchEvent(new none.root.ownerDocument.defaultView!.Event('change'));
      assert.equal(none.windows.querySelector<HTMLInputElement>('input[aria-label="Expression"]')!.value, '');
      assert.equal(none.windows.querySelector('.dc-ts-error')?.textContent, 'Write the series it applies to first, or build one from a column.');
    } finally {
      none.view.dispose();
    }
  });

  // the second audit's P-2b and S1: a level's statistics blank -- a "—" that said nothing -- said why, line by line
  it("a level's blank statistics said why: at or below zero over the dates shown, or under a year", async () => {
    const below: TsGrid = { ...grid, snapshot: { ...grid.snapshot,
      derived: [{ name: 'gap', type: 'Float', lambda: lambda(['x'], minus(fn('toOne', col('x', 'close')), lit.integer(1_000_000))) }] } };
    const { root, view } = viewer({ sources: SPEC.sources, series: [{ id: 'A', expr: parseExpr('gap[SPX]'), name: 'Gap' }], view: { stats: true } }, undefined, below);
    try {
      await until(() => root.querySelector('.dc-ts-stats-blanks') !== null, 'why its figures are blank');
      assert.match(root.querySelector('.dc-ts-stats-blanks')!.textContent!, /^Gap is at or below zero over these dates: it has no volatility, drawdown, growth or Sharpe ratio/);
      // and its Drawdowns tab says so, not that it never fell (the E9 review's 9)
      [...root.querySelectorAll<HTMLButtonElement>('.dc-ts-stats-tab')].find((b) => b.textContent === 'Drawdowns')!.click();
      await until(() => root.querySelector('.dc-ts-stats-blanks')?.textContent?.includes('drawdowns') === true
        && !root.querySelector('.dc-ts-stats-blanks')!.textContent!.startsWith('Asking'), 'why it has no drawdowns');
      assert.equal(root.querySelector('.dc-ts-stats-blanks')!.textContent, 'Gap is at or below zero over these dates: it has no drawdowns.');
    } finally {
      view.dispose();
    }
    // above zero over 200 business days, under a year: its growth not annualized (the review's N3), and said so alone
    const plain = viewer(SPEC);
    try {
      await until(() => statRows(plain.root).length === 2, 'its statistics');
      assert.equal(plain.root.querySelector('.dc-ts-stats-blanks')?.textContent,
        'S&P 500: these dates span under a year, so its growth is not annualized: its % change is its growth.');
    } finally {
      plain.view.dispose();
    }
  });
});

// the review's follow-ups P9 and the statistics unseen (docs/TIMESERIES_REVIEW_2026_10_25.md §9): a combination's grids
// opened together again for every query -- each series', each statistics' -- and the statistics asked with nothing
// showing them
describe('asking the database what is shown, once', () => {
  /** A viewer over two grids of the same rows (two cubes), a combination's grids opened together counted. */
  function pair(spec: TimeseriesSpec, asked: string[] = []): { view: TimeseriesView; root: HTMLElement; opens: () => number; disposed: () => number } {
    let opens = 0;
    let disposed = 0;
    const counted = (g: TsGrid): TsGrid => ({ ...g, run: (query, snapshot, signal) => { asked.push(toJson(query)); return g.run(query, snapshot, signal); } });
    const grids = new Map([['prices', counted(grid)], ['prices2', counted({ ...grid, cube: 'prices2', title: 'Prices again' })]]);
    const dom = new JSDOM('<!doctype html><div id="t" style="height:600px"></div><div id="w"></div>');
    const root = dom.window.document.getElementById('t') as unknown as HTMLElement;
    const view = new TimeseriesView(root, {
      grid: (cube) => grids.get(cube),
      heldFilter: (cube) => { const g = grids.get(cube); return g ? { filter: g.filter } : undefined; },
      grids: () => [...grids.values()],
      together: async (cubes): Promise<TsTogether> => {
        opens += 1;
        return {
          relations: new Map(cubes.map((c) => [c, grids.get(c)!.snapshot.source.query] as const)),
          run: (query) => { asked.push(toJson(query)); return plan(query); },
          dispose: () => { disposed += 1; },
        };
      },
      setSpec: () => {},
      windowHost: dom.window.document.getElementById('w') as unknown as HTMLElement,
      title: () => 'Markets',
      codeOf: () => ({ title: 'Markets', grids: { prices: { title: 'Prices', source: PRICES_FILE }, prices2: { title: 'Prices again', source: PRICES_FILE } } }),
      mount: () => ({
        show: () => {}, zoomTo: () => {}, zoomed: () => ({}), highlight: () => {}, measure: () => {}, draw: () => {}, height: () => 480, picture: () => null,
        exportUrl: () => Promise.reject(new Error('no picture here')), dispose: () => {},
      }),
    }, spec);
    return { view, root, opens: () => opens, disposed: () => disposed };
  }

  it('P9: a combination\'s grids opened together once a fetch -- its series and their statistics -- and put away', async () => {
    const spec: TimeseriesSpec = {
      sources: [{ alias: 'px', cube: 'prices', date: 'date', key: 'symbol' }, { alias: 'qx', cube: 'prices2', date: 'date', key: 'symbol' }],
      series: [
        { id: 'A', expr: parseExpr('px:close[SPX] / qx:close[NDX]'), name: 'Ratio' },
        { id: 'B', expr: parseExpr('px:close[SPX] + qx:close[NDX]'), name: 'Sum' },
      ],
      view: { stats: true },
    };
    const { view, root, opens, disposed } = pair(spec);
    try {
      await until(() => statRows(root).length === 2, 'its statistics');
      assert.equal(opens(), 1, 'opened together once for both series and their statistics');
      assert.equal(disposed(), 1, 'and put away once asked');
      view.refresh(0);
      await until(() => opens() === 2 && disposed() === 2, 'asked again: opened once more, as the grids are now');
    } finally {
      view.dispose();
    }
  });

  it('no statistics asked while neither the table nor the legend shows them; asked once one does', async () => {
    const asked: string[] = [];
    const spec: TimeseriesSpec = { ...SPEC, view: { stats: false, legend: false } };
    const { view, root } = pair(spec, asked);
    const stats = (): number => asked.filter((q) => q.includes('__ts_stat_')).length;
    try {
      await until(() => asked.some((q) => q.includes('__ts_value')) && root.getAttribute('aria-busy') === null
        && !root.querySelector('[aria-busy="true"]'), 'its lines');
      await new Promise((r) => setTimeout(r, 50));
      assert.equal(stats(), 0, 'nothing shows them: none asked');
      view.update({ ...SPEC, view: { stats: true, legend: false } });
      await until(() => statRows(root).length === 2, 'its statistics, once shown');
      assert.equal(stats(), 2, 'each line\'s, once');
    } finally {
      view.dispose();
    }
  });
});
