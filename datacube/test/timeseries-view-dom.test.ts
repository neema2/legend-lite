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
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { EChartsOption } from 'echarts';
import { marketsCsv } from '../src/samples.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { MountedTimeseries } from '../src/timeseries-render.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import type { TimeseriesSpec } from '../src/timeseries-spec.ts';
import { ingestFile } from '../src/upload.ts';
import { TimeseriesView, type TsGrid, type TsTogether } from '../src/ui/timeseries-view.ts';
import { MARKET_SPECS } from '../src/examples/markets.ts';
import { plannerFor } from './catalog-builder.ts';

const CSV = marketsCsv(2000);
const ROWS = CSV.trim().split('\n').slice(1).map((l) => l.split(','));
/** The rows of one instrument, oldest first: its days and its closes. */
const of = (symbol: string): { days: string[]; closes: number[] } => {
  const mine = ROWS.filter((r) => r[0] === symbol);
  return { days: mine.map((r) => r[2]!), closes: mine.map((r) => Number(r[6])) };
};

let engine: DuckDbEngine;
let grid: TsGrid;
let plan: (query: Parameters<TsGrid['run']>[0]) => Promise<ResultTable>;
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
  plan = async (query) => engine.execute(await planner.plan(query), 0);
  const snapshot: CubeSnapshot = {
    source: { query: r.source }, columns: [...MARKET_SPECS], derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
  };
  grid = {
    cube: 'prices', title: 'Prices', snapshot, filter: undefined,
    run: async (query): Promise<ResultTable> => {
      runs += 1;
      return engine.execute(await planner.plan(query), 0);
    },
    format: (value) => (typeof value === 'number' ? value.toFixed(2) : String(value)),
  };
});

after(async () => {
  await engine?.close();
});

/** A viewer over the grid in a fresh DOM: what it drew, and the specs it told. `refused`: the queries its grid's
 *  database refuses, by their text (a bounds query's, a statistics query's...), as a warehouse that is down refuses them;
 *  `over`: the grid as it is read (its columns), the markets sample's by default. */
function viewer(spec: TimeseriesSpec, refused?: (query: string) => boolean, over: TsGrid = grid):
  { view: TimeseriesView; root: HTMLElement; windows: HTMLElement; drawn: EChartsOption[]; told: TimeseriesSpec[]; zooms: unknown[] } {
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
  const mount = (): MountedTimeseries => ({
    show: (option) => { drawn.push(option); },
    zoomTo: (from, to) => { zooms.push({ from, to }); },
    zoomed: () => ({}),
    highlight: () => {},
    height: () => 480,
    picture: () => null,
    pngUrl: () => null,
    dispose: () => {},
  });
  const view = new TimeseriesView(root, {
    grid: (cube) => (cube === 'prices' ? g : undefined),
    heldFilter: (cube) => (cube === 'prices' ? { filter: g.filter } : undefined),
    grids: () => [g],
    together: () => Promise.reject(new Error('one grid here')),
    setSpec: (s) => { told.push(s); },
    windowHost: windows,
    title: () => 'Markets',
    mount,
  }, spec);
  return { view, root, windows, drawn, told, zooms };
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
      // its periods: where the close is above its 21-day mean, each from its first date to its last
      const spx = of('SPX');
      const mean = spx.closes.map((_, i) => (i < 20 ? null : spx.closes.slice(i - 20, i + 1).reduce((a, b) => a + b, 0) / 21));
      const holds = spx.closes.map((c, i) => mean[i] !== null && c > mean[i]!);
      const runs: [string, string][] = [];
      holds.forEach((h, i) => {
        if (!h) return;
        if (i > 0 && holds[i - 1]) runs.at(-1)![1] = spx.days[i]!;
        else runs.push([spx.days[i]!, spx.days[i]!]);
      });
      assert.deepEqual(shading[0]!.markArea!.data.map((d) => (d as { xAxis: string }[]).map((e) => e.xAxis.slice(0, 10))), runs);
      const chip = chips(root).find((c) => c.startsWith('Above its 21-day'))!;
      assert.match(chip, holds.at(-1) ? /holds$/ : /does not$/);
      assert.ok(!statRows(root).some((r) => r[0] === 'Above its 21-day'), 'a condition has no statistics');
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
      assert.match(typed('seasonally_adjusted($A)'), /seasonal decomposition/);
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
      mount: () => ({
        show: () => {}, zoomTo: () => {}, zoomed: () => ({}), highlight: () => {}, height: () => 480, picture: () => null, pngUrl: () => null, dispose: () => {},
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
