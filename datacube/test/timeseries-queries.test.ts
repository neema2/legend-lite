// A TIMESERIES' QUERIES, PLANNED AND RUN (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §4): each series of a
// timeseries tile through legend-lite's own planner, on a real DuckDB, its numbers read back and held to gs-quant's
// definitions -- written here, in the test, as plain arithmetic over the same prices (the reference a person can check
// by hand), never by the code under test.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { type TimeseriesSpec, type TsProjection, type TsSeries } from '../src/timeseries-spec.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import {
  boundsQuery, correlationsQuery, drawdownsQuery, keyValuesQuery, rangeStartName, seriesQuery, seriesSources, statName, statsQuery, TS_DATE, TS_EARLIEST, TS_HIGH, TS_LATEST, TS_LOW, TS_LOWER, TS_OPEN,
  TS_DD_DECLINE, TS_DD_DEPTH, TS_DD_PEAK, TS_DD_RANK, TS_DD_RECOVERED, TS_DD_RECOVERY, TS_DD_TROUGH,
  TS_CORR, TS_CORR_A, TS_CORR_B, TS_CORR_N, TS_FROM, TS_TO, TS_PER_YEAR, TS_ROWS, TS_SPLIT, TS_STAT_PER_YEAR, TS_UPPER, TS_VALUE, type TsInput, type TsInputs, type TsWindow,
  calendarQuery, curveOrder, curveQuery, histogramQuery, projectionQuery, scatterQuery, seasonalityQuery, viewSeries,
  TS_PJ_END, TS_PJ_END_VALUE, TS_PJ_FROM, TS_PJ_FROM_VALUE, TS_PJ_LAST, TS_PJ_LAST_VALUE, TS_PJ_N, TS_PJ_SLOPE, TS_CAL_LOW, TS_CAL_MONTH, TS_CAL_RETURN, TS_CAL_YEAR, TS_CAL_YEAR_RETURN,
  askKey, askQuery, askSources, tileAsks, curveColumns, TS_HIST_COUNT, TS_HIST_HIGH, TS_HIST_LOW, TS_HIST_N, TS_HIST_NORMAL, TS_HIST_P5, TS_SC_CORR, TS_SC_FIT_HIGH, TS_SC_FIT_LOW,
  TS_SC_INTERCEPT, TS_SC_N, TS_SC_R2, TS_SC_SLOPE, TS_SC_X, TS_SC_Y, TS_SEA_HIGH, TS_SEA_LATEST, TS_SEA_LOW, TS_SEA_MEAN, TS_SEA_PLACE, TS_SEA_SPAN, TS_SEA_YEAR,
} from '../src/timeseries-query.ts';
import { TAB_TABLES, openSources, type JoinOpening } from '../src/join-open.ts';
import type { FileSource } from '../src/cube-document.ts';
import { catalogColumns } from '../src/upload.ts';
import { asc, col as column, collection, colSpec, fn as call, from, lambda, lit, minus, Relation, toJson, walk, type Lambda } from '../../pure-protocol/src/index.ts';
import { DATACUBE_LIBRARY } from '../src/pure-library-text.ts';

// -- the prices: two instruments over 40 weekdays from 2024-01-01, and a third trading on fewer days --

function weekdays(n: number): string[] {
  const out: string[] = [];
  for (let d = new Date(Date.UTC(2024, 0, 1)); out.length < n; d = new Date(d.getTime() + 86400000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

const DAYS = weekdays(40);
const round = (v: number): number => Math.round(v * 100) / 100;
const AAA = DAYS.map((_, i) => round(100 * (1 + 0.03 * Math.sin(i / 2) + 0.004 * i)));
const BBB = DAYS.map((_, i) => round(50 * (1 + 0.02 * Math.cos(i / 1.7) + 0.002 * i + 0.01 * Math.sin(i / 3))));
// CCC trades on every third day only
const CCC = DAYS.map((_, i) => round(20 + i * 0.1 + (i % 4) * 0.3));
const VOL = DAYS.map((_, i) => 1000 + 37 * i);

const CSV = [
  'symbol,day,close,volume',
  ...DAYS.flatMap((d, i) => [
    `AAA,${d},${AAA[i]},${VOL[i]}`,
    `BBB,${d},${BBB[i]},${VOL[i]! * 2}`,
    ...(i % 3 === 0 ? [`CCC,${d},${CCC[i]},${VOL[i]! * 3}`] : []),
  ]),
].join('\n');

const writer = plannerFor('', 'local::RT');
let engine: DuckDbEngine;
let planner: LitePlanner;
let input: TsInput;
let inputs: TsInputs;
let files: DuckDbFiles;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  files = db.files;
  const r = await ingestFile(engine, files, new File([CSV], 'prices.csv'), writer);
  planner = plannerFor(r.model, r.runtime);
  const snapshot: CubeSnapshot = {
    source: { query: r.source },
    columns: [
      { name: 'symbol', type: 'String' },
      { name: 'day', type: 'StrictDate' },
      { name: 'close', type: 'Float' },
      { name: 'volume', type: 'Integer', kind: 'measure' },
    ],
    derived: [],
    rows: ['symbol'],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };
  input = { snapshot, filter: undefined };
  inputs = new Map([['px', input]]);
});

after(async () => {
  await engine?.close();
});

// -- the references: gs-quant's definitions, as arithmetic --

type Line = (number | null)[];
const lagged = (x: Line, n: number): Line => x.map((_, i) => (i >= n ? x[i - n]! : null));
const zip = (a: Line, b: Line, f: (p: number, q: number) => number | null): Line =>
  a.map((v, i) => (v === null || b[i] === null ? null : f(v, b[i]!)));
const simple = (x: Line): Line => zip(x, lagged(x, 1), (v, p) => v / p - 1);
/** The last `w` values ending at each one, once all w are there (gs-quant's ramp). */
const rolling = (x: Line, w: number, f: (vals: number[]) => number | null): Line => x.map((_, i) => {
  if (i < w - 1) return null;
  const vals = x.slice(i - w + 1, i + 1);
  if (vals.some((v) => v === null)) return null;
  return f(vals as number[]);
});
const avg = (v: number[]): number => v.reduce((a, b) => a + b, 0) / v.length;
const sd = (v: number[]): number => {
  const m = avg(v);
  return Math.sqrt(v.reduce((a, b) => a + (b - m) ** 2, 0) / (v.length - 1));
};
const cov = (a: number[], b: number[]): number => {
  const ma = avg(a);
  const mb = avg(b);
  return a.reduce((s, v, i) => s + (v - ma) * (b[i]! - mb), 0) / (a.length - 1);
};
/** An exponential average as its recursion (pandas' ewm, adjust=False): y0 = x0, y = beta·y[-1] + (1 - beta)·x. */
const ema = (x: number[], beta: number): number[] => {
  const out: number[] = [];
  for (const v of x) out.push(out.length === 0 ? v : beta * out.at(-1)! + (1 - beta) * v);
  return out;
};
/** pandas' ewm(alpha = 1 - beta, adjust=False).std(), its own loop (pandas/_libs/window/aggregations.pyx ewmcov). */
const ewmStd = (x: number[], beta: number): Line => {
  const alpha = 1 - beta;
  let mean = x[0]!;
  let cov = 0;
  let sumWt = 1;
  let sumWt2 = 1;
  let oldWt = 1;
  const out: Line = [null];
  for (let i = 1; i < x.length; i++) {
    const cur = x[i]!;
    sumWt *= beta;
    sumWt2 *= beta * beta;
    oldWt *= beta;
    const oldMean = mean;
    if (mean !== cur) mean = (oldWt * oldMean + alpha * cur) / (oldWt + alpha);
    cov = (oldWt * (cov + (oldMean - mean) * (oldMean - mean)) + alpha * (cur - mean) * (cur - mean)) / (oldWt + alpha);
    sumWt += alpha;
    sumWt2 += alpha * alpha;
    oldWt += alpha;
    sumWt /= oldWt;
    sumWt2 /= oldWt * oldWt;
    oldWt = 1;
    const numerator = sumWt * sumWt;
    const denominator = numerator - sumWt2;
    out.push(denominator > 0 ? Math.sqrt((numerator / denominator) * cov) : null);
  }
  return out;
};
/** gs-quant's smoothed_moving_average over w (technicals.py): none to the w-th, there the mean of the w values ending at
 *  it, then ((w - 1)·y[-1] + x) / w. */
const smma = (x: number[], w: number): Line => {
  const out: Line = [];
  for (let i = 0; i < x.length; i++) {
    out.push(i < w ? null : i === w ? avg(x.slice(1, w + 1)) : ((w - 1) * out[i - 1]! + x[i]!) / w);
  }
  return out;
};
const pairs = (a: Line, b: Line, w: number, f: (p: number[], q: number[]) => number): Line => a.map((_, i) => {
  if (i < w - 1) return null;
  const p = a.slice(i - w + 1, i + 1);
  const q = b.slice(i - w + 1, i + 1);
  if ([...p, ...q].some((v) => v === null)) return null;
  return f(p as number[], q as number[]);
});

// -- running a series --

const TS: TimeseriesSpec = { sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }], series: [] };

function spec(exprs: Record<string, string>, more: Partial<TimeseriesSpec> = {}, split?: string): TimeseriesSpec {
  return {
    ...TS,
    ...more,
    series: Object.entries(exprs).map(([id, text]): TsSeries => ({ id, expr: parseExpr(text), ...(split ? { split } : {}) })),
  };
}

async function run(query: { query: unknown }): Promise<ResultTable> {
  return engine.execute(await planner.plan(query.query as Parameters<LitePlanner['plan']>[0]), 0);
}

/** The #TS text a tile's query sends (P6: its every figure the platform's #TS builder's). */
function islandText(q: Lambda): string {
  let text = '';
  walk(q, (n) => {
    if (n._type === 'classInstance' && n.type === 'TS') text = (n.value as { ts: string }).ts;
  });
  return text;
}

/** The language's name of a tile function where they differ (timeseries-ts.ts RENAMED). */
const TS_NAMES: Readonly<Record<string, string>> = {
  range: 'range_of(', first: 'first_of(', last: 'last_of(', filter: 'filter_values(', union: 'union_of(',
  day: 'day_of(', month: 'month_of(', year: 'year_of(', quarter: 'quarter_of(', weekday: 'weekday_of(', log: 'ln(',
  floor: 'CASE WHEN', ceil: 'CASE WHEN', not: 'CASE WHEN',
};

function values(t: ResultTable, name: string): unknown[] {
  const c = t.columns.find((x) => x.name === name);
  assert.ok(c, `the answer has a column '${name}' (it has ${t.columns.map((x) => x.name).join(', ')})`);
  return c.values.map((v) => (typeof v === 'bigint' ? Number(v) : v));
}

const num = (v: unknown): number | null => (v === null || v === undefined ? null : Number(v));

/** A series' line, oldest first (the query answers latest first), one per split value. */
async function line(ts: TimeseriesSpec, id = 'A', view: TsWindow = {}, clip: TsWindow = {}): Promise<{ dates: string[]; values: Line; lower?: Line; upper?: Line; split?: unknown[] }> {
  const series = ts.series.find((s) => s.id === id)!;
  const q = seriesQuery(inputs, ts, series, view, clip);
  const t = await run(q);
  const dates = values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse();
  const out = { dates, values: values(t, TS_VALUE).map(num).reverse() };
  return {
    ...out,
    ...(q.band ? { lower: values(t, TS_LOWER).map(num).reverse(), upper: values(t, TS_UPPER).map(num).reverse() } : {}),
    ...(q.split !== undefined ? { split: values(t, TS_SPLIT).reverse() } : {}),
  };
}

function close(got: Line, want: Line, what: string, tolerance = 1e-9): void {
  assert.equal(got.length, want.length, `${what}: ${got.length} points, not ${want.length}`);
  for (let i = 0; i < want.length; i++) {
    const g = got[i] ?? null;
    const w = want[i] ?? null;
    if (w === null || g === null) {
      assert.equal(g, w, `${what} at ${i}: ${g} where ${w} was meant`);
      continue;
    }
    assert.ok(Math.abs(g - w) <= tolerance * Math.max(1, Math.abs(w)), `${what} at ${i}: ${g}, not ${w}`);
  }
}

describe('a column along the dates', () => {
  it('close[AAA]: one point a day, its price', async () => {
    const l = await line(spec({ A: 'close[AAA]' }));
    assert.deepEqual(l.dates, DAYS);
    close(l.values, AAA, 'close[AAA]');
  });

  it('split by symbol: a line per instrument, each its own', async () => {
    const l = await line(spec({ A: 'close' }, {}, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    close(of('AAA'), AAA, 'AAA');
    close(of('BBB'), BBB, 'BBB');
    close(of('CCC'), CCC.filter((_, i) => i % 3 === 0), 'CCC');
  });

  it('a ratio of two instruments, on the dates both have (gs-quant\'s intersect)', async () => {
    const l = await line(spec({ A: 'close[AAA] / close[CCC]' }));
    assert.deepEqual(l.dates, DAYS.filter((_, i) => i % 3 === 0));
    close(l.values, AAA.map((v, i) => v / CCC[i]!).filter((_, i) => i % 3 === 0), 'AAA / CCC');
  });

  it('arithmetic with numbers: a spread, a scale', async () => {
    const l = await line(spec({ A: '(close[AAA] - close[BBB]) * 2 + 1' }));
    close(l.values, AAA.map((v, i) => (v - BBB[i]!) * 2 + 1), 'spread');
  });

  it('a monthly frequency: each month\'s last price, and its volume summed', async () => {
    const months = [...new Set(DAYS.map((d) => d.slice(0, 7)))];
    const last = months.map((m) => AAA[DAYS.map((d) => d.slice(0, 7)).lastIndexOf(m)]!);
    const l = await line(spec({ A: 'close[AAA]' }, { grain: 'month' }));
    // each month dated at its last observation, the date its value is of (the review's Q6)
    assert.deepEqual(l.dates, months.map((m) => DAYS.filter((d) => d.startsWith(m)).at(-1)!));
    close(l.values, last, 'month-end');
    const v = await line(spec({ A: 'volume[AAA].sum' }, { grain: 'month' }));
    close(v.values, months.map((m) => DAYS.reduce((s, d, i) => s + (d.startsWith(m) ? VOL[i]! : 0), 0)), 'volume');
    const f = await line(spec({ A: 'close[AAA].first' }, { grain: 'month' }));
    close(f.values, months.map((m) => AAA[DAYS.findIndex((d) => d.startsWith(m))]!), 'month-start');
  });

  // a candle (ohlc) per period: each argument a plain column taken by the period as a candle's is -- the first, the most,
  // the least, the last -- so a weekly candle of daily closes is the week's first close, highest, lowest and last
  it('weekly candles: the week\'s first, highest, lowest and last, the database\'s', async () => {
    const ts = spec({ A: 'ohlc(close[AAA], close[AAA], close[AAA], close[AAA])' }, { grain: 'week' });
    const q = seriesQuery(inputs, ts, ts.series[0]!);
    assert.equal(q.candles, true);
    const t = await run(q);
    const weekOf = (d: string): string => {
      const at = new Date(`${d}T00:00:00Z`);
      return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(DAYS.map(weekOf))];
    const of = (w: string): number[] => AAA.filter((_, i) => weekOf(DAYS[i]!) === w);
    const got = (name: string): Line => values(t, name).map(num).reverse();
    // each candle dated at its week's last observation, the date its close is of
    assert.deepEqual(values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(), weeks.map((w) => DAYS.filter((d) => weekOf(d) === w).at(-1)!));
    close(got(TS_OPEN), weeks.map((w) => of(w)[0]!), 'open');
    close(got(TS_HIGH), weeks.map((w) => Math.max(...of(w))), 'high');
    close(got(TS_LOW), weeks.map((w) => Math.min(...of(w))), 'low');
    close(got(TS_VALUE), weeks.map((w) => of(w).at(-1)!), 'close');
    // and read by nothing
    await assert.rejects(async () => seriesQuery(inputs, spec({ A: 'returns(ohlc(close, close, close, close))' }), spec({ A: 'returns(ohlc(close, close, close, close))' }).series[0]!),
      /candles are drawn, never read/);
  });

  // the review's N4: a candle of a computed series at a frequency took each period's last of every column first, so
  // `ohlc(close / 2, ...)` weekly drew flat candles. Its series are computed on their sources' own dates, then each
  // period's first, highest, lowest and last taken -- a candle of the computed series, the database's
  it('N4: weekly candles of a computed series: its daily values\' first, highest, lowest and last', async () => {
    const r = 'close[AAA] / close[BBB]';
    const ts = spec({ A: `ohlc(${r}, ${r}, ${r}, ${r})` }, { grain: 'week' });
    const t = await run(seriesQuery(inputs, ts, ts.series[0]!));
    const weekOf = (d: string): string => {
      const at = new Date(`${d}T00:00:00Z`);
      return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(DAYS.map(weekOf))];
    const ratio = AAA.map((a, i) => a / BBB[i]!);
    const of = (w: string): number[] => ratio.filter((_, i) => weekOf(DAYS[i]!) === w);
    const got = (name: string): Line => values(t, name).map(num).reverse();
    assert.deepEqual(values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(), weeks.map((w) => DAYS.filter((d) => weekOf(d) === w).at(-1)!));
    close(got(TS_OPEN), weeks.map((w) => of(w)[0]!), 'open', 1e-12);
    close(got(TS_HIGH), weeks.map((w) => Math.max(...of(w))), 'high', 1e-12);
    close(got(TS_LOW), weeks.map((w) => Math.min(...of(w))), 'low', 1e-12);
    close(got(TS_VALUE), weeks.map((w) => of(w).at(-1)!), 'close', 1e-12);
    // a function inside a candle runs over its sources' dates: a 5-day mean's candle, the week's first, most, least, last
    const m = 'mean(close[AAA], 5)';
    const mean5 = rolling(AAA, 5, avg);
    const mt = await run(seriesQuery(inputs, spec({ A: `ohlc(${m}, ${m}, ${m}, ${m})` }, { grain: 'week' }), spec({ A: `ohlc(${m}, ${m}, ${m}, ${m})` }, { grain: 'week' }).series[0]!));
    const has = (w: string): number[] => mean5.filter((v, i) => v !== null && weekOf(DAYS[i]!) === w) as number[];
    const shown = weeks.filter((w) => has(w).length > 0);
    close(values(mt, TS_HIGH).map(num).reverse(), shown.map((w) => Math.max(...has(w))), 'a mean\'s high', 1e-9);
    close(values(mt, TS_OPEN).map(num).reverse(), shown.map((w) => has(w)[0]!), 'a mean\'s open', 1e-9);
  });

  // the review's N6: an average of levels is a level -- compared from its start, with a level's statistics (already
  // so since the second audit's B1, a basket of levels; pinned here, by its query)
  it('N6: an average of two levels is a level: rebased from its start, and its volatility', async () => {
    const ts: TimeseriesSpec = { ...spec({ A: 'close[AAA]', B: 'close[BBB]', C: '($A + $B) / 2' }, { view: { compare: 'rebase' } }) };
    const c = ts.series.find((x) => x.id === 'C')!;
    const q = seriesQuery(inputs, ts, c);
    assert.equal(q.level, true);
    const got = values(await run(q), TS_VALUE).map(num).reverse();
    const basket = AAA.map((a, i) => (a + BBB[i]!) / 2);
    close(got, basket.map((v) => (v / basket[0]!) * 100), 'rebased', 1e-9);
    const st = await run(statsQuery(inputs, ts, c));
    const lr = basket.slice(1).map((x, i) => Math.log(x / basket[i]!));
    close([num(values(st, statName('vol'))[0])], [sd(lr) * Math.sqrt(252) * 100], 'its volatility', 1e-7);
  });

  it('the grid\'s filter narrows every series', async () => {
    const filtered: TsInputs = new Map([['px', { ...input, filter: { kind: 'condition', column: 'day', operator: 'greaterThanEqual', value: DAYS[30]! } }]]);
    const series = spec({ A: 'close[AAA]' }).series[0]!;
    const t = await run(seriesQuery(filtered, spec({ A: 'close[AAA]' }), series));
    assert.equal(t.rowCount, 10);
  });

  // a timeseries frozen (the user, 2026-10-26: a chart's states): each source reads the filter it keeps, not its grid's
  // as it is now -- the compiler's rule (timeseries-spec.ts filterRead), so neither host decides it
  it("a frozen tile's source reads the filter it keeps, whatever its grid's is now: its series, its dates, its picker", async () => {
    const since = { kind: 'condition', column: 'day', operator: 'greaterThanEqual', value: DAYS[30]! } as const;
    const frozen = spec({ A: 'close[AAA]' }, { sources: [{ ...TS.sources[0]!, kept: { filter: since } }] });
    // its grid filtered since to the last ten days and then some: the tile reads as it was frozen
    const now: TsInputs = new Map([['px', { ...input, filter: { kind: 'condition', column: 'day', operator: 'greaterThanEqual', value: DAYS[35]! } }]]);
    assert.equal((await run(seriesQuery(now, frozen, frozen.series[0]!))).rowCount, 10);
    // frozen with no filter: every row, the grid's filter now ignored
    const all = spec({ A: 'close[AAA]' }, { sources: [{ ...TS.sources[0]!, kept: {} }] });
    assert.equal((await run(seriesQuery(now, all, all.series[0]!))).rowCount, DAYS.length);
    const bounds = await run({ query: boundsQuery(now.get('px')!, frozen.sources[0]!) });
    assert.equal(String(values(bounds, TS_EARLIEST)[0]).slice(0, 10), DAYS[30]);
    const picked = await run({ query: keyValuesQuery({ ...input, filter: { kind: 'condition', column: 'symbol', operator: 'equal', value: 'AAA' } }, all.sources[0]!, 'symbol') });
    assert.ok(picked.rowCount > 1, "its picker lists the instruments of the rows it reads, not its grid's now");
  });
});

describe('gs-quant\'s functions, as the database computes them', () => {
  it('returns: simple, log and absolute, over one and over five observations', async () => {
    close((await line(spec({ A: 'returns(close[AAA])' }))).values, simple(AAA), 'simple');
    close((await line(spec({ A: 'returns(close[AAA], 5)' }))).values, zip(AAA, lagged(AAA, 5), (v, p) => v / p - 1), 'simple 5');
    close((await line(spec({ A: 'returns(close[AAA], 1, log)' }))).values, zip(AAA, lagged(AAA, 1), (v, p) => Math.log(v) - Math.log(p)), 'log');
    close((await line(spec({ A: 'returns(close[AAA], type=absolute)' }))).values, zip(AAA, lagged(AAA, 1), (v, p) => v - p), 'absolute');
  });

  // a host reading a window (Python's Timeseries.frame): the window's dates alone, each value computed over every date
  // first -- a rolling figure's first values in the window have their history
  it('a window kept by the database after every value is computed: its first values have their history', async () => {
    const all = await line(spec({ A: 'volatility(close[AAA], 5)' }));
    const from = DAYS[10]!;
    const to = DAYS[20]!;
    const kept = await line(spec({ A: 'volatility(close[AAA], 5)' }), 'A', {}, { from, to });
    assert.deepEqual(kept.dates, DAYS.slice(10, 21));
    close(kept.values, all.values.slice(10, 21), 'the window\'s volatility, as over every date');
    assert.ok(kept.values[0] !== null, 'its first value has its five returns');
    // a value that is a window itself (a running sum, a running drawdown, a lag): every one over every date too -- a
    // plain filter after a window's extend is folded into its WHERE (core lowering/Fold.java filterSlot), never so here
    for (const text of ['sum(close[AAA], 0)', 'drawdown(close[AAA])', 'lag(close[AAA], 3)', 'max(close[AAA], 0)']) {
      const every = await line(spec({ A: text }));
      const cut = await line(spec({ A: text }), 'A', {}, { from, to });
      assert.deepEqual(cut.dates, DAYS.slice(10, 21), text);
      close(cut.values, every.values.slice(10, 21), `${text} over the window, as over every date`);
    }
  });

  it('diff and lag', async () => {
    close((await line(spec({ A: 'diff(close[AAA], 2)' }))).values, zip(AAA, lagged(AAA, 2), (v, p) => v - p), 'diff');
    close((await line(spec({ A: 'lag(close[AAA], 3)' }))).values, lagged(AAA, 3), 'lag');
  });

  it('rolling mean, sum, min, max, range, standard deviation and variance: drawn once the window is full', async () => {
    close((await line(spec({ A: 'mean(close[AAA], 5)' }))).values, rolling(AAA, 5, avg), 'mean');
    close((await line(spec({ A: 'moving_average(close[AAA], 5)' }))).values, rolling(AAA, 5, avg), 'moving_average');
    close((await line(spec({ A: 'sum(close[AAA], 4)' }))).values, rolling(AAA, 4, (v) => v.reduce((a, b) => a + b, 0)), 'sum');
    close((await line(spec({ A: 'min(close[AAA], 4)' }))).values, rolling(AAA, 4, (v) => Math.min(...v)), 'min');
    close((await line(spec({ A: 'max(close[AAA], 4)' }))).values, rolling(AAA, 4, (v) => Math.max(...v)), 'max');
    close((await line(spec({ A: 'range(close[AAA], 4)' }))).values, rolling(AAA, 4, (v) => Math.max(...v) - Math.min(...v)), 'range');
    close((await line(spec({ A: 'std(close[AAA], 6)' }))).values, rolling(AAA, 6, sd), 'std', 1e-7);
    close((await line(spec({ A: 'var(close[AAA], 6)' }))).values, rolling(AAA, 6, (v) => sd(v) ** 2), 'var', 1e-7);
  });

  it('from the start (w=0): a running total, a running mean', async () => {
    let acc = 0;
    close((await line(spec({ A: 'sum(close[AAA])' }))).values, AAA.map((v) => (acc += v)), 'cumsum', 1e-7);
    close((await line(spec({ A: 'mean(close[AAA], 0)' }))).values, AAA.map((_, i) => avg(AAA.slice(0, i + 1))), 'expanding mean', 1e-7);
  });

  it('z-scores over a window, and over every observation', async () => {
    const want = AAA.map((v, i) => (i < 9 ? null : (v - avg(AAA.slice(i - 9, i + 1))) / sd(AAA.slice(i - 9, i + 1))));
    close((await line(spec({ A: 'zscores(close[AAA], 10)' }))).values, want, 'rolling z', 1e-7);
    const m = avg(AAA);
    const s = sd(AAA);
    close((await line(spec({ A: 'zscores(close[AAA])' }))).values, AAA.map((v) => (v - m) / s), 'z', 1e-7);
  });

  it('volatility: the annualized standard deviation of returns, in percent (gs-quant: 20% is 20)', async () => {
    const r = simple(AAA);
    const want = r.map((_, i) => (i < 10 ? null : sd(r.slice(i - 9, i + 1) as number[]) * Math.sqrt(252) * 100));
    close((await line(spec({ A: 'volatility(close[AAA], 10)' }))).values, want, 'vol', 1e-7);
    close((await line(spec({ A: 'vol(close[AAA], 10)' }))).values, want, 'vol (alias)', 1e-7);
    const lr = zip(AAA, lagged(AAA, 1), (v, p) => Math.log(v / p));
    const logWant = lr.map((_, i) => (i < 10 ? null : sd(lr.slice(i - 9, i + 1) as number[]) * Math.sqrt(252) * 100));
    close((await line(spec({ A: 'volatility(close[AAA], 10, log)' }))).values, logWant, 'log vol', 1e-7);
  });

  // the user, 2026-10-26 (pure-library.ts): the tile's mean, returns and volatility are calls of DataCube's Pure library,
  // planned over the model with the library after it -- and a query that calls none over the model as it is
  // P6 (docs/TS_LANGUAGE_BUILD_2026_10_30.md): a series is its #TS query, every figure of it the platform's builder's
  // library call (core's TsIslandTest holds which); what the tile sends is the island, and the library beside its model
  it('a series is its #TS query, which a plan request carries with the library beside its model', async () => {
    const ts = spec({ A: 'volatility(close[AAA], 10) + mean(close[AAA], 5)' });
    const of = seriesQuery(inputs, ts, ts.series[0]!, {}, {});
    for (const f of ['volatility(', 'mean(']) assert.ok(islandText(of.query).includes(f), `the query calls ${f}`);
    // what a host that plans and runs is sent (Python's engine): the model as it is, and the library beside it
    const model = planner.source().model;
    const asked = await planner.planRequest(of.query);
    assert.equal(asked.model, model);
    assert.equal(asked.library, DATACUBE_LIBRARY);
    // a level too: every series is a #TS query, its lines the language's
    const plain = seriesQuery(inputs, spec({ A: 'close[AAA]' }), spec({ A: 'close[AAA]' }).series[0]!, {}, {});
    assert.equal((await planner.planRequest(plain.query)).model, model);
    assert.equal((await planner.planRequest(plain.query)).library, DATACUBE_LIBRARY);
  });

  it('the levels and lags, the multi-step windows, the self-joins, the exponentials, the technicals, the row maths, the functions along the line, the pairs and the combinations are each a #TS query that plans: a count or a calendar period, a base the line\'s first or as of the first date shown', async () => {
    const calls = async (expr: string, anchor?: string): Promise<string> => {
      const ts = spec({ A: expr });
      const q = seriesQuery(inputs, ts, ts.series[0]!, anchor === undefined ? {} : { from: anchor }, {}).query;
      await planner.plan(q);
      return islandText(q);
    };
    const want: readonly [string, string, string?][] = [
      ['diff(close[AAA], 2)', 'diff'], ['diff(close[AAA], 1m)', 'diffPeriod'], ['lag(close[AAA], 3)', 'lag'],
      ['lag(close[AAA], 1m)', 'lagPeriod'], ['returns(close[AAA], 1w)', 'returnsPeriod'], ['annualize(close[AAA])', 'annualize'],
      ['first(close[AAA])', 'first'], ['last(close[AAA])', 'last'], ['index(close[AAA], 100)', 'index'],
      ['index(close[AAA], 100)', 'indexFrom', '2024-01-15'], ['change(close[AAA])', 'change'],
      ['change(close[AAA])', 'changeFrom', '2024-01-15'], ['percent_change(close[AAA])', 'percentChange'],
      ['percent_change(close[AAA])', 'percentChangeFrom', '2024-01-15'],
      ['zscores(close[AAA], 10)', 'zscores'], ['zscores(close[AAA], 2w)', 'zscoresPeriod'], ['winsorize(close[AAA], 2)', 'winsorize'],
      ['drawdown(close[AAA], 5)', 'drawdown'], ['drawdown(close[AAA], 2w)', 'drawdownPeriod'], ['max_drawdown(close[AAA], 5)', 'maxDrawdown'],
      ['sharpe_ratio(close[AAA], 10)', 'sharpeRatio'], ['bollinger_bands(close[AAA], 5, 2)', 'bollingerBands'],
      ['bollinger_bands(close[AAA], 2w, 2)', 'bollingerBandsPeriod'], ['percentiles(close[AAA], 10)', 'percentiles'],
      ['percentiles(close[AAA], 2w)', 'percentilesPeriod'], ['median(close[AAA], 5)', 'quantile'], ['median(close[AAA], 2w)', 'quantilePeriod'],
      ['percentile(close[AAA], 90, 10)', 'quantile'], ['percentile(close[AAA], 25)', 'quantileOfLine'],
      ['exponential_moving_average(close[AAA], 0.9)', 'ewma'], ['exponential_std(close[AAA], 0.9)', 'ewmStd'],
      ['exponential_volatility(close[AAA], 0.9)', 'exponentialVolatility'], ['exponential_spread_volatility(close[AAA], 0.9)', 'exponentialSpreadVolatility'],
      ['macd(close[AAA], 12, 26, 9)', 'macd'], ['rsi(close[AAA], 14)', 'rsi'], ['smoothed_moving_average(close[AAA], 5)', 'smoothedMean'],
      ['obv(close[AAA], volume[AAA])', 'obv'], ['vwap(close[AAA], volume[AAA], 10)', 'vwap'],
      ['vwap(close[AAA], volume[AAA], 2w)', 'vwapPeriod'], ['volatility(close[AAA], 1m)', 'volatilityPeriod'],
      ['max_drawdown(close[AAA], 2w)', 'maxDrawdownPeriod'], ['sharpe_ratio(close[AAA], 1m)', 'sharpeRatioPeriod'],
      ['log(close[AAA])', 'log'], ['exp(close[AAA] / 100)', 'exp'], ['sqrt(close[AAA])', 'sqrt'], ['abs(close[AAA] - 105)', 'abs'],
      ['power(close[AAA], 2)', 'power'], ['floor(close[AAA], 104)', 'floor'], ['ceil(close[AAA], 104)', 'ceil'], ['not(close[AAA] > 101)', 'not'],
      ['filter(close[AAA], greater_than, 102)', 'filterValues'], ['product(close[AAA] / 100, 5)', 'product'],
      ['geometrically_aggregate(returns(close[AAA]))', 'geometricallyAggregate'], ['smooth_spikes(returns(close[AAA]), 0.1)', 'smoothSpikes'],
      ['vol_swap_volatility(close[AAA], 10)', 'volSwapVolatility'], ['consecutive(returns(close[AAA]))', 'consecutive'], ['count(close[AAA])', 'count'],
      ['day(close[AAA])', 'day'], ['month(close[AAA])', 'month'], ['year(close[AAA])', 'year'], ['quarter(close[AAA])', 'quarter'], ['weekday(close[AAA])', 'weekday'],
      ['correlation(close[AAA], close[BBB], 8)', 'correlation'], ['r_squared(close[AAA], close[BBB], 8)', 'rSquared'],
      ['cov(close[AAA], close[BBB], 8)', 'covariance'], ['beta(close[AAA], close[BBB], 8)', 'beta'], ['hedge_ratio(close[AAA], close[BBB], 8)', 'beta'],
      ['alpha(close[AAA], close[BBB], 8)', 'alpha'], ['residual(close[AAA], close[BBB], 8)', 'residual'],
      ['prices(returns(close[AAA]), 100)', 'prices'], ['if(close[AAA] > 2 * close[BBB], close[AAA], 0)', 'ifThen'],
      ['compare(close[AAA], 2 * close[BBB])', 'compare'], ['union(close[CCC], close[AAA])', 'union'],
      ['excess_returns(close[AAA], 0.05)', 'excessReturns'], ['excess_returns(close[AAA], close[BBB])', 'excessReturnsOver'],
      ['weighted_sum(close[AAA], 0.6, close[BBB], 0.4)', 'weightedSum'],
      // the last moved (2026-10-27): a calendar period's pairs, product and vol swap, and a splice
      ['correlation(close[AAA], close[BBB], 3w)', 'correlationPeriod'], ['r_squared(close[AAA], close[BBB], 3w)', 'rSquaredPeriod'],
      ['cov(close[AAA], close[BBB], 3w)', 'covariancePeriod'], ['beta(close[AAA], close[BBB], 3w)', 'betaPeriod'],
      ['hedge_ratio(close[AAA], close[BBB], 3w)', 'betaPeriod'], ['alpha(close[AAA], close[BBB], 3w)', 'alphaPeriod'],
      ['residual(close[AAA], close[BBB], 3w)', 'residualPeriod'], ['product(close[AAA] / 100, 2w)', 'productPeriod'],
      ['vol_swap_volatility(close[AAA], 1m)', 'volSwapVolatilityPeriod'], ['append(close[CCC], close[AAA])', 'append'],
      ['prepend(close[CCC], close[AAA])', 'prepend'],
    ];
    // the library function each is (the second column) is the platform's builder's to call: core's TsIslandTest
    for (const [expr, , anchor] of want) {
      const text = await calls(expr, anchor);
      const fn = /^([a-z_]+)\(/.exec(expr)![1]!;
      assert.ok(text.includes(TS_NAMES[fn] ?? `${fn}(`), `${expr}${anchor ? ` from ${anchor}` : ''} as #TS: ${text}`);
      if (anchor !== undefined) assert.ok(text.includes(`from => '${anchor}'`), `${expr} from ${anchor}: ${text}`);
    }
  });

  it('a person\'s own query calls the library over the table: its mean as the tile\'s', async () => {
    const source = input.snapshot.source.query;
    const query = lambda([], new Relation(source)
      .filter(lambda(['x'], call('equal', column('x', 'symbol'), lit.string('AAA'))))
      .call('timeseries::mean', call('over', colSpec('symbol'), collection([asc('day')]), call('rows', lit.integer(-4), lit.integer(0))),
        colSpec('close'), lit.integer(5), colSpec('m'))
      .sort([call('descending', colSpec('day'))])
      .node);
    const t = await engine.execute(await planner.plan(query), 0);
    close(values(t, 'm').map(num).reverse(), rolling(AAA, 5, avg), 'the library\'s mean');
  });

  // the user, 2026-10-28 ("so that these functions are usable from platform without TS"): a person's own Pure over the
  // table, as the tile's -- VWAP's traded value, the time fill's line between two values, a decomposition's place
  it('a person\'s own Pure: a price times its volume, a value between two in time, a period\'s number, by name', async () => {
    const source = (await planner.print(lambda([], input.snapshot.source.query))).trim().replace(/^\|/, '');
    const t = await engine.execute(await planner.plan(await planner.parse(`|${source}->filter(x|$x.symbol == 'AAA')
      ->extend(~t:x|dateDiff(%1970-01-01, $x.day->toOne(), meta::pure::functions::date::DurationUnit.DAYS))
      ->timeseries::tradedValue(~close, ~volume, ~pv)
      ->extend(over(~symbol, [~day->ascending()]), ~[x0:{p,w,r|$p->lag($r).t}, y0:{p,w,r|$p->lag($r).close}, x1:{p,w,r|$p->lead($r).t}, y1:{p,w,r|$p->lead($r).close}])
      ->timeseries::linearAt(~x0, ~y0, ~x1, ~y1, ~t, ~mid)
      ->timeseries::periodIndex(~day, timeseries::Grain.month, ~mi)->timeseries::periodIndex(~day, timeseries::Grain.quarter, ~qi)->timeseries::periodIndex(~day, timeseries::Grain.week, ~wi)
      ->timeseries::periodIndex(~day, timeseries::Grain.day, ~di)
      ->sort([~day->ascending()])`)), 0);
    close(values(t, 'pv').map(num), AAA.map((v, i) => v * VOL[i]!), 'price times volume', 1e-9);
    const at = DAYS.map((d) => Date.parse(`${d}T00:00:00Z`) / 86_400_000);
    close(values(t, 'mid').map(num), AAA.map((_, i) => (i === 0 || i === AAA.length - 1 ? null
      : AAA[i - 1]! + ((AAA[i + 1]! - AAA[i - 1]!) * (at[i]! - at[i - 1]!)) / (at[i + 1]! - at[i - 1]!))), 'between its neighbours in time', 1e-9);
    const date = (d: string): Date => new Date(`${d}T00:00:00Z`);
    assert.deepEqual(values(t, 'mi').map(num), DAYS.map((d) => date(d).getUTCFullYear() * 12 + date(d).getUTCMonth()));
    assert.deepEqual(values(t, 'qi').map(num), DAYS.map((d) => date(d).getUTCFullYear() * 4 + Math.floor(date(d).getUTCMonth() / 3)));
    assert.deepEqual(values(t, 'wi').map(num), DAYS.map((d) => Math.floor((Date.parse(`${d}T00:00:00Z`) - Date.UTC(1970, 0, 5)) / (7 * 86_400_000))));
    assert.deepEqual(values(t, 'di').map(num), DAYS.map((d) => Date.parse(`${d}T00:00:00Z`) / 86_400_000));
  });

  // the platform step (the user, 2026-10-28: "do timeseries RIGHT and EXPERT on platform"): the alignment the tile wrote
  // as joins and windows, the library's functions -- a column with gaps put onto every date by step, zero and time
  it('a person\'s own Pure: a column with gaps put onto every date by step, zero and time, by name', async () => {
    const source = (await planner.print(lambda([], input.snapshot.source.query))).trim().replace(/^\|/, '');
    const t = await engine.execute(await planner.plan(await planner.parse(`|${source}->filter(x|$x.symbol == 'AAA')
      ->extend(~[t:x|dateDiff(%1970-01-01, $x.day->toOne(), meta::pure::functions::date::DurationUnit.DAYS), g:x|if($x.close > 102, |$x.close, |[])])
      ->timeseries::fillStep(~symbol, ~day, ~g, ~g, ~st)
      ->timeseries::fillZero(~g, ~z)
      ->timeseries::fillTime(~symbol, ~day, ~g, ~t, ~t, ~g, ~tm)
      ->sort([~day->ascending()])`)), 0);
    const g = AAA.map((v) => (v > 102 ? v : null));
    const has = g.map((v, i) => (v === null ? -1 : i)).filter((i) => i >= 0);
    assert.ok(has.length >= 3 && has.length < g.length && has[0]! > 0, 'gaps to fill, before the first one too');
    const lastAt = (i: number): number | undefined => [...has].reverse().find((j) => j <= i);
    const nextAt = (i: number): number | undefined => has.find((j) => j >= i);
    close(values(t, 'st').map(num), g.map((_, i) => g[lastAt(i) ?? has[0]!]!), 'step: its last value, before its first its first', 0);
    close(values(t, 'z').map(num), g.map((v) => v ?? 0), 'zero', 0);
    const at = DAYS.map((d) => Date.parse(`${d}T00:00:00Z`) / 86_400_000);
    close(values(t, 'tm').map(num), g.map((_, i) => {
      const a = lastAt(i);
      const b = nextAt(i);
      if (a === undefined || b === undefined) return null;
      return a === b ? g[a]! : g[a]! + ((g[b]! - g[a]!) * (at[i]! - at[a]!)) / (at[b]! - at[a]!);
    }), 'time: linear between its values before and after, none outside them', 1e-9);
  });

  // the second audit's N8: its monthly form compared two nones (40 weekdays are two months); a frequency's own periods a
  // year read weekly here, where there are figures to compare (monthly by twelve: the Q3 test's)
  it('volatility at a weekly frequency annualizes by 52', async () => {
    const weekOf = (d: string): string => {
      const at = new Date(`${d}T00:00:00Z`);
      return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(DAYS.map(weekOf))];
    const last = weeks.map((w) => AAA[DAYS.map(weekOf).lastIndexOf(w)]!);
    const r = simple(last);
    const want = r.map((_, i) => (i < 2 ? null : sd(r.slice(i - 1, i + 1) as number[]) * Math.sqrt(52) * 100));
    assert.ok(want.filter((v) => v !== null).length >= 4, 'figures to compare');
    close((await line(spec({ A: 'volatility(close[AAA], 2)' }, { grain: 'week' }))).values, want, 'weekly vol', 1e-7);
  });

  it('drawdown from the running peak, and the deepest one over a window', async () => {
    let peak = -Infinity;
    const dd = AAA.map((v) => {
      peak = Math.max(peak, v);
      return v / peak - 1;
    });
    close((await line(spec({ A: 'drawdown(close[AAA])' }))).values, dd, 'drawdown', 1e-9);
    let worst = Infinity;
    close((await line(spec({ A: 'max_drawdown(close[AAA])' }))).values, dd.map((d) => (worst = Math.min(worst, d))), 'max drawdown', 1e-9);
    const rolledPeak = AAA.map((_, i) => Math.max(...AAA.slice(Math.max(0, i - 4), i + 1)));
    const rolledDd = AAA.map((v, i) => v / rolledPeak[i]! - 1);
    close((await line(spec({ A: 'max_drawdown(close[AAA], 5)' }))).values,
      rolledDd.map((_, i) => (i < 4 ? null : Math.min(...rolledDd.slice(i - 4, i + 1)))), 'rolling max drawdown', 1e-9);
  });

  it('correlation, beta and covariance of two instruments\' returns', async () => {
    const ra = simple(AAA);
    const rb = simple(BBB);
    const corr = pairs(ra, rb, 8, (p, q) => cov(p, q) / (sd(p) * sd(q)));
    close((await line(spec({ A: 'correlation(close[AAA], close[BBB], 8)' }))).values, corr, 'corr', 1e-7);
    const beta = pairs(ra, rb, 8, (p, q) => cov(p, q) / sd(q) ** 2);
    close((await line(spec({ A: 'beta(close[AAA], close[BBB], 8)' }))).values, beta, 'beta', 1e-7);
    const c = pairs(AAA, BBB, 8, (p, q) => cov(p, q));
    close((await line(spec({ A: 'cov(close[AAA], close[BBB], 8)' }))).values, c, 'cov', 1e-7);
  });

  // the conversion's review (2026-10-28): a correlation where a side does not vary is none -- every form of it, never
  // DuckDB's NaN (Postgres says none): over a count of rows and a calendar period, squared, and lagged
  it('a correlation where a side does not vary is none, in every form', async () => {
    const days = weekdays(12);
    const X = [10, 11, 13, 12, 14, 15, 17, 16, 18, 19, 21, 20];
    const Y = [50, 50, 50, 50, 50, 50, 50, 50, 51, 53, 52, 54];
    const csv = ['symbol,day,close', ...days.flatMap((d, i) => [`X,${d},${X[i]}`, `Y,${d},${Y[i]}`])].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'flat.csv'), writer, { table: 'flat_side' });
    const own = plannerFor(r.model, r.runtime);
    const flat: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    // Y's returns 0 to its eighth date: a window of them all none; one reaching its ninth (lagged, its tenth), a number
    const cases: readonly (readonly [string, number, number])[] = [
      ['correlation(close[X], close[Y], 4)', 4, 8], ['r_squared(close[X], close[Y], 4)', 4, 8],
      ['correlation(close[X], close[Y], 1w)', 6, 8], ['r_squared(close[X], close[Y], 1w)', 6, 8],
      ['lead_lag(close[X], close[Y], 1, 4)', 5, 9],
    ];
    for (const [expr, from, number] of cases) {
      const ts = spec({ A: expr });
      const t = await engine.execute(await own.plan(seriesQuery(flat, ts, ts.series[0]!).query), 0);
      const at = values(t, TS_DATE).map((d) => days.indexOf(String(d).slice(0, 10)));
      const vs = values(t, TS_VALUE).map(num);
      assert.ok(!vs.some((v) => v !== null && Number.isNaN(v)), `${expr}: no NaN`);
      for (let i = from; i < days.length; i++) {
        const v = vs[at.indexOf(i)];
        if (i < number) assert.equal(v ?? null, null, `${expr} at ${days[i]}: Y flat over the window, none`);
        else assert.ok(typeof v === 'number' && Number.isFinite(v), `${expr} at ${days[i]}: a number (${v})`);
      }
    }
    own.dispose();
  });

  // the expert functions (2026-10-26): a rolling regression of the first on the second, its every figure the database's
  it('a regression: alpha and R² of the returns, a hedge ratio and the residual of the levels', async () => {
    const ra = simple(AAA);
    const rb = simple(BBB);
    const slope = (p: number[], q: number[]): number => cov(p, q) / sd(q) ** 2;
    close((await line(spec({ A: 'r_squared(close[AAA], close[BBB], 8)' }))).values,
      pairs(ra, rb, 8, (p, q) => (cov(p, q) / (sd(p) * sd(q))) ** 2), 'R²', 1e-7);
    close((await line(spec({ A: 'alpha(close[AAA], close[BBB], 8)' }))).values,
      pairs(ra, rb, 8, (p, q) => (avg(p) - slope(p, q) * avg(q)) * 252), 'alpha, annualized', 1e-7);
    close((await line(spec({ A: 'hedge_ratio(close[AAA], close[BBB], 8)' }))).values, pairs(AAA, BBB, 8, slope), 'hedge ratio', 1e-7);
    close((await line(spec({ A: 'residual(close[AAA], close[BBB], 8)' }))).values,
      pairs(AAA, BBB, 8, (p, q) => p.at(-1)! - (avg(p) - slope(p, q) * avg(q)) - slope(p, q) * q.at(-1)!), 'residual', 1e-7);
    // over a calendar period too
    const start = onBy(DAYS[0]!, 3, 'w');
    close((await line(spec({ A: 'hedge_ratio(close[AAA], close[BBB], 3w)' }))).values, DAYS.map((d) => {
      if (d < start) return null;
      const js = DAYS.map((_, j) => j).filter((j) => DAYS[j]! > backBy(d, 3, 'w') && DAYS[j]! <= d);
      return slope(js.map((j) => AAA[j]!), js.map((j) => BBB[j]!));
    }), 'hedge ratio over 3w', 1e-7);
  });

  // the E9 review's 1: a second series with leading empties (a lag, a ramped mean) -- its means, its variance and the
  // ramp are of the window's pairs, the dates both have, or the intercept is of two different samples (its sign wrong)
  it('a regression on a second series that starts later: every figure over the window\'s pairs', async () => {
    const ra = simple(AAA);
    const rb = simple(lagged(BBB, 3));
    const slope = (p: number[], q: number[]): number => cov(p, q) / sd(q) ** 2;
    close((await line(spec({ A: 'alpha(close[AAA], lag(close[BBB], 3), 8)' }))).values,
      pairs(ra, rb, 8, (p, q) => (avg(p) - slope(p, q) * avg(q)) * 252), 'alpha on a lag', 1e-7);
    close((await line(spec({ A: 'beta(close[AAA], lag(close[BBB], 3), 8)' }))).values, pairs(ra, rb, 8, slope), 'beta on a lag', 1e-7);
    close((await line(spec({ A: 'correlation(close[AAA], lag(close[BBB], 3), 8)' }))).values,
      pairs(ra, rb, 8, (p, q) => cov(p, q) / (sd(p) * sd(q))), 'correlation on a lag', 1e-7);
    const mb = rolling(BBB, 5, avg);
    close((await line(spec({ A: 'residual(close[AAA], mean(close[BBB], 5), 8)' }))).values,
      pairs(AAA, mb, 8, (p, q) => p.at(-1)! - (avg(p) - slope(p, q) * avg(q)) - slope(p, q) * q.at(-1)!), 'residual on a mean', 1e-7);
    close((await line(spec({ A: 'hedge_ratio(close[AAA], mean(close[BBB], 5), 8)' }))).values, pairs(AAA, mb, 8, slope), 'hedge ratio on a mean', 1e-7);
  });

  it('a series read by another: $A', async () => {
    const ts = spec({ A: 'close[AAA]', B: 'returns($A)', C: 'volatility($A, 10) - volatility(close[BBB], 10)' });
    close((await line(ts, 'B')).values, simple(AAA), '$A');
    const ra = simple(AAA);
    const rb = simple(BBB);
    const want = ra.map((_, i) => (i < 10 ? null : (sd(ra.slice(i - 9, i + 1) as number[]) - sd(rb.slice(i - 9, i + 1) as number[])) * Math.sqrt(252) * 100));
    close((await line(ts, 'C')).values, want, 'vol spread', 1e-7);
  });

  it('Bollinger bands: k deviations either side of the moving average', async () => {
    const l = await line(spec({ A: 'bollinger_bands(close[AAA], 5, 2)' }));
    close(l.lower!, rolling(AAA, 5, (v) => avg(v) - 2 * sd(v)), 'lower', 1e-7);
    close(l.upper!, rolling(AAA, 5, (v) => avg(v) + 2 * sd(v)), 'upper', 1e-7);
  });

  it('RSI of Wilder\'s smoothed averages of the gains and the losses (gs-quant\'s)', async () => {
    const c = AAA.slice(1).map((v, i) => v - AAA[i]!);
    const rsi = (w: number): Line => {
      const up = smma(c.map((v) => Math.max(v, 0)), w);
      const down = smma(c.map((v) => Math.max(-v, 0)), w);
      return [null, ...c.map((_, j) => (up[j] === null ? null : down[j] === 0 ? 100 : 100 - 100 / (1 + up[j]! / down[j]!)))];
    };
    close((await line(spec({ A: 'rsi(close[AAA], 6)' }))).values, rsi(6), 'rsi', 1e-9);
    // of one: from the second change, gs-quant's ramp of 1 (the E9 review's nit)
    close((await line(spec({ A: 'rsi(close[AAA], 1)' }))).values, rsi(1), 'rsi of 1', 1e-9);
  });

  // the second audit's S5: initial at the first date -- gs-quant's product over the returns to date, as pandas' prod
  // takes it, skips an empty one (the first, with no date before it)
  it('prices from returns compound them back, from initial at the first date', async () => {
    const r = simple(AAA);
    let level = 1;
    const want = r.map((v) => (v === null ? level : (level *= 1 + v)));
    assert.equal(want[0], 1);
    close((await line(spec({ A: 'prices(returns(close[AAA]))' }))).values, want, 'prices', 1e-7);
    close((await line(spec({ A: 'prices(returns(close[AAA]), 100)' }))).values, want.map((v) => v * 100), 'from 100', 1e-7);
    close((await line(spec({ A: 'prices(diff(close[AAA]), 10, absolute)' }))).values, AAA.map((v) => 10 + v - AAA[0]!), 'absolute', 1e-7);
    close((await line(spec({ A: 'prices(returns(close[AAA], 1, log), 100, log)' }))).values, AAA.map((v) => (100 * v) / AAA[0]!), 'log', 1e-7);
  });

  it('math: log, sqrt, abs, power, a floor and a cap', async () => {
    close((await line(spec({ A: 'log(close[AAA])' }))).values, AAA.map(Math.log), 'log', 1e-9);
    close((await line(spec({ A: 'sqrt(close[AAA])' }))).values, AAA.map(Math.sqrt), 'sqrt', 1e-9);
    close((await line(spec({ A: 'abs(close[AAA] - 105)' }))).values, AAA.map((v) => Math.abs(v - 105)), 'abs', 1e-9);
    close((await line(spec({ A: 'power(close[AAA], 2)' }))).values, AAA.map((v) => v * v), 'power', 1e-9);
    close((await line(spec({ A: 'floor(close[AAA], 104)' }))).values, AAA.map((v) => Math.max(v, 104)), 'floor', 1e-9);
    close((await line(spec({ A: 'ceil(close[AAA], 104)' }))).values, AAA.map((v) => Math.min(v, 104)), 'ceil', 1e-9);
  });
});

describe('from the first date shown', () => {
  it('rebase: 100 at the first date, or at the first date shown', async () => {
    const ts = spec({ A: 'rebase(close[AAA])' });
    close((await line(ts)).values, AAA.map((v) => (v / AAA[0]!) * 100), 'from the start', 1e-9);
    close((await line(ts, 'A', { from: DAYS[10]! })).values, AAA.map((v) => (v / AAA[10]!) * 100), 'from day 10', 1e-9);
  });

  it('the first date shown with no value takes the next one that has it', async () => {
    const ts = spec({ A: 'index(close[CCC], 1)' });
    // CCC trades on days 0, 3, 6, ...: the view from day 4 starts at day 6
    const l = await line(ts, 'A', { from: DAYS[4]! });
    const ccc = CCC.filter((_, i) => i % 3 === 0);
    close(l.values, ccc.map((v) => v / CCC[6]!), 'from day 6', 1e-9);
  });

  it('change and percent change from the start', async () => {
    close((await line(spec({ A: 'change(close[AAA])' }), 'A', { from: DAYS[5]! })).values, AAA.map((v) => v - AAA[5]!), 'change', 1e-9);
    close((await line(spec({ A: 'percent_change(close[AAA])' }), 'A', { from: DAYS[5]! })).values, AAA.map((v) => v / AAA[5]! - 1), '%', 1e-9);
  });

  it('compare: every level rebased, a return left as it is', async () => {
    const ts = spec({ A: 'close', B: 'returns(close)' }, { view: { compare: 'rebase' } }, 'symbol');
    const l = await line(ts, 'A', { from: DAYS[2]! });
    close(l.values.filter((_, i) => l.split![i] === 'BBB'), BBB.map((v) => (v / BBB[2]!) * 100), 'BBB rebased', 1e-9);
    const r = await line(ts, 'B', { from: DAYS[2]! });
    close(r.values.filter((_, i) => r.split![i] === 'AAA'), simple(AAA), 'a return stays');
    const pct = await line({ ...ts, view: { compare: 'percent' } }, 'A');
    close(pct.values.filter((_, i) => pct.split![i] === 'AAA'), AAA.map((v) => v / AAA[0]! - 1), 'percent', 1e-9);
  });
});

// -- calendar periods (gs-quant's tenors: Window('3m'), returns(x, '1y')) --

/** A date a calendar period back, as the database's `adjust` takes it: a month back from the 31st is the month's last
 *  day (written here as plain arithmetic over the dates, the reference). */
function backBy(d: string, n: number, unit: 'd' | 'w' | 'm'): string {
  const at = new Date(`${d}T00:00:00Z`);
  if (unit !== 'm') return new Date(at.getTime() - n * (unit === 'w' ? 7 : 1) * 86_400_000).toISOString().slice(0, 10);
  const y = at.getUTCFullYear();
  const m = at.getUTCMonth() - n;
  const last = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();
  return new Date(Date.UTC(y, m, Math.min(at.getUTCDate(), last))).toISOString().slice(0, 10);
}
/** A date a calendar period on. */
const onBy = (d: string, n: number, unit: 'd' | 'w' | 'm'): string => backBy(d, -n, unit);

describe('calendar periods: windows of dates, and lags as of a date (gs-quant\'s tenors)', () => {
  it('mean over 2w: the dates after two weeks back, to this one; drawn from the first date two weeks on', async () => {
    const l = await line(spec({ A: 'mean(close[AAA], 2w)' }));
    const start = onBy(DAYS[0]!, 2, 'w');
    close(l.values, DAYS.map((d) => (d < start ? null : avg(AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d)))), 'mean 2w', 1e-9);
  });

  // the conversion to DataCube's Pure library (2026-10-27): every rolled statistic of a calendar period is one library
  // function's (overPeriod), its statistic a word the call is expanded with -- each held to its definition
  it('sum, min, max, range, std and var over 2w: the dates after two weeks back, to this one; from the first date two weeks on', async () => {
    const start = onBy(DAYS[0]!, 2, 'w');
    const of: Readonly<Record<string, (v: number[]) => number | null>> = {
      sum: (v) => v.reduce((a, b) => a + b, 0),
      min: (v) => Math.min(...v),
      max: (v) => Math.max(...v),
      range: (v) => Math.max(...v) - Math.min(...v),
      std: (v) => (v.length < 2 ? null : sd(v)),
      var: (v) => (v.length < 2 ? null : sd(v) ** 2),
    };
    for (const [f, stat] of Object.entries(of)) {
      const l = await line(spec({ A: `${f}(close[AAA], 2w)` }));
      close(l.values, DAYS.map((d) => (d < start ? null : stat(AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d)))), `${f} 2w`, 1e-9);
    }
  });

  // the multi-step functions over a calendar period, the library's (2026-10-27): each of overPeriod's statistics nested
  // in it, from the first date two weeks on -- each held to its definition
  it('z-scores, a drawdown and Bollinger bands over 2w: of the dates after two weeks back, to this one; from the first date two weeks on', async () => {
    const start = onBy(DAYS[0]!, 2, 'w');
    const within = (d: string): number[] => AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d);
    close((await line(spec({ A: 'zscores(close[AAA], 2w)' }))).values, DAYS.map((d, i) => {
      const v = within(d);
      return d < start || v.length < 2 || sd(v) === 0 ? null : (AAA[i]! - avg(v)) / sd(v);
    }), 'zscores 2w', 1e-7);
    close((await line(spec({ A: 'drawdown(close[AAA], 2w)' }))).values,
      DAYS.map((d, i) => (d < start ? null : AAA[i]! / Math.max(...within(d)) - 1)), 'drawdown 2w', 1e-9);
    const b = await line(spec({ A: 'bollinger_bands(close[AAA], 2w, 2)' }));
    const band = (k: number): Line => DAYS.map((d) => {
      const v = within(d);
      return d < start || v.length < 2 ? null : avg(v) + k * sd(v);
    });
    close(b.lower!, band(-2), 'lower 2w', 1e-7);
    close(b.upper!, band(2), 'upper 2w', 1e-7);
  });

  it('winsorize and a Sharpe ratio over a count: within the limit of every value\'s mean; the mean return over its deviation, annualized', async () => {
    const m = avg(AAA);
    const s = sd(AAA);
    close((await line(spec({ A: 'winsorize(close[AAA], 1)' }))).values, AAA.map((v) => Math.min(Math.max(v, m - s), m + s)), 'winsorize', 1e-9);
    const r = simple(AAA);
    close((await line(spec({ A: 'sharpe_ratio(close[AAA], 10)' }))).values, DAYS.map((_, i) => {
      const vals = r.slice(Math.max(0, i - 9), i + 1).filter((v) => v !== null) as number[];
      return vals.length < 10 ? null : (avg(vals) / sd(vals)) * Math.sqrt(252);
    }), 'sharpe 10', 1e-7);
  });

  // a figure whose ramp counts from the first date of the series it reads, not of the column it is over (the library's
  // rampPeriod, 2026-10-27): a maximum drawdown over 2w from the first price two weeks on
  it('max drawdown over 2w: each date\'s deepest fall within two weeks back, each from its own two weeks\' peak; from the first date two weeks on', async () => {
    const start = onBy(DAYS[0]!, 2, 'w');
    const back2 = (d: string): number[] => DAYS.map((_, j) => j).filter((j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d);
    const dd = DAYS.map((d, i) => AAA[i]! / Math.max(...back2(d).map((j) => AAA[j]!)) - 1);
    close((await line(spec({ A: 'max_drawdown(close[AAA], 2w)' }))).values,
      DAYS.map((d) => (d < start ? null : Math.min(...back2(d).map((j) => dd[j]!)))), 'max drawdown 2w', 1e-9);
  });

  it('returns over 1w: each date against the value as of a week before, its last at or before', async () => {
    const l = await line(spec({ A: 'returns(close[AAA], 1w)' }));
    const want = DAYS.map((d, i) => {
      const j = DAYS.findLastIndex((x) => x <= backBy(d, 1, 'w'));
      return j < 0 ? null : AAA[i]! / AAA[j]! - 1;
    });
    close(l.values, want, 'returns 1w', 1e-9);
    // the same as of a date for CCC, which trades on every third day: its last at or before, never the next
    const c = await line(spec({ A: 'diff(close[CCC], 1w)' }));
    const cDays = DAYS.filter((_, i) => i % 3 === 0);
    const cVals = CCC.filter((_, i) => i % 3 === 0);
    close(c.values, cDays.map((d, i) => {
      const j = cDays.findLastIndex((x) => x <= backBy(d, 1, 'w'));
      return j < 0 ? null : cVals[i]! - cVals[j]!;
    }), 'diff 1w of CCC', 1e-9);
  });

  // from its first price a month on, as gs-quant's ramp counts from the first price's date (its returns keep it, none): the
  // E9 review's nit, from the first return's before
  it('volatility over 1m: its returns\' deviation over each month back, annualized, from its first price a month on', async () => {
    const l = await line(spec({ A: 'volatility(close[AAA], 1m)' }));
    const r = simple(AAA);
    const start = onBy(DAYS[0]!, 1, 'm');
    close(l.values, DAYS.map((d) => {
      if (d < start) return null;
      const vals = r.filter((v, j) => v !== null && DAYS[j]! > backBy(d, 1, 'm') && DAYS[j]! <= d) as number[];
      return vals.length < 2 ? null : sd(vals) * Math.sqrt(252) * 100;
    }), 'volatility 1m', 1e-9);
    // a Sharpe ratio over 1m too: the mean return over its deviation, annualized, from the first price a month on
    close((await line(spec({ A: 'sharpe_ratio(close[AAA], 1m)' }))).values, DAYS.map((d) => {
      if (d < start) return null;
      const vals = r.filter((v, j) => v !== null && DAYS[j]! > backBy(d, 1, 'm') && DAYS[j]! <= d) as number[];
      return vals.length < 2 ? null : (avg(vals) / sd(vals)) * Math.sqrt(252);
    }), 'sharpe 1m', 1e-9);
  });

  it('correlation over 3w: two series\' returns over the dates both have, each three weeks back', async () => {
    const l = await line(spec({ A: 'correlation(close[AAA], close[BBB], 3w)' }));
    const ra = simple(AAA);
    const rb = simple(BBB);
    const start = onBy(DAYS[1]!, 3, 'w');
    close(l.values, DAYS.map((d) => {
      if (d < start) return null;
      const js = DAYS.map((_, j) => j).filter((j) => ra[j] !== null && DAYS[j]! > backBy(d, 3, 'w') && DAYS[j]! <= d);
      const a = js.map((j) => ra[j]!);
      const b = js.map((j) => rb[j]!);
      return cov(a, b) / (sd(a) * sd(b));
    }), 'correlation 3w', 1e-9);
  });

  // the library's calendar forms of the pairs (pure-library.ts pairPeriod, 2026-10-27): each over the pairs whose dates
  // are within three weeks back, from the first pair's date that period on
  it('the pairs over 3w: R², covariance, beta, alpha and residual, over the dates both have', async () => {
    const ra = simple(AAA);
    const rb = simple(BBB);
    const slope = (p: number[], q: number[]): number => cov(p, q) / sd(q) ** 2;
    const over = (xs: Line, ys: Line, f: (p: number[], q: number[]) => number): Line => {
      const start = onBy(DAYS[xs.findIndex((v, i) => v !== null && ys[i] !== null)]!, 3, 'w');
      return DAYS.map((d) => {
        if (d < start) return null;
        const js = DAYS.map((_, j) => j).filter((j) => xs[j] !== null && ys[j] !== null && DAYS[j]! > backBy(d, 3, 'w') && DAYS[j]! <= d);
        return f(js.map((j) => xs[j]!), js.map((j) => ys[j]!));
      });
    };
    close((await line(spec({ A: 'r_squared(close[AAA], close[BBB], 3w)' }))).values,
      over(ra, rb, (p, q) => (cov(p, q) / (sd(p) * sd(q))) ** 2), 'R² 3w', 1e-9);
    close((await line(spec({ A: 'cov(close[AAA], close[BBB], 3w)' }))).values, over(AAA, BBB, cov), 'cov 3w', 1e-9);
    close((await line(spec({ A: 'beta(close[AAA], close[BBB], 3w)' }))).values, over(ra, rb, slope), 'beta 3w', 1e-9);
    close((await line(spec({ A: 'alpha(close[AAA], close[BBB], 3w)' }))).values,
      over(ra, rb, (p, q) => (avg(p) - slope(p, q) * avg(q)) * 252), 'alpha 3w', 1e-9);
    close((await line(spec({ A: 'residual(close[AAA], close[BBB], 3w)' }))).values,
      over(AAA, BBB, (p, q) => p.at(-1)! - (avg(p) - slope(p, q) * avg(q)) - slope(p, q) * q.at(-1)!), 'residual 3w', 1e-9);
    // a second series that starts later: its pairs, and its ramp from the first pair's date
    const lb = simple(lagged(BBB, 3));
    close((await line(spec({ A: 'beta(close[AAA], lag(close[BBB], 3), 3w)' }))).values, over(ra, lb, slope), 'beta on a lag 3w', 1e-9);
  });

  // the library's calendar forms of a product and a vol swap (pure-library.ts productPeriod, volSwapVolatilityPeriod)
  it('a product over 2w and a vol swap over 1m, from the first value\'s date that period on', async () => {
    const start2w = onBy(DAYS[0]!, 2, 'w');
    close((await line(spec({ A: 'product(close[AAA] / 100, 2w)' }))).values, DAYS.map((d) => (d < start2w ? null
      : AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d).reduce((a, v) => a * (v / 100), 1))), 'product 2w', 1e-9);
    // a value of 0 (AAA's 23rd is 105.8) and values below it: a product of 0 where the period holds it, else a sign by
    // its negatives
    const signed = (await line(spec({ A: 'product(close[AAA] - 105.8, 2w)' }))).values;
    close(signed, DAYS.map((d) => (d < start2w ? null
      : AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d).reduce((a, v) => a * (v - 105.8), 1))), 'product of signs 2w', 1e-9);
    assert.ok(signed.some((v) => v === 0) && signed.some((v) => v !== null && v < 0) && signed.some((v) => v !== null && v > 0), 'zeros and both signs');
    const lr = AAA.map((v, i) => (i === 0 ? null : Math.log(v / AAA[i - 1]!)));
    const start1m = onBy(DAYS[0]!, 1, 'm');
    close((await line(spec({ A: 'vol_swap_volatility(close[AAA], 1m)' }))).values, DAYS.map((d) => {
      if (d < start1m) return null;
      const w = lr.filter((v, j) => v !== null && DAYS[j]! > backBy(d, 1, 'm') && DAYS[j]! <= d) as number[];
      return Math.sqrt(w.reduce((a, b) => a + b * b, 0) / w.length) * Math.sqrt(252) * 100;
    }), 'vol swap 1m', 1e-9);
  });

  it('nested: a z-score over three weeks of a volatility over two, planned and run', async () => {
    const l = await line(spec({ A: 'zscores(volatility(close[AAA], 2w), 3w)' }));
    const vol = (await line(spec({ A: 'volatility(close[AAA], 2w)' }))).values;
    const start = onBy(DAYS.find((_, i) => vol[i] !== null)!, 3, 'w');
    close(l.values, DAYS.map((d, i) => {
      if (d < start || vol[i] === null) return null;
      const vals = vol.filter((v, j) => v !== null && DAYS[j]! > backBy(d, 3, 'w') && DAYS[j]! <= d) as number[];
      return (vol[i]! - avg(vals)) / sd(vals);
    }), 'zscores of a volatility', 1e-7);
  });

  it('a split series: each line\'s window its own', async () => {
    const l = await line(spec({ A: 'sum(close, 1w)' }, {}, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    const start = onBy(DAYS[0]!, 1, 'w');
    close(of('BBB'), DAYS.map((d) => (d < start ? null : BBB.filter((_, j) => DAYS[j]! > backBy(d, 1, 'w') && DAYS[j]! <= d).reduce((x, y) => x + y, 0))), 'BBB', 1e-9);
  });
});

// -- exponentially weighted (the expert functions, 2026-10-26): each a recursion the database computes in its closed form,
// held here to the recursion itself, run over 700 days so its blocks of values turn over several times --

// -- by each window's rows (the expert functions, 2026-10-26): a self-join of each date to its window's --

/** pandas' quantile (linear, numpy's default): between the two nearest of the sorted values. */
const quantile = (v: number[], q: number): number => {
  const sorted = [...v].sort((a, b) => a - b);
  const at = q * (sorted.length - 1);
  const lo = Math.floor(at);
  return sorted[lo]! + (sorted[Math.ceil(at)]! - sorted[lo]!) * (at - lo);
};
/** scipy's percentileofscore(window, value, kind='mean'): the share below it, those equal counted half, in percent. */
const rankIn = (window: number[], v: number): number =>
  ((window.filter((x) => x < v).length + window.filter((x) => x <= v).length) / 2 / window.length) * 100;

// -- conditions (the expert functions, 2026-10-26): 1 where they hold, 0 where not, each date's run the database's --

// -- the correlations of the lines over the dates shown (the expert tables, 2026-10-26) --

// -- the dates of two series combined (the next phases' 1, 2026-10-26: gs-quant's Interpolate, its algebra's method) --

describe('two series on two calendars: the tile\'s align, and interpolate', () => {
  // CCC trades every third weekday, AAA every one: the union is AAA's, CCC's own every third
  const own = DAYS.map((_, i) => i).filter((i) => i % 3 === 0);
  const before = (i: number): number => own.filter((j) => j <= i).at(-1)!;
  const after = (i: number): number | undefined => own.find((j) => j >= i);
  const day = (d: string): number => Date.parse(`${d}T00:00:00Z`) / 86_400_000;
  const aligned = (align: string): TimeseriesSpec => spec({ A: 'close[AAA] - close[CCC]' }, { align } as Partial<TimeseriesSpec>);

  it('intersect: the dates both have, as before', async () => {
    const l = await line(aligned('intersect'));
    assert.deepEqual(l.dates, own.map((i) => DAYS[i]!));
  });

  it('step: every date either has, CCC its last close at or before each', async () => {
    const l = await line(aligned('step'));
    assert.deepEqual(l.dates, DAYS);
    close(l.values, AAA.map((v, i) => v - CCC[before(i)]!), 'step', 1e-9);
  });

  it('time: CCC linear in time between its closes, none after its last', async () => {
    const l = await line(aligned('time'));
    assert.deepEqual(l.dates, DAYS);
    close(l.values, AAA.map((v, i) => {
      const a = before(i);
      const b = after(i);
      if (b === undefined) return null;
      if (a === b) return v - CCC[a]!;
      const t = (day(DAYS[i]!) - day(DAYS[a]!)) / (day(DAYS[b]!) - day(DAYS[a]!));
      return v - (CCC[a]! + (CCC[b]! - CCC[a]!) * t);
    }), 'time', 1e-9);
  });

  // the platform step (the user, 2026-10-28): a person's own Pure puts two series on one calendar as the tile's align
  // does -- Pure's own join, then the library's fills -- and answers as the tile does
  it('a person\'s own Pure: two calendars joined and CCC filled by step and by time, as the tile\'s align', async () => {
    const source = (await planner.print(lambda([], input.snapshot.source.query))).trim().replace(/^\|/, '');
    const days = 'meta::pure::functions::date::DurationUnit.DAYS';
    const t = await engine.execute(await planner.plan(await planner.parse(`|${source}->filter(x|$x.symbol == 'AAA')->select(~[day, close])->rename(~close, ~a)
      ->join(${source}->filter(x|$x.symbol == 'CCC')->select(~[day, close])->rename(~day, ~cday)->rename(~close, ~c),
        meta::pure::functions::relation::JoinKind.LEFT, {l,r|$l.day == $r.cday})
      ->extend(~[ln:x|'one', ta:x|dateDiff(%1970-01-01, $x.day->toOne(), ${days}), tc:x|if($x.cday->isEmpty(), |[], |dateDiff(%1970-01-01, $x.cday->toOne(), ${days}))])
      ->timeseries::fillStep(~ln, ~day, ~c, ~c, ~cs)
      ->timeseries::fillTime(~ln, ~day, ~c, ~tc, ~ta, ~c, ~ct)
      ->extend(~[step:x|$x.a->toOne() - $x.cs->toOne(), time:x|if($x.ct->isEmpty(), |[], |$x.a->toOne() - $x.ct->toOne())])
      ->sort([~day->ascending()])`)), 0);
    close(values(t, 'step').map(num), (await line(aligned('step'))).values, 'step, as the tile\'s', 1e-9);
    close(values(t, 'time').map(num), (await line(aligned('time'))).values, 'time, as the tile\'s', 1e-9);
  });

  it('nan and zero: every date, none where CCC has none -- or 0 there', async () => {
    close((await line(aligned('nan'))).values, AAA.map((v, i) => (i % 3 === 0 ? v - CCC[i]! : null)), 'nan', 1e-9);
    close((await line(aligned('zero'))).values, AAA.map((v, i) => (i % 3 === 0 ? v - CCC[i]! : v)), 'zero', 1e-9);
  });

  it('step before a series\' first value: its first (gs-quant\'s ffill, then bfill)', async () => {
    const l = await line(spec({ A: 'close[AAA] - mean(close[CCC], 3)' }, { align: 'step' } as Partial<TimeseriesSpec>));
    const mean3 = (j: number): number => (CCC[j]! + CCC[j - 3]! + CCC[j - 6]!) / 3;
    close(l.values, AAA.map((v, i) => v - mean3(Math.max(6, before(i)))), 'bfilled', 1e-9);
  });

  it('a line per symbol: each its own dates and CCC\'s', async () => {
    const l = await line(spec({ A: 'close - close[CCC]' }, { align: 'step' } as Partial<TimeseriesSpec>, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    close(of('AAA'), AAA.map((v, i) => v - CCC[before(i)]!), 'AAA', 1e-9);
    close(of('BBB'), BBB.map((v, i) => v - CCC[before(i)]!), 'BBB', 1e-9);
    close(of('CCC'), own.map(() => 0), 'CCC with itself', 1e-9);
  });

  // the Phase 1 review's 1 (2026-10-26): an as-of join of each part and a spine of both read each part three times, so a
  // chain of them grew threefold a part (five parts under step: 544k characters, 4.5 s) -- merged by one full join,
  // each part is read once
  it('a chain of parts under each align grows by the same steps each, never multiplying -- split or not', () => {
    for (const align of ['step', 'time', 'nan', 'zero']) {
      for (const split of [undefined, 'symbol']) {
        const text = (n: number): string => [split ? 'close' : 'close[AAA]', ...Array.from({ length: n - 1 }, (_, i) => `close[${['CCC', 'BBB'][i % 2]}]`)].join(' - ');
        const size = (n: number): number => {
          const s = spec({ A: text(n) }, { align } as Partial<TimeseriesSpec>, split);
          return toJson(seriesQuery(inputs, s, s.series[0]!).query).length;
        };
        const [s2, s3, s4, s5] = [2, 3, 4, 5].map(size);
        const steps = [s3! - s2!, s4! - s3!, s5! - s4!];
        assert.ok(steps.every((d) => Math.abs(d - steps[0]!) <= 0.15 * steps[0]!), `${align}${split ? ', split' : ''}: each part the same steps: ${[s2, s3, s4, s5].join(', ')}`);
      }
    }
  });

  it('three parts under step: each its last value at or before each date, a lag of CCC on CCC\'s own dates', async () => {
    const l = await line(spec({ A: 'close[AAA] - close[CCC] + lag(close[CCC], 1)' }, { align: 'step' } as Partial<TimeseriesSpec>));
    assert.deepEqual(l.dates, DAYS);
    // the lag's first value at CCC's second date (bfilled before it)
    const lag = (i: number): number => { const j = own.indexOf(before(i)); return CCC[own[Math.max(1, j) - 1]!]!; };
    close(l.values, AAA.map((v, i) => v - CCC[before(i)]! + lag(i)), 'three parts', 1e-9);
  });

  // the Phase 1 review's 3: gs-quant's if_ is of the flags' dates -- each x or y there, none where that one has none --
  // whatever the tile's align
  it('if: on the flags\' dates, the branch taken none where it has none, under any align', async () => {
    const want = AAA.map((v, i) => (v > 101 ? (i % 3 === 0 ? CCC[i]! : null) : v));
    for (const align of ['intersect', 'step']) {
      const l = await line(spec({ A: 'if(close[AAA] > 101, close[CCC], close[AAA])' }, { align } as Partial<TimeseriesSpec>));
      assert.deepEqual(l.dates, DAYS, `${align}: the flags' dates`);
      close(l.values, want, `if under ${align}`, 0);
    }
  });

  // the Phase 1 review's 5: gs-quant's interpolate defaults to intersect, and its step walks the dates asked, each
  // carrying the last value among them -- an x date between two of them never read (but the first's, as of it)
  it('interpolate: as gs-quant\'s -- intersect unless told; step over the dates asked', async () => {
    const plain = await line(spec({ A: 'interpolate(close[CCC], close[AAA])' }));
    assert.deepEqual(plain.dates, own.map((i) => DAYS[i]!), 'intersect, gs-quant\'s default');
    // x: AAA at or above its median (filter drops those below), onto CCC's every third -- gs-quant's __interpolate_step
    // written out: from x's value as of the first date asked (its first, before it starts), each date asked its own
    // value, else the one before among them
    const m = [...AAA].sort((a, b) => a - b)[AAA.length / 2]!;
    const xs = DAYS.map((_, i) => i).filter((i) => AAA[i]! >= m);
    assert.ok(own.some((i) => !xs.includes(i)) && xs.some((i) => !own.includes(i)), 'each has dates the other lacks');
    const step = await line(spec({ A: `interpolate(filter(close[AAA], less_than, ${m}), close[CCC], step)` }));
    let current = AAA[xs.filter((j) => j <= own[0]!).at(-1) ?? xs[0]!]!;
    const want = own.map((i) => (xs.includes(i) ? (current = AAA[i]!) : current));
    assert.deepEqual(step.dates, own.map((i) => DAYS[i]!));
    close(step.values, want, 'step over the dates asked', 1e-12);
    // y begun later, on a date x has none of: x's value as of it, from x's own dates before it -- never the next date
    // asked's (x here the closes at or below the median: filter drops those above)
    const lows = DAYS.map((_, i) => i).filter((i) => !(AAA[i]! > m));
    const starts = own.map((i) => CCC[i]!).map((c) => ({ c, ys: own.filter((i) => CCC[i]! >= c) }))
      .filter(({ ys }) => !lows.includes(ys[0]!) && lows.some((j) => j < ys[0]!));
    const start = starts.find(({ ys }) => AAA[lows.filter((j) => j <= ys[0]!).at(-1)!] !== AAA[ys.find((i) => lows.includes(i)) ?? -1]);
    assert.ok(start !== undefined, 'a start of y x has none of, its value as of it not the next asked one\'s');
    const late = await line(spec({ A: `interpolate(filter(close[AAA], greater_than, ${m}), filter(close[CCC], less_than, ${start.c}), step)` }));
    let carried = AAA[lows.filter((j) => j <= start.ys[0]!).at(-1)!]!;
    assert.deepEqual(late.dates, start.ys.map((i) => DAYS[i]!));
    close(late.values, start.ys.map((i) => (lows.includes(i) ? (carried = AAA[i]!) : carried)), 'from x as of y\'s first', 1e-12);
  });

  it('interpolate: x onto the dates of y, by method', async () => {
    const onto = async (method: string): Promise<{ dates: string[]; values: Line }> => line(spec({ A: `interpolate(close[CCC], close[AAA], ${method})` }));
    const step = await onto('step');
    assert.deepEqual(step.dates, DAYS);
    close(step.values, DAYS.map((_, i) => CCC[before(i)]!), 'step', 1e-9);
    close((await onto('nan')).values, DAYS.map((_, i) => (i % 3 === 0 ? CCC[i]! : null)), 'nan', 1e-9);
    close((await onto('zero')).values, DAYS.map((_, i) => (i % 3 === 0 ? CCC[i]! : 0)), 'zero', 1e-9);
    const both = await onto('intersect');
    assert.deepEqual(both.dates, own.map((i) => DAYS[i]!));
  });
});

// -- gs-quant's remaining functions (the next phases' 1, 2026-10-26), each held to its definition written out --

/** statsmodels' seasonal_decompose (two-sided, extrapolate_trend 0), as gs-quant's trend and seasonally_adjusted read it. */
function decompose(x: number[], p: number, multiplicative: boolean): { trend: Line; adjusted: Line } {
  const k = Math.floor(p / 2);
  const filt = p % 2 === 1 ? Array.from({ length: p }, () => 1 / p) : [0.5 / p, ...Array.from({ length: p - 1 }, () => 1 / p), 0.5 / p];
  const trend: Line = x.map((_, t) => (t < k || t >= x.length - k ? null : filt.reduce((acc, f, j) => acc + f * x[t - k + j]!, 0)));
  const detrended = x.map((v, t) => (trend[t] === null ? null : multiplicative ? v / trend[t]! : v - trend[t]!));
  const places = Array.from({ length: p }, (_, i) => {
    const vals = detrended.filter((d, t) => t % p === i && d !== null) as number[];
    return vals.reduce((a, b) => a + b, 0) / vals.length;
  });
  const mean = places.reduce((a, b) => a + b, 0) / p;
  const season = places.map((v) => (multiplicative ? v / mean : v - mean));
  return { trend, adjusted: x.map((v, t) => (trend[t] === null ? null : multiplicative ? v / season[t % p]! : v - season[t % p]!)) };
}

describe('gs-quant\'s remaining functions, each as gs-quant defines it', () => {
  const weekOf = (d: string): string => {
    const at = new Date(`${d}T00:00:00Z`);
    return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
  };

  it('the date parts: day, month, year, quarter, weekday (0 a Monday)', async () => {
    const parts = (fn: string): Promise<Line> => line(spec({ A: `${fn}(close[AAA])` })).then((l) => l.values);
    const date = (d: string): Date => new Date(`${d}T00:00:00Z`);
    assert.deepEqual(await parts('day'), DAYS.map((d) => date(d).getUTCDate()));
    assert.deepEqual(await parts('month'), DAYS.map((d) => date(d).getUTCMonth() + 1));
    assert.deepEqual(await parts('year'), DAYS.map((d) => date(d).getUTCFullYear()));
    assert.deepEqual(await parts('quarter'), DAYS.map((d) => Math.floor(date(d).getUTCMonth() / 3) + 1));
    assert.deepEqual(await parts('weekday'), DAYS.map((d) => (date(d).getUTCDay() + 6) % 7));
  });

  it('if, not and compare: a branch by a condition, its opposite, which is greater', async () => {
    close((await line(spec({ A: 'if(close[AAA] > 2 * close[BBB], close[AAA], 0)' }))).values,
      AAA.map((v, i) => (v > 2 * BBB[i]! ? v : 0)), 'if', 1e-12);
    close((await line(spec({ A: 'not(close[AAA] > 101)' }))).values, AAA.map((v) => (v > 101 ? 0 : 1)), 'not', 0);
    close((await line(spec({ A: 'compare(close[AAA], 2 * close[BBB])' }))).values,
      AAA.map((v, i) => Math.sign(v - 2 * BBB[i]!)), 'compare', 0);
  });

  it('filter: the values it matches dropped -- before a window, the window reads the dates left; after one, the window read them all', async () => {
    const kept = AAA.map((v, i) => [v, i] as const).filter(([v]) => !(v > 102));
    const l = await line(spec({ A: 'filter(close[AAA], greater_than, 102)' }));
    assert.deepEqual(l.dates, kept.map(([, i]) => DAYS[i]!));
    close(l.values, kept.map(([v]) => v), 'filtered', 0);
    const meanOf = await line(spec({ A: 'mean(filter(close[AAA], greater_than, 102), 3)' }));
    close(meanOf.values, rolling(kept.map(([v]) => v), 3, avg), 'a window over the dates left', 1e-9);
    const m3 = rolling(AAA, 3, avg);
    const after = await line(spec({ A: 'filter(mean(close[AAA], 3), greater_than, 102)' }));
    close(after.values, m3.filter((v) => !(v !== null && v > 102)), 'a window read before', 1e-9);
    const empty = await line(spec({ A: 'filter(lag(close[AAA], 2))' }));
    assert.deepEqual(empty.dates, DAYS.slice(2), 'empty: the dates of none dropped');
    // every other operator, each a value of timeseries::FilterOperator (the enum round's review, 2026-10-28)
    const at = AAA[7]!;
    const ops: [string, (v: number) => boolean][] = [['less_than', (v) => v < at], ['l_equals', (v) => v <= at],
      ['g_equals', (v) => v >= at], ['equals', (v) => v === at], ['not_equals', (v) => v !== at]];
    for (const [op, drops] of ops) {
      const left = await line(spec({ A: `filter(close[AAA], ${op}, ${at})` }));
      assert.deepEqual(left.dates, DAYS.filter((_, i) => !drops(AAA[i]!)), op);
    }
  });

  it('product, geometrically_aggregate and weighted_sum', async () => {
    close((await line(spec({ A: 'product(close[AAA] / 100, 5)' }))).values,
      rolling(AAA.map((v) => v / 100), 5, (v) => v.reduce((a, b) => a * b, 1)), 'product of 5', 1e-9);
    close((await line(spec({ A: 'geometrically_aggregate(returns(close[AAA]))' }))).values,
      AAA.map((v, i) => (i === 0 ? null : v / AAA[0]! - 1)), 'compounded', 1e-9);
    close((await line(spec({ A: 'weighted_sum(close[AAA], 0.6, close[BBB], 0.4)' }))).values,
      AAA.map((v, i) => 0.6 * v + 0.4 * BBB[i]!), 'weighted', 1e-9);
  });

  // the Phase 1 review's 6: a neighbour of none is no spike's (gs-quant's comparison with NaN is never true): kept
  it('smooth_spikes beside none: kept, the first and last dates alone dropped', async () => {
    const r = simple(AAA);
    const l = await line(spec({ A: 'smooth_spikes(returns(close[AAA]), 0.1)' }));
    close(l.values, r.map((v, i) => {
      if (i === 0 || i === r.length - 1 || v === null) return null;
      const [p, q] = [r[i - 1], r[i + 1]];
      if (p === null || q === null) return v;
      const beyond = (a: number, b: number): boolean => a > b * 1.1;
      return (beyond(v, p!) && beyond(v, q!)) || (beyond(p!, v) && beyond(q!, v)) ? (p! + q!) / 2 : v;
    }), 'beside none', 1e-12);
  });

  it('a series where a number is: its argument named by its place', () => {
    const s = spec({ A: 'weighted_sum(close[AAA], 0.5, 3, 0.5)' });
    assert.throws(() => seriesQuery(inputs, s, s.series[0]!), /its third argument is a number/);
  });

  it('compare beside none: 0, as gs-quant\'s (x > y) - (x < y); filter not_equals drops the dates of none too', async () => {
    const c = await line(spec({ A: 'compare(close[AAA], close[CCC] * 5)' }, { align: 'nan' } as Partial<TimeseriesSpec>));
    close(c.values, AAA.map((v, i) => (i % 3 === 0 ? Math.sign(v - CCC[i]! * 5) : 0)), 'compare', 0);
    const f = await line(spec({ A: `filter(lag(close[AAA], 2), not_equals, ${AAA[0]})` }));
    assert.deepEqual(f.dates, [DAYS[2]!], 'the value equal to it alone: none is not equal to it');
  });

  it('smooth_spikes: a value beyond both neighbours their mean, the ends dropped', async () => {
    for (const [args, beyond] of [['0.004', (a: number, b: number) => a > b * 1.004], ['0.4, absolute', (a: number, b: number) => a > b + 0.4]] as const) {
      const l = await line(spec({ A: `smooth_spikes(close[AAA], ${args})` }));
      close(l.values, AAA.map((v, i) => {
        if (i === 0 || i === AAA.length - 1) return null;
        const [p, q] = [AAA[i - 1]!, AAA[i + 1]!];
        return (beyond(v, p) && beyond(v, q)) || (beyond(p, v) && beyond(q, v)) ? (p + q) / 2 : v;
      }), `spikes ${args}`, 1e-9);
    }
  });

  it('excess_returns: over a cash index, compounded; over a rate, accrued actual/360', async () => {
    const ra = simple(AAA);
    const rb = simple(BBB);
    let e = AAA[0]!;
    close((await line(spec({ A: 'excess_returns(close[AAA], close[BBB])' }))).values,
      AAA.map((_, i) => (i === 0 ? e : (e *= 1 + ra[i]! - rb[i]!))), 'over an index', 1e-9);
    const day = (d: string): number => Date.parse(`${d}T00:00:00Z`) / 86_400_000;
    let er = AAA[0]!;
    close((await line(spec({ A: 'excess_returns(close[AAA], 0.05)' }))).values,
      AAA.map((v, i) => (i === 0 ? er : (er = er + v - AAA[i - 1]! * (1 + 0.05 * (day(DAYS[i]!) - day(DAYS[i - 1]!)) / 360)))), 'over a rate', 1e-9);
  });

  it('vol_swap_volatility: the root mean square of log returns, annualized by 252, in percent', async () => {
    const lr = AAA.map((v, i) => (i === 0 ? null : Math.log(v / AAA[i - 1]!)));
    close((await line(spec({ A: 'vol_swap_volatility(close[AAA], 10)' }))).values, lr.map((_, i) => {
      if (i < 9) return null;
      const w = lr.slice(i - 9, i + 1).filter((v) => v !== null) as number[];
      return Math.sqrt(w.reduce((a, b) => a + b * b, 0) / w.length) * Math.sqrt(252) * 100;
    }), 'vol swap', 1e-9);
  });

  it('consecutive: the days in a row up (down), 0 where it breaks', async () => {
    const r = simple(AAA);
    for (const dir of ['positive', 'negative'] as const) {
      let run = 0;
      close((await line(spec({ A: `consecutive(returns(close[AAA]), ${dir})` }))).values,
        r.map((v) => (run = v !== null && (dir === 'positive' ? v > 0 : v < 0) ? run + 1 : 0)), dir, 0);
    }
  });

  it('first, last and count: its first and last value on every date it has, and how many to date', async () => {
    const lagged2 = lagged(AAA, 2);
    // on every date, those with none too: gs-quant's fill its index (the Phase 1 review's 6)
    close((await line(spec({ A: 'first(lag(close[AAA], 2))' }))).values, lagged2.map(() => AAA[0]!), 'first', 0);
    close((await line(spec({ A: 'day(lag(close[AAA], 2))' }))).values, DAYS.map((d) => Number(d.slice(8))), 'a date part of none', 0);
    close((await line(spec({ A: 'last(close[AAA])' }))).values, AAA.map(() => AAA.at(-1)!), 'last', 0);
    close((await line(spec({ A: 'count(lag(close[AAA], 2))' }))).values, lagged2.map((_, i) => Math.max(0, i - 1)), 'count', 0);
  });

  it('union, append and prepend: two series spliced on every date either has', async () => {
    const own = DAYS.map((_, i) => i).filter((i) => i % 3 === 0);
    close((await line(spec({ A: 'union(close[CCC], close[AAA])' }))).values, DAYS.map((_, i) => (i % 3 === 0 ? CCC[i]! : AAA[i]!)), 'union', 0);
    // CCC's dates to its last value at or below 22.5, then AAA's after it -- gs-quant's dates alone: the other's in
    // between none of them (the Phase 1 review's 2: kept as empty rows, they broke every function after it)
    const low = own.filter((i) => CCC[i]! <= 22.5);
    const lastLow = low.at(-1)!;
    const appended = [...low, ...DAYS.map((_, i) => i).filter((i) => i > lastLow)];
    const ap = await line(spec({ A: 'append(filter(close[CCC], greater_than, 22.5), close[AAA])' }));
    assert.deepEqual(ap.dates, appended.map((i) => DAYS[i]!));
    const spliced = appended.map((i) => (i <= lastLow ? CCC[i]! : AAA[i]!));
    close(ap.values, spliced, 'append', 0);
    close((await line(spec({ A: 'returns(append(filter(close[CCC], greater_than, 22.5), close[AAA]))' }))).values,
      simple(spliced), 'a return of the spliced series, date to date', 1e-12);
    // CCC's dates before AAA's first close at or above 101, then AAA's
    const firstHigh = AAA.findIndex((v) => v >= 101);
    const l = await line(spec({ A: 'prepend(close[CCC], filter(close[AAA], less_than, 101))' }));
    const dates = DAYS.map((_, i) => i).filter((i) => (i % 3 === 0 && i < firstHigh) || AAA[i]! >= 101);
    assert.deepEqual(l.dates, dates.map((i) => DAYS[i]!));
    close(l.values, dates.map((i) => (AAA[i]! >= 101 ? AAA[i]! : CCC[i]!)), 'prepend', 0);
  });

  it('trend and seasonally_adjusted: statsmodels\' classical decomposition over a cycle of the tile\'s frequency', async () => {
    // a cycle of 4 weeks (even: its ends halved), additive and multiplicative
    const weeks = [...new Set(DAYS.map(weekOf))];
    const last = weeks.map((w) => AAA[DAYS.map(weekOf).lastIndexOf(w)]!);
    for (const method of ['additive', 'multiplicative'] as const) {
      const want = decompose(last, 4, method === 'multiplicative');
      close((await line(spec({ A: `trend(close[AAA], ${method}, month)` }, { grain: 'week' }))).values, want.trend, `trend ${method}`, 1e-9);
      close((await line(spec({ A: `seasonally_adjusted(close[AAA], ${method}, month)` }, { grain: 'week' }))).values, want.adjusted, `adjusted ${method}`, 1e-9);
    }
    await assert.rejects(line(spec({ A: 'trend(close[AAA])' })), /give the tile a frequency/);
    await assert.rejects(line(spec({ A: 'trend(close[AAA], additive, week)' }, { grain: 'week' })), /a week is no cycle of weeks/);
    // each the language's of the same name, its cycle counted in SAMPLE BY's weeks
    for (const text of ['trend(close[AAA], additive, month)', 'seasonally_adjusted(close[AAA], additive, month)']) {
      const ts = spec({ A: text }, { grain: 'week' });
      const island = islandText(seriesQuery(inputs, ts, ts.series[0]!).query);
      assert.ok(island.includes(`${text.slice(0, text.indexOf('('))}(`) && island.includes('SAMPLE BY 1w'), island);
    }
  });
});

describe('correlations: each pair of lines over the dates shown', () => {
  it('two levels\' returns, over the dates both have shown; a level with a return its returns with the return', async () => {
    const ts = spec({ A: 'close[AAA]', B: 'close[BBB]', C: 'returns(close[CCC])' });
    const [A, B, C] = ts.series;
    const view = { from: DAYS[5]!, to: DAYS[34]! };
    const t = await run(correlationsQuery(inputs, ts, A!, B!, view));
    const shownA = AAA.slice(5, 35);
    const shownB = BBB.slice(5, 35);
    const ra = shownA.slice(1).map((x, i) => x / shownA[i]! - 1);
    const rb = shownB.slice(1).map((x, i) => x / shownB[i]! - 1);
    close([num(values(t, TS_CORR)[0])], [cov(ra, rb) / (sd(ra) * sd(rb))], 'AAA ~ BBB', 1e-9);
    assert.equal(Number(values(t, TS_CORR_N)[0]), ra.length);
    // CCC trades every third day: its returns over its own dates, the AAA returns on those dates beside them
    const ct = await run(correlationsQuery(inputs, ts, A!, C!, view));
    const cIdx = DAYS.map((_, i) => i).filter((i) => i % 3 === 0);
    const cR = new Map(cIdx.slice(1).map((d, k) => [d, CCC[d]! / CCC[cIdx[k]!]! - 1] as const));
    const both = DAYS.map((_, i) => i).filter((i) => i > 5 && i <= 34 && cR.has(i));
    const x = both.map((i) => AAA[i]! / AAA[i - 1]! - 1);
    const z = both.map((i) => cR.get(i)!);
    close([num(values(ct, TS_CORR)[0])], [cov(x, z) / (sd(x) * sd(z))], 'AAA ~ returns(CCC)', 1e-9);
    // counted over the dates both have (the E9 review's 11): CCC a level, its returns taken over its dates shown, its
    // first shown none -- the AAA return of that date not counted
    const D = spec({ A: 'close[AAA]', D: 'close[CCC]' });
    const dt = await run(correlationsQuery(inputs, D, D.series[0]!, D.series[1]!, view));
    assert.equal(Number(values(dt, TS_CORR_N)[0]), cIdx.filter((i) => i >= 5 && i <= 34).length - 1);
  });

  it('a split series\' lines against each other, each pair once', async () => {
    const ts = spec({ A: 'close' }, {}, 'symbol');
    const t = await run(correlationsQuery(inputs, ts, ts.series[0]!, ts.series[0]!));
    const pairs = values(t, TS_CORR_A).map((a, i) => `${String(a)}~${String(values(t, TS_CORR_B)[i])}`);
    // each with itself too, the diagonal the database's (the E9 review's 13)
    assert.deepEqual([...pairs].sort(), ['AAA~AAA', 'AAA~BBB', 'AAA~CCC', 'BBB~BBB', 'BBB~CCC', 'CCC~CCC']);
    close(['AAA~AAA', 'BBB~BBB', 'CCC~CCC'].map((p) => num(values(t, TS_CORR)[pairs.indexOf(p)])), [1, 1, 1], 'each line with itself', 1e-9);
    const ab = values(t, TS_CORR)[pairs.indexOf('AAA~BBB')];
    const at = values(t, TS_CORR_A).findIndex((a, i) => a === 'AAA' && values(t, TS_CORR_B)[i] === 'BBB');
    const ra = simple(AAA).slice(1) as number[];
    const rb = simple(BBB).slice(1) as number[];
    close([num(values(t, TS_CORR)[at])], [cov(ra, rb) / (sd(ra) * sd(rb))], 'AAA ~ BBB, split', 1e-9);
    void ab;
  });

  // the E9 review's 13: the diagonal the database's -- 1 where a line varies, none where it does not
  it('a series with itself: 1 where it varies, none where it is constant', async () => {
    const ts = spec({ A: 'close[AAA]', B: 'close[AAA] - close[AAA]' });
    const [A, B] = ts.series;
    close([num(values(await run(correlationsQuery(inputs, ts, A!, A!)), TS_CORR)[0])], [1], 'AAA with itself', 1e-9);
    assert.deepEqual(values(await run(correlationsQuery(inputs, ts, B!, B!)), TS_CORR).map(num), [null], 'a constant has no correlation');
  });
});

describe('conditions: a comparison, and two joined, each date\'s run where it holds', () => {
  it('close above its 5-day mean: 1 or 0 a date, none before its mean; each date that holds its run\'s first and last', async () => {
    const ts = spec({ A: 'close[AAA] > mean(close[AAA], 5)' });
    const q = seriesQuery(inputs, ts, ts.series[0]!);
    assert.equal(q.shade, true);
    assert.equal(q.level, false);
    const t = await run(q);
    const mean5 = rolling(AAA, 5, avg);
    const flag: Line = AAA.map((v, i) => (mean5[i] === null ? null : v > mean5[i]! ? 1 : 0));
    assert.deepEqual(values(t, TS_VALUE).map(num).reverse(), flag);
    // the runs, as plain arithmetic: a run of the same flag, from its first date to its last
    const runOf = flag.map((f, i) => {
      if (f !== 1) return null;
      let a = i;
      while (a > 0 && flag[a - 1] === 1) a--;
      let b = i;
      while (b < flag.length - 1 && flag[b + 1] === 1) b++;
      // shaded to the next date, from which it no longer holds; to its own last at the line's end (the E9 review's 4)
      return [DAYS[a]!, DAYS[Math.min(b + 1, DAYS.length - 1)]!] as const;
    });
    const day = (n: string): (string | null)[] => values(t, n).map((d) => (d === null ? null : String(d).slice(0, 10))).reverse();
    assert.deepEqual(day(TS_FROM), runOf.map((r) => r?.[0] ?? null), 'each run\'s first date');
    assert.deepEqual(day(TS_TO), runOf.map((r) => r?.[1] ?? null), 'and the date after its last');
    assert.ok(runOf.every((r) => r === null || r[0] < r[1] || r[1] === DAYS.at(-1)), 'every run of a width, but one of the last date alone');
    assert.ok(new Set(runOf.filter((r) => r !== null).map((r) => r![0])).size >= 2, 'more than one run in the fixture');
  });

  it('two joined: and, or -- and a comparison read as 1 and 0 by arithmetic', async () => {
    const both = await line(spec({ A: 'close[AAA] > 101 and close[BBB] < 50.5' }));
    close(both.values, AAA.map((v, i) => (v > 101 && BBB[i]! < 50.5 ? 1 : 0)), 'and', 0);
    const either = await line(spec({ A: 'close[AAA] <= 100 or close[BBB] >= 51' }));
    close(either.values, AAA.map((v, i) => (v <= 100 || BBB[i]! >= 51 ? 1 : 0)), 'or', 0);
    const signal = await line(spec({ A: '(returns(close[AAA]) > 0) * returns(close[BBB])' }));
    const ra = simple(AAA);
    const rb = simple(BBB);
    close(signal.values, ra.map((r, i) => (r === null ? null : (r > 0 ? 1 : 0) * rb[i]!)), 'a signal times a return', 1e-12);
    assert.equal(seriesQuery(inputs, spec({ A: '(close[AAA] > 100) * 2' }), spec({ A: '(close[AAA] > 100) * 2' }).series[0]!).shade, undefined, 'no condition, drawn as a line');
  });
});

describe('a window by its rows: percentile ranks, medians and percentiles', () => {
  it('percentiles: each value\'s rank among its window\'s, 0 to 100 -- over 10, from the start, over 2w', async () => {
    close((await line(spec({ A: 'percentiles(close[AAA], 10)' }))).values,
      AAA.map((v, i) => (i < 9 ? null : rankIn(AAA.slice(i - 9, i + 1), v))), 'over 10', 1e-12);
    close((await line(spec({ A: 'percentiles(close[AAA])' }))).values, AAA.map((v, i) => rankIn(AAA.slice(0, i + 1), v)), 'from the start', 1e-12);
    const start = onBy(DAYS[0]!, 2, 'w');
    close((await line(spec({ A: 'percentiles(close[AAA], 2w)' }))).values, DAYS.map((d, i) => (d < start ? null
      : rankIn(AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d), AAA[i]!))), 'over 2w', 1e-12);
    // a constant series: every value equal to every other, each the middle of its window
    close((await line(spec({ A: 'percentiles(close[AAA] * 0, 5)' }))).values, AAA.map((_, i) => (i < 4 ? null : 50)), 'all equal', 1e-12);
  });

  it('median and percentile: pandas\' quantile over each window, a percentile of the whole series a line', async () => {
    close((await line(spec({ A: 'median(close[AAA], 5)' }))).values, AAA.map((_, i) => (i < 4 ? null : quantile(AAA.slice(i - 4, i + 1), 0.5))), 'median of 5', 1e-12);
    close((await line(spec({ A: 'median(close[AAA], 4)' }))).values, AAA.map((_, i) => (i < 3 ? null : quantile(AAA.slice(i - 3, i + 1), 0.5))), 'median of 4: the two middle ones\' mean', 1e-12);
    close((await line(spec({ A: 'percentile(close[AAA], 90, 10)' }))).values, AAA.map((_, i) => (i < 9 ? null : quantile(AAA.slice(i - 9, i + 1), 0.9))), '90th of 10', 1e-12);
    close((await line(spec({ A: 'percentile(close[AAA], 25)' }))).values, AAA.map(() => quantile(AAA, 0.25)), 'the 25th of every one', 1e-12);
    // of every value: one aggregate a line joined back (the library's quantileOfLine), never each row joined to every
    // other (the E9 review's 5)
    // (the builder's call of it is the platform's: core's TsFunctions writes quantileOfLine for a whole line)
    const body = DATACUBE_LIBRARY.slice(DATACUBE_LIBRARY.indexOf('function timeseries::quantileOfLine'));
    assert.equal((body.slice(0, body.indexOf('\n}\n')).match(/->join\(/g) ?? []).length, 1, 'one join: the line\'s figure');
    // over a calendar period: the values after two weeks back, to this one, from the first date two weeks on
    const start = onBy(DAYS[0]!, 2, 'w');
    close((await line(spec({ A: 'median(close[AAA], 2w)' }))).values, DAYS.map((d) => (d < start ? null
      : quantile(AAA.filter((_, j) => DAYS[j]! > backBy(d, 2, 'w') && DAYS[j]! <= d), 0.5))), 'median over 2w', 1e-12);
  });
});

describe('exponential averages: gs-quant\'s recursions, the database\'s closed form', () => {
  const N = 700;
  const LONG = Array.from({ length: N }, (_, i) => round(100 * Math.exp(0.002 * i + 0.05 * Math.sin(i / 7) + 0.03 * Math.cos(i / 3.1))));
  const LONG_DAYS = Array.from({ length: N }, (_, i) => new Date(Date.UTC(2021, 0, 1) + i * 86_400_000).toISOString().slice(0, 10));
  let long: TsInputs;
  let own: LitePlanner;
  before(async () => {
    const csv = ['symbol,day,close', ...LONG_DAYS.map((d, i) => `L,${d},${LONG[i]}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'ewm-long.csv'), writer, { table: 'ewm_long' });
    own = plannerFor(r.model, r.runtime);
    long = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
  });
  const longLine = async (text: string): Promise<Line> => {
    const ts = spec({ A: text });
    const t = await engine.execute(await own.plan(seriesQuery(long, ts, ts.series[0]!).query), 0);
    return values(t, TS_VALUE).map(num).reverse();
  };

  it('exponential_moving_average: y0 = x0, y = beta·y[-1] + (1 - beta)·x, to its last bits over many blocks', async () => {
    close(await longLine('exponential_moving_average(close, 0.75)'), ema(LONG, 0.75), 'beta 0.75', 1e-12);
    close(await longLine('ema(close, 0.97)'), ema(LONG, 0.97), 'beta 0.97', 1e-12);
    close((await line(spec({ A: 'exponential_moving_average(close[AAA], 0.1)' }))).values, ema(AAA, 0.1), 'beta 0.1, three blocks of 40', 1e-12);
    close((await line(spec({ A: 'exponential_moving_average(close[AAA], 0)' }))).values, AAA, 'beta 0: the series', 0);
  });

  it('exponential_std, pandas\' bias-corrected; exponential_volatility of the returns, annualized, in percent', async () => {
    close(await longLine('exponential_std(close)'), ewmStd(LONG, 0.75), 'std, beta 0.75', 1e-9);
    const r = LONG.slice(1).map((v, i) => v / LONG[i]! - 1);
    // a day apart, every day: 252 a year as the database infers it from the dates (gs-quant's buckets: under 2.1 days)
    close(await longLine('exponential_volatility(close, 0.9)'), [null, ...ewmStd(r, 0.9).map((v) => (v === null ? null : v * Math.sqrt(252) * 100))], 'volatility', 1e-9);
    const d = LONG.slice(1).map((v, i) => v - LONG[i]!);
    close(await longLine('exponential_spread_volatility(close)'), [null, ...ewmStd(d, 0.75).map((v) => (v === null ? null : v * Math.sqrt(252)))], 'spread volatility', 1e-9);
  });

  it('macd: the 12-span average less the 26-span one, itself averaged over s', async () => {
    const span = (x: number[], k: number): number[] => ema(x, (k - 1) / (k + 1));
    const raw = span(LONG, 12).map((v, i) => v - span(LONG, 26)[i]!);
    close(await longLine('macd(close)'), raw, 'macd(12, 26)', 1e-10);
    close(await longLine('macd(close, 12, 26, 9)'), span(raw, 9), 'macd(12, 26, 9)', 1e-10);
  });

  it('smoothed_moving_average: Wilder\'s, from the mean of w', async () => {
    close(await longLine('smoothed_moving_average(close, 14)'), smma(LONG, 14), 'smma 14', 1e-10);
    // of one: each value itself, from the first after the first -- gs-quant's ramp of 1 (the E9 review's nit)
    close(await longLine('smoothed_moving_average(close, 1)'), smma(LONG, 1), 'smma 1', 0);
  });

  // the E9 review's weak tests: an average per line of a split, its blocks over each line's own values, and at a frequency
  it('a line per instrument, each its own average; and at a weekly frequency, of each week\'s last close', async () => {
    const l = await line(spec({ A: 'exponential_moving_average(close, 0.1)' }, {}, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    close(of('AAA'), ema(AAA, 0.1), 'AAA', 1e-12);
    close(of('BBB'), ema(BBB, 0.1), 'BBB', 1e-12);
    close(of('CCC'), ema(CCC.filter((_, i) => i % 3 === 0), 0.1), 'CCC, on its own dates', 1e-12);
    const weekOf = (d: string): string => {
      const at = new Date(`${d}T00:00:00Z`);
      return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(DAYS.map(weekOf))];
    const last = weeks.map((w) => AAA[DAYS.map(weekOf).lastIndexOf(w)]!);
    close((await line(spec({ A: 'exponential_moving_average(close[AAA], 0.1)' }, { grain: 'week' }))).values, ema(last, 0.1), 'weekly', 1e-12);
  });

  // the next phases' 1: an odd cycle, a quarter of months (3), its moving average centred whole
  // the next phases' 3: a seasonality and a calendar of returns, over 700 days
  it('seasonality: each year along its months, the years before\'s least, most and mean at each; each year from its start', async () => {
    const month = (d: string): string => d.slice(0, 7);
    const months = [...new Set(LONG_DAYS.map(month))];
    const lastOf = (m: string): number => LONG[LONG_DAYS.map(month).lastIndexOf(m)]!;
    const ts = spec({ A: 'close' }, { grain: 'month' });
    const sea = async (over: TimeseriesSpec, years: number): Promise<ResultTable> => engine.execute(await own.plan(seasonalityQuery(long, over, over.series[0]!, years).query), 0);
    const t = await sea(ts, 1);
    assert.deepEqual(values(t, TS_SEA_YEAR).map(num), months.map((m) => Number(m.slice(0, 4))));
    assert.deepEqual(values(t, TS_SEA_PLACE).map(num), months.map((m) => Number(m.slice(5, 7))));
    close(values(t, TS_VALUE).map(num), months.map(lastOf), 'each month\'s last', 0);
    // the years before the latest (2022): 2021 alone -- its value at each month the least, the most and the mean
    const before = (m: string): number => lastOf(`2021-${m.slice(5, 7)}`);
    for (const col_ of [TS_SEA_LOW, TS_SEA_HIGH, TS_SEA_MEAN]) close(values(t, col_).map(num), months.map(before), col_, 1e-12);
    assert.ok(values(t, TS_SEA_LATEST).every((y) => num(y) === 2022));
    // compare rebase: each year 100 at its first month
    const r = await sea({ ...ts, view: { compare: 'rebase' } }, 1);
    const firstOf = (y: string): number => lastOf(months.find((m) => m.startsWith(y))!);
    close(values(r, TS_VALUE).map(num), months.map((m) => (lastOf(m) / firstOf(m.slice(0, 4))) * 100), 'each year from its start', 1e-9);
    // compare percent: each year its first's x / x0 - 1 (gs-quant's percent_change); the range's span its most less its least
    const p = await sea({ ...ts, view: { compare: 'percent' } }, 1);
    close(values(p, TS_VALUE).map(num), months.map((m) => lastOf(m) / firstOf(m.slice(0, 4)) - 1), 'each year from its start, % change', 1e-12);
    close(values(p, TS_SEA_SPAN).map(num), values(p, TS_SEA_HIGH).map((h, i) => {
      const l = num(values(p, TS_SEA_LOW)[i]);
      return h === null || l === null ? null : num(h)! - l;
    }), 'the span', 1e-12);
  });

  // the Phase 3 review's 2: a daily seasonality placed each date by its day of the year -- weekends and holidays apart
  // from year to year, so each place held some of the years before, and a leap year a day off
  it('a daily seasonality: by the week of the year, each year its week\'s last value -- every year before at every place', async () => {
    const ts = spec({ A: 'close' });
    const t = await engine.execute(await own.plan(seasonalityQuery(long, ts, ts.series[0]!, 1).query), 0);
    const weekOf = (d: string): number => {
      const day = (Date.parse(`${d}T00:00:00Z`) - Date.UTC(Number(d.slice(0, 4)), 0, 1)) / 86_400_000 + 1;
      return Math.floor((day - 1) / 7) + 1;
    };
    const lastOf = new Map<string, number>();
    LONG_DAYS.forEach((d, i) => lastOf.set(`${d.slice(0, 4)}|${weekOf(d)}`, LONG[i]!));
    const keys = values(t, TS_SEA_YEAR).map((y, i) => `${num(y)}|${num(values(t, TS_SEA_PLACE)[i])}`);
    assert.deepEqual(keys, [...new Set(keys)], 'a year at a place once');
    assert.deepEqual(keys, [...lastOf.keys()], 'every week each year has, in order');
    close(values(t, TS_VALUE).map(num), keys.map((k) => lastOf.get(k)!), 'each week\'s last', 0);
    // 2022 against 2021, at every week 2022 has: 2021's own
    const late = keys.map((k, i) => [k, i] as const).filter(([k]) => k.startsWith('2022'));
    close(late.map(([, i]) => num(values(t, TS_SEA_LOW)[i])), late.map(([k]) => lastOf.get(`2021|${k.split('|')[1]}`)!), 'the year before at each', 1e-12);
  });

  // the user, 2026-10-28 ("so that these functions are usable from platform without TS"): the tile's tables and views
  // are the library's by name -- a person's own Pure over the table answers as the tile does, every figure
  it('a person\'s own Pure: the statistics, a seasonality and a projection by name, as the tile\'s', async () => {
    const source = (await own.print(lambda([], long.get('px')!.snapshot.source.query))).trim().replace(/^\|/, '');
    const ask = async (calls: string): Promise<ResultTable> => engine.execute(await own.plan(await own.parse(
      `|${source}->extend(~t:x|dateDiff(%1970-01-01, $x.day->toOne(), meta::pure::functions::date::DurationUnit.DAYS))${calls}`)), 0);
    const tile = async (q: { query: Parameters<LitePlanner['plan']>[0] }): Promise<ResultTable> => engine.execute(await own.plan(q.query), 0);
    const ts = spec({ A: 'close' });
    const A = ts.series[0]!;
    const same = (mine: ResultTable, theirs: ResultTable, pairs: readonly (readonly [string, string])[], tol: number): void => {
      for (const [m, c] of pairs) {
        const want = values(theirs, c);
        assert.ok(want.length > 0, `${c} answered`);
        if (typeof want.find((v) => v !== null) === 'number') close(values(mine, m).map(num), want.map(num), c, tol);
        else assert.deepEqual(values(mine, m).map((v) => (v === null ? null : String(v))), want.map((v) => (v === null ? null : String(v))), c);
      }
    };
    // the line's own figures
    same(await ask('->timeseries::summaryStatistics(~symbol, ~day, ~t, ~close, ~close, ~close, ~n, ~d0, ~d1, ~f, ~l, ~lo, ~la, ~hi, ~ha, ~m, ~s, ~dy, ~ch, ~pc)'),
      await tile(statsQuery(long, ts, A)), [['n', statName('count')], ['d0', statName('from')], ['d1', statName('to')], ['f', statName('first')], ['l', statName('last')],
        ['lo', statName('low')], ['la', statName('lowAt')], ['hi', statName('high')], ['ha', statName('highAt')], ['m', statName('mean')], ['s', statName('std')],
        ['dy', statName('days')], ['ch', statName('change')], ['pc', statName('pct')]], 1e-12);
    // a seasonality: by the week of the year, each year its week's last; and each year from its first value
    const seaCols = [['y', TS_SEA_YEAR], ['p', TS_SEA_PLACE], ['v', TS_VALUE], ['lo', TS_SEA_LOW], ['hi', TS_SEA_HIGH], ['mn', TS_SEA_MEAN], ['sp', TS_SEA_SPAN], ['ly', TS_SEA_LATEST]] as const;
    for (const compare of [undefined, 'rebase', 'percent'] as const) {
      same(await ask(`->timeseries::seasonality(~symbol, ~day, ~t, ~close, timeseries::Grain.day, timeseries::Compare.${compare ?? 'none'}, 1, ~y, ~p, ~v, ~lo, ~hi, ~mn, ~sp, ~ly)->sort([~y->ascending(), ~day->ascending()])`),
        await tile(seasonalityQuery(long, compare === undefined ? ts : { ...ts, view: { compare } }, A, 1)), seaCols, 1e-12);
    }
    // a projection: the line over its last 60 values, three months on
    same(await ask('->timeseries::projection(~symbol, ~day, ~t, ~close, 60, 3, meta::pure::functions::date::DurationUnit.MONTHS, meta::pure::functions::date::DurationUnit.DAYS, ~d0, ~v0, ~d1, ~v1, ~d2, ~v2, ~sl, ~k)'),
      await tile(projectionQuery(long, ts, { kind: 'projection', series: A.id, window: 60, ahead: '3m' })),
      [['d0', TS_PJ_FROM], ['v0', TS_PJ_FROM_VALUE], ['d1', TS_PJ_LAST], ['v1', TS_PJ_LAST_VALUE], ['d2', TS_PJ_END], ['v2', TS_PJ_END_VALUE], ['sl', TS_PJ_SLOPE], ['k', TS_PJ_N]], 1e-9);
    // an option it does not have is refused when the query compiles -- the type checker's, in its words -- never
    // written into the SQL, never read as another (AGENTS invariant 4; the second review's 2; the user, 2026-10-28: "I
    // hope it is the second", compile time): each a value of an enumeration of the library's
    const refused = async (calls: string, said: RegExp): Promise<void> => {
      await assert.rejects(own.plan(await own.parse(`|${source}${calls}`)), said);
    };
    await refused('->timeseries::seasonality(~symbol, ~day, ~symbol, ~close, timeseries::Grain.Day, timeseries::Compare.none, 1, ~y, ~p, ~v, ~lo, ~hi, ~mn, ~sp, ~ly)',
      /enumeration timeseries::Grain has no value 'Day'/);
    await refused('->timeseries::seasonality(~symbol, ~day, ~symbol, ~close, timeseries::Grain.day, timeseries::Compare.rebased, 1, ~y, ~p, ~v, ~lo, ~hi, ~mn, ~sp, ~ly)',
      /enumeration timeseries::Compare has no value 'rebased'/);
    await refused('->timeseries::periodIndex(~day, timeseries::Grain.months, ~i)', /enumeration timeseries::Grain has no value 'months'/);
    await refused("->timeseries::returns(over(~symbol, [~day->ascending()]), ~close, 1, timeseries::Returns.Log, ~r)", /enumeration timeseries::Returns has no value 'Log'/);
    // and a word where an option is: refused by its type, not compared
    await refused("->timeseries::returns(over(~symbol, [~day->ascending()]), ~close, 1, 'log', ~r)", /Returns/);
    // a valid one folds to the branch it names: the others never reach the SQL (a statistic of a calendar period: its
    // mean's AVG alone, no SUM, MAX, MIN or VAR beside it)
    const plan = JSON.stringify(await own.plan(await own.parse(`|${source}->timeseries::windowPeriod(~symbol, ~day, ~close, 1, meta::pure::functions::date::DurationUnit.WEEKS, timeseries::Statistic.mean, ~m)`)));
    assert.ok(plan.includes('AVG('), 'its mean');
    for (const other of ['SUM(', 'MAX(', 'MIN(', 'VAR_SAMP(', 'STDDEV_SAMP(', 'COUNT(']) assert.ok(!plan.includes(other), `no ${other} beside it`);
  });

  // a decomposition at a daily frequency: its place in the cycle each row's number (a cycle of business days), where it
  // starts counting moving no value to another place
  it('trend and seasonally_adjusted daily, over a week of five days: statsmodels\' decomposition', async () => {
    const ts = (text: string): TimeseriesSpec => spec({ A: text }, { grain: 'day' });
    const run = async (text: string): Promise<Line> => {
      const t = await engine.execute(await own.plan(seriesQuery(long, ts(text), ts(text).series[0]!).query), 0);
      return values(t, TS_VALUE).map(num).reverse();
    };
    for (const method of ['additive', 'multiplicative'] as const) {
      const want = decompose(LONG, 5, method === 'multiplicative');
      close(await run(`trend(close, ${method}, week)`), want.trend, `trend ${method}`, 1e-9);
      close(await run(`seasonally_adjusted(close, ${method}, week)`), want.adjusted, `adjusted ${method}`, 1e-9);
    }
  });

  // the Phase 3 review's 1: a month's return was from the last month present -- over a missing month, two months as one
  // -- and a quarterly tile's quarters were written under months
  it('a calendar over missing months: none after a gap, never two months as one; refused at a quarterly frequency', async () => {
    const kept = LONG_DAYS.map((d, i) => [d, LONG[i]!] as const).filter(([d]) => !d.startsWith('2021-03') && !d.startsWith('2021-12'));
    const csv = ['symbol,day,close', ...kept.map(([d, v]) => `L,${d},${v}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'cal-gaps.csv'), writer, { table: 'cal_gaps' });
    const gaps: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const ts = spec({ A: 'close' });
    const t = await engine.execute(await plannerFor(r.model, r.runtime).plan(calendarQuery(gaps, ts, ts.series[0]!).query), 0);
    const at = (y: number, m: number): number | null => {
      const i = values(t, TS_CAL_YEAR).findIndex((v, j) => num(v) === y && num(values(t, TS_CAL_MONTH)[j]) === m);
      assert.ok(i >= 0, `${y}-${m} has a row`);
      return num(values(t, TS_CAL_RETURN)[i]);
    };
    const lastOf = (m: string): number => kept.filter(([d]) => d.startsWith(m)).at(-1)![1];
    assert.equal(at(2021, 4), null, 'April after a missing March: none');
    assert.equal(at(2022, 1), null, 'January after a missing December: none');
    assert.ok(Math.abs(at(2021, 5)! - (lastOf('2021-05') / lastOf('2021-04') - 1)) < 1e-12, 'May from April');
    // a year's from the year before's last level -- November's, its December missing (pandas' year-end last)
    const y22 = values(t, TS_CAL_YEAR_RETURN).find((_, j) => num(values(t, TS_CAL_YEAR)[j]) === 2022);
    assert.ok(Math.abs(num(y22)! - (kept.at(-1)![1] / lastOf('2021-11') - 1)) < 1e-12);
    assert.throws(() => calendarQuery(long, { ...ts, grain: 'quarter' }, ts.series[0]!), /a calendar of months reads a level at least monthly/);
  });

  it('a calendar of returns: each month\'s from the month before\'s last, each year\'s from the year before\'s', async () => {
    const month = (d: string): string => d.slice(0, 7);
    const months = [...new Set(LONG_DAYS.map(month))];
    const lastOf = (m: string): number => LONG[LONG_DAYS.map(month).lastIndexOf(m)]!;
    const ts = spec({ A: 'close' });
    const t = await engine.execute(await own.plan(calendarQuery(long, ts, ts.series[0]!).query), 0);
    assert.deepEqual(values(t, TS_CAL_YEAR).map(num), months.map((m) => Number(m.slice(0, 4))));
    assert.deepEqual(values(t, TS_CAL_MONTH).map(num), months.map((m) => Number(m.slice(5, 7))));
    close(values(t, TS_CAL_RETURN).map(num), months.map((m, i) => (i === 0 ? null : lastOf(m) / lastOf(months[i - 1]!) - 1)), 'monthly', 1e-12);
    const yearEnd = (y: number): number => lastOf(months.filter((m) => m.startsWith(String(y))).at(-1)!);
    close(values(t, TS_CAL_YEAR_RETURN).map(num), months.map((m) => (m.startsWith('2021') ? null : yearEnd(2022) / yearEnd(2021) - 1)), 'yearly', 1e-12);
    // the line's least month-end level on every row: what says a return from at or below zero is none
    assert.ok(values(t, TS_CAL_LOW).every((v) => num(v) === Math.min(...months.map(lastOf))));
    assert.throws(() => calendarQuery(long, spec({ A: 'returns(close)' }), spec({ A: 'returns(close)' }).series[0]!), /no level/);
  });

  it('trend and seasonally adjusted over a quarter of months: an odd cycle', async () => {
    const month = (d: string): string => d.slice(0, 7);
    const months = [...new Set(LONG_DAYS.map(month))];
    const last = months.map((m) => LONG[LONG_DAYS.map(month).lastIndexOf(m)]!);
    const want = decompose(last, 3, false);
    const ts = (text: string): TimeseriesSpec => spec({ A: text }, { grain: 'month' });
    const run = async (text: string): Promise<Line> => {
      const t = await engine.execute(await own.plan(seriesQuery(long, ts(text), ts(text).series[0]!).query), 0);
      return values(t, TS_VALUE).map(num).reverse();
    };
    close(await run('trend(close, additive, quarter)'), want.trend, 'trend', 1e-9);
    close(await run('seasonally_adjusted(close, additive, quarter)'), want.adjusted, 'adjusted', 1e-9);
  });

  // the Phase 1 review's 4: under two whole cycles gs-quant refuses (statsmodels cannot centre a season), and its places
  // in the cycle are the calendar's, a missing month never moving the months after it
  it('a decomposition: none under two whole cycles; each place in the cycle the calendar\'s, a month missing', async () => {
    const ts = (text: string): TimeseriesSpec => spec({ A: text }, { grain: 'month' });
    const run = async (text: string): Promise<{ dates: string[]; values: Line }> => {
      const t = await engine.execute(await own.plan(seriesQuery(long, ts(text), ts(text).series[0]!).query), 0);
      return { dates: values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(), values: values(t, TS_VALUE).map(num).reverse() };
    };
    // 700 days are 24 months, two cycles of a year: decomposed; their last month dropped, 23 -- none (gs-quant refuses)
    assert.ok((await run('seasonally_adjusted(close, additive, year)')).values.some((v) => v !== null), 'two cycles: decomposed');
    const short = 'filter(if((month(close) == 12) and (year(close) == 2022), 0, close), equals, 0)';
    assert.equal((await run(short)).dates.length, 23);
    assert.ok((await run(`seasonally_adjusted(${short}, additive, year)`)).values.every((v) => v === null), 'none under two cycles');
    assert.ok((await run(`trend(${short}, additive, year)`)).values.every((v) => v === null), 'nor a trend');
    // March left out: each month's season its own still -- the same at each date of one place (Jan, Apr, Jul, Oct ...)
    const x = 'filter(if(month(close) == 3, 0, close), equals, 0)';
    const [xs, sa] = [await run(x), await run(`seasonally_adjusted(${x}, additive, quarter)`)];
    assert.ok(!xs.dates.some((d) => d.slice(5, 7) === '03'), 'no March');
    const season = new Map<number, number[]>();
    sa.values.forEach((v, i) => {
      if (v === null) return;
      const place = (Number(xs.dates[i]!.slice(5, 7)) - 1) % 3;
      season.set(place, [...(season.get(place) ?? []), xs.values[i]! - v]);
    });
    for (const [place, each] of season) assert.ok(each.every((v) => Math.abs(v - each[0]!) < 1e-9), `place ${place}: one season, ${each.join(', ')}`);
    assert.equal(season.size, 3);
  });

  // the E9 review's 6: each average had joined the query to itself, so a chain of them doubled it each time (five: 311k
  // characters); its block carried by windows, each adds the same few steps
  it('averages of averages: the query grows by the same steps each, never doubling', () => {
    const size = (text: string): number => toJson(seriesQuery(long, spec({ A: text }), spec({ A: text }).series[0]!).query).length;
    const nest = (n: number): string => (n === 0 ? 'close' : `ema(${nest(n - 1)}, 0.9)`);
    const [s1, s2, s3, s4] = [1, 2, 3, 4].map((n) => size(nest(n)));
    const steps = [s2! - s1!, s3! - s2!, s4! - s3!];
    assert.ok(steps.every((d) => Math.abs(d - steps[0]!) <= 0.1 * steps[0]!), `each average the same steps: ${[s1, s2, s3, s4].join(', ')}`);
  });
});

// -- the views of a tile (the next phases' 3, 2026-10-26), each held to its definition written out --

describe('a tile\'s views: a scatter, a histogram, a curve -- every figure the database\'s', () => {
  it('a scatter: a point a date both have, and y\'s least-squares regression on x over them', async () => {
    const ts = spec({ A: 'close[AAA]', B: 'close[BBB]' });
    const t = await run(scatterQuery(inputs, ts, ts.series[0]!, ts.series[1]!));
    const xs = values(t, TS_SC_X).map(num) as number[];
    const ys = values(t, TS_SC_Y).map(num) as number[];
    assert.deepEqual(values(t, TS_DATE).map((d) => String(d).slice(0, 10)), DAYS);
    close(xs, AAA, 'x', 0);
    close(ys, BBB, 'y', 0);
    const slope = cov(AAA, BBB) / (sd(AAA) ** 2);
    const intercept = avg(BBB) - slope * avg(AAA);
    const corr = cov(AAA, BBB) / (sd(AAA) * sd(BBB));
    const first = (name: string): number => num(values(t, name)[0])!;
    assert.ok(Math.abs(first(TS_SC_SLOPE) - slope) < 1e-9 * Math.abs(slope), `slope ${first(TS_SC_SLOPE)}, not ${slope}`);
    assert.ok(Math.abs(first(TS_SC_INTERCEPT) - intercept) < 1e-7, `intercept ${first(TS_SC_INTERCEPT)}, not ${intercept}`);
    assert.ok(Math.abs(first(TS_SC_CORR) - corr) < 1e-9 && Math.abs(first(TS_SC_R2) - corr * corr) < 1e-9);
    assert.equal(first(TS_SC_N), DAYS.length);
    const lo = Math.min(...AAA);
    const hi = Math.max(...AAA);
    assert.ok(Math.abs(first(TS_SC_FIT_LOW) - (intercept + slope * lo)) < 1e-7 && Math.abs(first(TS_SC_FIT_HIGH) - (intercept + slope * hi)) < 1e-7);
    // two calendars: the dates both have, over the dates shown
    const some = spec({ A: 'close[AAA]', C: 'close[CCC]' });
    const both = await run(scatterQuery(inputs, some, some.series[0]!, some.series[1]!, { from: DAYS[9]! }));
    assert.deepEqual(values(both, TS_DATE).map((d) => String(d).slice(0, 10)), DAYS.filter((_, i) => i % 3 === 0 && i >= 9));
    // a split series is no point a date
    const split = spec({ A: 'close' }, {}, 'symbol');
    assert.throws(() => scatterQuery(inputs, split, split.series[0]!, split.series[0]!), /a scatter is of one line/);
  });

  // Phase 5's trend projected: the line by least squares over its last observations, on their day numbers, its fit at
  // the first and the last of them and at the date `ahead` past the last -- every figure and date the database's
  it('a projection: the least-squares line over the last w observations, at their ends and `ahead` past the last', async () => {
    const ts: TimeseriesSpec = { ...spec({ A: 'close[AAA]' }), annotations: [{ kind: 'projection', series: 'A', window: 10, ahead: '20d' }] };
    const p = ts.annotations![0] as TsProjection;
    const t = await run(projectionQuery(inputs, ts, p));
    assert.equal(t.rowCount, 1, 'one row: the fit');
    const day = (d: string): number => Date.parse(`${d}T00:00:00Z`) / 86_400_000;
    const xs = DAYS.slice(-10).map(day);
    const ys = AAA.slice(-10);
    const slope = cov(xs, ys) / (sd(xs) ** 2);
    const intercept = avg(ys) - slope * avg(xs);
    const fit = (x: number): number => intercept + slope * x;
    const first = (name: string): unknown => values(t, name)[0];
    const at = (name: string): string => String(first(name)).slice(0, 10);
    assert.deepEqual([at(TS_PJ_FROM), at(TS_PJ_LAST)], [DAYS.at(-10)!, DAYS.at(-1)!], 'from the first of its last 10 to the last');
    const end = new Date((day(DAYS.at(-1)!) + 20) * 86_400_000).toISOString().slice(0, 10);
    assert.equal(at(TS_PJ_END), end, '20 days past the last, by the database\'s adjust');
    assert.ok(Math.abs(num(first(TS_PJ_SLOPE))! - slope) < 1e-9, `slope ${first(TS_PJ_SLOPE)}, not ${slope}`);
    for (const [name, x] of [[TS_PJ_FROM_VALUE, xs[0]!], [TS_PJ_LAST_VALUE, xs.at(-1)!], [TS_PJ_END_VALUE, xs.at(-1)! + 20]] as const) {
      assert.ok(Math.abs(num(first(name))! - fit(x)) < 1e-6, `${name} ${first(name)}, not ${fit(x)}`);
    }
    assert.equal(num(first(TS_PJ_N)), 10);
    // its ask, by the tile's one rule: listed, and refused under a compare (a line's own values), as the drawing leaves it
    assert.deepEqual(tileAsks(ts).filter((a) => a.kind === 'projection').map((a) => [a.series, a.projection]), [['A', 0]]);
    const compared: TimeseriesSpec = { ...ts, view: { compare: 'rebase' } };
    assert.deepEqual(tileAsks(compared).filter((a) => a.kind === 'projection'), []);
    assert.throws(() => projectionQuery(inputs, compared, p), /a projection is of a line's own values: under Compare/);
    // a split series is no one line
    const split: TimeseriesSpec = { ...spec({ A: 'close' }, {}, 'symbol'), annotations: [p] };
    assert.throws(() => projectionQuery(inputs, split, p), /a projection is of one line/);
    // the Phase 5 review's 1: a series read from the first date shown (an index) is fit on its values from there, as its
    // line is drawn -- its ask over the window's first date
    const indexed: TimeseriesSpec = { ...spec({ A: 'index(close[AAA])' }), annotations: [p] };
    const it = await run(askQuery(inputs, indexed, { kind: 'projection', series: 'A', projection: 0 }, { from: DAYS[10]! }));
    const iys = ys.map((v) => v / AAA[10]!);
    const islope = cov(xs, iys) / (sd(xs) ** 2);
    const ifit = (x: number): number => avg(iys) - islope * avg(xs) + islope * x;
    assert.ok(Math.abs(num(values(it, TS_PJ_LAST_VALUE)[0])! - ifit(xs.at(-1)!)) < 1e-9, `${values(it, TS_PJ_LAST_VALUE)[0]}, not ${ifit(xs.at(-1)!)}`);
  });

  it('a histogram: bins of one width from the least to the most, each counted, its normal and the 5th percentile', async () => {
    const ts = spec({ A: 'close[AAA]' });
    const t = await run(histogramQuery(inputs, ts, ts.series[0]!, {}, 5));
    const lo = Math.min(...AAA);
    const width = (Math.max(...AAA) - lo) / 5;
    const counts = [0, 0, 0, 0, 0];
    for (const v of AAA) counts[Math.min(4, Math.floor((v - lo) / width))]! += 1;
    // every bin, one a value fell in or not (the Phase 3 review's 6: the empty ones had no row, their normal never drawn)
    const filled = counts.map((c, i) => [c, i] as const);
    assert.equal(t.rowCount, 5);
    close(values(t, TS_HIST_LOW).map(num), filled.map(([, i]) => lo + i * width), 'edges', 1e-12);
    close(values(t, TS_HIST_HIGH).map(num), filled.map(([, i]) => lo + (i + 1) * width), 'edges', 1e-12);
    assert.deepEqual(values(t, TS_HIST_COUNT).map(num), filled.map(([c]) => c));
    const m = avg(AAA);
    const s = sd(AAA);
    close(values(t, TS_HIST_NORMAL).map(num), filled.map(([, i]) => {
      const z = (lo + (i + 0.5) * width - m) / s;
      return (AAA.length * width * Math.exp(-0.5 * z * z)) / (s * Math.sqrt(2 * Math.PI));
    }), 'the normal\'s counts', 1e-9);
    assert.ok(Math.abs(num(values(t, TS_HIST_P5)[0])! - quantile(AAA, 0.05)) < 1e-9);
    assert.equal(num(values(t, TS_HIST_N)[0]), AAA.length);
    // as many bins as asked, the empty ones counted 0: numpy's histogram
    const many = await run(histogramQuery(inputs, ts, ts.series[0]!, {}, 40));
    const w40 = (Math.max(...AAA) - lo) / 40;
    const c40 = Array.from({ length: 40 }, () => 0);
    for (const v of AAA) c40[Math.min(39, Math.floor((v - lo) / w40))]! += 1;
    assert.ok(c40.includes(0), 'the test has an empty bin');
    assert.deepEqual(values(many, TS_HIST_COUNT).map(num), c40);
  });

  it('a curve: each line as of each date asked -- the latest, a week back, a day -- its last value at or before it', async () => {
    const ts = spec({ A: 'close' }, {}, 'symbol');
    const asked = ['latest', '1w', DAYS[10]!];
    const t = await run(curveQuery(inputs, ts, ts.series[0]!, asked));
    const back = new Date(Date.parse(`${DAYS.at(-1)}T00:00:00Z`) - 7 * 86_400_000).toISOString().slice(0, 10);
    const targets = [DAYS.at(-1)!, back, DAYS[10]!];
    const of: Record<string, (readonly [string, number])[]> = {
      AAA: DAYS.map((d, i) => [d, AAA[i]!] as const), BBB: DAYS.map((d, i) => [d, BBB[i]!] as const),
      CCC: DAYS.map((d, i) => [d, CCC[i]!] as const).filter((_, i) => i % 3 === 0),
    };
    const want = targets.flatMap((d, k) => ['AAA', 'BBB', 'CCC'].map((sym) => {
      const last = of[sym]!.filter(([day]) => day <= d).at(-1)!;
      return [k, sym, last[1], last[0]] as const;
    }));
    // a row a line, each date asked its own columns
    assert.deepEqual(values(t, TS_SPLIT), ['AAA', 'BBB', 'CCC']);
    const got = targets.flatMap((_, k) => values(t, TS_SPLIT).map((sym, i) => [k, String(sym), num(values(t, curveColumns(k).value)[i]),
      String(values(t, curveColumns(k).date)[i]).slice(0, 10)] as const));
    assert.deepEqual(got.sort((a, b) => a[0] - b[0]), want);
    assert.deepEqual(targets.map((_, k) => String(values(t, curveColumns(k).asked)[0]).slice(0, 10)), targets);
    assert.throws(() => curveQuery(inputs, spec({ A: 'close[AAA]' }), spec({ A: 'close[AAA]' }).series[0]!), /a curve is of a line per value/);
    // the tile's compare is the time view's: a curve's lines as they are, never each from its own first value (the Phase
    // 3 review's 7)
    const compared = await run(curveQuery(inputs, { ...ts, view: { compare: 'rebase' } }, ts.series[0]!, asked));
    assert.deepEqual(values(compared, curveColumns(0).value), values(t, curveColumns(0).value));
  });

  it('the series each view draws: the view\'s own, else the tile\'s first it can', () => {
    const ts = spec({ A: 'close[AAA]', B: 'bollinger_bands($A, 2, 1)', C: 'close', D: 'close[BBB]' });
    const split: TimeseriesSpec = { ...ts, series: ts.series.map((s) => (s.id === 'C' ? { ...s, split: 'symbol' } : s)) };
    assert.deepEqual(viewSeries(split), { scatter: ['A', 'D'], one: 'A', curve: 'C', calendar: ['A', 'C', 'D'] });
    assert.deepEqual(viewSeries({ ...split, view: { of: ['D', 'A'] } }).scatter, ['D', 'A']);
    // an `of` a mode cannot draw is no mode's: a scatter's x for a curve, a curve's split for a histogram (the Phase 3
    // review's 4); one told to a scatter its x, the next its y
    assert.equal(viewSeries({ ...split, view: { of: ['D', 'A'] } }).curve, 'C');
    assert.equal(viewSeries({ ...split, view: { of: ['C'] } }).one, 'A');
    assert.deepEqual(viewSeries({ ...split, view: { of: ['D'] } }).scatter, ['D', 'A']);
    assert.deepEqual(viewSeries({ ...split, view: { of: ['B', 'C'] } }).scatter, ['A', 'D'], 'a band and a split: neither a scatter\'s');
    // candles are a level, but no one level a calendar of returns reads (the gallery's technicals tile found it)
    const candles = spec({ A: 'ohlc(close[AAA], close[AAA], close[AAA], close[AAA])', B: 'close[BBB]' });
    assert.deepEqual(viewSeries(candles).calendar, ['B']);
  });

  // the user, 2026-10-26: "show all the pure queries used to create a timeseries chart" -- the one rule the screen's
  // Query window and Python's queries() list them by, each written by the one function every round writes it by
  it('every question a tile asks: its bounds, each series, its view, then its panel\'s -- each query askQuery\'s', async () => {
    const ts = spec({ A: 'close[AAA]', B: 'returns(close[BBB])', C: 'bollinger_bands(close[AAA], 5)' }, { view: { mode: 'scatter', compare: 'rebase' } });
    const asks = tileAsks(ts);
    assert.deepEqual(asks.map((a) => [a.kind, a.series ?? a.source, a.with ?? null, a.serves]), [
      ['bounds', 'px', null, 'chart'],
      ['series', 'A', null, 'chart'], ['series', 'B', null, 'chart'], ['series', 'C', null, 'chart'],
      ['scatter', 'A', 'B', 'chart'],
      // a band has no figures of its own; a return no drawdown, no calendar
      ['statistics', 'A', null, 'statistics'], ['statistics', 'B', null, 'statistics'],
      ['drawdowns', 'A', null, 'drawdowns'],
      ['correlations', 'A', 'A', 'correlations'], ['correlations', 'A', 'B', 'correlations'], ['correlations', 'B', 'B', 'correlations'],
      ['calendar', 'A', null, 'calendar'],
    ]);
    assert.equal(new Set(asks.map(askKey)).size, asks.length, 'each asked once');
    // a series' points as the screen asks them: from the first date shown where it reads one (compare), the drawing's limit
    const shown = { from: DAYS[5]! };
    const a = ts.series[0]!;
    assert.equal(toJson(askQuery(inputs, ts, { kind: 'series', series: 'A' }, shown).query), toJson(seriesQuery(inputs, ts, a, { from: DAYS[5]! }).query));
    assert.equal(toJson(askQuery(inputs, ts, { kind: 'statistics', series: 'A' }, shown).query), toJson(statsQuery(inputs, ts, a, shown).query));
    // and each answers: the database runs every one
    for (const ask of asks.filter((x) => x.kind !== 'bounds')) {
      const t = await run(askQuery(inputs, ts, ask, shown));
      assert.ok(t.columns.length > 0, `${ask.what}: answered`);
    }
    assert.deepEqual(askSources(ts, asks.find((x) => x.kind === 'scatter')!), ['px']);
  });

  it('a curve\'s keys in order: tenors by their span (3M before 2Y), else their text, numbers as numbers', () => {
    const keys = ['10Y', '3M', '2Y', '1Y', '18M', '30Y', '1W'];
    assert.deepEqual(curveOrder(keys).map((i) => keys[i]), ['1W', '3M', '1Y', '18M', '2Y', '10Y', '30Y']);
    const words = ['strip 10', 'strip 2', 'strip 1'];
    assert.deepEqual(curveOrder(words).map((i) => words[i]), ['strip 1', 'strip 2', 'strip 10']);
    // a money market's: overnight, tom-next and spot-next by their days, any other key after the tenors (the Phase 3
    // review's 8: one key no tenor put every key by its text, 1Y before 3M)
    const mixed = ['10Y', 'Cash', '1Y', '3M', 'ON', '1W', 'TN'];
    assert.deepEqual(curveOrder(mixed).map((i) => mixed[i]), ['ON', 'TN', '1W', '3M', '1Y', '10Y', 'Cash']);
  });

  it('a seasonality of a yearly frequency is refused: a year has one place', () => {
    const ts: TimeseriesSpec = { ...spec({ A: 'close[AAA]' }), grain: 'year' };
    assert.throws(() => seasonalityQuery(inputs, ts, ts.series[0]!), /at a yearly frequency a year has one/);
  });
});

// the next phases' 4: an analyst's packs -- technicals of a candle's columns, a series against a benchmark, a total
// return, a basket, a pair's lead and its reversion. Each held to its textbook's arithmetic (TA-Lib's ATR and %K,
// StockCharts' OBV from 0, capture by arithmetic means, statsmodels' OLS) or to gs-quant's own loop
// (backtesting.basket_series), written here.
describe('an analyst\'s packs: technicals, a benchmark, a total return, a basket, a lead, a reversion', () => {
  const N = 110;
  const PACK_DAYS: string[] = [];
  for (let d = new Date(Date.UTC(2024, 0, 31)); PACK_DAYS.length < N; d = new Date(d.getTime() + 86_400_000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) PACK_DAYS.push(d.toISOString().slice(0, 10));
  }
  const PC = PACK_DAYS.map((_, i) => round(100 * (1 + 0.05 * Math.sin(i / 5) + 0.003 * i + 0.01 * Math.sin(i * 1.7))));
  const PH = PC.map((c, i) => round(c + 0.3 + 0.8 * Math.abs(Math.sin(i * 1.3))));
  const PL = PC.map((c, i) => round(c - 0.3 - 0.8 * Math.abs(Math.cos(i * 0.7))));
  const PV = PACK_DAYS.map((_, i) => 1000 + ((i * 37) % 500));
  // P's dividends on two of its dates; PD's (a series of dividends alone) on a Saturday P has no price of and a date it has
  const P_DIV = new Map([[20, 0.5], [45, 0.75]]);
  const SATURDAY = '2024-03-16';
  const Q_HAS = PACK_DAYS.map((_, i) => i % 7 !== 3);
  const QC = PACK_DAYS.map((_, i) => round(50 * (1 + 0.04 * Math.cos(i / 4) + 0.002 * i + 0.015 * Math.cos(i * 2.3))));
  const QH = QC.map((c, i) => round(c + 0.2 + 0.5 * Math.abs(Math.cos(i * 0.9))));
  const QL = QC.map((c, i) => round(c - 0.2 - 0.5 * Math.abs(Math.sin(i * 1.1))));
  let pack: TsInputs;
  let packs: LitePlanner;
  before(async () => {
    const csv = ['symbol,day,high,low,close,volume,dividend',
      ...PACK_DAYS.flatMap((d, i) => [
        `P,${d},${PH[i]},${PL[i]},${PC[i]},${PV[i]},${P_DIV.get(i) ?? ''}`,
        ...(Q_HAS[i] ? [`Q,${d},${QH[i]},${QL[i]},${QC[i]},${PV[i]! * 2},`] : []),
      ]),
      `PD,${SATURDAY},,,,,0.4`,
      `PD,${PACK_DAYS[60]},,,,,0.3`,
    ].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'packs.csv'), writer, { table: 'packs' });
    packs = plannerFor(r.model, r.runtime);
    pack = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: [
      { name: 'symbol', type: 'String' },
      { name: 'day', type: 'StrictDate' },
      { name: 'high', type: 'Float' },
      { name: 'low', type: 'Float' },
      { name: 'close', type: 'Float' },
      { name: 'volume', type: 'Integer', kind: 'measure' },
      { name: 'dividend', type: 'Float' },
    ] }, filter: undefined }]]);
  });
  const packLine = async (text: string, more: Partial<TimeseriesSpec> = {}): Promise<{ dates: string[]; values: Line; lower?: Line; upper?: Line }> => {
    const ts = spec({ A: text }, more);
    const q = seriesQuery(pack, ts, ts.series[0]!);
    const t = await engine.execute(await packs.plan(q.query), 0);
    return {
      dates: values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(),
      values: values(t, TS_VALUE).map(num).reverse(),
      ...(q.band ? { lower: values(t, TS_LOWER).map(num).reverse(), upper: values(t, TS_UPPER).map(num).reverse() } : {}),
    };
  };
  const Q_IDX = PACK_DAYS.map((_, i) => i).filter((i) => Q_HAS[i]);
  const Q_DAYS = Q_IDX.map((i) => PACK_DAYS[i]!);
  /** TA-Lib's true range: the most of the high less the low, the high less the close before, the close before less the low. */
  const trueRanges = (h: number[], l: number[], c: number[]): number[] =>
    h.map((v, i) => (i === 0 ? v - l[i]! : Math.max(v - l[i]!, Math.abs(v - c[i - 1]!), Math.abs(c[i - 1]! - l[i]!))));
  /** TA-Lib's ATR: none to the w-th, there the mean of the w true ranges after the first, then Wilder's ((w − 1)·y + tr) / w. */
  const talibAtr = (tr: number[], w: number): Line => {
    const out: Line = [];
    for (let i = 0; i < tr.length; i++) out.push(i < w ? null : i === w ? avg(tr.slice(1, w + 1)) : ((w - 1) * out[i - 1]! + tr[i]!) / w);
    return out;
  };
  const correlation = (a: number[], b: number[]): number => cov(a, b) / (sd(a) * sd(b));

  it('atr: Wilder\'s average of true ranges, as TA-Lib\'s -- of an instrument with gaps, each close before its own', async () => {
    const l = await packLine('atr(high[P], low[P], close[P], 14)');
    assert.deepEqual(l.dates, PACK_DAYS);
    close(l.values, talibAtr(trueRanges(PH, PL, PC), 14), 'ATR 14', 1e-9);
    const q = await packLine('atr(high[Q], low[Q], close[Q], 5)');
    assert.deepEqual(q.dates, Q_DAYS);
    const pick = (x: number[]): number[] => Q_IDX.map((i) => x[i]!);
    close(q.values, talibAtr(trueRanges(pick(QH), pick(QL), pick(QC)), 5), 'ATR 5 over Q\'s own dates', 1e-9);
  });

  it('stochastic: 100 × (close − lowest low) / (highest high − lowest low) over w, as TA-Lib\'s fast %K', async () => {
    const l = await packLine('stochastic(high[P], low[P], close[P], 14)');
    close(l.values, PC.map((c, i) => {
      if (i < 13) return null;
      const hh = Math.max(...PH.slice(i - 13, i + 1));
      const ll = Math.min(...PL.slice(i - 13, i + 1));
      return (100 * (c - ll)) / (hh - ll);
    }), '%K 14', 1e-9);
    assert.ok(l.values.every((v) => v === null || (v >= 0 && v <= 100)));
  });

  it('keltner_channels: k average true ranges either side of the close\'s exponential average', async () => {
    const l = await packLine('keltner_channels(high[P], low[P], close[P], 20, 10, 2)');
    const mid = ema(PC, 19 / 21);
    const atr = talibAtr(trueRanges(PH, PL, PC), 10);
    // drawn once the average holds its 20 closes (the Phase 4 review's 7), the ATR's 10 before it
    close(l.lower!, atr.map((a, i) => (a === null || i < 19 ? null : mid[i]! - 2 * a)), 'lower', 1e-9);
    close(l.upper!, atr.map((a, i) => (a === null || i < 19 ? null : mid[i]! + 2 * a)), 'upper', 1e-9);
  });

  it('the technicals of a high, a low and a close, and the statistics against a benchmark, are each a #TS query that plans', async () => {
    const calls = async (text: string): Promise<string> => {
      const q = seriesQuery(pack, spec({ A: text }), spec({ A: text }).series[0]!).query;
      await packs.plan(q);
      return islandText(q);
    };
    const want: readonly [string, string][] = [
      ['atr(high[P], low[P], close[P], 14)', 'atr'], ['stochastic(high[P], low[P], close[P], 14)', 'stochastic'],
      ['keltner_channels(high[P], low[P], close[P], 20, 10, 2)', 'keltnerChannels'], ['donchian_channels(high[P], low[P], 20)', 'donchianChannels'],
      ['donchian_channels(high[P], low[P], 2w)', 'donchianChannelsPeriod'], ['tracking_error(close[P], close[Q], 10)', 'trackingError'],
      ['information_ratio(close[P], close[Q], 10)', 'informationRatio'], ['up_capture(close[P], close[Q], 10)', 'upCapture'],
      ['down_capture(close[P], close[Q], 10)', 'downCapture'], ['lead_lag(close[P], close[Q], 1, 10)', 'leadLag'],
      ['dickey_fuller(close[P], 30)', 'dickeyFuller'], ['half_life(close[P], 30)', 'halfLife'],
      ['total_return(close[P], 0.03)', 'totalReturn'], ['total_return(close[P], dividend[P])', 'totalReturnOf'],
      ['basket(close[P], 0.6, close[Q], 0.4, rebalance=monthly)', 'basket'],
      // the last moved (2026-10-27): over a calendar period
      ['tracking_error(close[P], close[Q], 1m)', 'trackingErrorPeriod'], ['information_ratio(close[P], close[Q], 1m)', 'informationRatioPeriod'],
      ['up_capture(close[P], close[Q], 1m)', 'upCapturePeriod'], ['down_capture(close[P], close[Q], 1m)', 'downCapturePeriod'],
      ['lead_lag(close[P], close[Q], 1, 1m)', 'leadLagPeriod'], ['dickey_fuller(close[P], 2m)', 'dickeyFullerPeriod'],
      ['half_life(close[P], 2m)', 'halfLifePeriod'],
    ];
    for (const [expr] of want) {
      const fn = /^([a-z_]+)\(/.exec(expr)![1]!;
      assert.ok((await calls(expr)).includes(`${fn}(`), `${expr} as #TS`);
    }
  });

  it('donchian_channels: the lowest low and the highest high of the last w', async () => {
    const l = await packLine('donchian_channels(high[P], low[P], 20)');
    close(l.lower!, rolling(PL, 20, (v) => Math.min(...v)), 'lower', 0);
    close(l.upper!, rolling(PH, 20, (v) => Math.max(...v)), 'upper', 0);
    // over a calendar period (the library's donchianChannelsPeriod, 2026-10-27): the dates after two weeks back, to this
    // one, from the first date two weeks on
    const p = await packLine('donchian_channels(high[P], low[P], 2w)');
    const start = onBy(PACK_DAYS[0]!, 2, 'w');
    const within = (xs: number[], d: string): number[] => xs.filter((_, j) => PACK_DAYS[j]! > backBy(d, 2, 'w') && PACK_DAYS[j]! <= d);
    close(p.lower!, PACK_DAYS.map((d) => (d < start ? null : Math.min(...within(PL, d)))), 'lower 2w', 0);
    close(p.upper!, PACK_DAYS.map((d) => (d < start ? null : Math.max(...within(PH, d)))), 'upper 2w', 0);
  });

  it('obv: each volume added on a rise, taken off on a fall, from 0; vwap: Σ price × volume / Σ volume', async () => {
    let total = 0;
    close((await packLine('obv(close[P], volume[P])')).values,
      PC.map((c, i) => (i === 0 ? 0 : (total += Math.sign(c - PC[i - 1]!) * PV[i]!))), 'OBV', 0);
    const vwap = (v: number[][]): number => v.reduce((s, [p, q]) => s + p! * q!, 0) / v.reduce((s, [, q]) => s + q!, 0);
    const pv = PC.map((p, i) => [p, PV[i]!]);
    close((await packLine('vwap(close[P], volume[P], 10)')).values,
      pv.map((_, i) => (i < 9 ? null : vwap(pv.slice(i - 9, i + 1)))), 'VWAP 10', 1e-12);
    close((await packLine('vwap(close[P], volume[P], 0)')).values, pv.map((_, i) => vwap(pv.slice(0, i + 1))), 'VWAP to date', 1e-12);
    // over a calendar period (the library's vwapPeriod): the dates after two weeks back, to this one, from the first
    // price two weeks on
    const start = onBy(PACK_DAYS[0]!, 2, 'w');
    close((await packLine('vwap(close[P], volume[P], 2w)')).values, PACK_DAYS.map((d) => (d < start ? null
      : vwap(pv.filter((_, j) => PACK_DAYS[j]! > backBy(d, 2, 'w') && PACK_DAYS[j]! <= d)))), 'VWAP 2w', 1e-12);
  });

  /** P's simple returns and Q's, each on its own dates, on Q's (the dates both have); Q's `k` back (or on, below 0). */
  const pairsOn = (k = 0): { a: Line; b: Line } => {
    const rp = simple(PC);
    const rq: Line = Q_IDX.map((i, j) => (j === 0 ? null : QC[i]! / QC[Q_IDX[j - 1]!]! - 1));
    const shifted: Line = rq.map((_, j) => rq[j - k] ?? null);
    const a = Q_IDX.map((i, j) => (shifted[j] === null ? null : rp[i]!));
    return { a, b: shifted.map((v, j) => (a[j] === null ? null : v)) };
  };
  const windowed = (a: Line, b: Line, w: number, f: (p: number[], q: number[]) => number | null): Line => a.map((_, i) => {
    if (i < w - 1) return null;
    const p = a.slice(i - w + 1, i + 1);
    const q = b.slice(i - w + 1, i + 1);
    if (p.some((v) => v === null)) return null;
    return f(p as number[], q as number[]);
  });

  it('tracking error and information ratio: of P\'s returns less Q\'s, each on its own dates, annualized', async () => {
    const { a, b } = pairsOn();
    const active = zip(a, b, (p, q) => p - q);
    close((await packLine('tracking_error(close[P], close[Q], 10)', { grain: 'day' })).values,
      rolling(active, 10, (v) => sd(v) * Math.sqrt(252) * 100), 'TE', 1e-9);
    close((await packLine('information_ratio(close[P], close[Q], 10)', { grain: 'day' })).values,
      rolling(active, 10, (v) => (avg(v) / sd(v)) * Math.sqrt(252)), 'IR', 1e-9);
  });

  it('up and down capture: P\'s mean return on the periods Q rose (fell) over Q\'s mean then', async () => {
    const { a, b } = pairsOn();
    for (const [f, up] of [['up_capture', true], ['down_capture', false]] as const) {
      close((await packLine(`${f}(close[P], close[Q], 10)`)).values, windowed(a, b, 10, (p, q) => {
        const on = q.map((_, i) => i).filter((i) => (up ? q[i]! > 0 : q[i]! < 0));
        if (on.length === 0) return null;
        return avg(on.map((i) => p[i]!)) / avg(on.map((i) => q[i]!));
      }), f, 1e-9);
    }
  });

  it('lead_lag: P\'s returns against Q\'s lag observations before (after, below 0), on Q\'s own dates', async () => {
    for (const k of [0, 2, -2]) {
      const { a, b } = pairsOn(k);
      const l = await packLine(`lead_lag(close[P], close[Q], ${k}, 10)`);
      assert.deepEqual(l.dates, Q_DAYS);
      close(l.values, windowed(a, b, 10, correlation), `lag ${k}`, 1e-9);
    }
  });

  // the library's calendar forms (pure-library.ts trackingErrorPeriod, informationRatioPeriod, upCapturePeriod,
  // downCapturePeriod, leadLagPeriod; 2026-10-27): each over the dates within a month back, from the first return's
  // date that period on
  it('tracking error, information ratio, the captures and lead_lag over 1m', async () => {
    const over = (xs: Line, ys: Line, f: (p: number[], q: number[]) => number | null): Line => {
      const start = onBy(Q_DAYS[xs.findIndex((v) => v !== null)]!, 1, 'm');
      return Q_DAYS.map((d) => {
        if (d < start) return null;
        const js = Q_DAYS.map((_, j) => j).filter((j) => xs[j] !== null && ys[j] !== null && Q_DAYS[j]! > backBy(d, 1, 'm') && Q_DAYS[j]! <= d);
        return f(js.map((j) => xs[j]!), js.map((j) => ys[j]!));
      });
    };
    const { a, b } = pairsOn();
    const active = zip(a, b, (p, q) => p - q);
    close((await packLine('tracking_error(close[P], close[Q], 1m)', { grain: 'day' })).values,
      over(active, active, (p) => sd(p) * Math.sqrt(252) * 100), 'TE 1m', 1e-9);
    close((await packLine('information_ratio(close[P], close[Q], 1m)', { grain: 'day' })).values,
      over(active, active, (p) => (avg(p) / sd(p)) * Math.sqrt(252)), 'IR 1m', 1e-9);
    for (const [f, up] of [['up_capture', true], ['down_capture', false]] as const) {
      close((await packLine(`${f}(close[P], close[Q], 1m)`)).values, over(a, b, (p, q) => {
        const on = q.map((_, i) => i).filter((i) => (up ? q[i]! > 0 : q[i]! < 0));
        return on.length === 0 ? null : avg(on.map((i) => p[i]!)) / avg(on.map((i) => q[i]!));
      }), `${f} 1m`, 1e-9);
    }
    for (const k of [0, 2]) {
      const { a: la, b: lb } = pairsOn(k);
      close((await packLine(`lead_lag(close[P], close[Q], ${k}, 1m)`)).values, over(la, lb, correlation), `lead_lag ${k} 1m`, 1e-9);
    }
  });

  it('total_return: dividends reinvested on their dates -- a Saturday\'s paid at Monday; a yield accrued day by day', async () => {
    const day = (d: string): number => Date.parse(`${d}T00:00:00Z`) / 86_400_000;
    const tr = (paid: (i: number) => number): number[] => {
      const out = [PC[0]!];
      for (let i = 1; i < PC.length; i++) out.push(out[i - 1]! * ((PC[i]! + paid(i)) / PC[i - 1]!));
      return out;
    };
    close((await packLine('total_return(close[P], dividend[P])')).values, tr((i) => P_DIV.get(i) ?? 0), 'dividends on P\'s rows', 1e-9);
    const monday = PACK_DAYS.indexOf('2024-03-18');
    assert.ok(monday > 0 && !PACK_DAYS.includes(SATURDAY));
    const l = await packLine('total_return(close[P], dividend[PD])');
    assert.deepEqual(l.dates, PACK_DAYS, 'the price\'s dates alone: no Saturday');
    close(l.values, tr((i) => (i === monday ? 0.4 : i === 60 ? 0.3 : 0)), 'a Saturday\'s dividend paid at Monday', 1e-9);
    const y = await packLine('total_return(close[P], 0.03)');
    const accrued: number[] = [PC[0]!];
    for (let i = 1; i < PC.length; i++) {
      accrued.push(accrued[i - 1]! * (PC[i]! / PC[i - 1]! + (0.03 * (day(PACK_DAYS[i]!) - day(PACK_DAYS[i - 1]!))) / 365));
    }
    close(y.values, accrued, 'a 3% yield', 1e-9);
  });

  /** gs-quant's backtesting.basket_series, its loop (no costs): units held from 100, reset at each rebalance date --
   *  daily every date; weekly or monthly the first date on or after the first date plus k weeks or months
   *  (relativedelta's month: its day, or the month's last), each before the last date. */
  const gsBasket = (cal: string[], prices: number[][], weights: number[], freq: 'daily' | 'weekly' | 'monthly'): number[] => {
    const at = (d: string): number => Date.parse(`${d}T00:00:00Z`);
    const iso = (t: number): string => new Date(t).toISOString().slice(0, 10);
    let rebal: Set<string>;
    if (freq === 'daily') rebal = new Set(cal);
    else {
      const [c0, last] = [cal[0]!, cal.at(-1)!];
      const hyp: string[] = [];
      if (freq === 'weekly') {
        const n = Math.floor((at(last) - at(c0)) / (7 * 86_400_000));
        for (let i = 0; i <= n; i++) hyp.push(iso(at(c0) + i * 7 * 86_400_000));
      } else {
        const [y0, m0, d0] = c0.split('-').map(Number) as [number, number, number];
        const [y1, m1] = last.split('-').map(Number) as [number, number];
        const n = (y1 - y0) * 12 + m1 - m0;
        for (let i = 0; i <= n; i++) {
          const lastDay = new Date(Date.UTC(y0, m0 - 1 + i + 1, 0)).getUTCDate();
          hyp.push(iso(Date.UTC(y0, m0 - 1 + i, Math.min(d0, lastDay))));
        }
      }
      rebal = new Set(hyp.filter((h) => h < last).map((h) => cal.find((d) => d >= h)!));
    }
    const out = [100];
    let units = weights.map((w, j) => (100 * w) / prices[j]![0]!);
    for (let i = 1; i < cal.length; i++) {
      out.push(out[i - 1]! + units.reduce((s, u, j) => s + u * (prices[j]![i]! - prices[j]![i - 1]!), 0));
      if (rebal.has(cal[i]!)) units = weights.map((w, j) => (out[i]! * w) / prices[j]![i]!);
    }
    return out;
  };

  it('basket: gs-quant\'s backtest -- daily, weekly and monthly (a 31st\'s month its last day), on the dates all have', async () => {
    const prices = [Q_IDX.map((i) => PC[i]!), Q_IDX.map((i) => QC[i]!)];
    for (const freq of ['daily', 'weekly', 'monthly'] as const) {
      const l = await packLine(`basket(close[P], 0.6, close[Q], 0.4, rebalance=${freq})`);
      assert.deepEqual(l.dates, Q_DAYS, `${freq}: the dates both have`);
      close(l.values, gsBasket(Q_DAYS, prices, [0.6, 0.4], freq), freq, 1e-9);
    }
    // the rebalances differ: a month's units are not a day's
    const [d, m] = [gsBasket(Q_DAYS, prices, [0.6, 0.4], 'daily'), gsBasket(Q_DAYS, prices, [0.6, 0.4], 'monthly')];
    assert.ok(Math.abs(d.at(-1)! - m.at(-1)!) > 1e-6, 'the sample tells a daily basket from a monthly one');
    // three series, weights of no sum in particular
    const three = await packLine('basket(close[P], 0.5, close[Q], 0.3, high[P], 0.4, rebalance=weekly)');
    close(three.values, gsBasket(Q_DAYS, [...prices, Q_IDX.map((i) => PH[i]!)], [0.5, 0.3, 0.4], 'weekly'), 'three, weekly', 1e-9);
  });

  /** Δx = a + b·x[-1] by least squares over the pairs, written out: b, its standard error from the residuals. */
  const ols = (x: number[]): { b: number; t: number } => {
    const xs = x.slice(0, -1);
    const ys = x.slice(1).map((v, i) => v - x[i]!);
    const [mx, my] = [avg(xs), avg(ys)];
    const sxx = xs.reduce((s, v) => s + (v - mx) ** 2, 0);
    const b = xs.reduce((s, v, i) => s + (v - mx) * (ys[i]! - my), 0) / sxx;
    const a = my - b * mx;
    const ssr = xs.reduce((s, v, i) => s + (ys[i]! - a - b * v) ** 2, 0);
    return { b, t: b / Math.sqrt(ssr / (xs.length - 2) / sxx) };
  };

  it('dickey_fuller and half_life: the t-statistic of b in Δx = a + b·x[-1], and −ln 2 / ln(1 + b), over w', async () => {
    const df = await packLine('dickey_fuller(close[P], 30)');
    // 30 pairs: the 31st value on
    close(df.values, PC.map((_, i) => (i < 30 ? null : ols(PC.slice(i - 30, i + 1)).t)), 'DF over 30', 1e-9);
    const hl = await packLine('half_life(close[P], 30)');
    close(hl.values, PC.map((_, i) => {
      if (i < 30) return null;
      const { b } = ols(PC.slice(i - 30, i + 1));
      return b >= 0 || b <= -1 ? null : -Math.log(2) / Math.log(1 + b);
    }), 'half-life over 30', 1e-9);
    assert.ok(hl.values.some((v) => v !== null) && hl.values.some((v, i) => i >= 30 && v === null), 'the sample reverts and does not');
    // from the start: three pairs on
    close((await packLine('dickey_fuller(close[P])')).values, PC.map((_, i) => (i < 3 ? null : ols(PC.slice(0, i + 1)).t)), 'DF to date', 1e-9);
  });

  // the library's calendar forms (pure-library.ts dickeyFullerPeriod, halfLifePeriod, 2026-10-27): the pairs (Δx, x[-1])
  // whose dates are within two months back, from the first pair's date that period on
  it('dickey_fuller and half_life over 2m: the pairs of the dates two months back', async () => {
    const start = onBy(PACK_DAYS[1]!, 2, 'm');
    const within = (d: string): number[] => PC.filter((_, j) => j >= 1 && PACK_DAYS[j]! > backBy(d, 2, 'm') && PACK_DAYS[j]! <= d);
    // the window's pairs: each date's x and the x before it, the first of them the value before the period's first
    const fit = (i: number): { b: number; t: number } => {
      const d = PACK_DAYS[i]!;
      const first = PACK_DAYS.findIndex((x) => x > backBy(d, 2, 'm'));
      return ols(PC.slice(Math.max(first - 1, 0), i + 1));
    };
    assert.ok(within(PACK_DAYS.at(-1)!).length > 3, 'the period holds pairs');
    close((await packLine('dickey_fuller(close[P], 2m)')).values, PACK_DAYS.map((d, i) => (d < start ? null : fit(i).t)), 'DF over 2m', 1e-9);
    const hl = await packLine('half_life(close[P], 2m)');
    close(hl.values, PACK_DAYS.map((d, i) => {
      if (d < start) return null;
      const { b } = fit(i);
      return b >= 0 || b <= -1 ? null : -Math.log(2) / Math.log(1 + b);
    }), 'half-life over 2m', 1e-9);
  });

  // the Phase 4 review's 1: at a frequency, each period's highest high, lowest low, last close and whole volume (a
  // week's last high and last day's volume had been read), and a VWAP of each day's price times its volume
  it('at a weekly frequency, each week a bar: its highest high, lowest low, last close and whole volume', async () => {
    const week = (d: string): string => {
      const t = Date.parse(`${d}T00:00:00Z`);
      return new Date(t - ((new Date(t).getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(PACK_DAYS.map(week))];
    const of = (k: string): number[] => PACK_DAYS.map((_, i) => i).filter((i) => week(PACK_DAYS[i]!) === k);
    const WH = weeks.map((k) => Math.max(...of(k).map((i) => PH[i]!)));
    const WL = weeks.map((k) => Math.min(...of(k).map((i) => PL[i]!)));
    const WC = weeks.map((k) => PC[of(k).at(-1)!]!);
    const WV = weeks.map((k) => of(k).reduce((t, i) => t + PV[i]!, 0));
    const WPV = weeks.map((k) => of(k).reduce((t, i) => t + PC[i]! * PV[i]!, 0));
    const weekly = { grain: 'week' as const };
    const atr = await packLine('atr(high[P], low[P], close[P], 3)', weekly);
    assert.deepEqual(atr.dates, weeks.map((k) => PACK_DAYS[of(k).at(-1)!]!), 'each week at its last day');
    close(atr.values, talibAtr(trueRanges(WH, WL, WC), 3), 'weekly ATR', 1e-9);
    close((await packLine('stochastic(high[P], low[P], close[P], 4)', weekly)).values,
      WC.map((c, i) => (i < 3 ? null : (100 * (c - Math.min(...WL.slice(i - 3, i + 1)))) / (Math.max(...WH.slice(i - 3, i + 1)) - Math.min(...WL.slice(i - 3, i + 1))))), 'weekly %K', 1e-9);
    const dc = await packLine('donchian_channels(high[P], low[P], 4)', weekly);
    close(dc.upper!, rolling(WH, 4, (v) => Math.max(...v)), 'weekly upper', 0);
    close(dc.lower!, rolling(WL, 4, (v) => Math.min(...v)), 'weekly lower', 0);
    let total = 0;
    close((await packLine('obv(close[P], volume[P])', weekly)).values,
      WC.map((c, i) => (i === 0 ? 0 : (total += Math.sign(c - WC[i - 1]!) * WV[i]!))), 'weekly OBV', 0);
    close((await packLine('vwap(close[P], volume[P], 3)', weekly)).values,
      WV.map((_, i) => (i < 2 ? null : WPV.slice(i - 2, i + 1).reduce((a, b) => a + b, 0) / WV.slice(i - 2, i + 1).reduce((a, b) => a + b, 0))), 'weekly VWAP', 1e-12);
  });

  // the Phase 4 review's 3 and 8: a long and a short (weights summing to 0) is a basket as any; a price of none makes
  // the basket none from there, as gs-quant's NaN carries through its units (it had been taken as flat)
  it('basket: a long and a short; none from a price of none on; a rebalance it has no word for refused', async () => {
    const prices = [Q_IDX.map((i) => PC[i]!), Q_IDX.map((i) => QC[i]!)];
    close((await packLine('basket(close[P], 1, close[Q], -1, rebalance=monthly)')).values,
      gsBasket(Q_DAYS, prices, [1, -1], 'monthly'), 'long P, short Q', 1e-9);
    const gap = await packLine('basket(lag(close[P], 1), 0.5, close[Q], 0.5)');
    assert.ok(gap.values.length > 0 && gap.values.every((v) => v === null), 'none from its first price of none on');
    const ts = spec({ A: 'basket(close[P], 0.5, close[Q], 0.5)' });
    const yearly: TimeseriesSpec = { ...ts, series: [{ ...ts.series[0]!, expr: { ...ts.series[0]!.expr, params: { rebalance: 'yearly' } } as TsSeries['expr'] }] };
    assert.throws(() => seriesQuery(pack, yearly, yearly.series[0]!), /a basket rebalances daily, weekly or monthly, not "yearly"/);
  });

  it('dickey_fuller over 1 or 2 differences refused: no deviation left to judge it by', async () => {
    await assert.rejects(packLine('dickey_fuller(close[P], 2)'), /dickey_fuller over 2 differences has no deviation left: w is 0 \(every one to date\) or at least 3/);
    await assert.rejects(packLine('half_life(close[P], 1)'), /half_life over 1 differences/);
  });

  it('a pack\'s series read by name: what each is, and how many', () => {
    assert.throws(() => parseExpr('atr(high[P], low[P])'), /atr reads three series \(high, low and close\)/);
    assert.throws(() => parseExpr('obv(close[P])'), /obv reads two series \(close and volume\)/);
    assert.throws(() => parseExpr('basket(close[P], 0.5, close[Q], 0.5, rebalance=yearly)'), /rebalance/);
    // a weighted sum divides by its weights' sum, a basket by nothing
    assert.throws(() => parseExpr('weighted_sum(close[P], 1, close[Q], -1)'), /weights sum to 0/);
    assert.doesNotThrow(() => parseExpr('basket(close[P], 1, close[Q], -1)'));
  });
});

// the review's N7 (docs/TIMESERIES_REVIEW_2026_10_25.md §9): a period was dated at its last row though that row's value
// was none -- its value an earlier day's, its date the month's last
describe('a period of a column dated at its last row with a value', () => {
  // each month's last weekday without a close, its volume there
  const lastOfMonth = DAYS.map((d, i) => DAYS[i + 1]?.slice(0, 7) !== d.slice(0, 7));
  let gaps: TsInputs;
  let gapPlanner: LitePlanner;
  before(async () => {
    const csv = ['symbol,day,close,volume', ...DAYS.map((d, i) => `G,${d},${lastOfMonth[i] ? '' : AAA[i]},${VOL[i]}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'gaps.csv'), writer, { table: 'gaps' });
    gapPlanner = plannerFor(r.model, r.runtime);
    gaps = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source } }, filter: undefined }]]);
  });
  const gapLine = async (text: string, grain: TimeseriesSpec['grain']): Promise<{ dates: string[]; values: Line }> => {
    const ts = spec({ A: text }, grain === undefined ? {} : { grain });
    const t = await engine.execute(await gapPlanner.plan(seriesQuery(gaps, ts, ts.series[0]!).query), 0);
    return { dates: values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(), values: values(t, TS_VALUE).map(num).reverse() };
  };

  it('a month whose last row has no value: dated at the day its value is of', async () => {
    const months = [...new Set(DAYS.map((d) => d.slice(0, 7)))];
    const withValue = DAYS.map((_, i) => i).filter((i) => !lastOfMonth[i]);
    const lastWith = months.map((m) => withValue.filter((i) => DAYS[i]!.startsWith(m)).at(-1)!);
    const l = await gapLine('close[G]', 'month');
    assert.deepEqual(l.dates, lastWith.map((i) => DAYS[i]), 'each month at its last close\'s day');
    close(l.values, lastWith.map((i) => AAA[i]!), 'its last close');
    // a sum of the volume, which every day has, dated at the month's last day still
    const v = await gapLine('volume[G].sum', 'month');
    assert.deepEqual(v.dates, months.map((m) => DAYS.filter((d) => d.startsWith(m)).at(-1)!), 'the volume\'s last day');
  });
});

// a date-time source at a frequency (the library review, 2026-10-27): a period's days are counted from its last
// observation's time, never from its date -- an accrual over 23 hours is 23/24 of a day, a week from the first tick is
// a week of hours
describe('a date-time source at a daily frequency: a period\'s days its last observation\'s time', () => {
  const DAYS_T: string[] = [];
  for (let d = new Date(Date.UTC(2024, 0, 1)); DAYS_T.length < 12; d = new Date(d.getTime() + 86_400_000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) DAYS_T.push(d.toISOString().slice(0, 10));
  }
  // each day's last tick at 16:00, 15:00 or 14:00 -- the second Monday's at 14:00, under a week from the first's 16:00
  const LAST_HOUR = DAYS_T.map((_, i) => 16 - (i % 3));
  const SC = DAYS_T.map((_, i) => round(100 + i + 0.5 * Math.sin(i)));
  const BC = DAYS_T.map((_, i) => round(50 + 0.3 * i * Math.cos(i)));
  const T = DAYS_T.map((d, i) => Date.parse(`${d}T${String(LAST_HOUR[i]).padStart(2, '0')}:00:00Z`) / 1000);
  let ticks: TsInputs;
  let tickPlanner: LitePlanner;
  before(async () => {
    const csv = ['symbol,day,close', ...DAYS_T.flatMap((d, i) => [
      `S,${d} 10:00:00,${SC[i]! - 1}`, `S,${d} ${String(LAST_HOUR[i]).padStart(2, '0')}:00:00,${SC[i]}`,
      `B,${d} 10:00:00,${BC[i]! + 1}`, `B,${d} ${String(LAST_HOUR[i]).padStart(2, '0')}:00:00,${BC[i]}`,
    ])].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'ticks.csv'), writer, { table: 'ticks' });
    tickPlanner = plannerFor(r.model, r.runtime);
    ticks = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: [
      { name: 'symbol', type: 'String' }, { name: 'day', type: 'DateTime' }, { name: 'close', type: 'Float' },
    ] }, filter: undefined }]]);
  });
  const tickLine = async (text: string): Promise<Line> => {
    const ts = spec({ A: text }, { grain: 'day' });
    const q = seriesQuery(ticks, ts, ts.series[0]!);
    return values(await engine.execute(await tickPlanner.plan(q.query), 0), TS_VALUE).map(num).reverse();
  };
  const days = (i: number): number => (T[i]! - T[i - 1]!) / 86_400;

  it('excess returns over a rate and a total return with a yield accrue the hours between the last ticks', async () => {
    let accrued = 0;
    close(await tickLine('excess_returns(close[S], 0.05)'), SC.map((x, i) => {
      if (i > 0) accrued += SC[i - 1]! * (days(i) / 360);
      return x - 0.05 * accrued;
    }), 'excess returns', 1e-9);
    let tr = SC[0]!;
    close(await tickLine('total_return(close[S], 0.03)'), SC.map((x, i) => (i === 0 ? tr : (tr *= x / SC[i - 1]! + 0.03 * (days(i) / 365)))),
      'total return', 1e-9);
  });

  it('a weekly basket rebalances a week of hours from the first tick: not the Mondays, at 14:00 and 15:00, but the Tuesdays', async () => {
    const week = DAYS_T.map((_, i) => Math.floor((T[i]! - T[0]!) / 86_400 / 7));
    const rebalanced = new Set(DAYS_T.map((_, i) => i).filter((i) => i > 0 && week[i] !== week[i - 1]));
    // the Mondays' last ticks (14:00, 15:00) under a whole week from the first's 16:00: each week turns on its Tuesday
    assert.deepEqual([...rebalanced].map((i) => DAYS_T[i]), ['2024-01-09', '2024-01-16'], 'two rebalances, the Tuesdays');
    const out = [100];
    let units = [0.6 * 100 / SC[0]!, 0.4 * 100 / BC[0]!];
    for (let i = 1; i < DAYS_T.length; i++) {
      out.push(out[i - 1]! + units[0]! * (SC[i]! - SC[i - 1]!) + units[1]! * (BC[i]! - BC[i - 1]!));
      if (rebalanced.has(i)) units = [0.6 * out[i]! / SC[i]!, 0.4 * out[i]! / BC[i]!];
    }
    close(await tickLine('basket(close[S], 0.6, close[B], 0.4, rebalance=weekly)'), out, 'weekly basket', 1e-9);
  });

  // the conversion's second review (2026-10-28) fit a daily projection of date-times on each day's last tick's time;
  // since the timeseries language's ruling 9 (2026-10-30) a period of a day is dated at its day, and the line drawn is
  // of days: its least squares on them, ahead a week of them, its slope a day's
  it('a projection of date-times at a daily frequency: its line on its days, its end a week past the last, its slope a day\'s', async () => {
    const ts = spec({ A: 'close[S]' }, { grain: 'day' });
    const t = await engine.execute(await tickPlanner.plan(projectionQuery(ticks, ts, { kind: 'projection', series: 'A', window: 5, ahead: '1w' }).query), 0);
    const xs = DAYS_T.slice(-5).map((d) => Date.parse(`${d}T00:00:00Z`) / 1000);
    const ys = SC.slice(-5);
    const mx = avg(xs);
    const my = avg(ys);
    const slope = xs.reduce((a, x, i) => a + (x - mx) * (ys[i]! - my), 0) / xs.reduce((a, x) => a + (x - mx) ** 2, 0);
    const at = (x: number): number => my + slope * (x - mx);
    close([TS_PJ_FROM_VALUE, TS_PJ_LAST_VALUE, TS_PJ_END_VALUE].map((c) => num(values(t, c)[0])), [at(xs[0]!), at(xs[4]!), at(xs[4]! + 7 * 86_400)], 'its fit', 1e-6);
    close([num(values(t, TS_PJ_SLOPE)[0])], [slope * 86_400], 'a day\'s slope', 1e-9);
    assert.equal(num(values(t, TS_PJ_N)[0]), 5);
  });
});

// the review's Q12 (docs/TIMESERIES_REVIEW_2026_10_25.md §9): a cut at a count of rows kept some of a split's lines at
// its oldest date and not others
describe('a series cut at its most points keeps whole dates', () => {
  it('every line at each date it keeps, the date it crossed at too', async () => {
    const ts = spec({ A: 'close' }, {}, 'symbol');
    const datesOf = (t: ResultTable): string[] => values(t, TS_DATE).map((d) => String(d).slice(0, 10));
    const perDate = (t: ResultTable): Map<string, number> => datesOf(t).reduce((m, d) => m.set(d, (m.get(d) ?? 0) + 1), new Map<string, number>());
    const all = perDate(await run(seriesQuery(inputs, ts, ts.series[0]!, {}, {}, null)));
    const cut = await run(seriesQuery(inputs, ts, ts.series[0]!, {}, {}, 7));
    for (const [d, n] of perDate(cut)) assert.equal(n, all.get(d), `${d}: every line it has`);
    // the latest three dates (3, 2 and 2 rows) are 7; the fourth, the one it crossed at, kept beside them
    const kept = [...new Set(datesOf(cut))];
    assert.deepEqual(kept, [...all.keys()].slice(0, 4));
    assert.equal(datesOf(cut).filter((d) => d !== kept.at(-1)).length, 7, 'the dates after the one it crossed at');
  });
});

describe('a series of no instrument says how many rows each date had', () => {
  it('close with no [AAA] and no split: each date three rows or two, counted', async () => {
    const q = seriesQuery(inputs, spec({ A: 'close' }), spec({ A: 'close' }).series[0]!);
    assert.equal(q.counted, true);
    const t = await run(q);
    const rows = values(t, TS_ROWS).map(Number);
    assert.ok(rows.every((n) => n === 2 || n === 3), rows.join(','));
  });
});

describe('the tile\'s bounds: its latest date and where each range button starts, the database\'s', () => {
  it('the latest, the earliest, a month and a year back, the year\'s start', async () => {
    const t = await run({ query: boundsQuery(input, TS.sources[0]!) });
    const at = (n: string): string => String(values(t, n)[0]).slice(0, 10);
    assert.equal(at(TS_LATEST), DAYS[DAYS.length - 1]);
    assert.equal(at(TS_EARLIEST), DAYS[0]);
    assert.equal(at(rangeStartName('1m')), '2024-01-23');
    assert.equal(at(rangeStartName('1y')), '2023-02-23');
    assert.equal(at(rangeStartName('ytd')), '2024-01-01');
  });
});

describe('a series\' statistics over the dates shown', () => {
  it('first, last, change, extremes and when, mean, deviation, volatility, drawdown, growth and Sharpe', async () => {
    const ts = spec({ A: 'close[AAA]' });
    const view = { from: DAYS[5]!, to: DAYS[34]! };
    const t = await run(statsQuery(inputs, ts, ts.series[0]!, view));
    const v = (s: Parameters<typeof statName>[0]): unknown => values(t, statName(s))[0];
    const shown = AAA.slice(5, 35);
    assert.equal(Number(v('count')), 30);
    assert.equal(String(v('from')).slice(0, 10), DAYS[5]);
    assert.equal(String(v('to')).slice(0, 10), DAYS[34]);
    // the calendar days between them (Phase 5's measure): the database's date arithmetic, held to the calendar's
    assert.equal(Number(v('days')), (Date.parse(DAYS[34]!) - Date.parse(DAYS[5]!)) / 86_400_000);
    close([num(v('first')), num(v('last'))], [shown[0]!, shown[29]!], 'ends');
    close([num(v('change'))], [shown[29]! - shown[0]!], 'change', 1e-9);
    close([num(v('pct'))], [shown[29]! / shown[0]! - 1], 'pct', 1e-9);
    close([num(v('low')), num(v('high'))], [Math.min(...shown), Math.max(...shown)], 'extremes');
    assert.equal(String(v('highAt')).slice(0, 10), DAYS[5 + shown.indexOf(Math.max(...shown))]);
    close([num(v('mean')), num(v('std'))], [avg(shown), sd(shown)], 'moments', 1e-9);
    const lr = shown.slice(1).map((x, i) => Math.log(x / shown[i]!));
    close([num(v('vol'))], [sd(lr) * Math.sqrt(252) * 100], 'vol', 1e-7);
    let peak = -Infinity;
    close([num(v('mdd'))], [Math.min(...shown.map((x) => {
      peak = Math.max(peak, x);
      return x / peak - 1;
    }))], 'mdd', 1e-9);
    // under a year, no compound annual growth: its % change is its growth (follow-up N3, GIPS: a return of under a
    // year is never annualized)
    assert.equal(v('cagr'), null, 'no growth annualized over 30 weekdays');
    const r = shown.slice(1).map((x, i) => x / shown[i]! - 1);
    close([num(v('sharpe'))], [(avg(r) / sd(r)) * Math.sqrt(252)], 'sharpe', 1e-7);
  });

  // follow-up N3 (the first review's Q11): CAGR over four days read 8,507%. Annualized only over a calendar year or
  // more, the database's own date arithmetic deciding (a leap year's 366 days a year)
  it('N3: compound annual growth over a year or more, none under a year', async () => {
    const dates = ['2022-02-28', '2022-09-30', '2023-02-27', '2023-02-28', '2024-02-29', '2024-03-01'];
    const levels = [100, 104, 107, 108, 115, 116];
    const csv = ['symbol,day,close', ...dates.map((d, i) => `Y,${d},${levels[i]}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'yearly.csv'), writer, { table: 'n3_yearly' });
    const own = plannerFor(r.model, r.runtime);
    const yearly: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const ts = spec({ A: 'close' });
    const cagr = async (view: TsWindow): Promise<unknown> =>
      values(await engine.execute(await own.plan(statsQuery(yearly, ts, ts.series[0]!, view).query), 0), statName('cagr'))[0];
    const days = (a: string, b: string): number => (Date.parse(b) - Date.parse(a)) / 86400000;
    // a day short of a year: none; a year to the day: its growth
    assert.equal(await cagr({ from: '2022-02-28', to: '2023-02-27' }), null, 'a day short of a year');
    close([num(await cagr({ from: '2022-02-28', to: '2023-02-28' }))], [(108 / 100) ** (365.25 / 365) - 1], 'a year to the day', 1e-9);
    // over a leap day: 2023-02-28 to 2024-02-29 is a year; 2024-02-29 to 2024-03-01 none
    close([num(await cagr({ from: '2023-02-28', to: '2024-02-29' }))], [(115 / 108) ** (365.25 / days('2023-02-28', '2024-02-29')) - 1], 'over a leap day', 1e-9);
    assert.equal(await cagr({ from: '2024-02-29' }), null, 'a day');
    close([num(await cagr({}))], [(116 / 100) ** (365.25 / days('2022-02-28', '2024-03-01')) - 1], 'two years', 1e-9);
    // compared as its % change, the level's own growth (the third review's 5)
    const pct = spec({ A: 'close' }, { view: { compare: 'percent' } });
    const compared = await engine.execute(await own.plan(statsQuery(yearly, pct, pct.series[0]!, {}).query), 0);
    close([num(values(compared, statName('cagr'))[0])], [(116 / 100) ** (365.25 / days('2022-02-28', '2024-03-01')) - 1], 'compared', 1e-9);
  });

  // the expert statistics (the user, 2026-10-26): a level's returns' risk, pandas' definitions written out here
  it('a level\'s returns\' risk: skew, kurtosis, Sortino, hit rate, best, worst, value at risk and expected shortfall', async () => {
    const ts = spec({ A: 'close[AAA]' });
    const t = await run(statsQuery(inputs, ts, ts.series[0]!, { from: DAYS[5]!, to: DAYS[34]! }));
    const v = (st: Parameters<typeof statName>[0]): number | null => num(values(t, statName(st))[0]);
    const shown = AAA.slice(5, 35);
    const r = shown.slice(1).map((x, i) => x / shown[i]! - 1);
    const n = r.length;
    const m = avg(r);
    const moment = (k: number): number => avg(r.map((x) => (x - m) ** k));
    const [m2, m3, m4] = [moment(2), moment(3), moment(4)];
    close([v('skew')], [(m3 / m2 ** 1.5) * Math.sqrt(n * (n - 1)) / (n - 2)], 'skew (pandas)', 1e-9);
    close([v('kurtosis')], [((n + 1) * (n - 1) * m4 / (m2 * m2) - 3 * (n - 1) ** 2) / ((n - 2) * (n - 3))], 'kurtosis (pandas)', 1e-9);
    close([v('sortino')], [(m / Math.sqrt(avg(r.map((x) => Math.min(x, 0) ** 2)))) * Math.sqrt(252)], 'sortino', 1e-9);
    close([v('hit')], [r.filter((x) => x > 0).length / n], 'hit rate', 1e-12);
    close([v('best'), v('worst')], [Math.max(...r), Math.min(...r)], 'best and worst', 1e-12);
    const sorted = [...r].sort((a, b) => a - b);
    const at = 0.05 * (n - 1);
    const q = sorted[Math.floor(at)]! + (sorted[Math.ceil(at)]! - sorted[Math.floor(at)]!) * (at - Math.floor(at));
    close([v('var')], [q], 'the 5% quantile (interpolated)', 1e-12);
    close([v('es')], [avg(r.filter((x) => x <= q))], 'the average at or below it', 1e-12);
    // under a year, no growth: no Calmar
    assert.equal(v('calmar'), null, 'no Calmar under a year');
  });

  // the conversion's review (2026-10-28): the shortfall reads ranks -- those at or below the quantile are those ranked at
  // most floor((n - 1)q + 1) -- so its edges are pinned: (n - 1)q whole, the quantile one of the values, and ties at it
  it('the expected shortfall where the quantile is a value, and where several returns equal it', async () => {
    const levels = [100, 99, 101, 98, 102, 103, 97, 104, 105, 106, 104, 107, 108, 109, 107, 110, 111, 112, 113, 114, 115, 116];
    const days = weekdays(levels.length);
    const csv = ['symbol,day,close', ...days.map((d, i) => `E,${d},${levels[i]}`)].join('\n');
    const r0 = await ingestFile(engine, files, new File([csv], 'shortfall.csv'), writer, { table: 'es_edges' });
    const own = plannerFor(r0.model, r0.runtime);
    const shown: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r0.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const ts = spec({ A: 'close' });
    const t = await engine.execute(await own.plan(statsQuery(shown, ts, ts.series[0]!).query), 0);
    const r = levels.slice(1).map((x, i) => x / levels[i]! - 1);
    assert.equal(r.length, 21, 'twenty-one returns: (n - 1)q = 1, the quantile the second least');
    const sorted = [...r].sort((a, b) => a - b);
    close([num(values(t, statName('var'))[0])], [sorted[1]!], 'the 5% quantile, a value', 1e-12);
    close([num(values(t, statName('es'))[0])], [avg(r.filter((x) => x <= sorted[1]!))], 'the mean of those at or below it', 1e-12);
    // ties at the quantile: every one at it is in the tail
    const tied = [100, 98, 100, 98, 100, 98, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115];
    const csv2 = ['symbol,day,close', ...days.map((d, i) => `E,${d},${tied[i]}`)].join('\n');
    const r1 = await ingestFile(engine, files, new File([csv2], 'shortfall-tied.csv'), writer, { table: 'es_tied' });
    const tiedIn: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r1.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const t2 = await engine.execute(await plannerFor(r1.model, r1.runtime).plan(statsQuery(tiedIn, ts, ts.series[0]!).query), 0);
    const rt = tied.slice(1).map((x, i) => x / tied[i]! - 1);
    const st = [...rt].sort((a, b) => a - b);
    const at = 0.05 * (rt.length - 1);
    const q = st[Math.floor(at)]! + (st[Math.ceil(at)]! - st[Math.floor(at)]!) * (at - Math.floor(at));
    assert.ok(rt.filter((x) => x === st[0]).length === 3, 'three returns tied at the least');
    close([num(values(t2, statName('es'))[0])], [avg(rt.filter((x) => x <= q))], 'ties at the quantile all in the tail', 1e-12);
    own.dispose();
  });

  it('Calmar: its growth over its deepest drawdown, over a year or more', async () => {
    const dates = ['2022-01-31', '2022-04-29', '2022-07-29', '2022-10-31', '2023-01-31', '2023-04-28'];
    const levels = [100, 110, 88, 95, 120, 125];
    const csv = ['symbol,day,close', ...dates.map((d, i) => `Y,${d},${levels[i]}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'calmar.csv'), writer, { table: 'calmar_q' });
    const own = plannerFor(r.model, r.runtime);
    const quarterly: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const ts = spec({ A: 'close' });
    const t = await engine.execute(await own.plan(statsQuery(quarterly, ts, ts.series[0]!).query), 0);
    const days = (Date.parse(dates.at(-1)!) - Date.parse(dates[0]!)) / 86400000;
    const cagr = (125 / 100) ** (365.25 / days) - 1;
    close([num(values(t, statName('calmar'))[0])], [cagr / Math.abs(88 / 110 - 1)], 'calmar', 1e-9);
  });

  // the expert tables (2026-10-26): a level's deepest drawdowns, each from a peak to its recovery, the database's
  it('drawdowns: each from a peak to the next date at or above it, its trough between; the deepest five, deepest first', async () => {
    const ts = spec({ A: 'close[AAA]' });
    const t = await run(drawdownsQuery(inputs, ts, ts.series[0]!));
    // the reference, as plain arithmetic: runs from each date at or above every one before it to the next
    const runs: { peak: number; low: number; trough: number; next: number | null }[] = [];
    let best = -Infinity;
    for (let i = 0; i < AAA.length; i++) {
      if (AAA[i]! >= best) {
        best = AAA[i]!;
        if (runs.length > 0) runs.at(-1)!.next = i;
        runs.push({ peak: i, low: AAA[i]!, trough: i, next: null });
      } else if (AAA[i]! < runs.at(-1)!.low) {
        runs.at(-1)!.low = AAA[i]!;
        runs.at(-1)!.trough = i;
      }
    }
    const want = runs.filter((r) => r.low < AAA[r.peak]!).map((r) => ({ ...r, depth: r.low / AAA[r.peak]! - 1 }))
      .sort((a, b) => a.depth - b.depth).slice(0, 5);
    assert.ok(want.length >= 2, `${want.length} drawdowns in the fixture`);
    const day = (n: string): string[] => values(t, n).map((d) => (d === null ? '' : String(d).slice(0, 10)));
    assert.deepEqual(day(TS_DD_PEAK), want.map((r) => DAYS[r.peak]!), 'the peaks');
    assert.deepEqual(day(TS_DD_TROUGH), want.map((r) => DAYS[r.trough]!), 'the troughs');
    assert.deepEqual(day(TS_DD_RECOVERED), want.map((r) => (r.next === null ? '' : DAYS[r.next]!)), 'the recoveries');
    close(values(t, TS_DD_DEPTH).map(num), want.map((r) => r.depth), 'the depths', 1e-12);
    const span = (a: number, b: number): number => (Date.parse(DAYS[b]!) - Date.parse(DAYS[a]!)) / 86_400_000;
    assert.deepEqual(values(t, TS_DD_DECLINE).map(Number), want.map((r) => span(r.peak, r.trough)), 'days down');
    assert.deepEqual(values(t, TS_DD_RECOVERY).map((v) => (v === null ? null : Number(v))), want.map((r) => (r.next === null ? null : span(r.trough, r.next))), 'days back');
    assert.deepEqual(values(t, TS_DD_RANK).map(Number), want.map((_, i) => i + 1));
    // split by symbol: each line's its own, as the one instrument's alone
    const split = await run(drawdownsQuery(inputs, spec({ A: 'close' }, {}, 'symbol'), spec({ A: 'close' }, {}, 'symbol').series[0]!));
    const bbb = await run(drawdownsQuery(inputs, spec({ A: 'close[BBB]' }), spec({ A: 'close[BBB]' }).series[0]!));
    const of = (n: string): unknown[] => values(split, n).filter((_, i) => values(split, TS_SPLIT)[i] === 'BBB');
    assert.deepEqual(of(TS_DD_PEAK), values(bbb, TS_DD_PEAK), "BBB's line, its own peaks");
    close(of(TS_DD_DEPTH).map(num), values(bbb, TS_DD_DEPTH).map(num), "BBB's depths", 1e-12);
    assert.ok(new Set(values(split, TS_SPLIT)).size >= 2, 'a line per symbol');
    // a return is no level: refused in words
    assert.throws(() => drawdownsQuery(inputs, spec({ A: 'returns(close[AAA])' }), spec({ A: 'returns(close[AAA])' }).series[0]!), /is no level: .* has no drawdowns/);
  });

  it('a return\'s statistics leave out what only a level has', async () => {
    const ts = spec({ A: 'returns(close[AAA])' });
    const t = await run(statsQuery(inputs, ts, ts.series[0]!));
    assert.ok(!t.columns.some((c) => c.name === statName('vol')), 'no volatility of a return');
    assert.equal(Number(values(t, statName('count'))[0]), 39);
  });

  // the timeseries gallery, 2026-10-25: a level read through a lag (a difference, a lag, prices of returns) had its
  // statistics name their own lag as the series named its own -- the planner refused the column twice
  it('a level read through a lag: its statistics name their own columns after the series\'', async () => {
    const ts = spec({ A: 'diff(close[AAA], 5)' });
    const t = await run(statsQuery(inputs, ts, ts.series[0]!));
    const diffs = AAA.slice(5).map((x, i) => x - AAA[i]!);
    assert.equal(Number(values(t, statName('count'))[0]), diffs.length);
    close([num(values(t, statName('first'))[0]), num(values(t, statName('last'))[0])], [diffs[0]!, diffs.at(-1)!], 'ends', 1e-9);
  });

  it('per line of a split series', async () => {
    const ts = spec({ A: 'close' }, {}, 'symbol');
    const t = await run(statsQuery(inputs, ts, ts.series[0]!));
    assert.deepEqual(values(t, TS_SPLIT).map(String).sort(), ['AAA', 'BBB', 'CCC']);
    // each line's figures its own (the conversion's review, 2026-10-28: the library's figures joined to the line's own
    // by the line): the same as the one instrument's tile
    const lines = values(t, TS_SPLIT).map(String);
    for (const sym of ['AAA', 'BBB', 'CCC']) {
      const one = spec({ A: `close[${sym}]` });
      const own = await run(statsQuery(inputs, one, one.series[0]!));
      for (const st of ['count', 'vol', 'sharpe', 'mdd', 'skew', 'kurtosis', 'sortino', 'hit', 'var', 'es', 'best', 'worst'] as const) {
        const split = num(values(t, statName(st))[lines.indexOf(sym)]);
        const alone = num(values(own, statName(st))[0]);
        if (alone === null) assert.equal(split, null, `${sym}'s ${st}: none, as alone`);
        else close([split], [alone], `${sym}'s ${st}, split as alone`, 1e-12);
      }
    }
  });
});

// -- the review's findings, each pinned (docs/TIMESERIES_REVIEW_2026_10_25.md): the inputs its reviewer probed with, the
//    values worked by hand here --

describe('the review\'s findings on the numbers, each pinned', () => {
  // Q1: the dates were intersected before any function ran -- AAA's returns taken over CCC's calendar
  it('Q1: each operand on its own dates, then the dates both have (gs-quant): AAA\'s returns daily, CCC\'s every third day', async () => {
    const ownA = simple(AAA);
    const cDays = DAYS.filter((_, i) => i % 3 === 0);
    const ownC = simple(CCC.filter((_, i) => i % 3 === 0));
    const l = await line(spec({ A: 'returns(close[AAA]) - returns(close[CCC])' }));
    assert.deepEqual(l.dates, cDays);
    close(l.values, cDays.map((d, k) => (ownC[k] === null ? null : ownA[DAYS.indexOf(d)]! - ownC[k]!)), 'own-calendar spread', 1e-9);
    // and through refs: B and C each a series of its own
    const viaRefs = await line(spec({ A: '$B - $C', B: 'returns(close[AAA])', C: 'returns(close[CCC])' }));
    close(viaRefs.values, l.values, 'the same through refs', 1e-12);
  });

  // Q2: compare and the level statistics took every same-unit figure for a level
  it('Q2: compare rebases a level alone; a change, a difference, a spread of rebased lines drawn as they are', async () => {
    const rebased = (ts: TimeseriesSpec): TimeseriesSpec => ({ ...ts, view: { compare: 'rebase' } });
    const level = await line(rebased(spec({ A: 'close[AAA]' })));
    close(level.values, AAA.map((v) => (v / AAA[0]!) * 100), 'a level rebased', 1e-9);
    const change = await line(rebased(spec({ A: 'change(close[AAA])' })));
    close(change.values, AAA.map((v) => v - AAA[0]!), 'a change as it is', 1e-9);
    const diff = await line(rebased(spec({ A: 'diff(close[AAA])' })));
    close(diff.values, AAA.map((v, i) => (i === 0 ? null : v - AAA[i - 1]!)), 'a difference as it is', 1e-9);
    const spread = await line(rebased(spec({ A: 'rebase(close[AAA]) - rebase(close[BBB])' })));
    close(spread.values, AAA.map((v, i) => (v / AAA[0]!) * 100 - (BBB[i]! / BBB[0]!) * 100), 'a spread of rebased lines', 1e-9);
    // the statistics of a difference: no volatility, drawdown, growth or Sharpe ratio
    const ts = spec({ A: 'diff(close[AAA])' });
    const st = await run(statsQuery(inputs, ts, ts.series[0]!));
    for (const s of ['vol', 'mdd', 'cagr', 'sharpe'] as const) assert.ok(!st.columns.some((c) => c.name === statName(s)), `no ${s} of a difference`);
  });

  // the second audit's B1: a level shifted by a number -- index(close) - 1, gs-quant's cumulative return -- was compared
  // from its start of zero (every value none, unsaid) and given a level's statistics (a volatility of 1303%)
  it('a cumulative return (a level shifted) drawn as it is under compare, with no level statistics', async () => {
    const shifted = spec({ A: 'index(close[AAA]) - 1' }, { view: { compare: 'rebase' } });
    const got = await line(shifted);
    close(got.values, AAA.map((v) => v / AAA[0]! - 1), 'its own values, not divided by a start of zero', 1e-9);
    const st = await run(statsQuery(inputs, shifted, shifted.series[0]!));
    for (const s of ['vol', 'mdd', 'cagr', 'sharpe'] as const) assert.ok(!st.columns.some((c) => c.name === statName(s)), `no ${s} of a shifted level`);
  });

  // Q3: no frequency annualized by 252 whatever the dates were
  it('Q3: no frequency -- the periods a year the database infers from the dates: 252 for weekdays, none for every third weekday', async () => {
    const daily = await run(seriesQuery(inputs, spec({ A: 'volatility(close[AAA], 5)' }), spec({ A: 'volatility(close[AAA], 5)' }).series[0]!));
    assert.deepEqual([...new Set(values(daily, TS_PER_YEAR))], [252]);
    const sparse = spec({ A: 'volatility(close[CCC], 3)' });
    const t = await run(seriesQuery(inputs, sparse, sparse.series[0]!));
    assert.deepEqual([...new Set(values(t, TS_PER_YEAR))], [null], 'every third weekday (4.2 days apart) is no frequency');
    assert.ok(values(t, TS_VALUE).every((v) => v === null), 'so no annualized figure');
    // the statistics too: a level's, annualized by what the database inferred
    const st = await run(statsQuery(inputs, spec({ A: 'close[AAA]' }), spec({ A: 'close[AAA]' }).series[0]!));
    assert.equal(Number(values(st, TS_STAT_PER_YEAR)[0]), 252);
    // a frequency told: its periods a year, nothing inferred
    const weekly = spec({ A: 'volatility(close[AAA], 3)' }, { grain: 'week' });
    const w = await run(seriesQuery(inputs, weekly, weekly.series[0]!));
    assert.ok(!w.columns.some((c) => c.name === TS_PER_YEAR));
    // the second audit's S4: a combination of a daily part and one of no frequency is annualized by none -- its values
    // none, and said so -- not by its first part's 252
    const both = spec({ A: 'volatility(close[AAA], 5) - volatility(close[CCC], 3)' });
    const c = await run(seriesQuery(inputs, both, both.series[0]!));
    assert.deepEqual([...new Set(values(c, TS_PER_YEAR))], [null], 'one part no frequency: the whole none');
    const daily2 = spec({ A: 'volatility(close[AAA], 5) - volatility(close[BBB], 5)' });
    assert.deepEqual([...new Set(values(await run(seriesQuery(inputs, daily2, daily2.series[0]!)), TS_PER_YEAR))], [252]);
  });

  it('Q3: monthly rows with no frequency are annualized by 12', async () => {
    const months = [...Array.from({ length: 12 }, (_, i) => `2023-${String(i + 1).padStart(2, '0')}-28`), '2024-01-28'];
    const levels = [100, 101, 99, 103, 104, 102, 106, 107, 105, 109, 110, 108, 112];
    const csv = ['symbol,day,close', ...months.map((d, i) => `M,${d},${levels[i]}`)].join('\n');
    const r = await ingestFile(engine, files, new File([csv], 'monthly.csv'), writer, { table: 'q3_monthly' });
    const own = plannerFor(r.model, r.runtime);
    const monthly: TsInputs = new Map([['px', { snapshot: { ...input.snapshot, source: { query: r.source }, columns: input.snapshot.columns.filter((c) => c.name !== 'volume') }, filter: undefined }]]);
    const ts = spec({ A: 'volatility(close, 12, type=log)' });
    const t = await engine.execute(await own.plan(seriesQuery(monthly, ts, ts.series[0]!).query), 0);
    const logs = levels.slice(1).map((v, i) => Math.log(v / levels[i]!));
    const want = sd(logs) * Math.sqrt(12) * 100;
    close([num(values(t, TS_VALUE)[0])], [want], 'monthly volatility, by 12', 1e-9);
    assert.equal(Number(values(t, TS_PER_YEAR)[0]), 12);
  });

  // Q5: a split series whose column matched on the split's column kept only that line
  it('Q5: split by symbol, each line against one symbol\'s column (close / close[CCC]): every line, on the dates both have', async () => {
    const l = await line(spec({ A: 'close / close[CCC]' }, {}, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    const cIdx = DAYS.map((_, i) => i).filter((i) => i % 3 === 0);
    close(of('AAA'), cIdx.map((i) => AAA[i]! / CCC[i]!), 'AAA / CCC', 1e-9);
    close(of('BBB'), cIdx.map((i) => BBB[i]! / CCC[i]!), 'BBB / CCC', 1e-9);
    close(of('CCC'), cIdx.map(() => 1), 'CCC / CCC', 1e-9);
    // a split of one symbol's rows alone is refused, never drawn as one line
    assert.throws(() => seriesQuery(inputs, spec({ A: 'close[CCC]' }, {}, 'symbol'), spec({ A: 'close[CCC]' }, {}, 'symbol').series[0]!),
      /is split by symbol, but reads one symbol's rows alone/);
  });

  // the second audit's S2: split by a column that is not the instrument's, an operand naming one instrument
  // (close[CCC]) is one line every line divides by -- split by CCC's own group, every other group's line went
  it('S2: split by another column, an operand of one instrument is one line every line reads', async () => {
    const grouped: TsInputs = new Map([['px', { ...input, snapshot: { ...input.snapshot,
      derived: [{ name: 'grp', type: 'String', lambda: lambda(['x'], call('toLower', call('toOne', column('x', 'symbol')))) }] } }]]);
    const ts = spec({ A: 'close / close[CCC]' }, {}, 'grp');
    const q = seriesQuery(grouped, ts, ts.series[0]!);
    const t = await run(q);
    const groups = values(t, TS_SPLIT).map(String);
    assert.deepEqual([...new Set(groups)].sort(), ['aaa', 'bbb', 'ccc'], 'every group a line');
    const of = (g: string): Line => values(t, TS_VALUE).map(num).filter((_, i) => groups[i] === g).reverse();
    const cIdx = DAYS.map((_, i) => i).filter((i) => i % 3 === 0);
    close(of('aaa'), cIdx.map((i) => AAA[i]! / CCC[i]!), 'aaa / CCC', 1e-9);
    close(of('bbb'), cIdx.map((i) => BBB[i]! / CCC[i]!), 'bbb / CCC', 1e-9);
  });

  // the second audit's S1, as the user ruled it (2026-10-28, "match gsquant"): a level below zero (a grid's column of
  // yields, or of spreads, gone negative) compared from its start is gs-quant's x / x0 - 1, as percent_change -- and has
  // no figures of its returns, never a part above zero taken for the whole
  it('S1: a level below zero: compared as gs-quant\'s x / x0 - 1, and no volatility, drawdown, growth or Sharpe ratio', async () => {
    const below: TsInputs = new Map([['px', { ...input, snapshot: { ...input.snapshot,
      derived: [{ name: 'spread', type: 'Float', lambda: lambda(['x'], minus(call('toOne', column('x', 'close')), lit.integer(200))) }] } }]]);
    const ts = spec({ A: 'spread[AAA]' }, { view: { compare: 'percent' } });
    const q = seriesQuery(below, ts, ts.series[0]!);
    assert.equal(q.level, true, 'a column is a level');
    const got = values(await run(q), TS_VALUE).map(num).reverse();
    const spread = AAA.map((v) => v - 200);
    close(got, spread.map((v) => v / spread[0]! - 1), 'gs-quant\'s x / x0 - 1', 1e-9);
    const st = await run(statsQuery(below, ts, ts.series[0]!));
    for (const stat of ['vol', 'mdd', 'cagr', 'sharpe'] as const) assert.equal(values(st, statName(stat))[0], null, stat);
    // the third review's 5: a price compared as its % change from its start (zero there) has its own level's figures,
    // never blanked as "at or below zero"
    const plain = await run(statsQuery(inputs, spec({ A: 'close[AAA]' }), spec({ A: 'close[AAA]' }).series[0]!, { from: DAYS[5]! }));
    const pct = spec({ A: 'close[AAA]' }, { view: { compare: 'percent' } });
    const compared = await run(statsQuery(inputs, pct, pct.series[0]!, { from: DAYS[5]! }));
    // (its growth over these 35 weekdays none, compared or not: under a year, N3 below)
    for (const stat of ['vol', 'mdd', 'sharpe'] as const) {
      const want = Number(values(plain, statName(stat))[0]);
      assert.ok(Number.isFinite(want), `${stat} of the price`);
      close([Number(values(compared, statName(stat))[0])], [want], `${stat} compared as its % change`, 1e-9);
    }
    // above zero, as it was: x / x0, gs-quant's
    const up = spec({ A: 'close[AAA]' }, { view: { compare: 'rebase' } });
    close(values(await run(seriesQuery(inputs, up, up.series[0]!)), TS_VALUE).map(num).reverse(), AAA.map((v) => (v / AAA[0]!) * 100), 'rebased', 1e-9);
  });

  // Q6: a month dated at its start, valued at its end: a range from mid-month lost that month
  it('Q6: at a monthly frequency, an anchor mid-month starts at that month, whose value is of its last day', async () => {
    const months = [...new Set(DAYS.map((d) => d.slice(0, 7)))];
    const lastOf = (m: string): number => AAA[DAYS.map((d) => d.slice(0, 7)).lastIndexOf(m)]!;
    const ts = spec({ A: 'rebase(close[AAA])' }, { grain: 'month' });
    const l = await line(ts, 'A', { from: '2024-01-15' });
    close(l.values, months.map((m) => (lastOf(m) / lastOf(months[0]!)) * 100), 'January the base', 1e-9);
    const st = await run(statsQuery(inputs, ts, ts.series[0]!, { from: '2024-01-15' }));
    assert.equal(Number(values(st, statName('count'))[0]), months.length, 'January among the dates shown');
  });

  // the second audit's S3: a range starting just after a period's last observation -- 1Y from a Sunday, YTD from the
  // 1st -- is a change from that last observation, the value as of its start, not from the first period after it
  it('S3: an anchor between observations reads the value as of it: the last before, at any frequency', async () => {
    const months = [...new Set(DAYS.map((d) => d.slice(0, 7)))];
    const lastOf = (m: string): number => AAA[DAYS.map((d) => d.slice(0, 7)).lastIndexOf(m)]!;
    const monthly = spec({ A: 'rebase(close[AAA])' }, { grain: 'month' });
    // February's first day: January's month-end the base, so February's point carries February's move
    const l = await line(monthly, 'A', { from: '2024-02-01' });
    close(l.values, months.map((m) => (lastOf(m) / lastOf('2024-01')) * 100), 'January the base', 1e-9);
    // daily, from a Saturday: the Friday before it the base
    const saturday = '2024-01-13';
    const friday = DAYS.filter((d) => d < saturday).length - 1;
    const daily = await line(spec({ A: 'percent_change(close[AAA])' }), 'A', { from: saturday });
    close(daily.values, AAA.map((v) => v / AAA[friday]! - 1), 'the Friday before the base', 1e-9);
    // an anchor before the first date: the first date the base
    const early = await line(spec({ A: 'rebase(close[AAA])' }), 'A', { from: '2023-06-01' });
    close(early.values, AAA.map((v) => (v / AAA[0]!) * 100), 'its first date the base', 1e-9);
  });

  // Q7: which of several rows a period's value was, said only for the first source of no split and no match
  it('Q7: the rows a period\'s value was one of, counted for every column: two instruments a day, one', async () => {
    const all = await run(seriesQuery(inputs, spec({ A: 'close' }), spec({ A: 'close' }).series[0]!));
    assert.ok(values(all, TS_ROWS).some((n) => Number(n) >= 2), 'close of every instrument: several a day');
    const one = await run(seriesQuery(inputs, spec({ A: 'close[AAA] / close' }), spec({ A: 'close[AAA] / close' }).series[0]!));
    assert.ok(values(one, TS_ROWS).some((n) => Number(n) >= 2), 'said though another column is matched');
    const matched = await run(seriesQuery(inputs, spec({ A: 'close[AAA]' }), spec({ A: 'close[AAA]' }).series[0]!));
    assert.deepEqual([...new Set(values(matched, TS_ROWS).map(Number))], [1]);
  });

  // Q8: prices of a return of -100% or less: the product, not the log's sum that skipped it
  it('Q8: prices of returns compounds every factor: a -100% return zero from there, one below it turning the sign', async () => {
    const zero = await line(spec({ A: 'prices(returns(close[AAA]) * 0 - 1, 10)' }));
    close(zero.values, AAA.map((_, i) => (i === 0 ? 10 : 0)), 'a total loss', 1e-12);
    const sign = await line(spec({ A: 'prices(returns(close[AAA]) * 0 - 2, 10)' }));
    close(sign.values, AAA.map((_, i) => 10 * (-1) ** i), 'each factor -1', 1e-9);
  });

  // Q9: a candle series' least and most read off its closes
  it('Q9: a candle series\' statistics: its lowest low and highest high', async () => {
    const ts = spec({ A: 'ohlc(close[AAA], close[AAA], close[AAA], close[AAA])' }, { grain: 'week' });
    const st = await run(statsQuery(inputs, ts, ts.series[0]!));
    close([num(values(st, statName('low'))[0]), num(values(st, statName('high'))[0])], [Math.min(...AAA), Math.max(...AAA)], 'extremes', 1e-12);
  });

  // the conversion's second review (2026-10-28): a candle's lows and highs apart from its closes -- the statistics' least
  // its lowest low and when, its most its highest high and when, its first and last its closes (summaryStatistics' low,
  // high and value, each its own)
  it('a candle\'s statistics: its least and most its lows\' and highs\', at their own weeks; its first and last its closes', async () => {
    const ts = spec({ A: 'ohlc(close[AAA], close[AAA] + 1, close[AAA] - 2, close[AAA])' }, { grain: 'week' });
    const st = await run(statsQuery(inputs, ts, ts.series[0]!));
    const weekOf = (d: string): string => {
      const at = new Date(`${d}T00:00:00Z`);
      return new Date(at.getTime() - ((at.getUTCDay() + 6) % 7) * 86_400_000).toISOString().slice(0, 10);
    };
    const weeks = [...new Set(DAYS.map(weekOf))];
    const of = (w: string): number[] => DAYS.map((d, i) => [d, i] as const).filter(([d]) => weekOf(d) === w).map(([, i]) => i);
    const lows = weeks.map((w) => Math.min(...of(w).map((i) => AAA[i]! - 2)));
    const highs = weeks.map((w) => Math.max(...of(w).map((i) => AAA[i]! + 1)));
    const ats = weeks.map((w) => DAYS[of(w).at(-1)!]!);
    const [low, high] = [Math.min(...lows), Math.max(...highs)];
    const one = (stat: Parameters<typeof statName>[0]): unknown => values(st, statName(stat))[0];
    close([num(one('low')), num(one('high'))], [low, high], 'the lowest low and the highest high', 1e-9);
    assert.equal(String(one('lowAt')).slice(0, 10), ats[lows.indexOf(low)], 'the week of the lowest low');
    assert.equal(String(one('highAt')).slice(0, 10), ats[highs.indexOf(high)], 'the week of the highest high');
    close([num(one('first')), num(one('last'))], [AAA[of(weeks[0]!).at(-1)!]!, AAA.at(-1)!], 'its closes', 1e-12);
  });

  // a seasonality of a whole number (a volume): each week's last, as it is
  it('a seasonality of a volume: each week its last volume', async () => {
    const ts = spec({ A: 'volume[AAA]' });
    const t = await run(seasonalityQuery(inputs, ts, ts.series[0]!, 1));
    const weekOf = (d: string): number => {
      const day = (Date.parse(`${d}T00:00:00Z`) - Date.UTC(Number(d.slice(0, 4)), 0, 1)) / 86_400_000 + 1;
      return Math.floor((day - 1) / 7) + 1;
    };
    const lastOf = new Map<number, number>();
    DAYS.forEach((d, i) => lastOf.set(weekOf(d), VOL[i]!));
    close(values(t, TS_VALUE).map(num), [...lastOf.values()], 'each week\'s last volume', 0);
  });

  // Q11, as the user ruled it (2026-10-28, "match gsquant"): the % change from a start below zero is gs-quant's
  // x / x0 - 1, as Compare's and percent_change's
  it('Q11: the % change from a start below zero is gs-quant\'s x / x0 - 1; a rolling drawdown drawn once its window is full', async () => {
    const ts = spec({ A: 'close[AAA] - 200' });
    const st = await run(statsQuery(inputs, ts, ts.series[0]!));
    const [first, last] = [AAA[0]! - 200, AAA.at(-1)! - 200];
    close([num(values(st, statName('pct'))[0])], [last / first - 1], 'pct', 1e-9);
    const dd = await line(spec({ A: 'drawdown(close[AAA], 5)' }));
    assert.deepEqual(dd.values.slice(0, 4), [null, null, null, null]);
    assert.notEqual(dd.values[4], null);
  });

  // G2: a date of another type than a date was read as one
  it('G2: a series along a column that is no date is refused by name', () => {
    const bad: TimeseriesSpec = { ...spec({ A: 'close' }), sources: [{ alias: 'px', cube: 'prices', date: 'symbol', key: 'symbol' }] };
    assert.throws(() => seriesQuery(inputs, bad, bad.series[0]!), /symbol is a String, not a date/);
    const none: TimeseriesSpec = { ...spec({ A: 'close' }), sources: [{ alias: 'px', cube: 'prices', date: 'nope' }] };
    assert.throws(() => seriesQuery(inputs, none, none.series[0]!), /nope is no column of its grid/);
  });
});

// -- a series combining grids (D2): their tables over one model, each grouped by its own date, joined on the period --

describe('a series combining two grids, planned over one model of their tables (§5.2)', () => {
  // prices by weekday; a rates grid of its own, its own date and instrument columns, on fewer days and one more
  const RATE_DAYS = DAYS.filter((_, i) => i % 2 === 0);
  const TEN = RATE_DAYS.map((_, i) => round(4 + 0.05 * Math.sin(i) - 0.002 * i));
  const TWO = RATE_DAYS.map((_, i) => round(4.5 - 0.004 * i));
  let joint: TsInputs;
  let jointPlanner: LitePlanner;
  const file = (name: string, columns: [string, string][]): FileSource => ({
    _type: 'file', name: `${name}.csv`, format: 'csv', size: 1, sha256: name.padEnd(64, '0'),
    columns: columns.map(([n, type]) => ({ name: n, type })),
  });
  const PX = file('px_t', [['symbol', 'String'], ['day', 'StrictDate'], ['close', 'Float']]);
  const RATES = file('rates_t', [['tenor', 'String'], ['obs_date', 'StrictDate'], ['yield', 'Float']]);
  const COMBINED: TimeseriesSpec = {
    sources: [
      { alias: 'px', cube: 'grid-1', date: 'day', key: 'symbol' },
      { alias: 'rates', cube: 'grid-2', date: 'obs_date', key: 'tenor' },
    ],
    series: [],
  };
  const combined = (exprs: Record<string, string>, split?: string): TimeseriesSpec => ({
    ...COMBINED,
    series: Object.entries(exprs).map(([id, text]): TsSeries => ({ id, expr: parseExpr(text), ...(split ? { split } : {}) })),
  });
  const joinedLine = async (ts: TimeseriesSpec, id = 'A'): Promise<{ dates: string[]; values: Line; split?: unknown[] }> => {
    const series = ts.series.find((x) => x.id === id)!;
    const q = seriesQuery(joint, ts, series);
    const t = await engine.execute(await jointPlanner.plan(q.query), 0);
    return {
      dates: values(t, TS_DATE).map((d) => String(d).slice(0, 10)).reverse(),
      values: values(t, TS_VALUE).map(num).reverse(),
      ...(q.split !== undefined ? { split: values(t, TS_SPLIT).reverse() } : {}),
    };
  };

  before(async () => {
    await engine.run('CREATE TABLE px_t (symbol VARCHAR, day DATE, close DOUBLE)', 0);
    await engine.run(`INSERT INTO px_t VALUES ${DAYS.flatMap((d, i) => [`('AAA', '${d}', ${AAA[i]})`, `('BBB', '${d}', ${BBB[i]})`]).join(', ')}`, 0);
    await engine.run('CREATE TABLE rates_t (tenor VARCHAR, obs_date DATE, yield DOUBLE)', 0);
    await engine.run(`INSERT INTO rates_t VALUES ${RATE_DAYS.flatMap((d, i) => [`('10Y', '${d}', ${TEN[i]})`, `('2Y', '${d}', ${TWO[i]})`]).join(', ')}, ('10Y', '2024-12-31', 9)`, 0);
    const models = plannerFor('', 'local::RT');
    const opening: JoinOpening = {
      here: TAB_TABLES, tables: ['px_t', 'rates_t'], models, databaseType: 'DuckDB',
      catalog: (table) => catalogColumns(engine, models, table),
      typer: (model, runtime) => plannerFor(model, runtime),
    };
    const opened = await openSources([PX, RATES], opening);
    jointPlanner = plannerFor(opened.model, opened.runtime);
    const grid = (query: (typeof opened.relations)[number], f: FileSource): TsInput => ({
      snapshot: {
        source: { query }, columns: f.columns.map((c) => ({ name: c.name, type: c.type })), derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
      },
      filter: undefined,
    });
    joint = new Map([['px', grid(opened.relations[0]!, PX)], ['rates', grid(opened.relations[1]!, RATES)]]);
  });

  it('reads both sources, the first first', () => {
    const ts = combined({ A: 'px:close[AAA] / rates:yield[10Y]', B: 'rates:yield[2Y]' });
    assert.deepEqual(seriesSources(ts, ts.series[0]!), ['px', 'rates']);
    assert.deepEqual(seriesSources(ts, ts.series[1]!), ['rates']);
  });

  it('a ratio across grids, on the dates both have', async () => {
    const l = await joinedLine(combined({ A: 'px:close[AAA] / rates:yield[10Y]' }));
    assert.deepEqual(l.dates, RATE_DAYS);
    close(l.values, RATE_DAYS.map((d, i) => AAA[DAYS.indexOf(d)]! / TEN[i]!), 'AAA / 10Y', 1e-9);
  });

  it('a series of the second grid alone, over the joint model, is its own', async () => {
    const l = await joinedLine(combined({ A: 'rates:yield[2Y]' }));
    assert.deepEqual(l.dates, RATE_DAYS);
    close(l.values, TWO, '2Y');
  });

  // gs-quant's correlation: each series' returns on its own dates, then aligned on the dates both have (the review's Q1:
  // the dates were joined first, AAA's returns taken over the rates' calendar)
  it('a rolling correlation of two grids\' returns, each taken on its own dates, then joined', async () => {
    const own = simple(AAA);
    const a = RATE_DAYS.map((d) => own[DAYS.indexOf(d)]!);
    const corr = pairs(a, simple(TEN), 5, (p, q) => cov(p, q) / (sd(p) * sd(q)));
    close((await joinedLine(combined({ A: 'correlation(px:close[AAA], rates:yield[10Y], 5)' }))).values, corr, 'corr', 1e-7);
  });

  it('split by the first grid\'s instrument, each line against the second grid\'s one series', async () => {
    const l = await joinedLine(combined({ A: 'px:close - rates:yield[10Y]' }, 'symbol'));
    const of = (sym: string): Line => l.values.filter((_, i) => l.split![i] === sym);
    close(of('AAA'), RATE_DAYS.map((d, i) => AAA[DAYS.indexOf(d)]! - TEN[i]!), 'AAA - 10Y', 1e-9);
    close(of('BBB'), RATE_DAYS.map((d, i) => BBB[DAYS.indexOf(d)]! - TEN[i]!), 'BBB - 10Y', 1e-9);
  });

  it('at a monthly frequency, each grid\'s month-end, joined by month', async () => {
    const months = [...new Set(RATE_DAYS.map((d) => d.slice(0, 7)))];
    const lastOf = (dates: readonly string[], vals: readonly number[], m: string): number =>
      vals[dates.map((d) => d.slice(0, 7)).lastIndexOf(m)]!;
    const l = await joinedLine({ ...combined({ A: 'px:close[BBB] / rates:yield[10Y]' }), grain: 'month' });
    // each month dated at the later of the two grids' last observations in it
    const lastDay = (dates: readonly string[], m: string): string => dates.filter((d) => d.startsWith(m)).at(-1)!;
    assert.deepEqual(l.dates, months.map((m) => [lastDay(DAYS, m), lastDay(RATE_DAYS, m)].sort().at(-1)!));
    close(l.values, months.map((m) => lastOf(DAYS, BBB, m) / lastOf(RATE_DAYS, TEN, m)), 'monthly ratio', 1e-9);
  });
});

// THE MARKETS EXAMPLE AND THE TIMESERIES GALLERY (src/examples/markets.ts, timeseries-gallery.ts), end to end: their
// two samples read into the database as the tab reads them, typed as the pages declare them, and every series of every
// tile -- its statistics and its sources' bounds too -- planned by legend-lite's planner and answered by the database: a
// grid's alone by its own planner, the series across both grids over one model of their tables (openSources), as the
// tab runs them.
/** The gallery's series: every function at least once, and the ways to view (2026-10-26: 92, the regression tile's four;
 *  102, the exponential averages', volatilities' and MACD's ten; 106, the percentiles' four; 109, a condition's three;
 *  113, the Dates sheet's alignment and interpolate, four; 144, gs-quant's remaining functions, 31; 149, the other
 *  views' sheet, five: the next phases' 3; 171, an analyst's packs, 22: the next phases' 4; 173, the On the chart
 *  sheet's two: Phase 5). */
const GALLERY_SERIES = 173;

describe('the timeseries examples: every series of every tile, on its own samples', () => {
  const pages = [
    ['the markets example', 24, async () => (await import('../src/examples/markets.ts')).marketsPage()],
    ['the timeseries gallery', GALLERY_SERIES, async () => (await import('../src/examples/timeseries-gallery.ts')).timeseriesGalleryPage()],
  ] as const;
  for (const [what, count, build] of pages) it(`${what}: the samples read as the page declares them, and every query plans and answers`, async () => {
    const { MACRO_SPECS, MARKET_SPECS, RATE_SPECS } = await import('../src/examples/markets.ts');
    const { macroCsv, marketsCsv, ratesCsv } = await import('../src/samples.ts');
    const db = await openDatabase();
    const local = new DuckDbEngine(db.connection);
    try {
      const page = await build();
      const grids = new Map<string, { input: TsInput; planner: LitePlanner; table: string }>();
      // each grid the page holds, its sample read in
      const samples = ([['prices', marketsCsv(), MARKET_SPECS], ['rates', ratesCsv(), RATE_SPECS], ['macro', macroCsv(), MACRO_SPECS]] as const)
        .filter(([cube]) => page.cubes.some((c) => c.id === cube));
      for (const [cube, csv, specs] of samples) {
        const r = await ingestFile(local, db.files, new File([csv], `sample-${cube}.csv`), writer, { table: `ex_${cube}` });
        const own = plannerFor(r.model, r.runtime);
        const all = await local.execute(await own.plan(from(r.source).limit(1).lambda()), 0);
        assert.deepEqual(all.columns.map((c) => `${c.name}:${c.type}`), specs.map((c) => `${c.name}:${c.type}`), `${cube}: typed as the page says`);
        const saved = page.cubes.find((c) => c.id === cube)!.cube.query;
        grids.set(cube, { input: { snapshot: { ...saved, source: { query: r.source }, epoch: 1 }, filter: undefined }, planner: own, table: r.table });
      }
      // the grids over one model of their tables, for the series that read several
      const cubes = samples.map(([cube]) => cube);
      const sources = cubes.map((c) => page.cubes.find((x) => x.id === c)!.cube.source);
      const opened = await openSources(sources, {
        here: TAB_TABLES, tables: cubes.map((c) => grids.get(c)!.table), models: writer, databaseType: 'DuckDB',
        catalog: (table) => catalogColumns(local, writer, table),
        typer: (model, runtime) => plannerFor(model, runtime),
      });
      const joint = plannerFor(opened.model, opened.runtime);
      const together = new Map<string, TsInput>(cubes.map((c, i) => [c, { ...grids.get(c)!.input, snapshot: { ...grids.get(c)!.input.snapshot, source: { query: opened.relations[i]! } } }] as const));
      let ran = 0;
      for (const view of page.views) {
        if (view.kind !== 'timeseries') continue;
        const ts = view.spec;
        const cubeOf = (alias: string): string => ts.sources.find((x) => x.alias === alias)!.cube;
        for (const src of ts.sources) {
          const g = grids.get(src.cube)!;
          const t = await local.execute(await g.planner.plan(boundsQuery(g.input, src)), 0);
          assert.ok(t.rowCount === 1 && values(t, TS_LATEST)[0] !== null, `${view.id}: ${src.alias}'s bounds`);
        }
        for (const series of ts.series) {
          const aliases = seriesSources(ts, series);
          const [inputsOf, plannerOf] = aliases.length > 1
            ? [new Map(aliases.map((a) => [a, together.get(cubeOf(a))!] as const)), joint]
            : [new Map([[aliases[0]!, grids.get(cubeOf(aliases[0]!))!.input]]), grids.get(cubeOf(aliases[0]!))!.planner];
          const q = seriesQuery(inputsOf, ts, series);
          const t = await local.execute(await plannerOf.plan(q.query), 0);
          const shown = values(t, TS_VALUE).filter((v) => v !== null && Number.isFinite(Number(v)));
          // five years of days (or weeks), or of months
          assert.ok(shown.length > (ts.grain === 'month' ? 40 : 100), `${view.id} ${series.id} (${series.name}): ${shown.length} values`);
          if (!q.band) {
            const st = await local.execute(await plannerOf.plan(statsQuery(inputsOf, ts, series, {}).query), 0);
            assert.ok(st.rowCount >= 1, `${view.id} ${series.id}: its statistics`);
          }
          ran += 1;
        }
        // a tile drawn otherwise than along time: its view's query too, by the rule the screen's view reads its series
        const mode = ts.view?.mode;
        if (mode !== undefined) {
          const picked = viewSeries(ts);
          const named = (id: string | undefined): TsSeries => ts.series.find((x) => x.id === id)!;
          const ofOne = (one: TsSeries): [TsInputs, LitePlanner] => {
            const alias = seriesSources(ts, one)[0]!;
            return [new Map([[alias, grids.get(cubeOf(alias))!.input]]), grids.get(cubeOf(alias))!.planner];
          };
          const [one, other] = mode === 'scatter' ? [named(picked.scatter?.[0]), named(picked.scatter?.[1])] : [named(mode === 'curve' ? picked.curve : picked.one), undefined];
          const [inputsOf, plannerOf] = ofOne(one);
          const q = mode === 'scatter' ? scatterQuery(inputsOf, ts, one, other!) : mode === 'seasonality' ? seasonalityQuery(inputsOf, ts, one)
            : mode === 'histogram' ? histogramQuery(inputsOf, ts, one) : curveQuery(inputsOf, ts, one);
          const t = await local.execute(await plannerOf.plan(q.query), 0);
          assert.ok(t.rowCount > 3, `${view.id}'s ${mode}: ${t.rowCount} rows`);
        }
        // each trend projected (Phase 5): its query too, its one row a fit
        for (const [i, a] of (ts.annotations ?? []).entries()) {
          if (a.kind !== 'projection') continue;
          const one = ts.series.find((x) => x.id === a.series)!;
          const alias = seriesSources(ts, one)[0]!;
          const g = grids.get(cubeOf(alias))!;
          const t = await local.execute(await g.planner.plan(projectionQuery(new Map([[alias, g.input]]), ts, a).query), 0);
          assert.ok(t.rowCount === 1 && values(t, TS_PJ_SLOPE)[0] !== null, `${view.id}'s projection ${i + 1}`);
        }
        // and every level's calendar of returns (each tile's Calendar tab)
        for (const id of viewSeries(ts).calendar.slice(0, 1)) {
          const one = ts.series.find((x) => x.id === id)!;
          const aliases = seriesSources(ts, one);
          if (aliases.length > 1) continue;
          const g = grids.get(cubeOf(aliases[0]!))!;
          const t = await local.execute(await g.planner.plan(calendarQuery(new Map([[aliases[0]!, g.input]]), ts, one).query), 0);
          assert.ok(t.rowCount > 0, `${view.id} ${id}'s calendar`);
        }
      }
      assert.equal(ran, count, 'every series of the page');
    } finally {
      await local.close();
    }
  });
});

