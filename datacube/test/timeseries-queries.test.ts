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
import { type TimeseriesSpec, type TsSeries } from '../src/timeseries-spec.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import {
  boundsQuery, keyValuesQuery, rangeStartName, seriesQuery, seriesSources, statName, statsQuery, TS_DATE, TS_EARLIEST, TS_HIGH, TS_LATEST, TS_LOW, TS_LOWER, TS_OPEN,
  TS_PER_YEAR, TS_ROWS, TS_SPLIT, TS_STAT_PER_YEAR, TS_UPPER, TS_VALUE, type TsInput, type TsInputs, type TsWindow,
} from '../src/timeseries-query.ts';
import { TAB_TABLES, openSources, type JoinOpening } from '../src/join-open.ts';
import type { FileSource } from '../src/cube-document.ts';
import { catalogColumns } from '../src/upload.ts';
import { col as column, fn as call, from, lambda, lit, minus } from '../../pure-protocol/src/index.ts';

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

  it('RSI of simple averages of the gains and the losses', async () => {
    const d = zip(AAA, lagged(AAA, 1), (v, p) => v - p);
    const want = d.map((_, i) => {
      if (i < 6) return null;
      const win = d.slice(i - 5, i + 1) as number[];
      const up = avg(win.map((v) => Math.max(v, 0)));
      const down = avg(win.map((v) => Math.max(-v, 0)));
      return down === 0 ? 100 : 100 - 100 / (1 + up / down);
    });
    close((await line(spec({ A: 'rsi(close[AAA], 6)' }))).values, want, 'rsi', 1e-7);
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

  it('volatility over 1m: its returns\' deviation over each month back, annualized, from its first return a month on', async () => {
    const l = await line(spec({ A: 'volatility(close[AAA], 1m)' }));
    const r = simple(AAA);
    const start = onBy(DAYS[1]!, 1, 'm');
    close(l.values, DAYS.map((d) => {
      if (d < start) return null;
      const vals = r.filter((v, j) => v !== null && DAYS[j]! > backBy(d, 1, 'm') && DAYS[j]! <= d) as number[];
      return vals.length < 2 ? null : sd(vals) * Math.sqrt(252) * 100;
    }), 'volatility 1m', 1e-9);
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

  // the second audit's S1: a level below zero (a grid's column of yields, or of spreads, gone negative) compared from
  // its start rises as a rise -- its change over its start's size, the statistics' rule -- and has no figures of its
  // returns, never a sign turned or a part above zero taken for the whole
  it('S1: a level below zero: compared as a rise where it rises, and no volatility, drawdown, growth or Sharpe ratio', async () => {
    const below: TsInputs = new Map([['px', { ...input, snapshot: { ...input.snapshot,
      derived: [{ name: 'spread', type: 'Float', lambda: lambda(['x'], minus(call('toOne', column('x', 'close')), lit.integer(200))) }] } }]]);
    const ts = spec({ A: 'spread[AAA]' }, { view: { compare: 'percent' } });
    const q = seriesQuery(below, ts, ts.series[0]!);
    assert.equal(q.level, true, 'a column is a level');
    const got = values(await run(q), TS_VALUE).map(num).reverse();
    const spread = AAA.map((v) => v - 200);
    close(got, spread.map((v) => (v - spread[0]!) / Math.abs(spread[0]!)), 'its change over its start\'s size', 1e-9);
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

  // Q11: a change from a start below zero
  it('Q11: the % change from a start below zero rising is a rise; a rolling drawdown drawn once its window is full', async () => {
    const ts = spec({ A: 'close[AAA] - 200' });
    const st = await run(statsQuery(inputs, ts, ts.series[0]!));
    const [first, last] = [AAA[0]! - 200, AAA.at(-1)! - 200];
    close([num(values(st, statName('pct'))[0])], [(last - first) / Math.abs(first)], 'pct', 1e-9);
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
/** The gallery's series: every function at least once, and the ways to view. */
const GALLERY_SERIES = 88;

describe('the timeseries examples: every series of every tile, on its own samples', () => {
  const pages = [
    ['the markets example', 24, async () => (await import('../src/examples/markets.ts')).marketsPage()],
    ['the timeseries gallery', GALLERY_SERIES, async () => (await import('../src/examples/timeseries-gallery.ts')).timeseriesGalleryPage()],
  ] as const;
  for (const [what, count, build] of pages) it(`${what}: the samples read as the page declares them, and every query plans and answers`, async () => {
    const { MARKET_SPECS, RATE_SPECS } = await import('../src/examples/markets.ts');
    const { marketsCsv, ratesCsv } = await import('../src/samples.ts');
    const db = await openDatabase();
    const local = new DuckDbEngine(db.connection);
    try {
      const page = await build();
      const grids = new Map<string, { input: TsInput; planner: LitePlanner; table: string }>();
      for (const [cube, csv, specs] of [['prices', marketsCsv(), MARKET_SPECS], ['rates', ratesCsv(), RATE_SPECS]] as const) {
        const r = await ingestFile(local, db.files, new File([csv], `sample-${cube}.csv`), writer, { table: `ex_${cube}` });
        const own = plannerFor(r.model, r.runtime);
        const all = await local.execute(await own.plan(from(r.source).limit(1).lambda()), 0);
        assert.deepEqual(all.columns.map((c) => `${c.name}:${c.type}`), specs.map((c) => `${c.name}:${c.type}`), `${cube}: typed as the page says`);
        const saved = page.cubes.find((c) => c.id === cube)!.cube.query;
        grids.set(cube, { input: { snapshot: { ...saved, source: { query: r.source }, epoch: 1 }, filter: undefined }, planner: own, table: r.table });
      }
      // the two grids over one model of their tables, for the series that read both
      const sources = ['prices', 'rates'].map((c) => page.cubes.find((x) => x.id === c)!.cube.source);
      const opened = await openSources(sources, {
        here: TAB_TABLES, tables: [grids.get('prices')!.table, grids.get('rates')!.table], models: writer, databaseType: 'DuckDB',
        catalog: (table) => catalogColumns(local, writer, table),
        typer: (model, runtime) => plannerFor(model, runtime),
      });
      const joint = plannerFor(opened.model, opened.runtime);
      const together = new Map(['prices', 'rates'].map((c, i) => [c, { ...grids.get(c)!.input, snapshot: { ...grids.get(c)!.input.snapshot, source: { query: opened.relations[i]! } } }] as const));
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
      }
      assert.equal(ran, count, 'every series of the page');
    } finally {
      await local.close();
    }
  });
});
