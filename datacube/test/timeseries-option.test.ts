// A TIMESERIES TILE AS A DRAWING (timeseries-option.ts; docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §6), in node: the
// lines made of what each series' query answered -- its rows latest first, made oldest first; a split's a line per
// value, named by it; a band's edges -- and the ECharts option of them: a grid per pane, each with its time axis, a value
// axis per unit (a second on the right, its ticks in its line's colour), a bounded figure's axis whole, room on the right
// for each line's latest value, the annotations over the panes, the navigator under the last.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { LIGHT_THEME } from '../src/chart-option.ts';
import {
  bollinger_bands, column, index, ohlc, relative_strength_index, returns as returnsOf, series as seriesOf, volatility, zscores, type Term,
} from '../src/timeseries-builder.ts';
import { curveColumns, TS_SPLIT as SPLIT } from '../src/timeseries-query.ts';
import { curveNotes, curveOption } from '../src/timeseries-views.ts';
import { drawingsUnplaced, marksUnplaced, projectionsUnplaced, tickText, timeseriesCsv, timeseriesOption, timeseriesTable, timeseriesTsv, tsHeading, tsLines, tsMarks, tsPictureHeight, tsProjected, valueText, type TsResult } from '../src/timeseries-option.ts';
import {
  TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_PER_YEAR, TS_PJ_END, TS_PJ_END_VALUE, TS_PJ_FROM, TS_PJ_FROM_VALUE, TS_PJ_LAST, TS_PJ_LAST_VALUE, TS_PJ_N,
  TS_PJ_SLOPE, TS_SPLIT, TS_TEXT, TS_UPPER, TS_VALUE, TS_WIDTH, type TsQuery,
} from '../src/timeseries-query.ts';
import { TS_MARK_LIMIT, TS_POINT_LIMIT, type TimeseriesSpec, type TsSeries } from '../src/timeseries-spec.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import type { TsUnit } from '../src/timeseries-expr.ts';
import { writtenText } from '../src/timeseries-text.ts';

const DAYS = ['2024-01-04', '2024-01-03', '2024-01-02'];

/** What a series' query answered: its columns, latest first, as the database gives them. */
function answered(series: string, unit: TsUnit, columns: Record<string, readonly (string | number | null)[]>,
  more: Partial<TsQuery> = {}): TsResult {
  const query: TsQuery = { series, query: {} as Lambda, unit, level: unit === 'same' || unit === 'index', band: false, counted: false, sources: ['px'], ...more };
  const names = Object.keys(columns);
  const table: ResultTable = {
    columns: names.map((name) => ({ name, type: name === TS_DATE ? 'StrictDate' : 'Float', values: [...columns[name]!] })),
    rowCount: columns[names[0]!]!.length, epoch: 0, elapsedMs: 0,
  };
  return { query, table };
}

const S = (id: string, t: Term, rest: Omit<TsSeries, 'id' | 'text'> = {}): TsSeries => ({ id, text: writtenText(t.expr, []), ...rest });

function spec(series: TsSeries[], more: Partial<TimeseriesSpec> = {}): TimeseriesSpec {
  return { sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }], series, ...more };
}

describe('the lines, from what the database answered', () => {
  it('a series\' rows made oldest first, its unit its query\'s, its colour its place', () => {
    const ts = spec([S('A', column('close', 'SPX')), S('B', volatility(seriesOf('A'), { w: 2, returns_type: 'simple' }))]);
    const { lines, notes } = tsLines(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'percent', { [TS_DATE]: DAYS, [TS_VALUE]: [20, null, null] }),
    ], LIGHT_THEME);
    assert.deepEqual(notes, []);
    assert.deepEqual(lines[0]!.points, [['2024-01-02', 1], ['2024-01-03', 2], ['2024-01-04', 3]]);
    assert.deepEqual(lines.map((l) => [l.key, l.unit, l.color]), [['A', 'same', LIGHT_THEME.series[0]], ['B', 'percent', LIGHT_THEME.series[1]]]);
    // a level is written as its grid writes its column; a figure by its unit
    assert.equal(lines[0]!.formatColumn, 'close');
    assert.equal(valueText(lines[1]!, 20), '20.00%');
    // a plain number to its places; a small one (a covariance of daily returns) to three significant places, not zero
    assert.deepEqual([1.23456, 0.000123456, 0, -0.0000001234].map((v) => valueText({ unit: 'number' }, v)), ['1.235', '0.000123', '0.000', '-1.23E-7']);
  });

  it('a split series: a line per value, in the order a person reads them (2Y before 10Y), each named by it', () => {
    const ts = spec([S('A', column('yield'), { name: 'Yield', split: 'tenor' }), S('B', column('close'), { split: 'symbol' })]);
    const { lines } = tsLines(ts, [
      answered('A', 'same', { [TS_DATE]: ['2024-01-02', '2024-01-02'], [TS_VALUE]: [4.1, 4.4], [TS_SPLIT]: ['10Y', '2Y'] }, { split: 'tenor' }),
      answered('B', 'same', { [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [100], [TS_SPLIT]: ['SPX'] }, { split: 'symbol' }),
    ], LIGHT_THEME);
    assert.deepEqual(lines.map((l) => l.name), ['Yield 2Y', 'Yield 10Y', 'SPX']);
  });

  // the review's Q12: a cut keeps whole dates, the date it crossed at last (timeseries-query.ts seriesQuery) -- that
  // date left out of the drawing, so every line ends at the same date
  it('a split series over the point limit: the date the cut crossed at left out, every line from the same date', () => {
    const ts = spec([S('A', column('close'), { split: 'symbol' })]);
    // two lines a date, the limit's dates whole, and one more date of them (the crossed): latest first
    const n = TS_POINT_LIMIT / 2 + 1;
    const day = (i: number): string => new Date(Date.UTC(2000, 0, 1) + (n - i) * 86_400_000).toISOString().slice(0, 10);
    const dates = Array.from({ length: n }, (_, i) => [day(i), day(i)]).flat();
    const { lines, notes } = tsLines(ts, [answered('A', 'same', {
      [TS_DATE]: dates, [TS_VALUE]: dates.map((_, i) => i), [TS_SPLIT]: dates.map((_, i) => (i % 2 === 0 ? 'X' : 'Y')),
    }, { split: 'symbol' })], LIGHT_THEME);
    assert.deepEqual(lines.map((l) => [l.points.length, l.points[0]![0]]), [[n - 1, day(n - 2)], [n - 1, day(n - 2)]], 'both from the same date');
    assert.deepEqual(notes, [`close: its latest dates, ${TS_POINT_LIMIT.toLocaleString(UI_LOCALE)} points (it has more).`]);
  });

  it('a band: its edges and its width, point by point', () => {
    const ts = spec([S('A', column('close')), S('B', bollinger_bands(seriesOf('A'), { w: 2, k: 1 }))]);
    const { lines } = tsLines(ts, [
      answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, null], [TS_LOWER]: [2, 1, null], [TS_UPPER]: [4, 3, null], [TS_WIDTH]: [2, 2, null] }, { band: true }),
    ], LIGHT_THEME);
    assert.equal(lines[0]!.band, true);
    assert.deepEqual([lines[0]!.lower, lines[0]!.upper, lines[0]!.width], [[null, 1, 2], [null, 3, 4], [null, 2, 2]]);
  });

  it('as CSV: a row per line and date, the rows the database answered', () => {
    const ts = spec([S('A', column('close', 'SPX'), { name: 'SPX' })]);
    const { lines } = tsLines(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME);
    const csv = timeseriesCsv(lines).trim().split('\n');
    assert.equal(csv.length, 4, 'a header and a row per date');
    assert.ok(csv.slice(1).every((row) => row.includes('SPX')));
  });

  // the next phases' 2 (2026-10-26): the statistics panel's Data tab and its Copy
  it('side by side: a row per date any line has, oldest first; a band its edges, candles their prices; none where none', () => {
    const ts = spec([S('A', column('close', 'SPX'), { name: 'SPX' }), S('B', bollinger_bands(seriesOf('A'), { w: 2, k: 1 }), { name: 'Band' }), S('C', column('close', 'NDX'), { name: 'NDX' })]);
    const { lines } = tsLines(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'same', { [TS_DATE]: DAYS.slice(0, 2), [TS_VALUE]: [3, 2], [TS_LOWER]: [2, 1], [TS_UPPER]: [4, 3], [TS_WIDTH]: [2, 2] }, { band: true }),
      answered('C', 'same', { [TS_DATE]: ['2024-01-05', '2024-01-02'], [TS_VALUE]: [9, 7] }),
    ], LIGHT_THEME);
    const t = timeseriesTable(lines);
    assert.deepEqual(t.columns.map((c) => c.name), ['SPX', 'Band lower', 'Band upper', 'NDX']);
    assert.deepEqual(t.rows, [
      { date: '2024-01-02', values: [1, null, null, 7] },
      { date: '2024-01-03', values: [2, 1, 3, null] },
      { date: '2024-01-04', values: [3, 2, 4, null] },
      { date: '2024-01-05', values: [null, null, null, 9] },
    ]);
    // the dates shown alone
    assert.deepEqual(timeseriesTable(lines, { from: '2024-01-03', to: '2024-01-04' }).rows.map((r) => r.date), ['2024-01-03', '2024-01-04']);
    assert.equal(timeseriesTsv(timeseriesTable(lines, { to: '2024-01-02' })), 'date\tSPX\tBand lower\tBand upper\tNDX\n2024-01-02\t1\t\t\t7\n');
    const candles = spec([S('A', ohlc(column('open'), column('high'), column('low'), column('close')), { name: 'K' })]);
    const k = tsLines(candles, [answered('A', 'same', { [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [4], [TS_OPEN]: [1], [TS_HIGH]: [5], [TS_LOW]: [0.5] }, { candles: true })], LIGHT_THEME).lines;
    assert.deepEqual(timeseriesTable(k).columns.map((c) => c.name), ['K open', 'K high', 'K low', 'K close']);
    assert.deepEqual(timeseriesTable(k).rows[0]!.values, [1, 5, 0.5, 4]);
  });

  // the Phase 2 review's 7: a line's second value on a date was dropped without a word (Copy lost what the chart drew),
  // and one day written two ways -- a date, a date-time at midnight -- was two rows
  it('a line\'s two values on one date refused in words, never one dropped; one day written two ways, one row', () => {
    const one = spec([S('A', column('close', 'SPX'), { name: 'SPX' })]);
    const twice = tsLines(one, [answered('A', 'same', { [TS_DATE]: ['2024-01-02', '2024-01-02'], [TS_VALUE]: [2, 1] })], LIGHT_THEME).lines;
    assert.throws(() => timeseriesTable(twice), /SPX has 2 values on 2024-01-02: a table has a row a date/);
    const two = spec([S('A', column('close', 'SPX'), { name: 'SPX' }), S('B', column('close', 'NDX'), { name: 'NDX' })]);
    const mixed = tsLines(two, [
      answered('A', 'same', { [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [1] }),
      answered('B', 'same', { [TS_DATE]: ['2024-01-02T00:00:00'], [TS_VALUE]: [2] }),
    ], LIGHT_THEME).lines;
    assert.deepEqual(timeseriesTable(mixed).rows, [{ date: '2024-01-02', values: [1, 2] }]);
  });
});

// the Phase 3 review's 3: a curve drew a line's value of months before as of the date asked, and a date no line had a
// value by as an empty line -- neither said
describe('a curve\'s points of earlier days', () => {
  it('drawn hollow, their day in the tooltip, and said: an earlier day by line, a date of no line\'s value', () => {
    const [a0, a1] = [curveColumns(0), curveColumns(1)];
    const col_ = (name: string, values: readonly (string | number | null)[]) => ({ name, type: 'String', values: [...values] });
    const t: ResultTable = {
      columns: [col_(SPLIT, ['2Y', '10Y']), col_(a0.asked, ['2024-06-28', '2024-06-28']), col_(a0.value, [4.1, 4.4]), col_(a0.date, ['2024-06-28', '2023-12-29']),
        col_(a1.asked, ['2019-01-01', '2019-01-01']), col_(a1.value, [null, null]), col_(a1.date, [null, null])],
      rowCount: 2, epoch: 0, elapsedMs: 0,
    };
    assert.deepEqual(curveNotes(t, ['latest', '2019-01-01']), ['latest (2024-06-28): 10Y is of 2023-12-29.', '2019-01-01: no line has a value at or before it.']);
    const o = curveOption(spec([S('A', column('close'), { split: 'tenor' })]), t, ['latest', '2019-01-01'], 'Yield', { theme: LIGHT_THEME }) as Record<string, unknown>;
    const latest = (o['series'] as { data: { value: number; day: string; symbol?: string }[] }[])[0]!;
    assert.deepEqual(latest.data.map((d) => [d.value, d.day, d.symbol ?? 'circle']), [[4.1, '2024-06-28', 'circle'], [4.4, '2023-12-29', 'emptyCircle']]);
    const tip = (o['tooltip'] as { formatter: (items: unknown) => string }).formatter([{ seriesName: 'latest (2024-06-28)', axisValue: '10Y', data: latest.data[1] }]);
    assert.match(tip, /4\.4 \(of 2023-12-29\)/);
  });
});

describe('the drawing', () => {
  const draw = (ts: TimeseriesSpec, results: TsResult[]) => {
    const { lines } = tsLines(ts, results, LIGHT_THEME);
    return { lines, option: timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 600 }) as Record<string, unknown> };
  };

  it('a grid per pane, one time axis each; a second unit on its own axis, on the right', () => {
    const ts = spec([S('A', column('close', 'SPX')), S('B', volatility(seriesOf('A'), { w: 2, returns_type: 'simple' })), S('C', zscores(seriesOf('A')), { pane: 1 })]);
    const { option } = draw(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'percent', { [TS_DATE]: DAYS, [TS_VALUE]: [20, 19, null] }),
      answered('C', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [1, 0, -1] }),
    ]);
    const grids = option['grid'] as { top: number; height: number }[];
    const xAxes = option['xAxis'] as { type: string; gridIndex: number }[];
    const yAxes = option['yAxis'] as { gridIndex: number; position: string }[];
    assert.equal(grids.length, 2);
    assert.ok(grids[1]!.top > grids[0]!.top + grids[0]!.height, 'the second pane under the first');
    assert.deepEqual(xAxes.map((x) => [x.type, x.gridIndex]), [['time', 0], ['time', 1]]);
    assert.deepEqual(yAxes.map((y) => [y.gridIndex, y.position]), [[0, 'left'], [0, 'right'], [1, 'left']]);
    const series = option['series'] as { id: string; xAxisIndex: number; yAxisIndex: number }[];
    assert.deepEqual(series.map((s) => [s.id, s.xAxisIndex, s.yAxisIndex]), [['A', 0, 0], ['B', 0, 1], ['C', 1, 2]]);
    // one zoom for every pane, and its navigator
    const zoom = option['dataZoom'] as { xAxisIndex: number[]; type: string }[];
    assert.ok(zoom.some((z) => z.type === 'slider') && zoom.every((z) => z.xAxisIndex.length === 2));
  });

  // the timeseries gallery, 2026-10-25: a log pane of two prices, one on the right, drew the right one on a straight scale
  it('a log pane: its left axis and a right one of levels on a log scale; a ratio on the right keeps its zero', () => {
    const ts = spec([S('A', column('close', 'NVDA')), S('B', column('close', 'SPX'), { axis: 'right' })], { panes: [{ log: true }] });
    const levels = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [300, 200, 100] })];
    const types = (o: Record<string, unknown>): string[] => (o['yAxis'] as { type: string }[]).map((y) => y.type);
    assert.deepEqual(types(draw(ts, levels).option), ['log', 'log']);
    const ratio: TimeseriesSpec = { ...ts, series: [ts.series[0]!, S('B', returnsOf(seriesOf('A'), { obs: 1, type: 'simple' }), { axis: 'right' })] };
    assert.deepEqual(types(draw(ratio, [levels[0]!, answered('B', 'ratio', { [TS_DATE]: DAYS, [TS_VALUE]: [0.1, -0.2, null] })]).option), ['log', 'value']);
    // the review's G3: a figure that may be zero or below on the log pane's left axis -- dropped by a log axis without a
    // word -- keeps that axis straight, and the drawing says why
    const returns = spec([S('A', returnsOf(column('close', 'SPX'), { obs: 1, type: 'simple' }))], { panes: [{ log: true }] });
    const drawn = answered('A', 'ratio', { [TS_DATE]: DAYS, [TS_VALUE]: [0.01, -0.02, null] });
    assert.deepEqual(types(draw(returns, [drawn]).option), ['value']);
    assert.ok(tsLines(returns, [drawn], LIGHT_THEME).notes.some((n) => /no level .* not on a log scale/.test(n)));
  });

  // the second audit's S1: a level that goes to zero or below (a yield gone negative) on a log pane -- its points left
  // off a log axis without a word -- keeps its axis straight, said
  it('a level at or below zero on a log pane: its axis straight, and said', () => {
    const ts = spec([S('A', column('yield', '2Y'))], { panes: [{ log: true }] });
    const drawn = answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [0.1, -0.2, 0.5] });
    const types = (o: Record<string, unknown>): string[] => (o['yAxis'] as { type: string }[]).map((y) => y.type);
    assert.deepEqual(types(draw(ts, [drawn]).option), ['value']);
    assert.ok(tsLines(ts, [drawn], LIGHT_THEME).notes.some((n) => /values at or below zero, which a log scale cannot draw/.test(n)));
    const above = answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [0.1, 0.2, 0.5] });
    assert.deepEqual(types(draw(ts, [above]).option), ['log']);
  });

  // the second audit's P-2a: a split series' periods a year are each line's own -- one line of no frequency drawn empty
  // and said, by name, though the others were annualized
  // the review's G9: a tick a hair below zero was written "-0%"
  it('a tick that rounds to zero is zero, whatever its sign', () => {
    assert.deepEqual([tickText('ratio', -0.0001), tickText('percent', -0.01), tickText('same', -0.00001), tickText('ratio', -0.05)],
      ['0%', '0%', '0', '-5%']);
  });

  it("a split's line of no frequency to annualize by: said by its name", () => {
    const ts = spec([S('A', volatility(column('close'), { w: 3, returns_type: 'simple' }), { split: 'symbol' })]);
    const { notes } = tsLines(ts, [answered('A', 'percent', {
      [TS_DATE]: ['2024-01-04', '2024-01-04', '2024-01-03', '2024-01-03'], [TS_VALUE]: [20, null, 21, null],
      [TS_SPLIT]: ['AAA', 'CCC', 'AAA', 'CCC'], [TS_PER_YEAR]: [252, null, 252, null],
    }, { split: 'symbol' })], LIGHT_THEME);
    assert.ok(notes.some((n) => /^CCC\b.*no frequency it can be annualized by/.test(n)), notes.join(' | '));
    assert.ok(!notes.some((n) => /^AAA\b/.test(n)));
  });

  it('an RSI\'s axis shows its bounds whole', () => {
    const ts = spec([S('A', column('close')), S('B', relative_strength_index(seriesOf('A'), { w: 2 }), { pane: 1 })]);
    const { option } = draw(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [60, 40, null] }),
    ]);
    const rsi = (option['yAxis'] as { gridIndex: number; min?: number; max?: number }[]).find((y) => y.gridIndex === 1)!;
    assert.deepEqual([rsi.min, rsi.max], [0, 100]);
  });

  it('room on the right for each line\'s latest value, tagged at its end', () => {
    const ts = spec([S('A', column('close'))]);
    const without = draw({ ...ts, view: { lastValues: false } }, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [12345.67, 2, 1] })]).option;
    const tagged = draw(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [12345.67, 2, 1] })]).option;
    const right = (o: Record<string, unknown>): number => (o['grid'] as { right: number }[])[0]!.right;
    assert.ok(right(tagged) > right(without) + 40, `${right(tagged)} beside ${right(without)}`);
    assert.equal((tagged['series'] as { endLabel?: { show: boolean } }[])[0]!.endLabel?.show, true);
  });

  it('a band drawn as its fill stacked on its lower edge, and its two edges', () => {
    const ts = spec([S('A', column('close')), S('B', bollinger_bands(seriesOf('A'), { w: 2, k: 1 }))]);
    const { option } = draw(ts, [
      answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, null], [TS_LOWER]: [2, 1, null], [TS_UPPER]: [4, 3, null], [TS_WIDTH]: [2, 2, null] }, { band: true }),
    ]);
    const series = option['series'] as { id: string; stack?: string; areaStyle?: unknown }[];
    assert.deepEqual(series.map((s) => s.id), ['B:base', 'B:fill', 'B:lower', 'B:upper']);
    assert.equal(series[0]!.stack, series[1]!.stack);
    assert.ok(series[1]!.areaStyle, 'the fill');
  });

  it('candles: a candlestick per period, [date, open, close, low, high] as ECharts reads one, said O H L C on a hover', () => {
    const ts = spec([S('A', ohlc(column('open'), column('high'), column('low'), column('close')), { name: 'AAPL' })]);
    const { lines, option } = draw(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [12, 11, 10], [TS_OPEN]: [11, 10, 9], [TS_HIGH]: [13, 12, 11], [TS_LOW]: [10, 9, 8] }, { candles: true }),
    ]);
    assert.deepEqual(lines[0]!.candles, { open: [9, 10, 11], high: [11, 12, 13], low: [8, 9, 10] });
    const candle = (option['series'] as { type: string; data: unknown[]; endLabel?: unknown }[])[0]!;
    assert.equal(candle.type, 'candlestick');
    assert.deepEqual(candle.data[0], ['2024-01-02', 9, 10, 8, 11]);
    assert.equal(candle.endLabel, undefined, 'no latest-value tag on a candle');
    const tooltip = option['tooltip'] as { formatter: (items: unknown[]) => string };
    const said = tooltip.formatter([{ seriesId: 'A', value: ['2024-01-02', 9, 10, 8, 11], axisValue: '2024-01-02' }]);
    for (const words of ['O 9.00', 'H 11.00', 'L 8.00', 'C 10.00']) assert.ok(said.includes(words), `${words} in ${said}`);
  });

  // the next phases' 2 (2026-10-26): a yield's axis read upside down, its ends by hand; a picture's heading
  it('a pane upside down and its left axis\' ends by hand; the heading -- title, subtitle, note -- above the panes', () => {
    const ts = spec([S('A', column('close', 'SPX')), S('B', volatility(seriesOf('A'), { w: 2, returns_type: 'simple' }))], { panes: [{ invert: true, min: 0, max: 10 }] });
    const results = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'percent', { [TS_DATE]: DAYS, [TS_VALUE]: [20, 19, null] })];
    const yAxes = draw(ts, results).option['yAxis'] as { position: string; inverse?: boolean; min?: number; max?: number }[];
    assert.deepEqual(yAxes.map((y) => [y.position, y.inverse, y.min, y.max]), [['left', true, 0, 10], ['right', true, undefined, undefined]]);
    const plain = draw(spec([S('A', column('close', 'SPX'))]), [results[0]!]).option;
    assert.equal(plain['title'], undefined, 'no heading of its own on the screen: the tile\'s header says its title');
    const headed: TimeseriesSpec = { ...ts, view: { subtitle: 'Daily close', note: 'Source: the exchange' } };
    const { lines } = tsLines(headed, results, LIGHT_THEME);
    const picture = timeseriesOption(headed, lines, { theme: LIGHT_THEME, height: 600, title: 'Markets' }) as Record<string, unknown>;
    const titles = picture['title'] as { text: string; top: number }[];
    assert.deepEqual(titles.map((t) => t.text), ['Markets', 'Daily close', 'Source: the exchange']);
    assert.ok(titles[0]!.top < titles[1]!.top && titles[1]!.top < titles[2]!.top, 'each line under the one before');
    assert.equal(tsHeading(headed, 'Markets'), 22 + 17 + 15);
    const grids = (o: Record<string, unknown>): { top: number }[] => o['grid'] as { top: number }[];
    assert.equal(grids(picture)[0]!.top - grids(draw(ts, results).option)[0]!.top, tsHeading(headed, 'Markets'), 'the panes under the heading');
  });

  // the Phase 2 review's 4 and 10: a picture said no line's name and drew the navigator; it was taller than its panes
  // needed by the screen's heading; an empty title kept a blank line; a note of two lines ran into the panes
  it('a picture: its lines\' legend under its heading, no navigator, its panes as tall as on the screen', () => {
    const ts: TimeseriesSpec = { ...spec([S('A', column('close', 'SPX'), { name: 'SPX' }), S('B', volatility(seriesOf('A'), { w: 2, returns_type: 'simple' }), { name: 'Vol', pane: 1 })]),
      view: { subtitle: 'Daily', note: 'Source: the exchange\nand its vendor' } };
    const results = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'percent', { [TS_DATE]: DAYS, [TS_VALUE]: [20, 19, null] })];
    const { lines } = tsLines(ts, results, LIGHT_THEME);
    const screen = timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 480 }) as Record<string, unknown>;
    const picture = timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: tsPictureHeight(ts, 480, 'Markets'), title: 'Markets', picture: true }) as Record<string, unknown>;
    const heights = (o: Record<string, unknown>): number[] => (o['grid'] as { height: number }[]).map((g) => g.height);
    assert.deepEqual(heights(picture), heights(screen), 'its panes as tall as on the screen');
    assert.deepEqual((picture['legend'] as { data: { name: string }[] }).data.map((d) => d.name), ['SPX', 'Vol']);
    assert.equal(screen['legend'], undefined, 'on the screen the tile\'s chips name its lines');
    assert.ok(!(picture['dataZoom'] as { type: string }[]).some((z) => z.type === 'slider') && (screen['dataZoom'] as { type: string }[]).some((z) => z.type === 'slider'));
    assert.equal((picture['title'] as { text: string }[]).at(-1)!.text, 'Source: the exchange and its vendor', 'a note one line');
    const untitled = timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 480, title: '  ', picture: true }) as Record<string, unknown>;
    assert.deepEqual((untitled['title'] as { text: string }[]).map((t) => t.text), ['Daily', 'Source: the exchange and its vendor'], 'an empty title no line');
    assert.equal(tsHeading(ts, ''), tsHeading(ts));
  });

  // the Phase 2 review's 2: a pane's ends by hand were quietly ignored -- a bounded line's axis took its own bounds over
  // them, and a pane whose lines are on the right set them on its empty left axis
  it('a pane\'s ends by hand: over a bounded line\'s, and on the axis its lines are on', () => {
    const rsi = spec([S('A', relative_strength_index(column('close', 'SPX'), { w: 14 }))], { panes: [{ min: 20, max: 80 }] });
    const r = [answered('A', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [30, 50, 70] })];
    const bounded = draw(rsi, r).option['yAxis'] as { min?: number; max?: number }[];
    assert.deepEqual([bounded[0]!.min, bounded[0]!.max], [20, 80], 'the ends told, not the RSI\'s 0 and 100');
    const plain = draw(spec([S('A', relative_strength_index(column('close', 'SPX'), { w: 14 }))]), r).option['yAxis'] as { min?: number; max?: number }[];
    assert.deepEqual([plain[0]!.min, plain[0]!.max], [0, 100], 'its own bounds where none are told');
    const right = spec([S('A', column('close', 'SPX'), { axis: 'right' })], { panes: [{ max: 2 }] });
    const sides = draw(right, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })]).option['yAxis'] as { position: string; max?: number }[];
    assert.deepEqual(sides.map((y) => [y.position, y.max]), [['left', undefined], ['right', 2]], 'on the right, where its line is');
  });

  it('the annotations over the panes: a level on its pane, an event and a period across the dates', () => {
    const ts = spec([S('A', column('close')), S('B', zscores(seriesOf('A')), { pane: 1 })], {
      annotations: [
        { kind: 'level', value: 0, pane: 1, label: 'zero' },
        { kind: 'event', date: '2024-01-03', label: 'CPI' },
        { kind: 'period', from: '2024-01-02', to: '2024-01-03', label: 'Q' },
      ],
    });
    const { option } = draw(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [1, 0, -1] }),
    ]);
    const text = JSON.stringify(option['series']);
    for (const words of ['"zero"', '"CPI"', '"Q"']) assert.ok(text.includes(words), `${words} is drawn`);
  });

  // marks from a grid's rows (Phase 6's D4): what their query answered, latest first, drawn oldest first
  it('marks at a value: a triangle each in its pane, up or down, its row\'s text beside it; at a date alone, a line', () => {
    const ts = spec([S('A', column('close')), S('B', zscores(seriesOf('A')), { pane: 1 })], {
      annotations: [
        { kind: 'marks', source: 'px', at: 'price', text: 'quantity', shape: 'down', label: 'Sold' },
        { kind: 'marks', source: 'px', text: 'headline', label: 'Earnings' },
      ],
    });
    const table = (columns: Record<string, readonly (string | number | null)[]>): ResultTable => ({
      columns: Object.keys(columns).map((name) => ({ name, type: name === TS_DATE ? 'StrictDate' : name === TS_TEXT ? 'String' : 'Float', values: [...columns[name]!] })),
      rowCount: Object.values(columns)[0]!.length, epoch: 0, elapsedMs: 0,
    });
    const sold = tsMarks(ts, 0, table({ [TS_DATE]: ['2024-01-04', '2024-01-02'], [TS_VALUE]: [2.5, 1.5], [TS_TEXT]: ['300', '100'] }), LIGHT_THEME);
    assert.deepEqual(sold.marked.points, [{ date: '2024-01-02', value: 1.5, text: '100' }, { date: '2024-01-04', value: 2.5, text: '300' }], 'oldest first');
    assert.equal(sold.marked.color, LIGHT_THEME.series[7], 'a sell red');
    const earnings = tsMarks(ts, 1, table({ [TS_DATE]: ['2024-01-03'], [TS_TEXT]: ['Q4'] }), LIGHT_THEME).marked;
    const { lines } = tsLines(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [1, 0, -1] })], LIGHT_THEME);
    const option = timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 600, marks: [sold.marked, earnings] }) as Record<string, unknown>;
    const series = option['series'] as { id: string; type: string; symbol?: string; symbolRotate?: number; xAxisIndex: number; data: unknown[]; markLine?: { data: { xAxis: string }[] } }[];
    const marks = series.find((x) => x.id === 'marks:0')!;
    assert.deepEqual([marks.type, marks.symbol, marks.symbolRotate, marks.xAxisIndex], ['scatter', 'triangle', 180, 0], 'a triangle down in the main pane');
    assert.deepEqual(marks.data, [['2024-01-02', 1.5, '100'], ['2024-01-04', 2.5, '300']]);
    const lined = series.filter((x) => x.id.startsWith('marks:1'));
    assert.deepEqual(lined.map((x) => [x.xAxisIndex, x.markLine!.data.map((d) => d.xAxis)]), [[0, ['2024-01-03']], [1, ['2024-01-03']]], 'a line at its date across every pane');
    // past the most a set draws: its latest, said
    const many = Array.from({ length: TS_MARK_LIMIT + 1 }, (_, i) => `2024-01-${String(1 + (i % 28)).padStart(2, '0')}`);
    const cut = tsMarks(ts, 0, table({ [TS_DATE]: many, [TS_VALUE]: many.map(() => 1), [TS_TEXT]: many.map(() => 'x') }), LIGHT_THEME);
    assert.equal(cut.marked.points.length, TS_MARK_LIMIT);
    assert.deepEqual(cut.notes, [`Sold: the latest ${TS_MARK_LIMIT.toLocaleString(UI_LOCALE)} (there are more).`]);
    // marks in a pane that draws no line are said, never drawn elsewhere
    const lost = spec([S('A', column('close'))], { annotations: [{ kind: 'marks', source: 'px', at: 'price', pane: 2, label: 'Fills' }] });
    const fills = tsMarks(lost, 0, table({ [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [1] }), LIGHT_THEME).marked;
    const one = tsLines(lost, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME).lines;
    assert.deepEqual(marksUnplaced(lost, one, [fills]), ['Fills are drawn in pane 3, which draws no line.']);
    assert.ok(!(timeseriesOption(lost, one, { theme: LIGHT_THEME, marks: [fills] }) as { series: { id: string }[] }).series.some((x) => x.id === 'marks:0'));
  });

  // the Phase 6 review's 2: marks at a value on the left axis whatever the line was drawn on, and drawn among lines
  // compare had rebased to 100
  it('marks at a value on the axis of the line of their source in their pane; under compare, left out and said', () => {
    const table = (columns: Record<string, readonly (string | number | null)[]>): ResultTable => ({
      columns: Object.keys(columns).map((name) => ({ name, type: name === TS_DATE ? 'StrictDate' : 'Float', values: [...columns[name]!] })),
      rowCount: Object.values(columns)[0]!.length, epoch: 0, elapsedMs: 0,
    });
    const right = spec([S('A', column('close'), { axis: 'right' })], { annotations: [{ kind: 'marks', source: 'px', at: 'price', label: 'Fills' }] });
    const fills = tsMarks(right, 0, table({ [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [1] }), LIGHT_THEME).marked;
    const lines = tsLines(right, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME).lines;
    const option = timeseriesOption(right, lines, { theme: LIGHT_THEME, marks: [fills] }) as { series: { id: string; yAxisIndex: number }[] };
    assert.equal(option.series.find((x) => x.id === 'marks:0')!.yAxisIndex, option.series.find((x) => x.id === 'A')!.yAxisIndex, 'on its line\'s axis');
    const compared: TimeseriesSpec = { ...right, view: { compare: 'rebase' } };
    assert.deepEqual(marksUnplaced(compared, lines, [fills]), ['Fills are drawn at their price, the lines from one start (Compare): left out.']);
    assert.ok(!(timeseriesOption(compared, lines, { theme: LIGHT_THEME, marks: [fills] }) as { series: { id: string }[] }).series.some((x) => x.id === 'marks:0'));
  });

  // a trend projected (Phase 5): the database's one row read as three points, drawn dashed through its line's
  // observations and on past them, in its line's colour on its axis; under a compare, left out and said
  it('a projection: its three points the database\'s, dashed on its line\'s axis in its colour; refused rows said', () => {
    const ts = spec([S('A', column('close')), S('B', column('close'), { axis: 'right', hidden: true })], {
      annotations: [{ kind: 'projection', series: 'A', window: 3, label: 'Trend' }, { kind: 'projection', series: 'B' }],
    });
    const table = (row: Record<string, string | number | null>): ResultTable => ({
      columns: Object.entries(row).map(([name, v]) => ({ name, type: typeof v === 'string' ? 'StrictDate' : 'Float', values: [v] })), rowCount: 1, epoch: 0, elapsedMs: 0,
    });
    const answer = table({ [TS_PJ_FROM]: '2024-01-02', [TS_PJ_FROM_VALUE]: 1, [TS_PJ_LAST]: '2024-01-04', [TS_PJ_LAST_VALUE]: 3, [TS_PJ_END]: '2024-04-04', [TS_PJ_END_VALUE]: 94, [TS_PJ_SLOPE]: 1, [TS_PJ_N]: 3 });
    const read = tsProjected(ts, 0, answer);
    assert.deepEqual(read.notes, []);
    assert.deepEqual([read.projected!.name, read.projected!.from, read.projected!.last, read.projected!.end, read.projected!.slope, read.projected!.n],
      ['Trend', { date: '2024-01-02', value: 1 }, { date: '2024-01-04', value: 3 }, { date: '2024-04-04', value: 94 }, 1, 3]);
    const lines = tsLines(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME).lines;
    type Drawn = { series: { id: string; yAxisIndex: number; data: unknown[]; lineStyle?: { opacity?: number };
      markLine?: { data: { coord: unknown[]; lineStyle?: { color: string; type: string } }[][] } }[] };
    const option = timeseriesOption(ts, lines, { theme: LIGHT_THEME, projections: [read.projected!] }) as unknown as Drawn;
    const drawn = option.series.find((x) => x.id === 'projection:0')!;
    // its points carried unseen, for the axis to reach its end; drawn by mark lines, which a zoom never filters (the
    // Phase 5 review's 2)
    assert.deepEqual([drawn.data, drawn.lineStyle!.opacity], [[['2024-01-02', 1], ['2024-01-04', 3], ['2024-04-04', 94]], 0]);
    assert.deepEqual(drawn.markLine!.data.map(([a, b]) => [a!.coord, b!.coord]), [[['2024-01-02', 1], ['2024-01-04', 3]], [['2024-01-04', 3], ['2024-04-04', 94]]]);
    const a = option.series.find((x) => x.id === 'A')!;
    const style = drawn.markLine!.data[0]![0]!.lineStyle!;
    assert.deepEqual([drawn.yAxisIndex, style.type, style.color], [a.yAxisIndex, 'dashed', lines.find((l) => l.series.id === 'A')!.color]);
    // on a log scale a straight trend is no straight line: left out, said
    const logged: TimeseriesSpec = { ...ts, panes: [{ log: true }] };
    assert.ok(!(timeseriesOption(logged, lines, { theme: LIGHT_THEME, projections: [read.projected!] }) as unknown as Drawn).series.some((x) => x.id === 'projection:0'));
    assert.deepEqual(projectionsUnplaced(logged, lines), ['Trend: series A is drawn on a log scale, where a straight trend is no straight line: left out.', 'Projection 2: series B is not drawn.']);
    // of a hidden series: said, not drawn; under a compare, every one
    assert.deepEqual(projectionsUnplaced(ts, lines), ['Projection 2: series B is not drawn.']);
    const compared: TimeseriesSpec = { ...ts, view: { compare: 'rebase' } };
    assert.deepEqual(projectionsUnplaced(compared, lines), ['The projections are of the lines\' own values, the lines now from one start (Compare): left out.']);
    assert.ok(!(timeseriesOption(compared, lines, { theme: LIGHT_THEME, projections: [read.projected!] }) as unknown as Drawn).series.some((x) => x.id.startsWith('projection:')));
    // no fit (its figures none): said
    const none = table({ [TS_PJ_FROM]: '2024-01-02', [TS_PJ_FROM_VALUE]: null, [TS_PJ_LAST]: '2024-01-02', [TS_PJ_LAST_VALUE]: null, [TS_PJ_END]: '2024-04-02', [TS_PJ_END_VALUE]: null, [TS_PJ_SLOPE]: null, [TS_PJ_N]: 1 });
    assert.deepEqual(tsProjected(ts, 0, none), { notes: ['Trend: series A has under two observations of different times to fit a line over.'] });
  });

  // a person's own drawings (Phase 5): a line from its point to its point, a note at its point, in its pane, on the
  // axis of the pane's first line; while drawing, every pane drawing a line carries the series a click is read in
  it('drawings: a line and a note in their pane on its line\'s axis; while drawing, a series per pane, no pan, the first end shown', () => {
    type Drawn = { series: { id: string; xAxisIndex: number; yAxisIndex: number; markLine: { data: { coord: unknown[]; label?: { formatter: string } }[][] };
      markPoint: { data: { coord: unknown[]; value?: string; symbol?: string }[] } }[]; dataZoom: { type: string; moveOnMouseMove?: boolean }[] };
    const ts = spec([S('A', column('close')), S('B', relative_strength_index(seriesOf('A'), { w: 2 }), { pane: 1, axis: 'right' })], {
      annotations: [
        { kind: 'line', from: { date: '2024-01-02', value: 1 }, to: { date: '2024-01-04', value: 3 }, label: 'Trend' },
        { kind: 'note', at: { date: '2024-01-03', value: 60 }, text: 'Overbought', pane: 1 },
      ],
    });
    const lines = tsLines(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [70, 60, 50] }, { level: false })], LIGHT_THEME).lines;
    const option = timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 600 }) as unknown as Drawn;
    const main = option.series.find((x) => x.id === 'drawings:0')!;
    assert.deepEqual(main.markLine.data.map(([a, b]) => [a!.coord, b!.coord]), [[['2024-01-02', 1], ['2024-01-04', 3]]]);
    assert.equal(main.markLine.data[0]![0]!.label!.formatter, 'Trend');
    assert.deepEqual(main.markPoint.data, [], 'the note is in pane 2');
    const second = option.series.find((x) => x.id === 'drawings:1')!;
    assert.deepEqual(second.markPoint.data.map((d) => [d.coord, d.value]), [[['2024-01-03', 60], 'Overbought']]);
    assert.deepEqual([second.xAxisIndex, second.yAxisIndex], [1, option.series.find((x) => x.id === 'B')!.yAxisIndex], 'on its line\'s axis, the right');
    assert.equal(option.dataZoom.find((z) => z.type === 'inside')!.moveOnMouseMove, true);
    // while drawing: a series in every pane drawing a line (none drawn there yet), no pan, a line's first end shown
    const bare = spec([S('A', column('close')), S('B', relative_strength_index(seriesOf('A'), { w: 2 }), { pane: 1 })]);
    const drawing = timeseriesOption(bare, lines, { theme: LIGHT_THEME, height: 600, drawing: true, drawFrom: { pane: 1, at: { date: '2024-01-02', value: 55 } } }) as unknown as Drawn;
    assert.deepEqual(drawing.series.filter((x) => x.id.startsWith('drawings:')).map((x) => x.id), ['drawings:0', 'drawings:1']);
    assert.deepEqual(drawing.series.find((x) => x.id === 'drawings:1')!.markPoint.data.map((d) => [d.coord, d.symbol]), [[['2024-01-02', 55], 'circle']]);
    assert.equal(drawing.dataZoom.find((z) => z.type === 'inside')!.moveOnMouseMove, false, 'a click is a point, not a pan');
    assert.ok(!(timeseriesOption(bare, lines, { theme: LIGHT_THEME }) as unknown as Drawn).series.some((x) => x.id.startsWith('drawings:')), 'none drawn, none');
    // under a compare the values are the lines' from one start: left out, and said; in a pane that draws no line, said
    const compared: TimeseriesSpec = { ...ts, view: { compare: 'rebase' } };
    assert.ok(!(timeseriesOption(compared, lines, { theme: LIGHT_THEME }) as unknown as Drawn).series.some((x) => x.id.startsWith('drawings:')));
    assert.deepEqual(drawingsUnplaced(compared, lines), ['The drawings are on the values as they were drawn, the lines now from one start (Compare): left out.']);
    assert.deepEqual(drawingsUnplaced(ts, lines), []);
    const lost: TimeseriesSpec = { ...ts, annotations: [{ kind: 'note', at: { date: '2024-01-03', value: 1 }, text: 'Here', pane: 2 }] };
    assert.deepEqual(drawingsUnplaced(lost, lines), ['Pane 3 draws no line to draw on. Its drawings are left out.']);
    assert.ok(!(timeseriesOption(lost, lines, { theme: LIGHT_THEME }) as unknown as Drawn).series.some((x) => x.id.startsWith('drawings:')));
    // the Phase 5 review's 3: on the pane's first line by the tile's order -- hidden, none, said; never on whichever line
    // is shown first
    const two = spec([S('A', column('close')), S('V', column('volume'), { axis: 'right' })], { annotations: [{ kind: 'note', at: { date: '2024-01-03', value: 2 }, text: 'Here' }] });
    const both = tsLines(two, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('V', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [9e6, 8e6, 7e6] })], LIGHT_THEME).lines;
    const onA = timeseriesOption(two, both, { theme: LIGHT_THEME }) as unknown as Drawn;
    assert.equal(onA.series.find((x) => x.id === 'drawings:0')!.yAxisIndex, onA.series.find((x) => x.id === 'A')!.yAxisIndex);
    const hidden: TimeseriesSpec = { ...two, series: two.series.map((x) => (x.id === 'A' ? { ...x, hidden: true } : x)) };
    const hiddenLines = tsLines(hidden, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('V', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [9e6, 8e6, 7e6] })], LIGHT_THEME).lines;
    assert.ok(!(timeseriesOption(hidden, hiddenLines, { theme: LIGHT_THEME }) as unknown as Drawn).series.some((x) => x.id === 'drawings:0'), 'not on the volume\'s axis');
    assert.deepEqual(drawingsUnplaced(hidden, hiddenLines), ['The drawings in pane 1 are on series A\'s values, and it is hidden. Its drawings are left out.']);
    // on a line read from the first date shown (an index): its values move with the window -- left out, said
    const indexed = spec([S('A', index(column('close'), { initial: 1 }))], { annotations: [{ kind: 'note', at: { date: '2024-01-03', value: 2 }, text: 'Here' }] });
    const indexedLines = tsLines(indexed, [answered('A', 'index', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME).lines;
    assert.deepEqual(drawingsUnplaced(indexed, indexedLines), ['The drawings in pane 1 are on series A\'s values, which start from the first date shown and move with it. Its drawings are left out.']);
  });
});
