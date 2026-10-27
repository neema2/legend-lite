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
import { parseExpr } from '../src/timeseries-expr.ts';
import { tickText, timeseriesCsv, timeseriesOption, tsLines, valueText, type TsResult } from '../src/timeseries-option.ts';
import { TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_PER_YEAR, TS_SPLIT, TS_UPPER, TS_VALUE, TS_WIDTH, type TsQuery } from '../src/timeseries-query.ts';
import type { TimeseriesSpec, TsSeries } from '../src/timeseries-spec.ts';
import type { TsUnit } from '../src/timeseries-expr.ts';

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

const S = (id: string, expr: string, rest: Omit<TsSeries, 'id' | 'expr'> = {}): TsSeries => ({ id, expr: parseExpr(expr), ...rest });

function spec(series: TsSeries[], more: Partial<TimeseriesSpec> = {}): TimeseriesSpec {
  return { sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }], series, ...more };
}

describe('the lines, from what the database answered', () => {
  it('a series\' rows made oldest first, its unit its query\'s, its colour its place', () => {
    const ts = spec([S('A', 'close[SPX]'), S('B', 'volatility($A, 2)')]);
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
    const ts = spec([S('A', 'yield', { name: 'Yield', split: 'tenor' }), S('B', 'close', { split: 'symbol' })]);
    const { lines } = tsLines(ts, [
      answered('A', 'same', { [TS_DATE]: ['2024-01-02', '2024-01-02'], [TS_VALUE]: [4.1, 4.4], [TS_SPLIT]: ['10Y', '2Y'] }, { split: 'tenor' }),
      answered('B', 'same', { [TS_DATE]: ['2024-01-02'], [TS_VALUE]: [100], [TS_SPLIT]: ['SPX'] }, { split: 'symbol' }),
    ], LIGHT_THEME);
    assert.deepEqual(lines.map((l) => l.name), ['Yield 2Y', 'Yield 10Y', 'SPX']);
  });

  it('a band: its edges and its width, point by point', () => {
    const ts = spec([S('A', 'close'), S('B', 'bollinger_bands($A, 2, 1)')]);
    const { lines } = tsLines(ts, [
      answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, null], [TS_LOWER]: [2, 1, null], [TS_UPPER]: [4, 3, null], [TS_WIDTH]: [2, 2, null] }, { band: true }),
    ], LIGHT_THEME);
    assert.equal(lines[0]!.band, true);
    assert.deepEqual([lines[0]!.lower, lines[0]!.upper, lines[0]!.width], [[null, 1, 2], [null, 3, 4], [null, 2, 2]]);
  });

  it('as CSV: a row per line and date, the rows the database answered', () => {
    const ts = spec([S('A', 'close[SPX]', { name: 'SPX' })]);
    const { lines } = tsLines(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] })], LIGHT_THEME);
    const csv = timeseriesCsv(lines).trim().split('\n');
    assert.equal(csv.length, 4, 'a header and a row per date');
    assert.ok(csv.slice(1).every((row) => row.includes('SPX')));
  });
});

describe('the drawing', () => {
  const draw = (ts: TimeseriesSpec, results: TsResult[]) => {
    const { lines } = tsLines(ts, results, LIGHT_THEME);
    return { lines, option: timeseriesOption(ts, lines, { theme: LIGHT_THEME, height: 600 }) as Record<string, unknown> };
  };

  it('a grid per pane, one time axis each; a second unit on its own axis, on the right', () => {
    const ts = spec([S('A', 'close[SPX]'), S('B', 'volatility($A, 2)'), S('C', 'zscores($A)', { pane: 1 })]);
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
    const ts = spec([S('A', 'close[NVDA]'), S('B', 'close[SPX]', { axis: 'right' })], { panes: [{ log: true }] });
    const levels = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }), answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [300, 200, 100] })];
    const types = (o: Record<string, unknown>): string[] => (o['yAxis'] as { type: string }[]).map((y) => y.type);
    assert.deepEqual(types(draw(ts, levels).option), ['log', 'log']);
    const ratio: TimeseriesSpec = { ...ts, series: [ts.series[0]!, S('B', 'returns($A)', { axis: 'right' })] };
    assert.deepEqual(types(draw(ratio, [levels[0]!, answered('B', 'ratio', { [TS_DATE]: DAYS, [TS_VALUE]: [0.1, -0.2, null] })]).option), ['log', 'value']);
    // the review's G3: a figure that may be zero or below on the log pane's left axis -- dropped by a log axis without a
    // word -- keeps that axis straight, and the drawing says why
    const returns = spec([S('A', 'returns(close[SPX])')], { panes: [{ log: true }] });
    const drawn = answered('A', 'ratio', { [TS_DATE]: DAYS, [TS_VALUE]: [0.01, -0.02, null] });
    assert.deepEqual(types(draw(returns, [drawn]).option), ['value']);
    assert.ok(tsLines(returns, [drawn], LIGHT_THEME).notes.some((n) => /no level .* not on a log scale/.test(n)));
  });

  // the second audit's S1: a level that goes to zero or below (a yield gone negative) on a log pane -- its points left
  // off a log axis without a word -- keeps its axis straight, said
  it('a level at or below zero on a log pane: its axis straight, and said', () => {
    const ts = spec([S('A', 'yield[2Y]')], { panes: [{ log: true }] });
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
    const ts = spec([S('A', 'volatility(close, 3)', { split: 'symbol' })]);
    const { notes } = tsLines(ts, [answered('A', 'percent', {
      [TS_DATE]: ['2024-01-04', '2024-01-04', '2024-01-03', '2024-01-03'], [TS_VALUE]: [20, null, 21, null],
      [TS_SPLIT]: ['AAA', 'CCC', 'AAA', 'CCC'], [TS_PER_YEAR]: [252, null, 252, null],
    }, { split: 'symbol' })], LIGHT_THEME);
    assert.ok(notes.some((n) => /^CCC\b.*no frequency it can be annualized by/.test(n)), notes.join(' | '));
    assert.ok(!notes.some((n) => /^AAA\b/.test(n)));
  });

  it('an RSI\'s axis shows its bounds whole', () => {
    const ts = spec([S('A', 'close'), S('B', 'rsi($A, 2)', { pane: 1 })]);
    const { option } = draw(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, 1] }),
      answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [60, 40, null] }),
    ]);
    const rsi = (option['yAxis'] as { gridIndex: number; min?: number; max?: number }[]).find((y) => y.gridIndex === 1)!;
    assert.deepEqual([rsi.min, rsi.max], [0, 100]);
  });

  it('room on the right for each line\'s latest value, tagged at its end', () => {
    const ts = spec([S('A', 'close')]);
    const without = draw({ ...ts, view: { lastValues: false } }, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [12345.67, 2, 1] })]).option;
    const tagged = draw(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [12345.67, 2, 1] })]).option;
    const right = (o: Record<string, unknown>): number => (o['grid'] as { right: number }[])[0]!.right;
    assert.ok(right(tagged) > right(without) + 40, `${right(tagged)} beside ${right(without)}`);
    assert.equal((tagged['series'] as { endLabel?: { show: boolean } }[])[0]!.endLabel?.show, true);
  });

  it('a band drawn as its fill stacked on its lower edge, and its two edges', () => {
    const ts = spec([S('A', 'close'), S('B', 'bollinger_bands($A, 2, 1)')]);
    const { option } = draw(ts, [
      answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [3, 2, null], [TS_LOWER]: [2, 1, null], [TS_UPPER]: [4, 3, null], [TS_WIDTH]: [2, 2, null] }, { band: true }),
    ]);
    const series = option['series'] as { id: string; stack?: string; areaStyle?: unknown }[];
    assert.deepEqual(series.map((s) => s.id), ['B:base', 'B:fill', 'B:lower', 'B:upper']);
    assert.equal(series[0]!.stack, series[1]!.stack);
    assert.ok(series[1]!.areaStyle, 'the fill');
  });

  it('candles: a candlestick per period, [date, open, close, low, high] as ECharts reads one, said O H L C on a hover', () => {
    const ts = spec([S('A', 'ohlc(open, high, low, close)', { name: 'AAPL' })]);
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

  it('the annotations over the panes: a level on its pane, an event and a period across the dates', () => {
    const ts = spec([S('A', 'close'), S('B', 'zscores($A)', { pane: 1 })], {
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
});
