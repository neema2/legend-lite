// A TIMESERIES DRAWN BY REAL ECHARTS (the review's G5, docs/TIMESERIES_REVIEW_2026_10_25.md: no test drew one), in node:
// timeseries-option.ts's drawing rendered to SVG as chart-echarts.test.ts renders a chart's -- its lines, a band, a
// pane on a log scale, the annotations -- candles once their kind is registered (chart-render-kinds.ts: the order the
// viewer keeps, a chart made again after it), and the renderer that mounts it (timeseries-render.ts mountTimeseries):
// the drawing shown, its zoom moved and read back.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import * as echarts from 'echarts/core';
import { SVGRenderer } from 'echarts/renderers';
import type { EChartsOption } from 'echarts';
import '../src/chart-render.ts';   // registers the charts and components a page draws -- a timeseries' among them

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { LIGHT_THEME } from '../src/chart-option.ts';
import { parseExpr, type TsUnit } from '../src/timeseries-expr.ts';
import { timeseriesOption, tsLines, type TsResult } from '../src/timeseries-option.ts';
import { TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_UPPER, TS_VALUE, TS_WIDTH, type TsQuery } from '../src/timeseries-query.ts';
import { mountTimeseries } from '../src/timeseries-render.ts';
import type { TimeseriesSpec, TsSeries } from '../src/timeseries-spec.ts';

echarts.use([SVGRenderer]);

/** Five days, latest first, as the database gives them. */
const DAYS = ['2024-01-08', '2024-01-05', '2024-01-04', '2024-01-03', '2024-01-02'];

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
const spec = (series: TsSeries[], more: Partial<TimeseriesSpec> = {}): TimeseriesSpec =>
  ({ sources: [{ alias: 'px', cube: 'prices', date: 'day' }], series, ...more });

/** A chart rendered with no DOM, as the page draws on canvas: the SVG it makes, and the shapes it drew, by kind. */
function rendered(option: EChartsOption): { svg: string; kinds: Map<string, number> } {
  const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width: 720, height: 480 });
  try {
    chart.setOption({ ...option, animation: false });
    const kinds = new Map<string, number>();
    for (const e of chart.getZr().storage.getDisplayList()) kinds.set(e.type, (kinds.get(e.type) ?? 0) + 1);
    return { svg: chart.renderToSVGString(), kinds };
  } finally {
    chart.dispose();
  }
}

const drawing = (ts: TimeseriesSpec, results: readonly TsResult[]): EChartsOption =>
  timeseriesOption(ts, tsLines(ts, results, LIGHT_THEME).lines, { theme: LIGHT_THEME, formatOf: () => undefined, view: {}, height: 480 });

describe('a timeseries drawn by ECharts, as the page draws it', () => {
  it('its lines, a band, a pane of its own and the annotations: drawn, every value a number', () => {
    const ts = spec([S('A', 'close', { name: 'Close' }), S('B', 'bollinger_bands($A, 2, 1)'), S('C', 'rsi($A, 2)', { pane: 1 })], {
      annotations: [{ kind: 'level', value: 70, pane: 1, label: 'overbought' }, { kind: 'event', date: '2024-01-04', label: 'CPI' }],
    });
    const option = drawing(ts, [
      answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [105, 104, 102, 101, 100] }),
      answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [104, 103, 101, null, null], [TS_LOWER]: [102, 101, 100, null, null], [TS_UPPER]: [106, 105, 102, null, null], [TS_WIDTH]: [4, 4, 2, null, null] }, { band: true }),
      answered('C', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [80, 75, 60, null, null] }, { level: false }),
    ]);
    const { svg, kinds } = rendered(option);
    // its lines' own shapes: more than the same drawing with no series makes (its axes, its navigator) -- the second
    // audit's G-3: a count of every polyline passed with no line drawn at all
    const bare = rendered({ ...option, series: [] }).kinds;
    const lines = (k: Map<string, number>): number => k.get('ec-polyline') ?? 0;
    assert.ok(lines(kinds) - lines(bare) >= 3, `its lines drawn: ${lines(kinds)} polylines, ${lines(bare)} with none`);
    assert.ok(svg.includes('overbought') && svg.includes('CPI'), 'its annotations labelled');
    assert.ok(!/NaN|Infinity/.test(svg), 'every coordinate a number');
  });

  it('a pane on a log scale: its axis a log one, its ticks the powers its values span', () => {
    const ts = spec([S('A', 'close')], { panes: [{ log: true }] });
    const option = drawing(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [1000, 300, 100, 30, 10] })]);
    const axes = ([] as { type?: string }[]).concat(option.yAxis as { type?: string }[]);
    assert.ok(axes.some((a) => a.type === 'log'), 'a log axis');
    const { svg } = rendered(option);
    const labels = [...svg.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map((m) => m[1]!.replace(/,/g, ''));
    assert.ok(['10', '100', '1000'].every((v) => labels.includes(v)), `its ticks the powers of ten it spans: ${labels.join(' | ')}`);
    assert.ok(!/NaN|Infinity/.test(svg));
  });

  it('candles: drawn as candles once their kind is registered -- a chart made before it draws none (the viewer makes it again)', async () => {
    const ts = spec([S('A', 'ohlc(open, high, low, close)')]);
    const option = drawing(ts, [answered('A', 'same', {
      [TS_DATE]: DAYS, [TS_OPEN]: [104, 102, 101, 100, 99], [TS_HIGH]: [106, 105, 103, 102, 101],
      [TS_LOW]: [103, 101, 100, 99, 98], [TS_VALUE]: [105, 104, 102, 101, 100],
    }, { candles: true })]);
    const before = rendered(option).kinds.get('normalCandlestickBox') ?? 0;
    assert.equal(before, 0, 'a chart made before the candles are registered draws none');
    await import('../src/chart-render-kinds.ts');
    const after = rendered(option).kinds.get('normalCandlestickBox') ?? 0;
    assert.equal(after, DAYS.length, 'a chart made after draws a candle a day');
  });
});

describe('the renderer that mounts a timeseries (timeseries-render.ts)', () => {
  it('shows a drawing, moves its zoom to the dates told, and reads them back as ISO days', () => {
    const el = { setAttribute: () => {}, ownerDocument: { defaultView: undefined } } as unknown as HTMLElement;
    const zooms: (string | undefined)[][] = [];
    const mounted = mountTimeseries(el, { onZoom: (from, to) => zooms.push([from, to]) },
      () => echarts.init(null, null, { renderer: 'svg', ssr: true, width: 720, height: 480 }));
    try {
      const ts = spec([S('A', 'close')]);
      mounted.show(drawing(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [105, 104, 102, 101, 100] })]), 'Close');
      assert.equal(mounted.height(), 480);
      mounted.zoomTo('2024-01-03', '2024-01-05');
      assert.deepEqual(mounted.zoomed(), { from: '2024-01-03', to: '2024-01-05' });
      assert.deepEqual(zooms.at(-1), ['2024-01-03', '2024-01-05'], 'the viewer told the dates shown');
    } finally {
      mounted.dispose();
    }
  });
});
