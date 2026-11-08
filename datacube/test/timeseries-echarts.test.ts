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
import type { TsUnit } from '../src/timeseries-expr.ts';
import { bollinger_bands, column, mean, ohlc, relative_strength_index, series, type Term } from '../src/timeseries-builder.ts';
import { timeseriesOption, tsLines, tsPictureHeight, type TsResult } from '../src/timeseries-option.ts';
import { TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_UPPER, TS_VALUE, TS_WIDTH, type TsQuery } from '../src/timeseries-query.ts';
import { mountTimeseries } from '../src/timeseries-render.ts';
import type { TimeseriesSpec, TsSeries } from '../src/timeseries-spec.ts';
import { writtenText } from '../src/timeseries-text.ts';

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

const S = (id: string, t: Term, rest: Omit<TsSeries, 'id' | 'text'> = {}): TsSeries => ({ id, text: writtenText(t.expr, []), ...rest });
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
    const ts = spec([S('A', column('close'), { name: 'Close' }), S('B', bollinger_bands(series('A'), { w: 2, k: 1 })), S('C', relative_strength_index(series('A'), { w: 2 }), { pane: 1 })], {
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
    const ts = spec([S('A', column('close'))], { panes: [{ log: true }] });
    const option = drawing(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [1000, 300, 100, 30, 10] })]);
    const axes = ([] as { type?: string }[]).concat(option.yAxis as { type?: string }[]);
    assert.ok(axes.some((a) => a.type === 'log'), 'a log axis');
    const { svg } = rendered(option);
    const labels = [...svg.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map((m) => m[1]!.replace(/,/g, ''));
    assert.ok(['10', '100', '1000'].every((v) => labels.includes(v)), `its ticks the powers of ten it spans: ${labels.join(' | ')}`);
    assert.ok(!/NaN|Infinity/.test(svg));
  });

  it('candles: drawn as candles once their kind is registered -- a chart made before it draws none (the viewer makes it again)', async () => {
    const ts = spec([S('A', ohlc(column('open'), column('high'), column('low'), column('close')))]);
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
      const ts = spec([S('A', column('close'))]);
      mounted.show(drawing(ts, [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [105, 104, 102, 101, 100] })]), 'Close');
      assert.equal(mounted.height(), 480);
      mounted.zoomTo('2024-01-03', '2024-01-05');
      assert.deepEqual(mounted.zoomed(), { from: '2024-01-03', to: '2024-01-05' });
      assert.deepEqual(zooms.at(-1), ['2024-01-03', '2024-01-05'], 'the viewer told the dates shown');
    } finally {
      mounted.dispose();
    }
  });

  // Phase 5's drawings: a click while drawing read in the axes of the pane under it -- its date and its value -- and
  // nothing told while not drawing; the line and the note drawn where their points are
  it('while drawing, a click is the point under it in its pane\'s axes; its drawings drawn there', () => {
    const el = { setAttribute: () => {}, ownerDocument: { defaultView: undefined } } as unknown as HTMLElement;
    const points: [number, string, number][] = [];
    const spans: [string, string, boolean][] = [];
    let chart: echarts.ECharts | undefined;
    const mounted = mountTimeseries(el, {
      onZoom: () => {}, onPoint: (pane, date, value) => points.push([pane, date, value]), onSpan: (from, to, done) => spans.push([from, to, done]),
    }, () => {
      chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width: 720, height: 480 });
      return chart;
    });
    try {
      const ts = spec([S('A', column('close')), S('B', relative_strength_index(series('A'), { w: 2 }), { pane: 1 })], {
        annotations: [{ kind: 'line', from: { date: '2024-01-02', value: 100 }, to: { date: '2024-01-08', value: 105 }, label: 'Trend' },
          { kind: 'note', at: { date: '2024-01-04', value: 60 }, text: 'Overbought', pane: 1 }],
      });
      const results = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [105, 104, 102, 101, 100] }),
        answered('B', 'number', { [TS_DATE]: DAYS, [TS_VALUE]: [80, 75, 60, 40, 20] }, { level: false })];
      const option = timeseriesOption(ts, tsLines(ts, results, LIGHT_THEME).lines, { theme: LIGHT_THEME, view: {}, height: 480, drawing: true });
      mounted.show({ ...option, animation: false } as EChartsOption, 'Close');
      const svg = rendered({ ...option, animation: false } as EChartsOption).svg;
      assert.ok(svg.includes('Trend') && svg.includes('Overbought'), 'the line\'s label and the note drawn');
      const click = (pane: number, date: string, value: number): void => {
        const [x, y] = chart!.convertToPixel({ seriesId: `drawings:${pane}` }, [date, value]) as unknown as number[];
        chart!.getZr().trigger('click', { offsetX: x, offsetY: y });
      };
      click(1, '2024-01-05', 50);
      assert.equal(points.length, 0, 'not drawing: a click is nothing');
      mounted.draw(true);
      click(1, '2024-01-05', 50);
      click(0, '2024-01-03', 101.5);
      assert.deepEqual(points.map(([p, d]) => [p, d]), [[1, '2024-01-05'], [0, '2024-01-03']], 'each in its pane, its day');
      assert.ok(Math.abs(points[0]![2] - 50) < 0.5 && Math.abs(points[1]![2] - 101.5) < 0.05, `its value: ${points.map((p) => p[2])}`);
      // a click outside every pane is nothing
      chart!.getZr().trigger('click', { offsetX: 2, offsetY: 2 });
      assert.equal(points.length, 2);
      // the Phase 5 review's notes: a measure dragged across a lower pane is measured too, each end its nearest day
      mounted.draw(false);
      mounted.measure(true);
      // a pointer event as zrender hands it on: its pixel, and the browser's event under it (the zoom's own handler reads it)
      const px = (date: string): { offsetX: number; offsetY: number; event: unknown } => {
        const [x, y] = chart!.convertToPixel({ seriesId: 'drawings:1' }, [date, 50]) as unknown as number[];
        return { offsetX: x!, offsetY: y!, event: { preventDefault: () => {}, stopPropagation: () => {} } };
      };
      chart!.getZr().trigger('mousedown', px('2024-01-03'));
      chart!.getZr().trigger('mousemove', px('2024-01-05'));
      chart!.getZr().trigger('mouseup', px('2024-01-05'));
      assert.deepEqual(spans.at(-1), ['2024-01-03', '2024-01-05', true]);
    } finally {
      mounted.dispose();
    }
  });

  // the Phase 2 review's 11: no test ran a picture's export through ECharts' SVG painter
  it('a picture made as SVG off the screen, by the SVG painter: its heading and its lines\' names in it', async () => {
    const { JSDOM } = await import('jsdom');
    const { svgChart } = await import('../src/chart-render.ts');
    const dom = new JSDOM('<!doctype html><body><div id="c"></div></body>');
    const el = dom.window.document.getElementById('c') as unknown as HTMLElement;
    // zrender's SVG painter makes its elements in the page's document: the test's own, while it draws
    const g = globalThis as { document?: unknown };
    const had = g.document;
    g.document = dom.window.document;
    const mounted = mountTimeseries(el, { onZoom: () => {} }, () => echarts.init(null, null, { renderer: 'svg', ssr: true, width: 720, height: 480 }), svgChart);
    try {
      const ts: TimeseriesSpec = { ...spec([S('A', column('close'), { name: 'Close' }), S('B', mean(series('A'), { w: 2 }), { name: 'Two-day mean' })]), view: { subtitle: 'Daily', note: 'Source: the exchange' } };
      const results = [answered('A', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [105, 104, 102, 101, 100] }),
        answered('B', 'same', { [TS_DATE]: DAYS, [TS_VALUE]: [104.5, 103, 101.5, 100.5, null] })];
      const tall = tsPictureHeight(ts, 480, 'Markets');
      const option = timeseriesOption(ts, tsLines(ts, results, LIGHT_THEME).lines, { theme: LIGHT_THEME, height: tall, title: 'Markets', picture: true });
      const url = await mounted.exportUrl(option, 'svg', 720, tall, '#ffffff');
      const body = url.slice(url.indexOf(',') + 1);
      const svg = url.includes(';base64,') ? Buffer.from(body, 'base64').toString('utf8') : decodeURIComponent(body);
      assert.match(svg, /^<svg/, 'an SVG');
      for (const words of ['Markets', 'Daily', 'Source: the exchange', 'Close', 'Two-day mean']) assert.ok(svg.includes(words), `${words} in the picture`);
      assert.equal(dom.window.document.body.children.length, 1, 'its box off the screen put away');
    } finally {
      mounted.dispose();
      g.document = had;
    }
  });
});
