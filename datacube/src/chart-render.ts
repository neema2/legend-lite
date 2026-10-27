// The ONE file that draws with ECharts.
//
// Only what DataCube uses is registered (`echarts/core`), never the whole
// library: bar, line, scatter, pie and heatmap, with the grid, tooltip,
// legend, visual map, dataset and aria components, the mark lines, areas and
// points of a chart's layers and an axis's breaks, drawn on canvas. (The
// tests draw the same options as SVG in node, registering the SVG renderer
// themselves, so the page does not ship it.)
//
// Everything a chart shows comes from `chart-option.ts`; this file only
// mounts it, keeps it sized to its box, reads the theme from CSS tokens,
// and turns a click on a mark into the values behind it.

import * as echarts from 'echarts/core';
// Each chart and component by its own module, which registers it as it loads (ECharts' `lib/<kind>.js`:
// `use(install)`), never through the `echarts/charts` or `echarts/components` index: chart-render-kinds.ts registers
// batch 3's rarer charts the same way, and an index both imported would be code the two chunks share -- the bundler
// would put every chart in it, and a page's first bar chart would download the Sankey (test/bundle-budget.test.ts).
// These are ECharts' internal paths (its package exports them, `./*`, and lists them as side effects): an ECharts bump
// checks them, here and in chart-render-kinds.ts -- the chart chunk's budget catches a size regression, not a rename.
import 'echarts/lib/chart/bar.js';
import 'echarts/lib/chart/line.js';
import 'echarts/lib/chart/scatter.js';
import 'echarts/lib/chart/pie.js';
import 'echarts/lib/chart/heatmap.js';
import 'echarts/lib/chart/treemap.js';
import 'echarts/lib/component/grid.js';
import 'echarts/lib/component/tooltip.js';
import 'echarts/lib/component/legend.js';
import 'echarts/lib/component/title.js';
import 'echarts/lib/component/dataset.js';
import 'echarts/lib/component/visualMap.js';
import 'echarts/lib/component/aria.js';
// version 2's layers: reference lines, bands, pinned extremes
import 'echarts/lib/component/markLine.js';
import 'echarts/lib/component/markArea.js';
import 'echarts/lib/component/markPoint.js';
// brushing a range to filter (and the toolbox it starts from)
import 'echarts/lib/component/brush.js';
import 'echarts/lib/component/toolbox.js';
// a long series' slider, and zooming inside the chart
import 'echarts/lib/component/dataZoom.js';
import { AxisBreak } from 'echarts/features';
import { CanvasRenderer } from 'echarts/renderers';
import type { EChartsOption } from 'echarts';
import type { AxisHold, BrushArea, Brushed, ChartDrawing, MarkKey } from './chart-option.ts';
import type { Scalar } from '../../engine-client/src/result.ts';

// a value axis's breaks, and the canvas the page draws on
echarts.use([AxisBreak, CanvasRenderer]);

/**
 * A chart as a picture, for an export of the page: the live chart's own pixels at `ratio`
 * times its size, on white (a PNG for a page or a workbook, a JPEG a PDF embeds as it is).
 */
/** A canvas chart in `el` over the charts' ECharts, as every chart is drawn: a timeseries tile's (timeseries-render.ts),
 *  which registers nothing of its own -- one build of ECharts, fetched with whichever draws first. */
export function canvasChart(el: HTMLElement): echarts.ECharts {
  return echarts.init(el, null, { renderer: 'canvas' });
}

export interface ChartPicture {
  /** The chart's size on screen, in CSS pixels. */
  readonly width: number;
  readonly height: number;
  /** The image's size in pixels: `width` and `height` times the ratio. */
  readonly pixelWidth: number;
  readonly pixelHeight: number;
  readonly png: Uint8Array;
  readonly jpeg: Uint8Array;
}

/** A chart mounted in an element. */
export interface MountedChart {
  /** Draw (or redraw) a drawing; `ticked`, a tick's (its values changed, nothing else): drawn at once, no animation
   *  left running behind the next tick (docs/DEEPHAVEN_PLAN_2026_10_22.md V3). */
  show(drawing: ChartDrawing, how?: { readonly ticked?: boolean }): void;
  /**
   * A crosshair shared with the other charts of `group` (batch 2 item 12): hovering one points at the same place on
   * each -- the page's charts with the same column across. Undefined: its own alone.
   */
  setGroup(group: string | undefined): void;
  /** The chart as drawn now, as a picture; null before it has drawn or where there is no canvas. */
  picture(ratio?: number): ChartPicture | null;
  dispose(): void;
}

/** A value axis's extent as drawn: its least and its greatest value. */
type Extent = readonly [number, number];

/**
 * A TICK'S DRAWING WITH ITS SCALE HELD (docs/DEEPHAVEN_PLAN_2026_10_22.md C2): each value axis `hold` names kept to the
 * extent the chart drew it at (`held`), so a tick moves the marks, never the scale under them -- but for a side the
 * tick's values leave, which is let go for ECharts to widen. An axis held at nothing (none drawn yet, or one no tick
 * holds) is drawn as the option says.
 */
export function heldOption(option: EChartsOption, hold: AxisHold, held: readonly (Extent | null)[]): EChartsOption {
  const axes = (option as Record<string, unknown>)[hold.key];
  const list: unknown[] = Array.isArray(axes) ? axes : [axes];
  if (list.length !== hold.extents.length || list.length !== held.length) return option;
  const kept = list.map((axis, i) => {
    const values = hold.extents[i];
    const drawn = held[i];
    if (!values || !drawn || typeof axis !== 'object' || axis === null) return axis;
    return {
      ...axis,
      ...(values[0] >= drawn[0] ? { min: drawn[0] } : {}),
      ...(values[1] <= drawn[1] ? { max: drawn[1] } : {}),
    };
  });
  return { ...option, [hold.key]: Array.isArray(axes) ? kept : kept[0] } as EChartsOption;
}

/** A dataZoom's window as ECharts holds it: its shares of the axis (%) and its values at either end. */
export interface ZoomNow {
  readonly start?: number;
  readonly end?: number;
  readonly startValue?: number;
  readonly endValue?: number;
}

/**
 * A TICK'S WINDOW AS THE LAST ONE LEFT IT (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K14): a chart drawn again at a tick
 * is drawn afresh (its option replaced), which would put a window a person zoomed or panned back where its option says.
 * So each window keeps its place: one that ended at the latest time across moves with it, the same width (a stock's
 * last five minutes stay the last five); one left elsewhere stays where it was; one showing all of it shows all of the
 * tick's (its option's own window: a span shown). `before` is each window as ECharts had it, `latestBefore` and
 * `latest` the latest time across then and now (none: a category axis, its window kept by its shares).
 */
export function keptZoom(option: EChartsOption, before: readonly ZoomNow[] | null, latestBefore: number | undefined,
  latest: number | undefined): EChartsOption {
  const zooms = (option as { dataZoom?: unknown }).dataZoom;
  if (!before || !Array.isArray(zooms) || zooms.length !== before.length) return option;
  const kept = zooms.map((z: Record<string, unknown>, i) => {
    const b = before[i]!;
    // all of it shown: the tick's window, as its option has it
    if ((b.start ?? 0) <= 0.01 && (b.end ?? 100) >= 99.99) return z;
    const { start: _s, end: _e, startValue: _sv, endValue: _ev, ...rest } = z;
    if (latest !== undefined && latestBefore !== undefined && b.startValue !== undefined && b.endValue !== undefined) {
      const width = b.endValue - b.startValue;
      return b.endValue >= latestBefore - 1
        ? { ...rest, startValue: latest - width, endValue: latest }
        : { ...rest, startValue: b.startValue, endValue: b.endValue };
    }
    return { ...rest, start: b.start ?? 0, end: b.end ?? 100 };
  });
  return { ...option, dataZoom: kept } as EChartsOption;
}

/** Each of the chart's windows as ECharts holds it now (none: no window). */
function zoomNow(chart: echarts.ECharts): ZoomNow[] | null {
  const zooms = (chart.getOption() as { dataZoom?: ZoomNow[] } | undefined)?.dataZoom;
  return Array.isArray(zooms) && zooms.length > 0
    ? zooms.map((z) => ({
      ...(typeof z.start === 'number' ? { start: z.start } : {}), ...(typeof z.end === 'number' ? { end: z.end } : {}),
      ...(typeof z.startValue === 'number' ? { startValue: z.startValue } : {}),
      ...(typeof z.endValue === 'number' ? { endValue: z.endValue } : {}),
    })) : null;
}

/** The extent ECharts drew each of `hold`'s axes at (its model's scale: what the axis shows), null where none. */
export function drawnExtents(chart: echarts.ECharts, hold: AxisHold): (Extent | null)[] {
  // ECharts' model, which its public API has no other way to read: an axis's scale, as `getExtent` gives it
  const model = (chart as unknown as {
    getModel?: () => { getComponent(type: string, index: number): { axis?: { scale?: { getExtent?: () => number[] } } } | undefined };
  }).getModel?.();
  return hold.extents.map((values, i) => {
    if (values === null) return null;
    const e = model?.getComponent(hold.key, i)?.axis?.scale?.getExtent?.();
    const [lo, hi] = e ?? [];
    return lo !== undefined && hi !== undefined && Number.isFinite(lo) && Number.isFinite(hi) ? [lo, hi] : null;
  });
}

/** A chart in a crosshair group: shows the category another chart points at, or hides its tooltip. */
interface Peer {
  show(key: string): void;
  hide(): void;
}

/** The crosshair groups: each page's charts with one column across. */
const GROUPS = new Map<string, Set<Peer>>();

/** A data: URL's bytes. */
function bytesOf(dataUrl: string): Uint8Array {
  const b64 = dataUrl.slice(dataUrl.indexOf(',') + 1);
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Mount a chart in `el`: canvas, sized to the element and kept so.
 * `onPick` receives the values behind a clicked mark (a bar's category,
 * a slice, a heatmap cell); a mark that is not a group (a scatter point)
 * picks nothing.
 */
export function mountChart(
  el: HTMLElement,
  onPick?: (key: MarkKey, upper?: Scalar) => void,
  more: {
    /** A mark double-clicked: drill into it (batch 2 item 10). */
    readonly onDrill?: (key: MarkKey) => void;
    /** A brush took a range (or was cleared: null), batch 2 item 11. */
    readonly onBrush?: (took: Brushed | null) => void;
    /** The toolbox drew the chart the other way, bars or a line: keep it (batch 2 item 12). */
    readonly onKind?: (mark: 'bar' | 'line') => void;
  } = {},
): MountedChart {
  const chart = echarts.init(el, null, { renderer: 'canvas' });
  let drawing: ChartDrawing | null = null;
  /** The extent its value axes were drawn at (C2): held by a tick, drawn afresh by a view. */
  let held: (Extent | null)[] | null = null;
  const markOf = (params: unknown): { key: MarkKey; dataIndex: number; datum: unknown } | null => {
    const p = params as { seriesIndex?: number; dataIndex?: number; data?: unknown };
    if (!drawing || p.seriesIndex === undefined || p.dataIndex === undefined) return null;
    const key = drawing.keyAt(p.seriesIndex, p.dataIndex, p.data);
    return key ? { key, dataIndex: p.dataIndex, datum: p.data } : null;
  };
  chart.on('click', (params: unknown) => {
    const mark = markOf(params);
    if (mark && onPick) onPick(mark.key, drawing?.upperAt?.(mark.dataIndex, mark.datum));
  });
  chart.on('dblclick', (params: unknown) => {
    const mark = markOf(params);
    if (mark) more.onDrill?.(mark.key);
  });
  chart.on('magictypechanged', (params: unknown) => {
    const kind = (params as { currentType?: string }).currentType;
    if (kind === 'bar' || kind === 'line') more.onKind?.(kind);
  });
  chart.on('brushEnd', (params: unknown) => {
    const areas = ((params as { areas?: BrushArea[] }).areas ?? []);
    if (!drawing?.brushed) return;
    // dates read as the time axis reads them: ECharts' own parser (local time for a date with no zone)
    more.onBrush?.(areas.length === 0 ? null : drawing.brushed(areas, (t) => echarts.number.parseDate(t).getTime()));
  });
  // A CROSSHAIR SHARED with the page's charts of the same column across (setGroup): the pointer's category, and only
  // that, shown on each -- ECharts' own `connect` would pass on every action (a brush, a zoom, a legend), and a brush
  // on one chart would filter the others' grids
  let group: string | undefined;
  let relaying = false;
  const timeOf = (t: string): number => echarts.number.parseDate(t).getTime();
  const peer: Peer = {
    show: (key) => {
      const i = drawing?.indexOfKey?.(key);
      relaying = true;
      try {
        if (i === undefined) chart.dispatchAction({ type: 'hideTip' });
        else chart.dispatchAction({ type: 'showTip', seriesIndex: 0, dataIndex: i });
      } finally {
        relaying = false;
      }
    },
    hide: () => {
      relaying = true;
      try { chart.dispatchAction({ type: 'hideTip' }); } finally { relaying = false; }
    },
  };
  chart.on('updateAxisPointer', (params: unknown) => {
    if (relaying || group === undefined || !drawing?.pointerKey) return;
    const info = (params as { axesInfo?: { axisDim?: string; value?: number }[] }).axesInfo ?? [];
    const across = info.find((a) => a.axisDim === 'x') ?? info[0];
    const key = across?.value === undefined ? undefined : drawing.pointerKey(across.value, timeOf);
    for (const other of GROUPS.get(group) ?? []) {
      if (other === peer) continue;
      if (key === undefined) other.hide();
      else other.show(key);
    }
  });
  chart.getZr().on('globalout', () => {
    if (group === undefined) return;
    for (const other of GROUPS.get(group) ?? []) if (other !== peer) other.hide();
  });
  const view = el.ownerDocument.defaultView;
  const Observer = view?.ResizeObserver;
  const observer = Observer ? new Observer(() => chart.resize()) : undefined;
  observer?.observe(el);
  return {
    picture(ratio = 2) {
      if (!drawing) return null;
      const width = chart.getWidth();
      const height = chart.getHeight();
      if (!(width > 0 && height > 0)) return null;
      try {
        const png = bytesOf(chart.getDataURL({ type: 'png', pixelRatio: ratio, backgroundColor: '#ffffff' }));
        const jpeg = bytesOf(chart.getDataURL({ type: 'jpeg', pixelRatio: ratio, backgroundColor: '#ffffff' }));
        if (png.length === 0 || jpeg.length === 0) return null;
        return { width, height, pixelWidth: Math.round(width * ratio), pixelHeight: Math.round(height * ratio), png, jpeg };
      } catch {
        // no canvas to read (a DOM without one): the export goes on without the picture
        return null;
      }
    },
    show(d, how = {}) {
      // a tick's window where the last one left it (keptZoom): read before the drawing is replaced
      const zoomBefore = how.ticked === true ? zoomNow(chart) : null;
      const latestBefore = drawing?.latest;
      drawing = d;
      if (el.getAttribute('aria-label') !== d.description) el.setAttribute('aria-label', d.description);
      // a tick's scale the one drawn before it (widened where its values leave it); a view's, its own
      const option = how.ticked === true
        ? keptZoom({ ...(d.hold && held ? heldOption(d.option, d.hold, held) : d.option), animation: false }, zoomBefore,
          latestBefore, d.latest)
        : d.option;
      chart.setOption(option as EChartsOption, { notMerge: true });
      held = d.hold ? drawnExtents(chart, d.hold) : null;
      // a chart that brushes starts brushing on a drag, without its button pressed first
      const brush = (d.option as { brush?: { brushType?: string } }).brush;
      if (brush?.brushType) {
        chart.dispatchAction({ type: 'takeGlobalCursor', key: 'brush', brushOption: { brushType: brush.brushType, brushMode: 'single' } });
      }
    },
    setGroup(next) {
      if (group === next) return;
      if (group !== undefined) GROUPS.get(group)?.delete(peer);
      group = next;
      if (group !== undefined) {
        const peers = GROUPS.get(group) ?? new Set<Peer>();
        peers.add(peer);
        GROUPS.set(group, peers);
      }
    },
    dispose() {
      if (group !== undefined) GROUPS.get(group)?.delete(peer);
      observer?.disconnect();
      chart.dispose();
    },
  };
}
