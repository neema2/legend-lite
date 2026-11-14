// THE FILE THAT DRAWS A TIMESERIES TILE with ECharts (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5): what it shows
// comes from timeseries-option.ts; this mounts it, keeps it sized to its box, and hands the viewer back what a person
// did on it -- the dates a zoom or a pan shows, a date double-clicked, a span dragged, a point clicked.
//
// Its own chunk, fetched with the first timeseries tile -- over the charts' own ECharts, which it is handed (`init`:
// chart-render.ts canvasChart, fetched beside it): chart-render.ts registers every chart and component a timeseries draws
// with (lines, bars and dots, the grids, the crosshair and its hover, the zoom and the navigator, the annotations' lines
// and areas), so a page's charts and its timeseries share one build of it, still reached by a dynamic import alone, and
// a timeseries adds no other way to split it (test/bundle-budget.test.ts: a first chart's fetch is unchanged by it).

import type { EChartsOption } from 'echarts';
import type { ECharts } from 'echarts/core';
import type { ChartPicture } from './chart-render.ts';
import { isoOf } from './timeseries-option.ts';

const HALF_DAY = 43_200_000;

/** A timeseries mounted in an element. */
export interface MountedTimeseries {
  /** Draw this option, replacing what was drawn. */
  show(option: EChartsOption, description: string): void;
  /** Zoom to the dates given (either end open: the first or the last date). */
  zoomTo(from: string | undefined, to: string | undefined): void;
  /** The dates the zoom shows now, as ISO days (undefined at an end that is the series' own). */
  zoomed(): { readonly from?: string; readonly to?: string };
  /** One line brought forward (its key), or none. */
  highlight(key: string | null): void;
  /** Measuring (Phase 5): a drag across the chart is a span measured, told by `onSpan`, never a pan. */
  measure(on: boolean): void;
  /** Drawing (Phase 5): a click in a pane is a point put there, told by `onPoint`, never a pan. */
  draw(on: boolean): void;
  /** The drawing's own height in pixels: what its panes are laid out in. */
  height(): number;
  picture(ratio?: number): ChartPicture | null;
  /**
   * `option` drawn off the screen at `width` by `height` on `background` -- a picture's own, its heading in it -- as a PNG
   * at three times its pixels, or as SVG (its painter fetched then): a data: URL, for its download (the next phases' 2).
   */
  exportUrl(option: EChartsOption, kind: 'png' | 'svg', width: number, height: number, background: string): Promise<string>;
  dispose(): void;
}

/** A data: URL's bytes. */
function bytesOf(dataUrl: string): Uint8Array {
  const b64 = dataUrl.slice(dataUrl.indexOf(',') + 1);
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Mount a timeseries in `el`, sized to it and kept so: `onZoom` is told the dates shown after a zoom or a pan (once it
 * settles), `onDate` the date a person double-clicked (to put an event there), `onResize` that the box changed size (its
 * panes are laid out in pixels).
 */
export function mountTimeseries(el: HTMLElement, on: {
  readonly onZoom: (from: string | undefined, to: string | undefined) => void;
  readonly onDate?: (date: string) => void;
  readonly onResize?: () => void;
  /** While measuring: a drag across the chart, its two ends' dates in order -- `done` once it is let go. */
  readonly onSpan?: (from: string, to: string, done: boolean) => void;
  /** While drawing: a click in a pane, its date and its value on the axis the pane's drawings are drawn on. */
  readonly onPoint?: (pane: number, date: string, value: number) => void;
}, init: (el: HTMLElement) => ECharts, svg?: (el: HTMLElement) => Promise<ECharts>): MountedTimeseries {
  const chart = init(el);
  let shown = false;
  let lastKey: string | null = null;
  let measuring = false;
  let drawing = false;
  let dragFrom: { readonly t: number; readonly x: number } | undefined;
  /** The pane (its grid) under a pixel of the chart, or none: every pane shares the dates (the Phase 5 review's notes:
   *  a drag in a lower pane measured nothing). */
  const gridAt = (e: { offsetX: number; offsetY: number }): number | undefined => {
    const grids = ((chart.getOption() as { grid?: unknown[] }).grid ?? []).length;
    for (let g = 0; g < grids; g++) if (chart.containPixel({ gridIndex: g }, [e.offsetX, e.offsetY])) return g;
    return undefined;
  };
  /** The date under a pixel of the chart, in its grids' shared time. */
  const timeAt = (e: { offsetX: number; offsetY: number }): number | undefined => {
    const at = chart.convertFromPixel({ gridIndex: gridAt(e) ?? 0 }, [e.offsetX, e.offsetY]) as unknown as number[] | undefined;
    const t = at?.[0];
    return typeof t === 'number' && Number.isFinite(t) ? t : undefined;
  };
  // each end the day nearest it, as a point drawn is
  const spanned = (t: number, done: boolean): void => {
    const [a, b] = dragFrom!.t <= t ? [dragFrom!.t, t] : [t, dragFrom!.t];
    on.onSpan?.(isoOf(a + HALF_DAY), isoOf(b + HALF_DAY), done);
  };
  chart.getZr().on('mousedown', (e: { offsetX: number; offsetY: number }) => {
    if (!measuring || gridAt(e) === undefined) return;
    const t = timeAt(e);
    dragFrom = t === undefined ? undefined : { t, x: e.offsetX };
  });
  chart.getZr().on('mousemove', (e: { offsetX: number; offsetY: number }) => {
    if (!measuring || dragFrom === undefined) return;
    const t = timeAt(e);
    if (t !== undefined && Math.abs(e.offsetX - dragFrom.x) > 3) spanned(t, false);
  });
  chart.getZr().on('mouseup', (e: { offsetX: number; offsetY: number }) => {
    if (!measuring || dragFrom === undefined) return;
    const t = timeAt(e);
    // a click, no drag, measures nothing
    if (t !== undefined && Math.abs(e.offsetX - dragFrom.x) > 3) spanned(t, true);
    dragFrom = undefined;
  });
  // a click while drawing: the pane under it -- its drawings' series (timeseries-option.ts `drawings:<pane>`, drawn in
  // every pane while drawing) -- and the point in that series' own axes, so a point is drawn back where it was put
  chart.getZr().on('click', (e: { offsetX: number; offsetY: number }) => {
    if (!drawing || !on.onPoint) return;
    const px = [e.offsetX, e.offsetY];
    const ids = ((chart.getOption() as { series?: { id?: string }[] }).series ?? []).map((x) => x.id).filter((id): id is string => id?.startsWith('drawings:') === true);
    const id = ids.find((x) => chart.containPixel({ seriesId: x }, px));
    if (id === undefined) return;
    const at = chart.convertFromPixel({ seriesId: id }, px) as unknown as number[] | undefined;
    const [t, v] = [at?.[0], at?.[1]];
    // the day nearest the click: a day's values are drawn at its start
    if (typeof t === 'number' && Number.isFinite(t) && typeof v === 'number' && Number.isFinite(v)) on.onPoint(Number(id.slice('drawings:'.length)), isoOf(t + HALF_DAY), v);
  });
  const window = (): { from?: string; to?: string } => {
    const zoom = (chart.getOption() as { dataZoom?: { startValue?: number | string; endValue?: number | string }[] }).dataZoom?.[0];
    const at = (v: number | string | undefined): string | undefined => (typeof v === 'number' ? isoOf(v) : typeof v === 'string' ? v.slice(0, 10) : undefined);
    const from = at(zoom?.startValue);
    const to = at(zoom?.endValue);
    return { ...(from !== undefined ? { from } : {}), ...(to !== undefined ? { to } : {}) };
  };
  chart.on('datazoom', () => {
    const w = window();
    on.onZoom(w.from, w.to);
  });
  chart.getZr().on('dblclick', (e: { offsetX: number; offsetY: number }) => {
    if (!on.onDate) return;
    if (!chart.containPixel({ gridIndex: 0 }, [e.offsetX, e.offsetY])) return;
    const at = chart.convertFromPixel({ gridIndex: 0 }, [e.offsetX, e.offsetY]) as unknown as number[] | undefined;
    const t = at?.[0];
    if (typeof t === 'number' && Number.isFinite(t)) on.onDate(isoOf(t));
  });
  const view = el.ownerDocument.defaultView;
  const Observer = view?.ResizeObserver;
  const observer = Observer ? new Observer(() => {
    chart.resize();
    on.onResize?.();
  }) : undefined;
  observer?.observe(el);
  return {
    show(option, description) {
      el.setAttribute('aria-label', description);
      chart.setOption(option, { notMerge: true, lazyUpdate: false });
      shown = true;
      if (lastKey !== null) chart.dispatchAction({ type: 'highlight', seriesId: lastKey });
    },
    zoomTo(from, to) {
      chart.dispatchAction({ type: 'dataZoom', dataZoomIndex: 0, ...(from !== undefined ? { startValue: from } : { start: 0 }), ...(to !== undefined ? { endValue: to } : { end: 100 }) });
    },
    zoomed: window,
    highlight(key) {
      if (lastKey !== null) chart.dispatchAction({ type: 'downplay', seriesId: lastKey });
      lastKey = key;
      if (key !== null) chart.dispatchAction({ type: 'highlight', seriesId: key });
    },
    measure(next) {
      measuring = next;
      dragFrom = undefined;
    },
    draw(next) {
      drawing = next;
    },
    height() {
      return chart.getHeight();
    },
    picture(ratio = 2) {
      if (!shown) return null;
      const width = chart.getWidth();
      const height = chart.getHeight();
      if (!(width > 0 && height > 0)) return null;
      try {
        const png = bytesOf(chart.getDataURL({ type: 'png', pixelRatio: ratio, backgroundColor: '#ffffff' }));
        const jpeg = bytesOf(chart.getDataURL({ type: 'jpeg', pixelRatio: ratio, backgroundColor: '#ffffff' }));
        if (png.length === 0 || jpeg.length === 0) return null;
        return { width, height, pixelWidth: Math.round(width * ratio), pixelHeight: Math.round(height * ratio), png, jpeg };
      } catch {
        return null;
      }
    },
    async exportUrl(option, kind, width, height, background) {
      if (kind === 'svg' && svg === undefined) throw new Error('this chart was mounted with no SVG painter');
      const doc = el.ownerDocument;
      // a box of the picture's size off the screen: the chart on the screen is left as it is (its zoom, its size)
      const off = doc.createElement('div');
      off.style.cssText = `position:fixed;left:-10000px;top:0;width:${width}px;height:${height}px`;
      doc.body.append(off);
      try {
        const picture = kind === 'svg' ? await svg!(off) : init(off);
        try {
          picture.setOption({ ...option, animation: false, backgroundColor: background }, { notMerge: true, lazyUpdate: false });
          return picture.getDataURL(kind === 'svg' ? { type: 'svg' } : { type: 'png', pixelRatio: 3, backgroundColor: background });
        } finally {
          picture.dispose();
        }
      } finally {
        off.remove();
      }
    },
    dispose() {
      observer?.disconnect();
      chart.dispose();
    },
  };
}
