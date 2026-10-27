// THE FILE THAT DRAWS A TIMESERIES TILE with ECharts (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5): what it shows
// comes from timeseries-option.ts; this mounts it, keeps it sized to its box, and hands the viewer back what a person
// did on it -- the dates a zoom or a pan shows, a date double-clicked.
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
  /** The drawing's own height in pixels: what its panes are laid out in. */
  height(): number;
  picture(ratio?: number): ChartPicture | null;
  /** The drawing as a PNG's data: URL, for its download. */
  pngUrl(): string | null;
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
}, init: (el: HTMLElement) => ECharts): MountedTimeseries {
  const chart = init(el);
  let shown = false;
  let lastKey: string | null = null;
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
    pngUrl() {
      if (!shown) return null;
      try {
        return chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#ffffff' });
      } catch {
        return null;
      }
    },
    dispose() {
      observer?.disconnect();
      chart.dispose();
    },
  };
}
