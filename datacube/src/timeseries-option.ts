// A TIMESERIES TILE AS A DRAWING (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5): its series' rows, as the database
// answered them (timeseries-query.ts), laid out as lines in stacked panes on one time axis -- the ECharts option.
//
// PURE and library-free, as chart-option.ts is: it imports nothing from ECharts but its types, so it is tested in node
// (timeseries-render.ts is the file that draws). Nothing here computes a figure: a line's points are its rows, oldest
// first; a band's fill is stacked on its lower edge by the width the database gave it; a value is written as the grid
// writes it, or as its unit says (a return as a percentage).
//
// The house rules (chart-option.ts's, the dataviz references'): 2px lines in the palette's fixed order, a hairline
// recessive grid, ink-coloured text; and a trader's: the panes share one time axis and one crosshair, each line's latest
// value is tagged at its end, the navigator under the chart zooms and pans every pane at once.

import type { EChartsOption } from 'echarts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import { escapeHtml, periodText, type ChartTheme } from './chart-option.ts';
import {
  MAX_PANES, TS_POINT_LIMIT, viewSetting, type TimeseriesSpec, type TsExpr, type TsSeries,
} from './timeseries-spec.ts';
import { boundsOf, exprText, inlined, seriesName, splitName, type TsUnit } from './timeseries-expr.ts';
import {
  TS_DATE, TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_SPLIT, TS_UPPER, TS_VALUE, TS_WIDTH, seriesNotes, type TsQuery,
} from './timeseries-query.ts';

/** A series' query and what the database answered. */
export interface TsResult {
  readonly query: TsQuery;
  readonly table: ResultTable;
}

/** Whether every figure a line draws is above zero -- a log axis's to draw -- by comparing them (its candles' lows and
 *  its band's lower edge too). */
function aboveZero(l: TsLine): boolean {
  const above = (v: number | null): boolean => v === null || v > 0;
  return l.points.every(([, v]) => above(v)) && (l.lower ?? []).every(above) && (l.candles?.low ?? []).every(above);
}

/** One drawn line: a series, or one value of a split series. */
export interface TsLine {
  /** `A`, or `A` and its split value: what the legend and a hover know it by. */
  readonly key: string;
  readonly series: TsSeries;
  readonly name: string;
  /** Its split value, for a split series. */
  readonly split?: Scalar;
  readonly unit: TsUnit;
  /** Whether its figures are a level: a log scale draws only levels (a return may be zero or below). */
  readonly level: boolean;
  readonly band: boolean;
  readonly color: string;
  readonly dash: 'solid' | 'dashed' | 'dotted';
  /** Its points, oldest first: an ISO date (or date-time) and a value, null where there is none (a gap). */
  readonly points: readonly (readonly [string, number | null])[];
  /** A band's edges and its width, point by point, as its points. */
  readonly lower?: readonly (number | null)[];
  readonly upper?: readonly (number | null)[];
  readonly width?: readonly (number | null)[];
  /** A candle line's opens, highs and lows, point by point, as its points (its closes). */
  readonly candles?: { readonly open: readonly (number | null)[]; readonly high: readonly (number | null)[]; readonly low: readonly (number | null)[] };
  /** The grid's column whose format its values take (a level's own); undefined for a figure of its own unit. */
  readonly formatColumn?: string;
  /** The source whose grid formats it: the one its series reads first. */
  readonly formatSource?: string;
  /** The least and the most its figures can be (an RSI's): its axis shows them whole. */
  readonly bounds?: readonly [number, number];
}

/** A value read off a result: a number, a bigint, or a number's exact text (a Number column's). */
export function toNumber(v: Scalar | undefined): number | null {
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/** A date read off a result as the time axis reads it: its ISO text. */
function dateText(v: Scalar | undefined): string | null {
  if (v === null || v === undefined) return null;
  return String(v).replace(' ', 'T');
}

const column = (t: ResultTable, name: string): readonly Scalar[] | undefined => t.columns.find((c) => c.name === name)?.values;

/** The column a level is written in the format of: its expression's first column of the grid. */
function firstColumn(e: TsExpr): string | undefined {
  switch (e.kind) {
    case 'column': return e.column;
    case 'binary': return firstColumn(e.left) ?? firstColumn(e.right);
    case 'call': return e.args.map(firstColumn).find((c) => c !== undefined);
    default: return undefined;
  }
}

const DASHES = ['solid', 'dashed', 'dotted'] as const;

/**
 * THE LINES, from what each series' query answered: a line per series -- per split value, of a split series -- its
 * rows oldest first, its colour its own slot or its place, its dash after the palette's slots run out. And what the
 * reader should know: a series cut at its most points, a series of no instrument whose dates had several rows.
 */
export function tsLines(ts: TimeseriesSpec, results: readonly TsResult[], theme: ChartTheme): { lines: TsLine[]; notes: string[] } {
  const lines: TsLine[] = [];
  const notes: string[] = [];
  let place = 0;
  for (const s of ts.series) {
    const r = results.find((x) => x.query.series === s.id);
    if (!r) continue;
    const t = r.table;
    // every series' query answers with its dates and values (timeseries-query.ts seriesQuery): one without is a bug
    const dates = column(t, TS_DATE);
    const values = column(t, TS_VALUE);
    if (dates === undefined || values === undefined) throw new Error(`series ${s.id}'s answer has no ${dates === undefined ? TS_DATE : TS_VALUE}`);
    const lower = column(t, TS_LOWER);
    const upper = column(t, TS_UPPER);
    const width = column(t, TS_WIDTH);
    const opens = column(t, TS_OPEN);
    const highs = column(t, TS_HIGH);
    const lows = column(t, TS_LOW);
    const splits = column(t, TS_SPLIT);
    if (t.rowCount > TS_POINT_LIMIT) notes.push(`${seriesName(s)}: its latest ${TS_POINT_LIMIT.toLocaleString(UI_LOCALE)} points (it has more).`);
    // what its rows say a reader should know -- several rows a date, no frequency to annualize by -- by the rule
    // Python's frame says them by too (timeseries-query.ts seriesNotes)
    notes.push(...seriesNotes(ts, s, t, r.query.sources[0]));
    // a log scale draws levels alone: a line that may be zero or below keeps its axis straight (the review's G3)
    if (ts.panes?.[s.pane ?? 0]?.log === true && !r.query.level) {
      notes.push(`${seriesName(s)} is no level (it may be zero or below), so its axis is not on a log scale.`);
    }
    // the rows came latest first (the query's order, so a cut keeps the latest): each line's, oldest first
    const groups = new Map<string, { split: Scalar | undefined; at: number[] }>();
    const count = Math.min(t.rowCount, TS_POINT_LIMIT);
    for (let i = count - 1; i >= 0; i--) {
      const split = splits ? splits[i] ?? null : undefined;
      const k = split === undefined ? '' : JSON.stringify(split);
      let g = groups.get(k);
      if (!g) groups.set(k, (g = { split, at: [] }));
      g.at.push(i);
    }
    // in the split's order as a person reads it: 2Y before 10Y
    const ordered = [...groups.values()].sort((a, b) => String(a.split ?? '').localeCompare(String(b.split ?? ''), UI_LOCALE, { numeric: true }));
    // answered, so written as a query already: its expression inlines (a failure here is a bug, never a line unformatted)
    const expr = inlined(s.expr, ts.series);
    const formatColumn = r.query.unit === 'same' ? firstColumn(expr) : undefined;
    const bounds = boundsOf(expr);
    for (const g of ordered) {
      const slot = s.style?.color ?? place;
      place++;
      const points = g.at.flatMap((i) => {
        const d = dateText(dates[i]);
        return d === null ? [] : [[d, toNumber(values[i])] as const];
      });
      const take = (c: readonly Scalar[] | undefined): (number | null)[] | undefined => (c ? g.at.map((i) => toNumber(c[i])) : undefined);
      const lo = take(lower);
      const hi = take(upper);
      const wd = take(width);
      const candle = r.query.candles && opens && highs && lows ? { open: take(opens)!, high: take(highs)!, low: take(lows)! } : undefined;
      lines.push({
        key: g.split === undefined ? s.id : `${s.id}\u0000${String(g.split)}`,
        series: s,
        name: splitName(s, g.split),
        ...(g.split !== undefined ? { split: g.split } : {}),
        unit: r.query.unit,
        level: r.query.level,
        band: r.query.band,
        color: theme.series[slot % theme.series.length]!,
        dash: s.style?.dash ?? DASHES[Math.floor(slot / theme.series.length) % DASHES.length]!,
        points,
        ...(lo ? { lower: lo } : {}),
        ...(hi ? { upper: hi } : {}),
        ...(wd ? { width: wd } : {}),
        ...(candle ? { candles: candle } : {}),
        ...(formatColumn !== undefined ? { formatColumn } : {}),
        ...(formatColumn !== undefined && r.query.sources[0] !== undefined ? { formatSource: r.query.sources[0] } : {}),
        ...(bounds !== undefined ? { bounds } : {}),
      });
    }
    // a level at or below zero on a log pane: a log scale cannot draw it, so its axis stays straight (the second
    // audit's S1: a yield gone negative, its points left off a log axis without a word)
    if (ts.panes?.[s.pane ?? 0]?.log === true && r.query.level) {
      const below = lines.filter((l) => l.series.id === s.id && !aboveZero(l)).map((l) => l.name);
      if (below.length > 0) {
        notes.push(`${below.join(', ')} ${below.length === 1 ? 'has' : 'have'} values at or below zero, which a log scale cannot draw, so ${below.length === 1 ? 'its' : 'their'} axis is not on a log scale.`);
      }
    }
  }
  if (lines.length > theme.series.length) notes.push(`${lines.length} lines: past the palette's ${theme.series.length} colours they repeat, dashed.`);
  return { lines, notes };
}


// ------------------------------------------------------------------ writing a value

/** `v` to `digits` places; a value that rounds to zero is zero, never "-0.000". */
const fixed = (v: number, digits: number): string =>
  (Math.abs(v) < 0.5 * 10 ** -digits ? 0 : v).toLocaleString(UI_LOCALE, { minimumFractionDigits: digits, maximumFractionDigits: digits });

/** A value written as its grid writes its column (the grid's formatter, bound to the line's column). */
export type TsFormat = (v: number) => string;

/**
 * A line's value as it is written -- on its axis, in a hover, at its end: a level in its grid column's format (`format`),
 * a ratio as a percentage, a percent figure with its sign, an index and a plain number to their places.
 */
export function valueText(line: Pick<TsLine, 'unit'>, v: number | null, format?: TsFormat): string {
  if (v === null) return '—';
  switch (line.unit) {
    case 'ratio': return `${fixed(v * 100, 2)}%`;
    case 'percent': return `${fixed(v, 2)}%`;
    case 'index': return fixed(v, 2);
    case 'number': {
      const a = Math.abs(v);
      if (a >= 1000) return fixed(v, 0);
      // a small figure (a covariance of daily returns) to three significant places, not rounded to nothing
      if (a !== 0 && a < 0.01) return v.toLocaleString(UI_LOCALE, { maximumSignificantDigits: 3, ...(a < 1e-6 ? { notation: 'scientific' } : {}) });
      return fixed(v, a >= 10 ? 2 : 3);
    }
    case 'same':
      return format ? format(v) : fixed(v, Math.abs(v) >= 1000 ? 0 : 2);
  }
}

/** A number to `digits` places, as a tick says it: one that rounds to zero is zero, never "-0" (the review's G9). */
function places(v: number, digits: number): string {
  const r = Math.round(v * 10 ** digits) / 10 ** digits;
  return (r === 0 ? 0 : r).toLocaleString(UI_LOCALE, { maximumFractionDigits: digits });
}

/** An axis' tick written by the unit of the lines on it: shorter than a hover's (no places a tick does not need). */
export function tickText(unit: TsUnit, v: number): string {
  switch (unit) {
    case 'ratio': return `${places(v * 100, 1)}%`;
    case 'percent': return `${places(v, 1)}%`;
    default: {
      const a = Math.abs(v);
      if (a >= 1e9) return `${places(v / 1e9, 1)}B`;
      if (a >= 1e6) return `${places(v / 1e6, 1)}M`;
      if (a >= 1e4) return `${places(v / 1e3, 1)}K`;
      return places(v, a >= 100 ? 0 : a >= 1 ? 2 : 4);
    }
  }
}

// ------------------------------------------------------------------ the layout

/** The panes in use, top to bottom: each its index, its share of the height, its scale. */
export function panesOf(ts: TimeseriesSpec, lines: readonly TsLine[]): { index: number; height: number; log: boolean }[] {
  const used = new Set<number>([0, ...lines.filter((l) => !l.series.hidden).map((l) => l.series.pane ?? 0)]);
  return [...used].filter((p) => p < MAX_PANES).sort((a, b) => a - b).map((index) => ({
    index,
    height: ts.panes?.[index]?.height ?? (index === 0 ? 3 : 1),
    log: ts.panes?.[index]?.log === true,
  }));
}

/** Where the viewer is: the dates its zoom shows (absent: every date), the container's height, and how to write. */
export interface TsDrawContext {
  readonly theme: ChartTheme;
  /** How a line's values are written as its grid writes its column (a level's); undefined: by its unit alone. */
  readonly formatOf?: (line: TsLine) => TsFormat | undefined;
  readonly view?: { readonly from?: string; readonly to?: string };
  readonly height?: number;
}

const NAVIGATOR = 30;
const AXIS_LABELS = 22;
const PANE_GAP = 14;
const TOP = 10;

/** A line's tooltip and axis' place: its pane's index in the drawing, and its axis' index (left, right). */
interface Placed {
  readonly line: TsLine;
  readonly grid: number;
  readonly yAxis: number;
}

/**
 * THE DRAWING: a grid per pane, stacked, each with its time axis (one zoom moves them all, one crosshair crosses them),
 * a value axis on the left and -- for a second unit, or a line told so -- one on the right, each in its unit; every line
 * in its pane and on its axis, drawn as its style says; a band's fill on its lower edge; the annotations over the
 * panes; the navigator under the last.
 */
export function timeseriesOption(ts: TimeseriesSpec, lines: readonly TsLine[], ctx: TsDrawContext): EChartsOption {
  const theme = ctx.theme;
  const panes = panesOf(ts, lines);
  const total = ctx.height ?? 420;
  const navigator = viewSetting(ts, 'navigator');
  const bottom = AXIS_LABELS + (navigator ? NAVIGATOR + 8 : 0);
  const room = Math.max(60, total - TOP - bottom - PANE_GAP * (panes.length - 1));
  const weight = panes.reduce((a, p) => a + p.height, 0);
  const shown = lines.filter((l) => !l.series.hidden);
  // room on the right for each line's latest value, tagged at its end (the axes' labels are inside the grid already)
  const lastValues = viewSetting(ts, 'lastValues');
  const tag = lastValues ? Math.max(0, ...shown.filter((l) => !l.band && !l.candles && !['bars', 'dots'].includes(l.series.style?.kind ?? 'line'))
    .map((l) => valueText(l, [...l.points].reverse().find((p) => p[1] !== null)?.[1] ?? null, ctx.formatOf?.(l)).length)) : 0;
  const right = 16 + (tag > 0 ? tag * 6 + 14 : 0);
  let y = TOP;
  const grids = panes.map((p) => {
    const h = Math.max(40, Math.round((room * p.height) / weight));
    const g = { left: 12, right, top: y, height: h, containLabel: true };
    y += h + PANE_GAP;
    return g;
  });
  // each pane's units, in the order its lines come: the first on the left, a second on the right
  const yAxes: Record<string, unknown>[] = [];
  const axisOf = new Map<string, number>();
  const placed: Placed[] = [];
  panes.forEach((p, gi) => {
    const here = shown.filter((l) => (l.series.pane ?? 0) === p.index);
    const units: TsUnit[] = [];
    for (const l of here) if (l.series.axis === undefined && !units.includes(l.unit)) units.push(l.unit);
    const sideOf = (l: TsLine): 'left' | 'right' => l.series.axis ?? (units.indexOf(l.unit) === 0 ? 'left' : 'right');
    for (const side of ['left', 'right'] as const) {
      const on = here.filter((l) => sideOf(l) === side);
      if (on.length === 0 && !(side === 'left')) continue;
      const unit = on[0]?.unit ?? 'same';
      // every line on it bounded alike (RSIs): the axis shows the bounds whole
      const bounds = on[0]?.bounds;
      const bounded = bounds !== undefined && on.every((l) => l.bounds?.[0] === bounds[0] && l.bounds?.[1] === bounds[1]) ? bounds : undefined;
      axisOf.set(`${gi}|${side}`, yAxes.length);
      yAxes.push({
        // a log pane's axes of levels alone: a return, a difference, a z-score may be zero or below, which a log axis drops
        // without a word (the review's G3) -- that axis straight, and the drawing says why (tsLines)
        type: p.log && on.length > 0 && on.every((l) => l.level && aboveZero(l)) ? 'log' : 'value',
        gridIndex: gi,
        position: side,
        scale: bounded === undefined,
        // as many ticks as the pane has room for: a short pane's labels never run into each other; a bounded axis's at
        // even steps of its bounds (an RSI's 0, 50, 100 -- or every 25)
        ...(bounded !== undefined
          ? { min: bounded[0], max: bounded[1], interval: (bounded[1] - bounded[0]) / (grids[gi]!.height >= 160 ? 4 : 2) }
          : { splitNumber: Math.max(1, Math.min(5, Math.floor(grids[gi]!.height / 45))) }),
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: side === 'left', lineStyle: { color: theme.grid, width: 1 } },
        axisLabel: {
          // two units, two axes: the right one's ticks in its line's colour, so which line reads which axis is seen
          color: side === 'right' && on[0] !== undefined ? on[0].color : theme.inkSecondary,
          fontSize: 11,
          fontFamily: theme.font,
          formatter: (v: number) => tickText(unit, v),
        },
        axisPointer: { label: { formatter: (o: { value: number }) => tickText(unit, o.value) } },
      });
    }
    for (const l of here) placed.push({ line: l, grid: gi, yAxis: axisOf.get(`${gi}|${sideOf(l)}`) ?? axisOf.get(`${gi}|left`)! });
  });
  const lastGrid = panes.length - 1;
  const xAxes = panes.map((_, gi) => ({
    type: 'time',
    gridIndex: gi,
    axisLine: { lineStyle: { color: theme.axis } },
    axisTick: { show: gi === lastGrid, lineStyle: { color: theme.axis } },
    axisLabel: { show: gi === lastGrid, color: theme.inkSecondary, fontSize: 11, fontFamily: theme.font, hideOverlap: true },
    splitLine: { show: false },
    axisPointer: { label: { show: gi === lastGrid } },
  }));
  const series: Record<string, unknown>[] = [];
  for (const p of placed) {
    const l = p.line;
    const kind = l.series.style?.kind ?? 'line';
    const width = l.series.style?.width ?? 2;
    const common = {
      id: l.key,
      name: l.name,
      xAxisIndex: p.grid,
      yAxisIndex: p.yAxis,
      emphasis: { focus: 'series' },
      animation: false,
    };
    if (l.band) {
      // the band: its lower edge (no line of its own), its width stacked on it and filled, its edges drawn faint
      const stack = `band:${l.key}`;
      const at = (vals: readonly (number | null)[] | undefined): (readonly [string, number | null])[] =>
        l.points.map((pt, i) => [pt[0], vals?.[i] ?? null] as const);
      series.push(
        { ...common, id: `${l.key}:base`, type: 'line', stack, data: at(l.lower), showSymbol: false, lineStyle: { opacity: 0 }, silent: true, tooltip: { show: false }, emphasis: { disabled: true } },
        { ...common, id: `${l.key}:fill`, type: 'line', stack, data: at(l.width), showSymbol: false, lineStyle: { opacity: 0 }, areaStyle: { color: l.color, opacity: 0.12 }, silent: true, tooltip: { show: false }, emphasis: { disabled: true } },
        { ...common, id: `${l.key}:lower`, type: 'line', data: at(l.lower), showSymbol: false, lineStyle: { color: l.color, width: 1, opacity: 0.7 } },
        { ...common, id: `${l.key}:upper`, type: 'line', data: at(l.upper), showSymbol: false, lineStyle: { color: l.color, width: 1, opacity: 0.7 } },
      );
      continue;
    }
    if (l.candles) {
      // a candle per period: [date, open, close, low, high], as ECharts' candlestick reads one; up green, down red
      const c = l.candles;
      const up = theme.series[2] ?? l.color;
      const down = theme.bad ?? theme.series[7] ?? l.color;
      series.push({
        ...common, type: 'candlestick', barMaxWidth: 14,
        data: l.points.map((pt, i) => [pt[0], c.open[i] ?? null, pt[1], c.low[i] ?? null, c.high[i] ?? null]),
        encode: { x: 0, y: [1, 2, 3, 4] },
        itemStyle: { color: up, color0: down, borderColor: up, borderColor0: down },
      });
      continue;
    }
    const data = l.points as unknown as unknown[];
    const end = lastValues && kind !== 'bars' && kind !== 'dots' ? {
      endLabel: {
        show: true,
        formatter: (o: { value: readonly [string, number | null] }) => valueText(l, o.value[1], ctx.formatOf?.(l)),
        color: theme.surface,
        backgroundColor: l.color,
        padding: [2, 4],
        borderRadius: 2,
        fontSize: 10,
        fontFamily: theme.font,
        distance: 4,
      },
    } : {};
    switch (kind) {
      case 'bars':
        series.push({ ...common, type: 'bar', data, itemStyle: { color: l.color, opacity: 0.8 }, barMaxWidth: 8, large: true });
        break;
      case 'dots':
        series.push({ ...common, type: 'scatter', data, symbolSize: 4, itemStyle: { color: l.color } });
        break;
      default:
        series.push({
          ...common,
          type: 'line',
          data,
          showSymbol: false,
          symbol: 'circle',
          symbolSize: 5,
          connectNulls: false,
          sampling: 'lttb',
          ...(kind === 'step' ? { step: 'end' } : {}),
          lineStyle: { color: l.color, width, type: l.dash },
          itemStyle: { color: l.color },
          ...(kind === 'area' ? { areaStyle: { color: l.color, opacity: 0.14 } } : {}),
          ...end,
        });
    }
  }
  // the annotations: a level on its pane, an event and a period across every pane -- hosted by an empty line per pane,
  // so they stand whatever lines are shown
  panes.forEach((p, gi) => {
    const levels = (ts.annotations ?? []).filter((a) => a.kind === 'level' && (a.pane ?? 0) === p.index);
    const events = (ts.annotations ?? []).filter((a) => a.kind === 'event');
    const periods = (ts.annotations ?? []).filter((a) => a.kind === 'period');
    if (levels.length + events.length + periods.length === 0) return;
    const label = (text: string | undefined, show: boolean): Record<string, unknown> => ({
      show: show && text !== undefined && text !== '', formatter: text ?? '', color: theme.inkSecondary, fontSize: 10, fontFamily: theme.font,
    });
    series.push({
      id: `annotations:${p.index}`,
      type: 'line',
      xAxisIndex: gi,
      yAxisIndex: axisOf.get(`${gi}|left`)!,
      data: [],
      silent: true,
      tooltip: { show: false },
      markLine: {
        symbol: 'none',
        silent: true,
        animation: false,
        data: [
          ...levels.map((a) => ({ yAxis: (a as { value: number }).value, label: { ...label(a.label, true), position: 'insideEndTop' }, lineStyle: { color: theme.muted, type: 'dashed', width: 1 } })),
          ...events.map((a) => ({ xAxis: (a as { date: string }).date, label: { ...label(a.label, gi === 0), position: 'insideEndTop', rotate: 0 }, lineStyle: { color: theme.muted, type: 'solid', width: 1 } })),
        ],
      },
      markArea: {
        silent: true,
        itemStyle: { color: theme.grid, opacity: 0.45 },
        label: { ...label(undefined, false) },
        data: periods.map((a) => {
          const pr = a as { from: string; to: string; label?: string };
          return [{ xAxis: pr.from, label: { ...label(pr.label, gi === 0), position: 'insideTop' } }, { xAxis: pr.to }];
        }),
      },
    });
  });
  const allX = panes.map((_, gi) => gi);
  const view = ctx.view ?? {};
  const zoomAt = { ...(view.from !== undefined ? { startValue: view.from } : {}), ...(view.to !== undefined ? { endValue: view.to } : {}) };
  const dataZoom: Record<string, unknown>[] = [
    { type: 'inside', xAxisIndex: allX, filterMode: 'filter', zoomOnMouseWheel: true, moveOnMouseMove: true, ...zoomAt },
  ];
  if (navigator) {
    dataZoom.push({
      type: 'slider', xAxisIndex: allX, filterMode: 'filter', bottom: AXIS_LABELS - 14, height: NAVIGATOR, ...zoomAt,
      borderColor: theme.grid, fillerColor: 'rgba(42,120,214,0.10)', handleStyle: { color: theme.surface, borderColor: theme.axis },
      moveHandleStyle: { color: theme.axis }, textStyle: { color: theme.inkSecondary, fontSize: 10, fontFamily: theme.font },
      dataBackground: { lineStyle: { color: theme.axis, width: 1 }, areaStyle: { color: theme.grid, opacity: 0.5 } },
      selectedDataBackground: { lineStyle: { color: theme.series[0], width: 1 }, areaStyle: { color: theme.series[0], opacity: 0.15 } },
      labelFormatter: (_: number, text: string) => text.slice(0, 10),
    });
  }
  const grain = ts.grain ?? 'day';
  const crosshair = viewSetting(ts, 'crosshair');
  return {
    backgroundColor: 'transparent',
    textStyle: { fontFamily: theme.font },
    animation: false,
    grid: grids,
    xAxis: xAxes,
    yAxis: yAxes,
    series,
    dataZoom,
    axisPointer: {
      link: [{ xAxisIndex: 'all' }],
      lineStyle: { color: theme.muted, width: 1, type: 'dashed' },
      label: { backgroundColor: theme.inkSecondary, color: theme.surface, fontSize: 10, fontFamily: theme.font, formatter: (o: { value: number | string; axisDimension?: string }) => (o.axisDimension === 'x' ? dateLabel(o.value, grain) : String(o.value)) },
    },
    tooltip: crosshair ? {
      trigger: 'axis',
      axisPointer: { type: 'cross', snap: true },
      backgroundColor: theme.surface,
      borderColor: theme.grid,
      textStyle: { color: theme.ink, fontSize: 12, fontFamily: theme.font },
      confine: true,
      order: 'valueDesc',
      formatter: (items: readonly { seriesId?: string; value?: unknown; axisValue?: number | string; color?: string }[]) => {
        const at = items[0]?.axisValue;
        const rows: string[] = [];
        const seen = new Set<string>();
        for (const it of items) {
          const id = it.seriesId ?? '';
          const key = id.replace(/:(lower|upper)$/, '');
          const l = lines.find((x) => x.key === key);
          if (!l || seen.has(id)) continue;
          seen.add(id);
          const v = Array.isArray(it.value) ? toNumber(it.value[1] as Scalar) : null;
          const which = id.endsWith(':lower') ? ' (lower)' : id.endsWith(':upper') ? ' (upper)' : '';
          if (l.candles && Array.isArray(it.value)) {
            // a candle: its open, high, low and close, each as the grid writes its column
            const [, o, c, lo, hi] = it.value as Scalar[];
            const say = (x: Scalar | undefined): string => escapeHtml(valueText(l, toNumber(x), ctx.formatOf?.(l)));
            rows.push(`<div><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${l.color};margin-right:6px"></span>${escapeHtml(l.name)}</div>`
              + `<div style="display:grid;grid-template-columns:auto auto auto auto;gap:2px 10px;font-variant-numeric:tabular-nums;margin:0 0 4px 14px">`
              + `<span>O ${say(o)}</span><span>H ${say(hi)}</span><span>L ${say(lo)}</span><b>C ${say(c)}</b></div>`);
            continue;
          }
          rows.push(`<div style="display:flex;gap:12px;justify-content:space-between"><span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${l.color};margin-right:6px"></span>${escapeHtml(l.name)}${which}</span><b style="font-variant-numeric:tabular-nums">${escapeHtml(valueText(l, v, ctx.formatOf?.(l)))}</b></div>`);
        }
        return `<div style="font-weight:600;margin-bottom:4px">${escapeHtml(at === undefined ? '' : dateLabel(at, grain))}</div>${rows.join('')}`;
      },
    } : { show: false },
    aria: { enabled: true, label: { description: describeTimeseries(ts, lines) } },
  } as unknown as EChartsOption;
}

/** A date pointed at, as its frequency says it: a day, a week, a month, a quarter, a year. */
export function dateLabel(v: number | string, grain: NonNullable<TimeseriesSpec['grain']>): string {
  const text = typeof v === 'number' ? isoOf(v) : String(v);
  if (grain === 'day') {
    const d = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
    if (!d) return text;
    const day = new Date(Date.UTC(Number(d[1]), Number(d[2]) - 1, Number(d[3])));
    const time = /T(\d{2}:\d{2})/.exec(text);
    return `${day.toLocaleDateString(UI_LOCALE, { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })}${time && time[1] !== '00:00' ? ` ${time[1]}` : ''}`;
  }
  return periodText(text, grain);
}

/** A time the axis gives, as the day it falls on in the viewer's zone (the axis reads a date as local midnight). */
export function isoOf(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number): string => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

/** The tile in words, for a screen reader. */
export function describeTimeseries(ts: TimeseriesSpec, lines: readonly TsLine[]): string {
  const shown = lines.filter((l) => !l.series.hidden);
  if (shown.length === 0) return 'A timeseries with nothing to plot.';
  const names = shown.slice(0, 6).map((l) => l.name);
  const grids = ts.sources.length === 1 ? 'one grid' : `${ts.sources.length} grids`;
  return `A timeseries of ${shown.length} line${shown.length === 1 ? '' : 's'} from ${grids}${ts.grain ? `, by ${ts.grain}` : ''}: ${names.join(', ')}${shown.length > 6 ? ', ...' : ''}.`;
}

/** A series' expression, as the editor shows it. */
export function seriesText(s: TsSeries): string {
  return exprText(s.expr);
}

/**
 * THE LINES AS CSV, long form -- a row per line and date, its value (a band's edges beside it) -- exactly the rows the
 * database answered, for the toolbox's download.
 */
export function timeseriesCsv(lines: readonly TsLine[]): string {
  const cell = (v: string): string => (/[",\n\r]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
  const out = ['line,date,value,lower,upper,open,high,low'];
  const at = (xs: readonly (number | null)[] | undefined, i: number): string => (xs?.[i] == null ? '' : String(xs[i]));
  for (const l of lines) {
    l.points.forEach(([d, v], i) => {
      out.push([l.name, d, v === null ? '' : String(v), at(l.lower, i), at(l.upper, i), at(l.candles?.open, i), at(l.candles?.high, i), at(l.candles?.low, i)]
        .map(cell).join(','));
    });
  }
  return `${out.join('\n')}\n`;
}
