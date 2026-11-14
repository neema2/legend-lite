// A TIMESERIES TILE'S OTHER VIEWS, DRAWN (the next phases' 3, docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11): a
// scatter of one series against another with their regression, each year's seasonality against the years before, a
// histogram of a series' values, a split series' curve across its key -- each an ECharts option of the rows its query
// answered (timeseries-query.ts scatterQuery, seasonalityQuery, histogramQuery, curveQuery). Every figure drawn is the
// database's: the regression's fit, the bins and the normal's counts, the years' range -- this file places and words them.

import type { EChartsOption } from 'echarts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import type { ChartTheme } from './chart-option.ts';
import { MAX_SPLIT_LINES, type TimeseriesSpec, type TsGrain } from './timeseries-spec.ts';
import {
  curveColumns, curveOrder, TS_DATE, TS_HIST_COUNT, TS_HIST_HIGH, TS_HIST_LOW, TS_HIST_MEAN, TS_HIST_NORMAL, TS_HIST_P5, TS_SC_CORR,
  TS_SC_FIT_HIGH, TS_SC_FIT_LOW, TS_SC_INTERCEPT, TS_SC_N, TS_SC_R2, TS_SC_SLOPE, TS_SC_X, TS_SC_X_HIGH, TS_SC_X_LOW, TS_SC_Y,
  TS_HIST_MID, TS_SEA_LATEST, TS_SEA_LOW, TS_SEA_MEAN, TS_SEA_PLACE, TS_SEA_SPAN, TS_SEA_YEAR, TS_SPLIT, TS_VALUE,
} from './timeseries-query.ts';
import { headingLine, toNumber, tsHeading } from './timeseries-option.ts';

/** What a view is drawn with: the theme, its height, a picture's title (its export), how a value is written. */
export interface TsViewContext {
  readonly theme: ChartTheme;
  readonly height?: number;
  readonly title?: string;
  /** A value as the series' grid writes it (its line's format); else a number as the page writes one. */
  readonly format?: (v: number) => string;
}

/** A column of an answer, by name: refused by name when the database answered without it (never drawn as nothing). */
function column(t: ResultTable, name: string): readonly unknown[] {
  const c = t.columns.find((x) => x.name === name);
  if (!c) throw new Error(`the database answered without the view's ${name}`);
  return c.values;
}
const numbers = (t: ResultTable, name: string): (number | null)[] => column(t, name).map((v) => toNumber(v as Scalar));
const first = (t: ResultTable, name: string): number | null => (t.rowCount > 0 ? toNumber(column(t, name)[0] as Scalar) : null);
const plain = (v: number | null, digits = 4): string => (v === null ? '—' : v.toLocaleString(UI_LOCALE, { maximumSignificantDigits: digits }));

/** A view's heading -- a picture's title, the view's subtitle and note -- above it, as the time view's is
 *  (timeseries-option.ts tsHeading), and its height. */
function heading(ts: TimeseriesSpec, ctx: TsViewContext): { readonly top: number; readonly title?: EChartsOption['title'] } {
  // each a line, none where empty, as the time view's (timeseries-option.ts headingLine)
  const title = headingLine(ctx.title);
  const top = tsHeading(ts, title);
  const subtitle = headingLine(ts.view?.subtitle);
  const note = headingLine(ts.view?.note);
  const t = ctx.theme;
  const heads = [
    ...(title !== undefined ? [{ text: title, size: 15, weight: 600, color: t.ink, top: 4 }] : []),
    ...(subtitle !== undefined ? [{ text: subtitle, size: 12, weight: 400, color: t.ink, top: 4 + (title !== undefined ? 22 : 0) }] : []),
    ...(note !== undefined ? [{ text: note, size: 11, weight: 400, color: t.inkSecondary, top: 4 + top - 15 }] : []),
  ];
  return {
    top,
    ...(heads.length > 0 ? { title: heads.map((h) => ({ text: h.text, left: 12, top: h.top, textStyle: { fontSize: h.size, fontWeight: h.weight, color: h.color, fontFamily: t.font } })) } : {}),
  };
}

const axisStyle = (t: ChartTheme): Record<string, unknown> => ({
  axisLine: { lineStyle: { color: t.axis } }, axisLabel: { color: t.inkSecondary }, splitLine: { lineStyle: { color: t.grid } }, nameTextStyle: { color: t.inkSecondary },
});

/**
 * A SCATTER (scatterQuery's rows): a point a date, y against x, coloured from the earliest date (light) to the latest
 * (dark), the latest marked and dated; the regression's line from the least x to the most, its equation, R² and count
 * in its legend -- each the database's.
 */
export function scatterOption(ts: TimeseriesSpec, t: ResultTable, names: { readonly x: string; readonly y: string }, ctx: TsViewContext): EChartsOption {
  const theme = ctx.theme;
  const xs = numbers(t, TS_SC_X);
  const ys = numbers(t, TS_SC_Y);
  const dates = column(t, TS_DATE).map((d) => String(d).slice(0, 10));
  const points = xs.map((x, i) => [x, ys[i], i, dates[i]] as const).filter(([x, y]) => x !== null && y !== null);
  const slope = first(t, TS_SC_SLOPE);
  const intercept = first(t, TS_SC_INTERCEPT);
  const fit = slope === null ? [] : [[first(t, TS_SC_X_LOW), first(t, TS_SC_FIT_LOW)], [first(t, TS_SC_X_HIGH), first(t, TS_SC_FIT_HIGH)]];
  const said = slope === null
    ? 'no regression: x does not vary, or under two dates'
    : `y = ${plain(intercept)} ${slope >= 0 ? '+' : '−'} ${plain(Math.abs(slope))}·x   R² ${plain(first(t, TS_SC_R2), 3)}   corr ${plain(first(t, TS_SC_CORR), 3)}   n ${plain(first(t, TS_SC_N), 9)}`;
  const head = heading(ts, ctx);
  const last = points.at(-1);
  return {
    backgroundColor: 'transparent', animation: false, textStyle: { fontFamily: theme.font },
    ...(head.title !== undefined ? { title: head.title } : {}),
    grid: { left: 12, right: 24, top: head.top + 34, bottom: 28, containLabel: true },
    legend: { top: head.top + 4, left: 12, textStyle: { color: theme.ink }, data: [names.y, said] },
    xAxis: { type: 'value', name: names.x, nameLocation: 'middle', nameGap: 24, scale: true, ...axisStyle(theme) },
    yAxis: { type: 'value', name: names.y, scale: true, ...axisStyle(theme) },
    tooltip: { trigger: 'item', formatter: (p: unknown) => {
      const d = (p as { data?: readonly unknown[] }).data;
      return d === undefined ? '' : `${String(d[3] ?? '')}<br/>${names.x} ${plain(d[0] as number)}<br/>${names.y} ${plain(d[1] as number)}`;
    } },
    visualMap: { show: false, dimension: 2, min: 0, max: Math.max(1, points.length - 1), seriesIndex: 0, inRange: { color: [...theme.sequential] } },
    series: [
      { id: 'points', name: names.y, type: 'scatter', symbolSize: 6, data: points.map((p) => [...p]) },
      { id: 'fit', name: said, type: 'line', data: fit, showSymbol: false, lineStyle: { color: theme.series[1] ?? theme.ink, width: 2 }, silent: true },
      ...(last !== undefined ? [{
        id: 'latest', name: names.y, type: 'scatter' as const, symbolSize: 12, data: [[...last]], itemStyle: { color: theme.series[0], borderColor: theme.surface, borderWidth: 2 },
        label: { show: true, position: 'right' as const, color: theme.ink, formatter: last[3] },
      }] : []),
    ],
  } as EChartsOption;
}

/** The months' names, as the page writes them: a seasonality's of months, a calendar's columns. */
export const TS_MONTH_NAMES: readonly string[] = Array.from({ length: 12 }, (_, i) => new Date(Date.UTC(2024, i, 1)).toLocaleString(UI_LOCALE, { month: 'short', timeZone: 'UTC' }));

/**
 * SEASONALITY (seasonalityQuery's rows): each year a line along its places in the year -- the latest the boldest, the
 * years before fainter the older they are -- over the years before's range (their least to their most at each place,
 * shaded) and their mean (dashed): every figure the database's.
 */
export function seasonalityOption(ts: TimeseriesSpec, t: ResultTable, name: string, grain: TsGrain | undefined, ctx: TsViewContext): EChartsOption {
  const theme = ctx.theme;
  const years = numbers(t, TS_SEA_YEAR);
  const places = numbers(t, TS_SEA_PLACE);
  const vals = numbers(t, TS_VALUE);
  const low = numbers(t, TS_SEA_LOW);
  const span = numbers(t, TS_SEA_SPAN);
  const mean = numbers(t, TS_SEA_MEAN);
  const latest = first(t, TS_SEA_LATEST);
  const distinct = [...new Set(years.filter((y): y is number => y !== null))].sort((a, b) => a - b);
  // the range and the mean at each place, once (every row of a place carries them)
  const byPlace = new Map<number, readonly [number | null, number | null, number | null]>();
  places.forEach((p, i) => { if (p !== null && !byPlace.has(p)) byPlace.set(p, [low[i]!, span[i]!, mean[i]!]); });
  const placed = [...byPlace].sort((a, b) => a[0] - b[0]);
  // a month's name, a quarter's number, else a week's (a daily series' seasonality is by the week of the year)
  const label = (p: number): string => (grain === 'month' ? TS_MONTH_NAMES[p - 1] ?? String(p) : grain === 'quarter' ? `Q${p}` : `W${p}`);
  const head = heading(ts, ctx);
  const older = distinct.filter((y) => y !== latest);
  const lines = distinct.map((y) => {
    const isLatest = y === latest;
    const fade = isLatest ? 1 : 0.25 + 0.6 * ((older.indexOf(y) + 1) / Math.max(1, older.length));
    return {
      id: `y${y}`, name: String(y), type: 'line' as const, showSymbol: false, z: isLatest ? 5 : 3,
      lineStyle: { width: isLatest ? 2.5 : 1.2, color: isLatest ? theme.series[0] : theme.inkSecondary, opacity: fade },
      itemStyle: { color: isLatest ? theme.series[0] : theme.inkSecondary },
      data: places.map((p, i) => [p, vals[i], years[i]] as const).filter(([, , yy]) => yy === y).map(([p, v]) => [p, v]),
    };
  });
  const bandName = older.length > 0 ? `${older[0]}–${older.at(-1)} range` : 'no years before';
  return {
    backgroundColor: 'transparent', animation: false, textStyle: { fontFamily: theme.font },
    ...(head.title !== undefined ? { title: head.title } : {}),
    grid: { left: 12, right: 24, top: head.top + 34, bottom: 28, containLabel: true },
    legend: { top: head.top + 4, left: 12, type: 'scroll', textStyle: { color: theme.ink } },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'value', min: placed[0]?.[0] ?? 1, max: placed.at(-1)?.[0] ?? 1, minInterval: 1, ...axisStyle(theme), axisLabel: { color: theme.inkSecondary, formatter: (v: number) => label(v) } },
    yAxis: { type: 'value', scale: true, name, ...axisStyle(theme) },
    series: [
      // the range: its least, then the span to its most stacked on it and shaded
      { id: 'low', name: bandName, type: 'line', stack: 'range', data: placed.map(([p, [l]]) => [p, l]), showSymbol: false, lineStyle: { opacity: 0 }, silent: true, tooltip: { show: false } },
      { id: 'span', name: bandName, type: 'line', stack: 'range', data: placed.map(([p, [, w]]) => [p, w]), showSymbol: false,
        lineStyle: { opacity: 0 }, areaStyle: { color: theme.series[0], opacity: 0.1 }, silent: true, tooltip: { show: false } },
      { id: 'mean', name: `${older.length > 0 ? `${older[0]}–${older.at(-1)} mean` : 'mean'}`, type: 'line', data: placed.map(([p, [, , m]]) => [p, m]), showSymbol: false,
        lineStyle: { type: 'dashed', color: theme.ink, width: 1.2 }, itemStyle: { color: theme.ink } },
      ...lines,
    ],
  } as EChartsOption;
}

/**
 * A HISTOGRAM (histogramQuery's rows): a bar a bin over its edges, its count; the normal of the values' mean and
 * deviation as the counts it would give each bin; the mean and the 5th percentile marked -- each the database's.
 */
export function histogramOption(ts: TimeseriesSpec, t: ResultTable, name: string, ctx: TsViewContext): EChartsOption {
  const theme = ctx.theme;
  const lo = numbers(t, TS_HIST_LOW);
  const middle = numbers(t, TS_HIST_MID);
  const hi = numbers(t, TS_HIST_HIGH);
  const n = numbers(t, TS_HIST_COUNT);
  const normal = numbers(t, TS_HIST_NORMAL);
  const p5 = first(t, TS_HIST_P5);
  const mean = first(t, TS_HIST_MEAN);
  const fmt = (v: number | null): string => (v === null ? '—' : ctx.format?.(v) ?? plain(v));
  const head = heading(ts, ctx);
  const bins = lo.map((l, i) => (l === null || hi[i] === null || middle[i] === null ? null : [l, middle[i]!, hi[i]!] as const));
  const marks = [
    ...(mean !== null ? [{ xAxis: mean, name: `mean ${fmt(mean)}`, lineStyle: { color: theme.ink, type: 'dashed' as const } }] : []),
    ...(p5 !== null ? [{ xAxis: p5, name: `5th percentile ${fmt(p5)}`, lineStyle: { color: theme.bad ?? theme.ink } }] : []),
  ];
  return {
    backgroundColor: 'transparent', animation: false, textStyle: { fontFamily: theme.font },
    ...(head.title !== undefined ? { title: head.title } : {}),
    grid: { left: 12, right: 24, top: head.top + 34, bottom: 28, containLabel: true },
    legend: { top: head.top + 4, left: 12, textStyle: { color: theme.ink }, data: [name, 'normal'] },
    tooltip: { trigger: 'item' },
    xAxis: { type: 'value', scale: true, ...axisStyle(theme), axisLabel: { color: theme.inkSecondary, formatter: (v: number) => fmt(v) } },
    yAxis: { type: 'value', name: 'count', minInterval: 1, ...axisStyle(theme) },
    series: [
      // each bin from its low edge to its high: its middle, its width the bins' one
      { id: 'bins', name, type: 'bar', barWidth: '98%', itemStyle: { color: theme.series[0], opacity: 0.75 },
        data: bins.map((b, i) => (b === null ? null : { value: [b[1], n[i]], name: `${fmt(b[0])} – ${fmt(b[2])}` })).filter((x) => x !== null),
        markLine: { symbol: 'none', silent: true, label: { formatter: '{b}', color: theme.ink }, data: marks } },
      { id: 'normal', name: 'normal', type: 'line', smooth: true, showSymbol: false, lineStyle: { color: theme.series[1] ?? theme.ink, width: 1.5 },
        data: bins.map((b, i) => (b === null ? null : [b[1], normal[i]])).filter((x) => x !== null) },
    ],
  } as EChartsOption;
}

/** A curve's dates asked, in words: the latest, a period back, a day -- with the date the database found. */
export function curveDateName(asked: string, found: unknown): string {
  const day = found === null || found === undefined ? '' : ` (${String(found).slice(0, 10)})`;
  return asked === 'latest' ? `latest${day}` : /^\d{4}-/.test(asked) ? asked : `${asked} ago${day}`;
}

/** A curve's day as it is written: its date's ten characters; none, none. */
const dayOf = (v: unknown): string | null => (v === null || v === undefined ? null : String(v).slice(0, 10));

/**
 * WHAT A CURVE'S ROWS SAY A READER SHOULD KNOW (the Phase 3 review's 3: a line's value of months before drawn as of the
 * date asked, a date with no line's value an empty line -- neither said): each date asked whose lines' values are of
 * earlier days, by line and day; each with no line's value at or before it. Compared, never computed.
 */
export function curveNotes(t: ResultTable, dates: readonly string[]): string[] {
  const keys = column(t, TS_SPLIT).map((v) => String(v));
  const out: string[] = [];
  // a split's lines are its first MAX_SPLIT_LINES by their text (timeseries-query.ts seriesRelation): a curve of that
  // many may have lost a tenor (9M after 30Y), said
  if (keys.length >= MAX_SPLIT_LINES) out.push(`The first ${MAX_SPLIT_LINES} lines by their text: a split draws at most ${MAX_SPLIT_LINES}.`);
  dates.forEach((d, k) => {
    const c = curveColumns(k);
    const asked = dayOf(column(t, c.asked)[0]);
    const own = column(t, c.date).map(dayOf);
    const name = curveDateName(d, asked);
    const none = keys.filter((_, i) => own[i] === null);
    if (none.length === keys.length) {
      out.push(`${name}: no line has a value at or before it.`);
      return;
    }
    const older = keys.map((key, i) => [key, own[i] ?? null] as const).filter(([, o]) => o !== null && asked !== null && o < asked);
    if (older.length > 0) out.push(`${name}: ${older.map(([key, o]) => `${key} is of ${o}`).join(', ')}.`);
    if (none.length > 0) out.push(`${name}: ${none.join(', ')} ${none.length === 1 ? 'has' : 'have'} no value at or before it.`);
  });
  return out;
}

/**
 * A CURVE (curveQuery's rows): a split series' lines across its key (3M before 2Y before 10Y: curveOrder), a line per
 * date asked -- the latest the boldest -- each point a line's value as of that date, the database's; a point of an
 * earlier day hollow, its own day in its tooltip (curveNotes says them).
 */
export function curveOption(ts: TimeseriesSpec, t: ResultTable, dates: readonly string[], name: string, ctx: TsViewContext): EChartsOption {
  const theme = ctx.theme;
  const keys = column(t, TS_SPLIT).map((v) => String(v));
  const order = curveOrder(keys).map((i) => [keys[i]!, i] as const);
  const head = heading(ts, ctx);
  const fmt = (v: number | null): string => (v === null ? '—' : ctx.format?.(v) ?? plain(v));
  const series = dates.map((d, k) => {
    const c = curveColumns(k);
    const vals = numbers(t, c.value);
    const own = column(t, c.date).map(dayOf);
    const asked = column(t, c.asked)[0];
    const day = dayOf(asked);
    return {
      id: `d${k}`, name: curveDateName(d, asked), type: 'line' as const, symbolSize: 6, z: k === 0 ? 5 : 3,
      lineStyle: { width: k === 0 ? 2.5 : 1.5, color: theme.series[k % theme.series.length] },
      itemStyle: { color: theme.series[k % theme.series.length] },
      data: order.map(([, i]) => ({ value: vals[i], day: own[i], ...(own[i] !== null && day !== null && own[i]! < day ? { symbol: 'emptyCircle' } : {}) })),
    };
  });
  return {
    backgroundColor: 'transparent', animation: false, textStyle: { fontFamily: theme.font },
    ...(head.title !== undefined ? { title: head.title } : {}),
    grid: { left: 12, right: 24, top: head.top + 34, bottom: 28, containLabel: true },
    legend: { top: head.top + 4, left: 12, textStyle: { color: theme.ink } },
    tooltip: { trigger: 'axis', formatter: (items: unknown) => {
      const list = items as readonly { seriesName?: string; axisValue?: string; data?: { value: number | null; day: string | null } }[];
      return [String(list[0]?.axisValue ?? ''), ...list.map((it) => `${it.seriesName ?? ''}: ${fmt(it.data?.value ?? null)}${it.data?.day ? ` (of ${it.data.day})` : ''}`)].join('<br/>');
    } },
    xAxis: { type: 'category', data: order.map(([k]) => k), ...axisStyle(theme) },
    yAxis: { type: 'value', scale: true, name, ...axisStyle(theme) },
    series,
  } as EChartsOption;
}

/** A view described for a reader of the screen (the chart's aria label). */
export function describeView(mode: string, about: string, rows: number): string {
  return `A ${mode} of ${about}: ${rows.toLocaleString(UI_LOCALE)} rows the database answered.`;
}
