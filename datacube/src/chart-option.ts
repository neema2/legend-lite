// A chart spec and its rows, as a drawing: the ECharts option.
//
// PURE and library-free: this builds a plain object and imports nothing
// from ECharts but its types, so it is tested in node without a DOM
// (chart-render.ts is the one file that draws).
//
// The rows arrive AGGREGATED by the database (`chart-spec.ts`); nothing
// here sums or counts. What it does is presentation: lay the rows out as
// categories and series, and -- for a percent stack only -- scale each
// category's values to its own total.
//
// Marks follow the dataviz rules: bars no wider than 24px with a 4px
// rounded data-end, 2px lines, 8px markers, a 2px surface gap between
// touching fills, hairline recessive grid, ink-coloured text, a legend
// only when there are two series or more, a hover tooltip on every mark,
// and ONE value axis (never two).

import type { EChartsOption } from 'echarts';
import { measureName, type ChartSpec } from './chart-spec.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';
import { numberOf } from '../../engine-client/src/values.ts';
import { isNumeric } from '../../engine-client/src/types.ts';
import { TREE_COLUMN } from './treeview.ts';

/** The colours a chart is drawn in, read from CSS tokens by the renderer. */
export interface ChartTheme {
  readonly surface: string;
  readonly ink: string;
  readonly inkSecondary: string;
  readonly muted: string;
  readonly grid: string;
  readonly axis: string;
  /** Categorical slots, in their fixed order. */
  readonly series: readonly string[];
  /** One hue, light to dark, for a heatmap's magnitude. */
  readonly sequential: readonly string[];
}

/** The reference palette's light mode (dataviz references/palette.md). */
export const LIGHT_THEME: ChartTheme = {
  surface: '#fcfcfb',
  ink: '#0b0b0b',
  inkSecondary: '#52514e',
  muted: '#898781',
  grid: '#e1e0d9',
  axis: '#c3c2b7',
  series: ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948'],
  sequential: ['#cde2fb', '#86b6ef', '#3987e5', '#256abf', '#184f95', '#0d366b'],
};

/**
 * The theme, from the element's CSS tokens: `--dc-chart-*`, falling back
 * to the reference palette. Canvas cannot read CSS variables itself, so
 * they are read here, once per draw.
 */
export function themeOf(el: Element): ChartTheme {
  const view = el.ownerDocument.defaultView;
  const css = view ? view.getComputedStyle(el) : undefined;
  const read = (name: string, fallback: string): string =>
    css?.getPropertyValue(name).trim() || fallback;
  const list = (name: string, fallback: readonly string[]): string[] => {
    const v = read(name, '');
    return v ? v.split(',').map((s) => s.trim()).filter(Boolean) : [...fallback];
  };
  return {
    surface: read('--dc-chart-surface', LIGHT_THEME.surface),
    ink: read('--dc-chart-ink', LIGHT_THEME.ink),
    inkSecondary: read('--dc-chart-ink-secondary', LIGHT_THEME.inkSecondary),
    muted: read('--dc-chart-muted', LIGHT_THEME.muted),
    grid: read('--dc-chart-grid', LIGHT_THEME.grid),
    axis: read('--dc-chart-axis', LIGHT_THEME.axis),
    series: list('--dc-chart-series', LIGHT_THEME.series),
    sequential: list('--dc-chart-sequential', LIGHT_THEME.sequential),
  };
}

/** At most this many series; past it the chart says so rather than invent a colour. */
export const MAX_SERIES = 8;
/** A scatter's points overlap, and only the first three slots stay apart for every viewer. */
export const MAX_SCATTER_SERIES = 3;

/** How a value is written where the reader sees it (the grid's own formatter). */
export type LabelOf = (value: Scalar, column: string, type: string | undefined) => string;

const defaultLabel: LabelOf = (v) => (v === null ? '(empty)' : String(v));

/**
 * An option built here, as ECharts' type. Its declared types are stricter than
 * `exactOptionalPropertyTypes` can satisfy for unions of series; what the
 * options DRAW is pinned by the SVG render tests instead.
 */
const asOption = (o: object): EChartsOption => o as EChartsOption;

/** A clicked mark, as the column values it stands for. */
export type MarkKey = Readonly<Record<string, Scalar>>;

export interface ChartDrawing {
  readonly option: EChartsOption;
  /**
   * The values behind a clicked mark, or null when a mark is not a group (a scatter point).
   * `datum` is the clicked item itself, for a mark whose index is not enough (a treemap's node).
   */
  keyAt(seriesIndex: number, dataIndex: number, datum?: unknown): MarkKey | null;
  /** Anything the reader should know that the picture cannot say. */
  readonly notes: readonly string[];
  /** The chart in words, for a screen reader (the container's aria-label). */
  readonly description: string;
}

/** One column of the result, with its type. */
function column(t: ResultTable, name: string): { values: readonly Scalar[]; type: string | undefined } | undefined {
  const c = t.columns.find((x) => x.name === name);
  return c ? { values: c.values as readonly Scalar[], type: c.type } : undefined;
}

/** Distinct values in first-seen order, with the value itself kept (not its text). */
function distinct(values: readonly Scalar[]): Scalar[] {
  const seen = new Set<string>();
  const out: Scalar[] = [];
  for (const v of values) {
    const k = keyText(v);
    if (!seen.has(k)) {
      seen.add(k);
      out.push(v);
    }
  }
  return out;
}

function keyText(v: Scalar): string {
  return v === null ? '\u0000null' : `${typeof v}:${String(v)}`;
}

export function chartOption(
  spec: ChartSpec,
  rows: ResultTable,
  theme: ChartTheme = LIGHT_THEME,
  label: LabelOf = defaultLabel,
): ChartDrawing {
  const notes: string[] = [];
  const text = { color: theme.inkSecondary, fontFamily: 'inherit' };
  const axisCommon = {
    axisLine: { lineStyle: { color: theme.axis } },
    axisTick: { show: false },
    axisLabel: { color: theme.muted, hideOverlap: true },
    splitLine: { lineStyle: { color: theme.grid, width: 1, type: 'solid' as const } },
  };
  const tooltip = {
    backgroundColor: theme.surface,
    borderColor: theme.axis,
    textStyle: { color: theme.ink },
    confine: true,
  };
  const base: EChartsOption = {
    backgroundColor: 'transparent',
    textStyle: text,
    color: [...theme.series],
    // ECharts' decal patterns stay available, but its own generated
    // description ("the data for EMEA is 0, 15054092") is off: the
    // container carries ours (`description`).
    aria: { enabled: true, label: { enabled: false } },
    grid: { left: 8, right: 16, top: 40, bottom: 8, containLabel: true },
  };

  const xCol = spec.x !== undefined ? column(rows, spec.x) : undefined;
  const first = spec.y[0];
  if (!xCol || !first) {
    return { option: base, keyAt: () => null, notes: ['Nothing to draw.'], description: 'An empty chart.' };
  }

  // -- scatter: rows are points, x and y both values --------------------
  if (spec.mark === 'scatter') {
    const yCol = column(rows, first.column);
    const splitCol = spec.split !== undefined ? column(rows, spec.split) : undefined;
    const numericX = xCol.values.some((v) => numberOf(v, xCol.type) !== null);
    const groups = splitCol ? distinct(splitCol.values) : [null];
    if (groups.length > MAX_SCATTER_SERIES) {
      notes.push(`Showing the first ${MAX_SCATTER_SERIES} of ${groups.length} groups: more cannot be told apart.`);
    }
    const shown = groups.slice(0, MAX_SCATTER_SERIES);
    const series = shown.map((g) => ({
      type: 'scatter' as const,
      name: splitCol ? label(g, spec.split!, splitCol.type) : first.column,
      symbolSize: 8,
      itemStyle: { borderColor: theme.surface, borderWidth: 2 },
      data: xCol.values.flatMap((xv, i) => {
        if (splitCol && keyText(splitCol.values[i] ?? null) !== keyText(g)) return [];
        const yv = numberOf(yCol?.values[i] ?? null, yCol?.type);
        const xn = numericX ? numberOf(xv, xCol.type) : label(xv, spec.x!, xCol.type);
        return yv === null || xn === null ? [] : [[xn, yv]];
      }),
    }));
    return {
      option: asOption({
        ...base,
        tooltip: { ...tooltip, trigger: 'item' },
        legend: legendOf(spec, series.length, theme),
        xAxis: { ...axisCommon, type: numericX ? 'value' : 'category', name: spec.x, nameTextStyle: text, scale: true },
        yAxis: { ...axisCommon, type: 'value', name: first.column, nameTextStyle: text, scale: true },
        series,
      }),
      keyAt: () => null,
      notes,
      description: describe(spec, rows),
    };
  }

  const measure = (m: ChartSpec['y'][number]) => column(rows, measureName(m));
  const categories = distinct(xCol.values);
  const catLabels = categories.map((c) => label(c, spec.x!, xCol.type));
  const indexOf = new Map(categories.map((c, i) => [keyText(c), i]));

  // -- pie: one category per slice ---------------------------------------
  if (spec.mark === 'pie') {
    const m = measure(first);
    const data = categories.map((c, i) => {
      const at = xCol.values.findIndex((v) => keyText(v) === keyText(c));
      return { name: catLabels[i]!, value: numberOf(m?.values[at] ?? null, m?.type) ?? 0 };
    });
    if (data.length > MAX_SERIES) {
      notes.push(`${data.length} slices: a bar reads more than ${MAX_SERIES} categories better than a pie.`);
    }
    return {
      option: asOption({
        ...base,
        tooltip: { ...tooltip, trigger: 'item' },
        legend: legendOf(spec, data.length, theme),
        series: [{
          type: 'pie',
          radius: ['45%', '72%'],
          itemStyle: { borderColor: theme.surface, borderWidth: 2, borderRadius: 4 },
          label: { show: spec.options.labels, color: theme.inkSecondary },
          data,
        }],
      }),
      keyAt: (_s, i) => (categories[i] === undefined ? null : { [spec.x!]: categories[i]! }),
      notes,
      description: describe(spec, rows),
    };
  }

  // a treemap draws the GRID'S rows as shown, not a query of its own (`treemapOfGrid`)
  if (spec.mark === 'treemap') {
    throw new Error('a treemap draws the grid\'s rows as shown: treemapOfGrid, not chartOption');
  }

  // -- heatmap: x across, split down, the first measure as colour ---------
  if (spec.mark === 'heatmap' && spec.split !== undefined) {
    const splitCol = column(rows, spec.split);
    const m = measure(first);
    const rowsCats = splitCol ? distinct(splitCol.values) : [];
    const rowIndex = new Map(rowsCats.map((c, i) => [keyText(c), i]));
    const data: [number, number, number][] = [];
    let lo = Infinity;
    let hi = -Infinity;
    xCol.values.forEach((xv, i) => {
      const v = numberOf(m?.values[i] ?? null, m?.type);
      const xi = indexOf.get(keyText(xv));
      const yi = rowIndex.get(keyText(splitCol?.values[i] ?? null));
      if (v === null || xi === undefined || yi === undefined) return;
      data.push([xi, yi, v]);
      lo = Math.min(lo, v);
      hi = Math.max(hi, v);
    });
    return {
      option: asOption({
        ...base,
        grid: { ...base.grid, bottom: 48 },
        tooltip: { ...tooltip, trigger: 'item' },
        xAxis: { ...axisCommon, type: 'category', data: catLabels, splitArea: { show: false } },
        yAxis: {
          ...axisCommon,
          type: 'category',
          data: rowsCats.map((c) => label(c, spec.split!, splitCol?.type)),
        },
        visualMap: {
          min: Number.isFinite(lo) ? lo : 0,
          max: Number.isFinite(hi) ? hi : 1,
          calculable: true,
          orient: 'horizontal',
          left: 'center',
          bottom: 0,
          inRange: { color: [...theme.sequential] },
          textStyle: { color: theme.muted },
        },
        series: [{
          type: 'heatmap',
          name: measureName(first),
          data,
          itemStyle: { borderColor: theme.surface, borderWidth: 2 },
          label: { show: spec.options.labels, color: theme.ink },
        }],
      }),
      keyAt: (_s, i) => {
        const cell = data[i];
        if (!cell) return null;
        return { [spec.x!]: categories[cell[0]]!, [spec.split!]: rowsCats[cell[1]]! };
      },
      notes,
      description: describe(spec, rows),
    };
  }

  // -- bar, line, area: categories across, one series per measure or split value
  const splitCol = spec.split !== undefined ? column(rows, spec.split) : undefined;
  type Series = { key: Scalar | undefined; name: string; values: (number | null)[] };
  let series: Series[];
  if (splitCol) {
    const m = measure(first);
    const groups = distinct(splitCol.values);
    series = groups.map((g) => ({
      key: g,
      name: label(g, spec.split!, splitCol.type),
      values: categories.map(() => null),
    }));
    const byGroup = new Map(groups.map((g, i) => [keyText(g), i]));
    xCol.values.forEach((xv, i) => {
      const s = series[byGroup.get(keyText(splitCol.values[i] ?? null)) ?? -1];
      const xi = indexOf.get(keyText(xv));
      if (s && xi !== undefined) s.values[xi] = numberOf(m?.values[i] ?? null, m?.type);
    });
  } else {
    series = spec.y.map((m) => {
      const c = measure(m);
      const values: (number | null)[] = categories.map(() => null);
      xCol.values.forEach((xv, i) => {
        const xi = indexOf.get(keyText(xv));
        if (xi !== undefined) values[xi] = numberOf(c?.values[i] ?? null, c?.type);
      });
      return { key: undefined, name: measureName(m), values };
    });
  }
  if (series.length > MAX_SERIES) {
    notes.push(`Showing the first ${MAX_SERIES} of ${series.length} series; filter or split by a smaller column to see the rest.`);
    series = series.slice(0, MAX_SERIES);
  }
  if (spec.options.stack === 'percent') {
    const totals = categories.map((_, i) => series.reduce((t, s) => t + Math.abs(s.values[i] ?? 0), 0));
    series = series.map((s) => ({
      ...s,
      values: s.values.map((v, i) => (v === null || totals[i] === 0 ? v : (100 * v) / totals[i]!)),
    }));
  }

  const horizontal = spec.mark === 'bar' && spec.options.orientation === 'horizontal';
  const stacked = spec.options.stack !== 'none';
  const categoryAxis = { ...axisCommon, type: 'category' as const, data: catLabels, splitLine: { show: false } };
  const valueAxis = {
    ...axisCommon,
    type: 'value' as const,
    ...(spec.options.stack === 'percent' ? { max: 100, axisLabel: { ...axisCommon.axisLabel, formatter: '{value}%' } } : {}),
  };
  const out = series.map((s) => {
    const common = {
      name: s.name,
      data: s.values,
      ...(stacked ? { stack: 'total' } : {}),
      label: { show: spec.options.labels, color: theme.inkSecondary, position: horizontal ? 'right' : 'top' },
      emphasis: { focus: 'series' as const },
    };
    if (spec.mark === 'bar') {
      return {
        ...common,
        type: 'bar' as const,
        barMaxWidth: 24,
        itemStyle: {
          // the data-end rounded, the baseline square; a stack's inner
          // segments are separated by the surface gap instead
          borderRadius: stacked ? 0 : horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0],
          borderColor: theme.surface,
          borderWidth: stacked ? 2 : 0,
        },
      };
    }
    return {
      ...common,
      type: 'line' as const,
      lineStyle: { width: 2 },
      symbol: 'circle',
      symbolSize: 8,
      showSymbol: categories.length <= 40,
      ...(spec.mark === 'area' ? { areaStyle: { opacity: 0.2 } } : {}),
    };
  });
  return {
    option: asOption({
      ...base,
      tooltip: {
        ...tooltip,
        trigger: 'axis',
        axisPointer: { type: spec.mark === 'bar' ? 'shadow' : 'line' },
      },
      legend: legendOf(spec, out.length, theme),
      xAxis: horizontal ? valueAxis : categoryAxis,
      yAxis: horizontal ? { ...categoryAxis, inverse: true } : valueAxis,
      series: out,
    }),
    keyAt: (seriesIndex, dataIndex) => {
      if (categories[dataIndex] === undefined) return null;
      const key: Record<string, Scalar> = { [spec.x!]: categories[dataIndex]! };
      const s = series[seriesIndex];
      if (spec.split !== undefined && s && s.key !== undefined) key[spec.split] = s.key;
      return key;
    },
    notes,
    description: describe(spec, rows),
  };
}

const MARK_WORDS: Record<ChartSpec['mark'], string> = {
  bar: 'Bar chart', line: 'Line chart', area: 'Area chart', scatter: 'Scatter plot',
  pie: 'Pie chart', heatmap: 'Heatmap', treemap: 'Treemap',
};

/** The chart in one sentence: what is plotted against what, and how much of it. */
export function describe(spec: ChartSpec, rows: ResultTable): string {
  const what = spec.mark === 'scatter'
    ? `${spec.y[0]?.column ?? ''} against ${spec.x ?? ''}`
    : `${spec.y.map(measureName).join(', ')} by ${spec.x ?? ''}`;
  const split = spec.split !== undefined ? `, split by ${spec.split}` : '';
  return `${MARK_WORDS[spec.mark]} of ${what}${split}: ${rows.rowCount} rows.`;
}

function legendOf(spec: ChartSpec, count: number, theme: ChartTheme): EChartsOption['legend'] {
  // one series is named by the chart's title; a legend box is for two or more
  if (count < 2 || spec.options.legend === 'none') return { show: false };
  const at = spec.options.legend;
  return {
    show: true,
    type: 'scroll',
    textStyle: { color: theme.inkSecondary },
    ...(at === 'right' ? { right: 0, top: 'middle', orient: 'vertical' } : at === 'bottom' ? { bottom: 0 } : { top: 0 }),
  };
}

/**
 * What the grid shows now, as a treemap draws it: its rows, their tree, its visible columns in
 * order, and the row dimensions (a path's keys, level by level).
 */
export interface GridShown {
  readonly rows: ResultTable;
  readonly tree: readonly {
    readonly path: readonly Scalar[];
    readonly level: number;
    readonly isGroup: boolean;
    readonly expanded: boolean;
    readonly isTotal: boolean;
    readonly isDetail?: boolean;
  }[];
  readonly leaves: readonly { readonly name: string; readonly type: string; readonly label?: string }[];
  readonly levels: readonly string[];
}

const TREE = TREE_COLUMN;

/**
 * A TREEMAP OF THE GRID AS SHOWN (the user, 2026-09-30: the old treemap's way): one block per
 * row on screen -- a collapsed group is a block, an expanded one a box around its visible
 * children -- sized by the first numeric column the grid shows, labelled and valued in the grid's
 * formats. It cannot disagree with the grid beside it: every filter, pivot, sort and expansion is
 * already in what it is given. The grand total is the whole, not a block.
 *
 * NON-POSITIVE VALUES ARE DROPPED, not clamped: a treemap encodes magnitude as AREA, and a
 * negative area does not exist -- drawing a zero-size box would hide the row silently, and its
 * absolute value would say the opposite of the truth. How many went is said.
 */
export function treemapOfGrid(shown: GridShown, theme: ChartTheme = LIGHT_THEME, label: LabelOf = defaultLabel): ChartDrawing {
  const notes: string[] = [];
  const rows = shown.rows;
  const tree = shown.tree;
  const value = shown.leaves.find((l) => l.name !== TREE && isNumeric(l.type));
  const valueCol = value ? column(rows, value.name) : undefined;
  const treeCol = column(rows, TREE);
  // a flat cube's label: the first visible column that is not a number
  const flatLabel = shown.leaves.find((l) => l.name !== TREE && !isNumeric(l.type));
  const flatCol = flatLabel ? column(rows, flatLabel.name) : undefined;
  type Node = { name: string; value?: number; text?: string; key: MarkKey; children?: Node[]; level: number };
  const top: Node[] = [];
  const open: Node[] = [];
  let dropped = 0;
  for (let i = 0; i < rows.rowCount; i++) {
    const t = tree[i];
    // the grand total is the whole treemap, not a block of it
    if (t && t.isTotal && t.level === 0) continue;
    const level = t ? t.level : 1;
    const path = t?.path ?? [];
    const key: MarkKey = Object.fromEntries(path.map((v, j) => [shown.levels[j] ?? `level${j}`, v]));
    const raw = treeCol?.values[i] ?? null;
    const last = path.length > 0 ? path[path.length - 1]! : null;
    const name = t
      ? (raw !== null && raw !== '' ? String(raw)
        : label(last, shown.levels[path.length - 1] ?? '', undefined))
      : label(flatCol?.values[i] ?? null, flatLabel?.name ?? '', flatLabel?.type);
    while (open.length > 0 && open[open.length - 1]!.level >= level) open.pop();
    const parent = open[open.length - 1];
    const into = (n: Node): void => { if (parent) (parent.children ??= []).push(n); else top.push(n); };
    if (t && t.isGroup && t.expanded) {
      const box: Node = { name, key, level, children: [] };
      into(box);
      open.push(box);
      continue;
    }
    const v = numberOf(valueCol?.values[i] ?? null, valueCol?.type);
    if (v === null || v <= 0) {
      dropped += 1;
      continue;
    }
    into({ name, value: v, text: label(valueCol?.values[i] ?? null, value?.name ?? '', value?.type), key, level });
  }
  // a box whose children were all dropped is not a box
  const prune = (nodes: Node[]): Node[] => nodes.flatMap((n) => {
    if (!n.children) return [n];
    const kids = prune(n.children);
    return kids.length > 0 ? [{ ...n, children: kids }] : [];
  });
  const data = prune(top);
  if (dropped > 0) {
    notes.push(`${dropped} ${dropped === 1 ? 'row' : 'rows'} not shown: zero or negative cannot have an area.`);
  }
  if (!value) notes.push('The grid shows no numeric column to size the blocks by.');
  const depth = (nodes: Node[]): number => nodes.reduce((m, n) => Math.max(m, n.children ? 1 + depth(n.children) : 1), 0);
  const levels = Math.max(1, depth(data));
  const text = { fontFamily: 'Roboto, ui-sans-serif, system-ui, sans-serif', fontSize: 11, color: theme.ink };
  return {
    option: asOption({
      backgroundColor: 'transparent',
      textStyle: text,
      color: [...theme.series],
      aria: { enabled: true, label: { enabled: false } },
      tooltip: {
        backgroundColor: theme.surface,
        borderColor: theme.axis,
        textStyle: { color: theme.ink },
        confine: true,
        trigger: 'item',
        // the value as the grid writes it
        formatter: (p: { name?: string; data?: { text?: string } }) =>
          (p.data?.text !== undefined ? `${p.name ?? ''}: ${p.data.text}` : (p.name ?? '')),
      },
      series: [{
        type: 'treemap',
        roam: false,
        nodeClick: false,
        breadcrumb: { show: false },
        top: 8, left: 8, right: 8, bottom: 8,
        leafDepth: levels,
        label: { show: true, color: '#ffffff', overflow: 'truncate' },
        upperLabel: { show: levels > 1, height: 18, color: theme.ink },
        itemStyle: { borderColor: theme.surface, borderWidth: 1, gapWidth: 1 },
        levels: [
          { itemStyle: { borderColor: theme.surface, borderWidth: 2, gapWidth: 2 } },
          { colorSaturation: [0.35, 0.6], itemStyle: { gapWidth: 1 } },
        ],
        data,
      }],
    }),
    // the clicked block's own row: its path, column by column
    keyAt: (_s, _i, datum) => {
      const key = (datum as { key?: MarkKey } | undefined)?.key;
      return key && Object.keys(key).length > 0 ? key : null;
    },
    notes,
    description: `Treemap of ${value?.label ?? value?.name ?? 'nothing'} by the grid's rows as shown.`,
  };
}

