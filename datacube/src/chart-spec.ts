// A chart of a cube: what it shows, and the query that answers it.
//
// A chart is a VIEW of a cube, not a second engine. Its query is the
// cube's own snapshot -- same source, same filter, same calculated
// columns -- regrouped by the chart's encoding: x (and a split) become
// the row dimensions, each y a measure. So `levelLambda` builds it, and
// grouping, typing and filtering are the cube's, never re-implemented
// here. The database aggregates; the chart library only draws the rows
// it is handed (docs/DATACUBE_DASHBOARDS_DESIGN_2026_09_28.md §3.3, §6).
//
// The spec is OURS and plain JSON: it is what a saved cube will keep, so
// a chart never depends on the drawing library's version, and it holds
// no functions (a stored formatter would need eval to come back).

import { allColumns, copiedColumn, measuresAsColumns, type GridDraft } from './grid-edits.ts';
import { columnSpecs, defaultMeasure, levelLambda, type LevelScope } from './query.ts';
import { binColumns, binName, grainColumn, grainEndColumn, grainName, marimekkoColumns, paretoColumns, rangeConditions, ringTotalColumn, showAsColumns, showAsName, statColumn, upperName, waffleColumns, waterfallColumns } from './calc-presets.ts';
import type { Scalar } from '../../engine-client/src/result.ts';
import {
  AGGREGATE_FNS,
  rowColumns,
  type AggregateFn,
  type CubeShape,
  type CubeSnapshot,
  type DerivedColumn,
  type FilterNode,
  type Measure,
  type QuerySnapshot,
  type RowColumn,
  type SortSpec,
  withoutConditions,
} from './snapshot.ts';
import { applyToSnapshot, type CubeConfiguration } from './config.ts';
import { defaultKind, familyOf, isDay, isFractional, isNumeric, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';

export type ChartMark =
  | 'bar' | 'line' | 'area' | 'scatter' | 'pie' | 'heatmap' | 'treemap' | 'kpi'
  // batch 3 (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3): drawn by chart-kinds.ts, a chunk of its own
  | 'lollipop' | 'dumbbell' | 'range' | 'waterfall' | 'pareto' | 'funnel' | 'rose' | 'polar' | 'radar' | 'parallel'
  | 'sankey' | 'chord' | 'stream' | 'calendar' | 'gauge' | 'sunburst' | 'tree' | 'matrix'
  // batch 4 (statistics): a box per category, its five figures the database's (BOX_FNS)
  | 'boxplot'
  // batch 5 (maps, §4): drawn by chart-maps.ts on the world's countries, a chunk of its own
  | 'map' | 'bubblemap'
  // batch 3's leftovers: shares as cells (a Marimekko, a waffle), and a start and an end per row (a Gantt)
  | 'marimekko' | 'waffle' | 'gantt';

/** Batch 5's maps: drawn by chart-maps.ts on Natural Earth's countries, fetched with the first of them. */
export const MAP_MARKS: ReadonlySet<ChartMark> = new Set(['map', 'bubblemap']);

/** A map asks for this many places: the world has about two hundred countries. */
export const MAP_LIMIT = 250;

/** The kinds that draw the GRID'S rows as shown, with no query of their own: a treemap, a sunburst, a tree. */
export const GRID_MARKS: ReadonlySet<ChartMark> = new Set(['treemap', 'sunburst', 'tree']);

/** Batch 3's kinds: drawn by chart-kinds.ts, fetched with the first of them (the rarer ECharts charts with it). */
export const KIND_MARKS: ReadonlySet<ChartMark> = new Set([
  'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto', 'funnel', 'rose', 'polar', 'radar', 'parallel',
  'sankey', 'chord', 'stream', 'calendar', 'gauge', 'sunburst', 'tree', 'matrix', 'boxplot', 'marimekko', 'waffle', 'gantt',
]);

/** The kinds that plot ROWS, not groups: a scatter, and a scatter matrix (every pair of its values). */
export function plotsRows(spec: Pick<ChartSpec, 'mark'>): boolean {
  return spec.mark === 'scatter' || spec.mark === 'matrix';
}

/** The most values a scatter matrix compares: five make twenty scatters. */
export const MAX_MATRIX = 5;

/**
 * A box plot's five figures of its one column, in the order its boxes are drawn: the least, the first quartile, the
 * median, the third quartile, the most -- each an aggregate the database computes per category (`percentile(0.25)`,
 * `median()`, ...), never a figure computed here.
 */
export const BOX_FNS = ['min', 'percentile25', 'median', 'percentile75', 'max'] as const satisfies readonly AggregateFn[];

/** A box plot's values: its column's five figures (BOX_FNS). */
export function boxMeasures(column: string): ChartMeasure[] {
  return BOX_FNS.map((fn) => ({ column, fn }));
}

/** The kinds whose marks are the categories across, one per value: a click drills down from one. */
const DRILLABLE: ReadonlySet<ChartMark> = new Set([
  'bar', 'line', 'area', 'pie', 'heatmap', 'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto', 'funnel', 'rose',
  'polar', 'boxplot',
]);

/** The kinds that draw one series of categories: no split (a colour per value) has a place on them. */
export const UNSPLIT: ReadonlySet<ChartMark> = new Set([
  'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto', 'funnel', 'rose', 'radar', 'parallel', 'calendar', 'gauge',
  'boxplot', 'map', 'bubblemap', 'waffle', 'gantt',
]);

/** The kinds that plot several values of their own: following the cube, they keep them. */
const SEVERAL_VALUES: ReadonlySet<ChartMark> = new Set(['dumbbell', 'range', 'radar', 'parallel', 'boxplot', 'gantt']);

/** The kinds that need a second grouping (the split): a heatmap's rows, a flow's target, a stream's layers. */
export const SPLIT_NEEDED: Readonly<Partial<Record<ChartMark, string>>> = {
  heatmap: 'A heatmap needs a second column, for its rows.',
  sankey: 'A Sankey needs a second column, where each flow goes (Split by).',
  chord: 'A chord diagram needs a second column, where each flow goes (Split by).',
  stream: 'A streamgraph needs a second column, for its layers (Split by).',
  marimekko: 'A Marimekko needs a second column, what each of its columns is made of (Split by).',
};

/** The kinds that need a date across. */
const DATE_ACROSS: ReadonlySet<ChartMark> = new Set(['stream', 'calendar']);

/** The kinds whose reference layers, label slot and hover measures are drawn: the rest refuse them by name. */
const LAYERED: ReadonlySet<ChartMark> = new Set(['bar', 'line', 'area']);
const LABELLED: ReadonlySet<ChartMark> = new Set(['bar', 'line', 'area']);
const HOVER_EXTRAS: ReadonlySet<ChartMark> = new Set([
  'bar', 'line', 'area', 'lollipop', 'dumbbell', 'range', 'waterfall', 'pareto', 'funnel', 'rose', 'polar', 'calendar',
]);

/**
 * The spec's version (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.1). 2: a slot per role. 1: x, y, split, options. Only
 * 2 is read (the user, 2026-10-17: no older chart is read forward).
 */
export const CHART_VERSION = 2;

/**
 * How a measure is aggregated: one of the grid's aggregates, or `value` -- a GROUP-LEVEL column (the cube's or the
 * chart's own), already one figure per group, plotted as it is.
 */
export type ChartAggregate = AggregateFn | 'value';

/** The kind of mark one measure is drawn with, in a chart that draws several (bar and line together). */
export type MeasureMark = 'bar' | 'line' | 'area';

/** A measure's value axis: the first (left) or the second (right). Absent: by its unit (§6). */
export type MeasureAxis = 'left' | 'right';

/**
 * A measure shown as something computed from it (§3.2's presets): each is a group-level calculated column the
 * planner already plans -- a window over the chart's rows, sometimes with an expression over it.
 */
export type ShowAsKind = 'runningTotal' | 'percentOfTotal' | 'difference' | 'percentChange' | 'movingAverage' | 'rank';

export interface ShowAs {
  readonly kind: ShowAsKind;
  /** A moving average's rows, this one included (default 3). */
  readonly rows?: number;
}

/** One measure the chart plots: a column and how its rows are aggregated. */
export interface ChartMeasure {
  readonly column: string;
  readonly fn: ChartAggregate;
  /** Its own kind of mark (bar, line, area): a combination chart. Absent: the chart's. */
  readonly mark?: MeasureMark;
  /** Its value axis. Absent: one axis, or the second for a measure whose unit differs (§6). */
  readonly axis?: MeasureAxis;
  /** Shown as a running total, a share of the total, a change, a moving average or a rank. */
  readonly showAs?: ShowAs;
}

/** A date across, taken to its period: the grain's first day (the compiler's `firstDayOf...`), or its year. */
export type TimeGrain = 'day' | 'week' | 'month' | 'quarter' | 'year';

/** A number across, taken to its bin: `width` wide, or `count` bins over the column's range. */
export interface Bins {
  readonly width?: number;
  readonly count?: number;
}

/** The statistic a reference line draws: the database's figure over the chart's rows (a window over them). */
export type LayerStat = 'average' | 'min' | 'max';

/**
 * What a chart draws over its marks (§3.1's layers):
 *  - `line`: a constant or a target, at `value`;
 *  - `stat`: the average, minimum or maximum of a measure over the chart's rows;
 *  - `band`: a shaded range, `from` to `to`, across (`x`, categories or dates) or up (`y`, values);
 *  - `threshold`: a measure's marks coloured by value -- below `value` in the bad colour;
 *  - `extremes`: a measure's highest and lowest marks pinned.
 * `measure` is the index of the measure it reads (default 0); `label` what it is called.
 */
export type ChartLayer =
  | { readonly kind: 'line'; readonly value: number; readonly measure?: number; readonly label?: string }
  | { readonly kind: 'stat'; readonly stat: LayerStat; readonly measure?: number; readonly label?: string }
  | {
    readonly kind: 'band'; readonly axis: 'x' | 'y'; readonly from: string | number; readonly to: string | number;
    readonly label?: string;
  }
  | { readonly kind: 'threshold'; readonly value: number; readonly measure?: number; readonly label?: string }
  | { readonly kind: 'extremes'; readonly measure?: number; readonly label?: string };

/** The layers' kinds, one word each (Python's LAYERS). */
export type ChartLayerKind = 'line' | 'stat' | 'band' | 'threshold' | 'extremes';

/** A column per small chart: small multiples on shared axes, by row, by column, or both. */
export interface ChartFacet {
  readonly row?: string;
  readonly column?: string;
}

/** Where a bubble map places each row: the columns of its latitude and its longitude, in degrees (one, while chosen). */
export interface ChartPlace {
  readonly lat?: string;
  readonly lon?: string;
}

/** A chart's own calculated column (§3.2 item 3): the column editor's own kind, at the row or the group stage. */
export interface ChartCalc {
  readonly stage: 'row' | 'group';
  readonly column: DerivedColumn;
}

/** How deep a chart has drilled down its grid's row groupings: the value clicked at each level so far. */
export interface ChartDrill {
  /** The groupings, outermost first: the grid's rows when the drill began. */
  readonly levels: readonly string[];
  /** One value per level drilled into; the chart shows the level after the last. */
  readonly path: readonly (string | number | boolean | null)[];
}

export interface ChartSpec {
  readonly version: 1 | 2;
  readonly mark: ChartMark;
  /** The category (bar, line, area, pie, heatmap columns), the x value (scatter), or a KPI's trend (optional). */
  readonly x?: string;
  /** A date across, by its period (§3.2). */
  readonly grain?: TimeGrain;
  /** A number across, by its bins (§3.2). */
  readonly bins?: Bins;
  /** What is plotted. Scatter reads the first as its y; heatmap and pie the first as the value. */
  readonly y: readonly ChartMeasure[];
  /** A second dimension -- the colour slot: one series per value (bar, line, area), the rows of a heatmap. */
  readonly split?: string;
  /** A measure sizing each mark (a bubble). */
  readonly size?: ChartMeasure;
  /** A measure written on each mark instead of its own value (revenue's bars labelled with their margin). */
  readonly label?: ChartMeasure;
  /** Further measures a hover lists, beside the plotted ones. */
  readonly tooltip?: readonly ChartMeasure[];
  /** Small multiples: a chart per value. */
  readonly facet?: ChartFacet;
  /** A bubble map's places: each row's latitude and longitude columns (the chart groups by them, with its x). */
  readonly at?: ChartPlace;
  /** Reference lines, bands, thresholds, pinned extremes. */
  readonly layers?: readonly ChartLayer[];
  /** The chart's own calculated columns. */
  readonly calcs?: readonly ChartCalc[];
  /** Drilled down the grid's groupings. */
  readonly drill?: ChartDrill;
  readonly options: ChartOptions;
  /**
   * Frozen: the chart keeps its own grouping. Absent, it is LIVE -- it
   * follows the cube's pivots (`followCube`) as they change.
   */
  readonly frozen?: boolean;
}

export interface ChartOptions {
  readonly orientation: 'vertical' | 'horizontal';
  readonly stack: 'none' | 'stacked' | 'percent';
  /** Order of the categories: by the category itself, or by the (first) value. */
  readonly sort: { readonly by: 'x' | 'y'; readonly direction: 'asc' | 'desc' };
  /** The most rows the chart asks for (top N after the sort). */
  readonly limit: number;
  readonly labels: boolean;
  readonly legend: 'top' | 'bottom' | 'right' | 'none';
  /** A KPI's target: its value is said against it -- above or below, by how much. */
  readonly target?: number;
  /** Which way is good for a KPI against its target: above it (higher, the default) or below it. */
  readonly better?: 'higher' | 'lower';
  /** A line drawn as a smooth curve. */
  readonly smooth?: boolean;
  /** A line drawn as steps. */
  readonly step?: boolean;
  /** The value axis on a log scale. */
  readonly logAxis?: boolean;
  /** Ranges of the value axis left out (an axis break): a tall outlier no longer flattens the rest. */
  readonly breaks?: readonly { readonly from: number; readonly to: number }[];
  /** Where a mark's label sits: on it (inside) or beyond its end (outside, the default). */
  readonly labelPosition?: 'inside' | 'outside';
  /** The labels across, turned (degrees): long names that would overlap. */
  readonly labelRotate?: number;
  /** A slider to zoom and pan across (a long series). */
  readonly zoom?: boolean;
  /** Drag across (or around, on a scatter) to filter the grid to a range. */
  readonly brush?: boolean;
  /** Pattern fills as well as colours, for readers who cannot tell colours apart (ECharts' decal). */
  readonly decal?: boolean;
  /** Hovering a series dims the rest (default on); false keeps every series as it is. */
  readonly dim?: boolean;
  /** The toolbox: save as a picture, see the chart's data, download it. */
  readonly toolbox?: boolean;
  /** A pie as half a ring (a half donut). */
  readonly half?: boolean;
  /** A gauge drawn as a progress ring rather than a dial. */
  readonly ring?: boolean;
}

/** The options that are flags, each true or false (a Record over them: an option flag added and not here fails the build). */
type FlagKeys<T> = { [K in keyof T]-?: NonNullable<T[K]> extends boolean ? K : never }[keyof T];
const OPTION_FLAGS: Readonly<Record<FlagKeys<ChartOptions>, true>> = {
  labels: true, smooth: true, step: true, logAxis: true, zoom: true, brush: true, decal: true, dim: true, toolbox: true,
  half: true, ring: true,
};

// The words a saved spec may use, each list exhaustive by construction (a Record over its union): a page's reader
// refuses a word it does not know -- Python's ll.Page too, which keeps no list of them and asks this reader
// (headless gridChart).
const AGGREGATE_WORDS: Readonly<Record<string, true>> = Object.fromEntries([...AGGREGATE_FNS, 'value'].map((a) => [a, true]));
const MEASURE_MARK_WORDS: Readonly<Record<MeasureMark, true>> = { bar: true, line: true, area: true };
const AXIS_WORDS: Readonly<Record<MeasureAxis, true>> = { left: true, right: true };
const SHOW_AS_WORDS: Readonly<Record<ShowAsKind, true>> = {
  runningTotal: true, percentOfTotal: true, difference: true, percentChange: true, movingAverage: true, rank: true,
};
const GRAIN_WORDS: Readonly<Record<TimeGrain, true>> = { day: true, week: true, month: true, quarter: true, year: true };
const LAYER_WORDS: Readonly<Record<ChartLayerKind, true>> = {
  line: true, stat: true, band: true, threshold: true, extremes: true,
};
const STAT_WORDS: Readonly<Record<LayerStat, true>> = { average: true, min: true, max: true };

export const SHOW_AS_KINDS = Object.keys(SHOW_AS_WORDS) as ShowAsKind[];
export const TIME_GRAINS = Object.keys(GRAIN_WORDS) as TimeGrain[];
export const LAYER_STATS = Object.keys(STAT_WORDS) as LayerStat[];

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const known = (words: Readonly<Record<string, true>>, v: unknown): boolean => typeof v === 'string' && Object.hasOwn(words, v);

// The keys a saved spec's objects may have, each exhaustive by construction (a Record over the type's own keys): a key
// it has no place for -- a newer DataCube's, or one misspelt -- is refused by name, never ignored
const SPEC_KEYS: Readonly<Record<keyof ChartSpec, true>> = {
  version: true, mark: true, x: true, grain: true, bins: true, y: true, split: true, size: true, label: true,
  tooltip: true, facet: true, at: true, layers: true, calcs: true, drill: true, options: true, frozen: true,
};
const PLACE_KEYS: Readonly<Record<keyof ChartPlace, true>> = { lat: true, lon: true };
const MEASURE_KEYS: Readonly<Record<keyof ChartMeasure, true>> = { column: true, fn: true, mark: true, axis: true, showAs: true };
const FACET_KEYS: Readonly<Record<keyof ChartFacet, true>> = { row: true, column: true };
const OPTION_KEYS: Readonly<Record<keyof ChartOptions, true>> = {
  orientation: true, stack: true, sort: true, limit: true, labels: true, legend: true, target: true, better: true,
  smooth: true, step: true, logAxis: true, breaks: true, labelPosition: true, labelRotate: true, zoom: true, brush: true,
  decal: true, dim: true, toolbox: true, half: true, ring: true,
};
const SHOW_AS_KEYS: Readonly<Record<keyof ShowAs, true>> = { kind: true, rows: true };
const BINS_KEYS: Readonly<Record<keyof Bins, true>> = { width: true, count: true };
const SORT_KEYS: Readonly<Record<keyof ChartOptions['sort'], true>> = { by: true, direction: true };
const DRILL_KEYS: Readonly<Record<keyof ChartDrill, true>> = { levels: true, path: true };
// a chart's own calculated column: its stage and the column (the column's own keys are the column editor's to read)
const CALC_KEYS: Readonly<Record<keyof ChartCalc, true>> = { stage: true, column: true };
// each kind of layer its own keys
const LAYER_KEYS: { readonly [K in ChartLayerKind]: Readonly<Record<keyof Extract<ChartLayer, { kind: K }>, true>> } = {
  line: { kind: true, value: true, measure: true, label: true },
  stat: { kind: true, stat: true, measure: true, label: true },
  band: { kind: true, axis: true, from: true, to: true, label: true },
  threshold: { kind: true, value: true, measure: true, label: true },
  extremes: { kind: true, measure: true, label: true },
};

/** The first key of `o` that `keys` has no place for, refused by name; or null. */
function unknownKey(o: Readonly<Record<string, unknown>>, keys: Readonly<Record<string, true>>, what: string): string | null {
  const extra = Object.keys(o).find((k) => !Object.hasOwn(keys, k));
  return extra === undefined ? null : `there is no ${JSON.stringify(extra)} in ${what} (it says ${Object.keys(keys).join(', ')})`;
}

/** What is wrong with a measure as saved, or null. */
function measureProblem(m: unknown, what: string): string | null {
  if (!isRecord(m) || typeof m['column'] !== 'string' || !known(AGGREGATE_WORDS, m['fn'])) {
    return `${what} is not a column and an aggregate`;
  }
  const extra = unknownKey(m, MEASURE_KEYS, what);
  if (extra) return extra;
  if (m['mark'] !== undefined && !known(MEASURE_MARK_WORDS, m['mark'])) return `${what}'s mark ${JSON.stringify(m['mark'])} is not bar, line or area`;
  if (m['axis'] !== undefined && !known(AXIS_WORDS, m['axis'])) return `${what}'s axis ${JSON.stringify(m['axis'])} is not left or right`;
  const showAs = m['showAs'];
  if (showAs !== undefined && (!isRecord(showAs) || !known(SHOW_AS_WORDS, showAs['kind']))) {
    return `${what} is shown as something this DataCube does not know (${JSON.stringify(showAs)})`;
  }
  return isRecord(showAs) ? unknownKey(showAs, SHOW_AS_KEYS, `${what}'s showAs`) : null;
}

/**
 * What stops a saved spec from being read, in words, or null: its version, its mark and every word of version 2's
 * slots (a newer DataCube's word is refused by name, never drawn as something else). `marks` are the kinds this
 * DataCube draws.
 */
export function specProblem(spec: unknown, marks: readonly string[]): string | null {
  if (!isRecord(spec)) return 'it is not a chart';
  if (spec['version'] !== CHART_VERSION) {
    return `its version ${JSON.stringify(spec['version'] ?? null)} is not one this DataCube reads (${CHART_VERSION})`;
  }
  if (!marks.includes(spec['mark'] as string)) return `its kind ${JSON.stringify(spec['mark'] ?? null)} is not one this DataCube draws`;
  if (spec['y'] === undefined) return 'it plots nothing (no y)';
  if (!Array.isArray(spec['y'])) return `its y ${JSON.stringify(spec['y'])} is not a list of measures`;
  if (spec['options'] === undefined) return 'it has no options';
  if (!isRecord(spec['options'])) return `its options ${JSON.stringify(spec['options'])} are not an object`;
  const extra = unknownKey(spec, SPEC_KEYS, 'a chart') ?? unknownKey(spec['options'], OPTION_KEYS, 'its options');
  if (extra) return extra;
  for (const [i, m] of spec['y'].entries()) {
    const p = measureProblem(m, `measure ${i + 1}`);
    if (p) return p;
  }
  for (const slot of ['size', 'label'] as const) {
    if (spec[slot] === undefined) continue;
    const p = measureProblem(spec[slot], `its ${slot}`);
    if (p) return p;
  }
  if (spec['tooltip'] !== undefined) {
    if (!Array.isArray(spec['tooltip'])) return 'its tooltip is not a list';
    for (const [i, m] of spec['tooltip'].entries()) {
      const p = measureProblem(m, `tooltip measure ${i + 1}`);
      if (p) return p;
    }
  }
  if (spec['grain'] !== undefined && !known(GRAIN_WORDS, spec['grain'])) {
    return `its grain ${JSON.stringify(spec['grain'])} is not one of ${TIME_GRAINS.join(', ')}`;
  }
  const bins = spec['bins'];
  if (bins !== undefined && (!isRecord(bins) || (typeof bins['width'] !== 'number' && typeof bins['count'] !== 'number'))) {
    return 'its bins have neither a width nor a count';
  }
  // a bin is some width of a number, and there is a whole count of them (calc-presets.ts binColumns)
  const width = isRecord(bins) ? bins['width'] : undefined;
  if (width !== undefined && !(typeof width === 'number' && width > 0)) {
    return `its bins' width ${JSON.stringify(width)} is not a positive number`;
  }
  const count = isRecord(bins) ? bins['count'] : undefined;
  if (count !== undefined && !(Number.isInteger(count) && (count as number) >= 1)) {
    return `its bins' count ${JSON.stringify(count)} is not a whole number from 1`;
  }
  const binsExtra = isRecord(bins) ? unknownKey(bins, BINS_KEYS, 'its bins') : null;
  if (binsExtra) return binsExtra;
  // the options' words: a newer DataCube's stack or position is refused by name, never drawn as another
  const o = spec['options'];
  const word = (key: string, words: readonly string[]): string | null =>
    o[key] === undefined || words.includes(o[key] as string) ? null : `its ${key} ${JSON.stringify(o[key])} is not one of ${words.join(', ')}`;
  const optionProblem = word('orientation', ['vertical', 'horizontal']) ?? word('stack', ['none', 'stacked', 'percent'])
    ?? word('legend', ['top', 'bottom', 'right', 'none']) ?? word('labelPosition', ['inside', 'outside'])
    ?? word('better', ['higher', 'lower']);
  if (optionProblem) return optionProblem;
  for (const flag of Object.keys(OPTION_FLAGS)) {
    if (o[flag] !== undefined && typeof o[flag] !== 'boolean') return `its ${flag} ${JSON.stringify(o[flag])} is not true or false`;
  }
  if (spec['frozen'] !== undefined && typeof spec['frozen'] !== 'boolean') {
    return `its frozen ${JSON.stringify(spec['frozen'])} is not true or false`;
  }
  if (o['labelRotate'] !== undefined && typeof o['labelRotate'] !== 'number') {
    return `its labelRotate ${JSON.stringify(o['labelRotate'])} is not a number`;
  }
  // the axis breaks: each a range of the value axis, left out
  const breaks = o['breaks'];
  if (breaks !== undefined && !(Array.isArray(breaks) && breaks.every((b) => isRecord(b) && typeof b['from'] === 'number'
    && typeof b['to'] === 'number' && b['from'] < b['to'] && Object.keys(b).every((k) => k === 'from' || k === 'to')))) {
    return `its breaks ${JSON.stringify(breaks)} are not a list of ranges (each a from below its to)`;
  }
  // how many it draws, a whole number; a target, a number
  if (o['limit'] !== undefined && !(Number.isInteger(o['limit']) && (o['limit'] as number) >= 1)) {
    return `its limit ${JSON.stringify(o['limit'])} is not a whole number from 1`;
  }
  if (o['target'] !== undefined && typeof o['target'] !== 'number') {
    return `its target ${JSON.stringify(o['target'])} is not a number`;
  }
  const sort = o['sort'];
  if (sort !== undefined && (!isRecord(sort) || (sort['by'] !== 'x' && sort['by'] !== 'y')
    || (sort['direction'] !== 'asc' && sort['direction'] !== 'desc'))) {
    return `its order ${JSON.stringify(sort)} is not by x or y, asc or desc`;
  }
  const sortExtra = isRecord(sort) ? unknownKey(sort, SORT_KEYS, 'its order') : null;
  if (sortExtra) return sortExtra;
  const facet = spec['facet'];
  if (facet !== undefined && (!isRecord(facet) || (typeof facet['row'] !== 'string' && typeof facet['column'] !== 'string'))) {
    return 'its facet names neither a row nor a column';
  }
  const facetExtra = isRecord(facet) ? unknownKey(facet, FACET_KEYS, 'its facet') : null;
  if (facetExtra) return facetExtra;
  const at = spec['at'];
  if (at !== undefined && (!isRecord(at) || (at['lat'] !== undefined && typeof at['lat'] !== 'string')
    || (at['lon'] !== undefined && typeof at['lon'] !== 'string'))) {
    return `its places ${JSON.stringify(at)} are not the columns of a latitude and a longitude (at: lat, lon)`;
  }
  const atExtra = isRecord(at) ? unknownKey(at, PLACE_KEYS, 'its places') : null;
  if (atExtra) return atExtra;
  const layers = spec['layers'];
  if (layers !== undefined) {
    if (!Array.isArray(layers)) return 'its layers are not a list';
    for (const [i, l] of layers.entries()) {
      if (!isRecord(l) || !known(LAYER_WORDS, l['kind'])) return `layer ${i + 1} is of a kind this DataCube does not know (${JSON.stringify(isRecord(l) ? l['kind'] ?? null : l)})`;
      if (l['kind'] === 'stat' && !known(STAT_WORDS, l['stat'])) return `layer ${i + 1}'s statistic ${JSON.stringify(l['stat'] ?? null)} is not one of ${LAYER_STATS.join(', ')}`;
      const layerExtra = unknownKey(l, LAYER_KEYS[l['kind'] as ChartLayerKind], `layer ${i + 1} (a ${l['kind']})`);
      if (layerExtra) return layerExtra;
      if (l['measure'] !== undefined && !(Number.isInteger(l['measure']) && (l['measure'] as number) >= 0)) {
        return `layer ${i + 1}'s value ${JSON.stringify(l['measure'])} is not one of the chart's (a whole number, 0 its first)`;
      }
      if ((l['kind'] === 'line' || l['kind'] === 'threshold') && typeof l['value'] !== 'number') return `layer ${i + 1} has no value`;
      if (l['kind'] === 'band' && (l['from'] === undefined || l['to'] === undefined || (l['axis'] !== 'x' && l['axis'] !== 'y'))) {
        return `layer ${i + 1} (a band) needs an axis, a from and a to`;
      }
    }
  }
  const calcs = spec['calcs'];
  if (calcs !== undefined) {
    if (!Array.isArray(calcs)) return 'its calculated columns are not a list';
    for (const [i, c] of calcs.entries()) {
      if (!isRecord(c) || (c['stage'] !== 'row' && c['stage'] !== 'group') || !isRecord(c['column'])
        || typeof c['column']['name'] !== 'string') {
        return `calculated column ${i + 1} has no stage (row or group) and column`;
      }
      const calcExtra = unknownKey(c, CALC_KEYS, `calculated column ${i + 1}`);
      if (calcExtra) return calcExtra;
    }
  }
  const drill = spec['drill'];
  if (drill !== undefined && (!isRecord(drill) || !Array.isArray(drill['levels']) || !Array.isArray(drill['path'])
    || drill['path'].length >= drill['levels'].length)) {
    return 'its drill path is not a path down its levels';
  }
  const drillExtra = isRecord(drill) ? unknownKey(drill, DRILL_KEYS, 'its drill') : null;
  if (drillExtra) return drillExtra;
  return null;
}

/**
 * A saved chart as a page reads it (page-document.ts), or what stops it from being read, in words (specProblem). An
 * option it leaves out is the chart's own default. Python's charts are read the same, headless (headless/grid.ts gridChart): one reader --
 * as the Python pages design's §3 has it, Python adds no second set of rules.
 */
export function readChart(spec: unknown): ChartSpec | string {
  const problem = specProblem(spec, CHART_MARKS.map((m) => m.value));
  if (problem !== null) return problem;
  const saved = spec as Record<string, unknown>;
  return { ...saved, version: CHART_VERSION, options: { ...DEFAULT_OPTIONS, ...(saved['options'] as object) } } as unknown as ChartSpec;
}

export const CHART_MARKS: readonly { value: ChartMark; label: string }[] = [
  { value: 'bar', label: 'Bar' },
  { value: 'line', label: 'Line' },
  { value: 'area', label: 'Area' },
  { value: 'scatter', label: 'Scatter' },
  { value: 'pie', label: 'Pie' },
  { value: 'heatmap', label: 'Heatmap' },
  { value: 'treemap', label: 'Treemap' },
  { value: 'kpi', label: 'KPI' },
  { value: 'lollipop', label: 'Lollipop' },
  { value: 'dumbbell', label: 'Dumbbell' },
  { value: 'range', label: 'Range' },
  { value: 'waterfall', label: 'Waterfall' },
  { value: 'pareto', label: 'Pareto' },
  { value: 'funnel', label: 'Funnel' },
  { value: 'rose', label: 'Rose' },
  { value: 'polar', label: 'Polar bar' },
  { value: 'radar', label: 'Radar' },
  { value: 'parallel', label: 'Parallel' },
  { value: 'sankey', label: 'Sankey' },
  { value: 'chord', label: 'Chord' },
  { value: 'stream', label: 'Streamgraph' },
  { value: 'calendar', label: 'Calendar' },
  { value: 'gauge', label: 'Gauge' },
  { value: 'sunburst', label: 'Sunburst' },
  { value: 'tree', label: 'Tree' },
  { value: 'matrix', label: 'Scatter matrix' },
  { value: 'boxplot', label: 'Box plot' },
  { value: 'map', label: 'Map' },
  { value: 'bubblemap', label: 'Bubble map' },
  { value: 'marimekko', label: 'Marimekko' },
  { value: 'waffle', label: 'Waffle' },
  { value: 'gantt', label: 'Gantt' },
];

/** A kind's name, as a sentence says it ("a Pareto", "a polar bar"). */
export function markLabel(mark: ChartMark): string {
  const l = CHART_MARKS.find((m) => m.value === mark)?.label ?? mark;
  return l === 'KPI' || l === 'Sankey' || l === 'Pareto' || l === 'Marimekko' || l === 'Gantt' ? l : l.toLowerCase();
}

export const DEFAULT_CHART_LIMIT = 50;
/** A date across asks for this many points: a time series is long, and its slider and point thinning keep it quick. */
export const TIME_LIMIT = 2000;
/** A scatter plots rows, not groups, so it may ask for more of them. */
export const SCATTER_LIMIT = 2000;

/** A chart's options before anyone sets one: a saved chart that leaves one out has it (page-document.ts). */
export const DEFAULT_OPTIONS: ChartOptions = {
  orientation: 'vertical',
  stack: 'none',
  sort: { by: 'y', direction: 'desc' },
  limit: DEFAULT_CHART_LIMIT,
  labels: false,
  legend: 'top',
};

/** The name a measure's column has in the chart's result. */
export function measureName(m: ChartMeasure): string {
  const base = baseName(m);
  return m.showAs ? showAsName(m.showAs.kind, base) : base;
}

/** The name a measure has before any show-as: its aggregate's column, or a group-level column's own name. */
export function baseName(m: ChartMeasure): string {
  // a group-level column is already one figure per group: it keeps its name
  if (m.fn === 'value') return m.column;
  return m.fn === 'count' ? 'count' : `${m.fn}_${m.column}`;
}

/**
 * The column the chart groups by across: its grain's column (a date by month), its bins' column, or `x` itself. The
 * drawing reads the categories from it; a click is said back on `x` (`markConditions`).
 */
export function axisName(spec: ChartSpec): string | undefined {
  if (spec.x === undefined) return undefined;
  if (spec.grain !== undefined) return grainName(spec.x, spec.grain);
  if (spec.bins !== undefined) return binName(spec.x);
  return spec.x;
}

/**
 * WHAT A BRUSH TOOK, as conditions on the cube's own columns (batch 2 item 11): a range across is from its first value
 * to its last -- a period's to the next period's start, a bin's to the next bin's edge -- for a date or a number, and
 * the values themselves (`in`) for anything else; a scatter's rectangle is its two columns' ranges. A drilled chart's
 * path comes with it.
 */
export function brushConditions(
  drilled: ChartSpec,
  s: CubeSnapshot,
  took: { readonly kind: 'across'; readonly values: readonly Scalar[]; readonly upper?: Scalar }
    | { readonly kind: 'rect'; readonly x: readonly [number, number]; readonly y: readonly [number, number] },
): FilterNode[] {
  const { spec, conditions } = drilledView(drilled);
  const plain = (v: Scalar): string | number | boolean | null => (typeof v === 'bigint' ? v.toString() : v);
  if (took.kind === 'rect') {
    const y = spec.y[0]?.column;
    if (spec.x === undefined || y === undefined) return [];
    const range = (column: string, [a, b]: readonly [number, number]): FilterNode[] => [
      { kind: 'condition', column, operator: 'greaterThanEqual', value: Math.min(a, b) },
      { kind: 'condition', column, operator: 'lessThanEqual', value: Math.max(a, b) },
    ];
    return [...conditions, ...range(spec.x, took.x), ...range(y, took.y)];
  }
  const column = spec.x;
  const values = took.values.filter((v): v is Exclude<Scalar, null> => v !== null);
  if (column === undefined || values.length === 0) return [...conditions];
  const type = rowColumns(s).find((c) => c.name === column)?.type;
  // the range is from the least value taken to the greatest -- by VALUE, whatever order the chart draws them in
  const ordered = isNumeric(type) || spec.bins !== undefined
    ? [...values].sort((a, b) => Number(a) - Number(b))
    : [...values].sort((a, b) => String(a).localeCompare(String(b)));
  const lo = ordered[0]!;
  const hi = ordered[ordered.length - 1]!;
  if (rangesAcross(spec)) {
    // a run of periods or bins: from the first's start to the last's end, the database's
    const up = typeof took.upper === 'bigint' ? took.upper.toString() : took.upper;
    if (up === undefined || up === null) throw new Error(`the end of ${column}'s last ${spec.grain ?? 'bin'} was not answered`);
    return [...conditions, ...rangeConditions(column, type, plain(lo)!, up)];
  }
  if (isNumeric(type) || isTemporal(type)) {
    return [
      ...conditions,
      { kind: 'condition', column, operator: 'greaterThanEqual', value: plain(lo)! },
      { kind: 'condition', column, operator: 'lessThanEqual', value: plain(hi)! },
    ];
  }
  return [...conditions, { kind: 'condition', column, operator: 'in', value: values.map((v) => plain(v)!) as string[] }];
}

/**
 * A DRILLED chart as it draws (batch 2 item 10): the level after the path across, and the path's values as conditions
 * on the chart's own query (never the cube's filter). The column across's grain or bins were chosen for the top level
 * and go with it. Undrilled, the spec itself and no conditions.
 */
export function drilledView(spec: ChartSpec): { readonly spec: ChartSpec; readonly conditions: readonly FilterNode[] } {
  const drill = spec.drill;
  if (!drill || drill.path.length === 0) return { spec, conditions: [] };
  const x = drill.levels[drill.path.length];
  if (x === undefined) return { spec, conditions: [] };
  const { grain: _g, bins: _b, drill: _d, ...rest } = spec;
  return {
    spec: { ...rest, x },
    conditions: drill.path.map((v, i): FilterNode => (v === null
      ? { kind: 'condition', column: drill.levels[i]!, operator: 'isEmpty' }
      : { kind: 'condition', column: drill.levels[i]!, operator: 'equal', value: v })),
  };
}

/**
 * The levels a chart drills through: the grid's own row groupings from the column across down (region, then desk),
 * never the split. Null when there is nothing below, or the column across is not one of them (a period, a bin, a column
 * the grid does not group by).
 */
export function drillLevels(spec: ChartSpec, s: CubeShape): readonly string[] | null {
  return drillRefusal(spec, s) === null ? drillLevelsOf(spec, s) : null;
}

/**
 * Why a chart does not drill (drillLevels null), in words -- what Python's chart.drill is told (D8) -- or null: it does.
 */
export function drillRefusal(spec: ChartSpec, s: CubeShape): string | null {
  if (spec.drill) return null;
  if (spec.x === undefined) return 'the chart has nothing across to drill into';
  if (!DRILLABLE.has(spec.mark)) return `a ${spec.mark} chart does not drill: its marks are not its categories across`;
  if (spec.facet !== undefined) return 'a chart of small multiples does not drill';
  // a period or a bin is not a level of the grid's groupings: there is nowhere "down" from a month
  if (spec.grain !== undefined || spec.bins !== undefined) {
    return `${spec.x} across is in ${spec.grain !== undefined ? `${spec.grain}s` : 'bins'}, not a grouping of the grid's: there is nothing below one`;
  }
  if (!s.rows.includes(spec.x)) return `the grid does not group by ${spec.x}, the chart's column across: it has no level below it`;
  return drillLevelsOf(spec, s) === null ? `nothing is below ${spec.x} in the grid's groupings (${s.rows.join(', ')})` : null;
}

function drillLevelsOf(spec: ChartSpec, s: CubeShape): readonly string[] | null {
  if (spec.drill) return spec.drill.levels;
  const levels = s.rows.slice(s.rows.indexOf(spec.x!)).filter((c) => c !== spec.split);
  return levels.length > 1 ? levels : null;
}

/** The chart one level down from a clicked mark's value across, or null at the bottom. */
export function drillDown(spec: ChartSpec, s: CubeShape, value: Scalar): ChartSpec | null {
  const levels = drillLevels(spec, s);
  const path = spec.drill?.path ?? [];
  if (!levels || path.length + 1 >= levels.length) return null;
  const v = typeof value === 'bigint' ? value.toString() : value;
  return { ...spec, drill: { levels, path: [...path, v] } };
}

/** The chart back up to `depth` values of its path (0: the top, undrilled). */
export function drillUp(spec: ChartSpec, depth: number): ChartSpec {
  if (!spec.drill) return spec;
  const { drill, ...rest } = spec;
  return depth <= 0 ? rest : { ...rest, drill: { ...drill, path: drill.path.slice(0, depth) } };
}

/** The row-stage columns the chart's axis needs: a grain's (its period and the period's end), or its bins'. */
function axisCalcs(spec: ChartSpec): DerivedColumn[] {
  if (spec.x === undefined || plotsRows(spec) || spec.mark === 'gauge') return [];
  if (spec.grain !== undefined) {
    const name = grainName(spec.x, spec.grain);
    return [grainColumn(spec.x, spec.grain, name), grainEndColumn(spec.x, spec.grain, name)];
  }
  if (spec.bins !== undefined) return binColumns(spec.x, spec.bins);
  return [];
}

/**
 * The measure a chart of periods or bins carries for each category: the END of its period or bin, the database's
 * (exclusive: the next period's first day, the next bin's edge), the most of its rows' -- what a click or a brush
 * filters up to.
 */
export const AXIS_UPPER = '__axis_upper';

/** Whether a chart's categories are periods or bins: ranges of the cube's column, each with its database end. */
export function rangesAcross(spec: ChartSpec): boolean {
  return spec.mark !== 'scatter' && spec.mark !== 'gauge' && spec.x !== undefined
    && (spec.grain !== undefined || spec.bins !== undefined);
}

/**
 * A clicked mark as conditions on the cube's own columns: a period of a grain from its first day to the next's, a bin
 * from its edge to the next bin's (`upper`, the next category's edge, when there is one), anything else its value.
 */
export function markConditions(
  spec: ChartSpec,
  s: CubeShape,
  key: Readonly<Record<string, Scalar>>,
  upper?: Scalar,
): FilterNode[] {
  const typeOf = new Map(rowColumns(s).map((c) => [c.name, c.type]));
  // a drilled chart's mark is its path and its own value: the grid narrows to both
  const { spec: view, conditions } = drilledView(spec);
  spec = view;
  return [...conditions, ...Object.entries(key).flatMap(([column, raw]): FilterNode[] => {
    // a big integer as its digits, as the context menu's value filters do
    const value = typeof raw === 'bigint' ? raw.toString() : raw;
    if (column === spec.x && rangesAcross(spec) && value !== null) {
      // a period or a bin: from its start to its end, the database's
      const up = typeof upper === 'bigint' ? upper.toString() : upper;
      if (up === undefined || up === null) throw new Error(`the end of ${column}'s ${spec.grain ?? 'bin'} was not answered`);
      return rangeConditions(column, typeOf.get(column), value, up);
    }
    return value === null
      ? [{ kind: 'condition', column, operator: 'isEmpty' }]
      : [{ kind: 'condition', column, operator: 'equal', value }];
  })];
}

/**
 * A MARK NAMED BY ITS VALUES, as a click on it names it (the drawing's key: chart-option.ts, chart-kinds.ts; D8, Python's
 * chart.select): its value across -- the level drilled to, on a drilled chart -- and, on a chart split by a column, its
 * series' (`split`, which a chart split by nothing takes none of). A chart whose marks are not its categories across
 * names none so: rows (a scatter, a scatter matrix), the grid's own rows (a treemap, a sunburst, a tree), layers no click
 * takes (a stream), one figure (a KPI, a gauge). A period or a bin across is a range that ends where the database says
 * (`AXIS_UPPER`), not named by its start (D8: not at first). The mark's key, for `markConditions`, or why not, in words.
 */
export function namedMark(
  drilled: ChartSpec, value: Scalar, split?: { readonly value: Scalar },
): Readonly<Record<string, Scalar>> | string {
  const { spec } = drilledView(drilled);
  if (plotsRows(spec) || GRID_MARKS.has(spec.mark) || spec.mark === 'stream' || spec.mark === 'kpi' || spec.mark === 'gauge') {
    return `a ${spec.mark} chart's marks are not named by a value across`;
  }
  if (spec.x === undefined) return 'the chart has nothing across: no mark of it is named by a value';
  if (rangesAcross(spec)) {
    return `the chart's marks across are ${spec.grain !== undefined ? `${spec.grain}s` : 'bins'} of ${spec.x}, each a range `
      + 'the database ends: a range is not selected by its start';
  }
  if (spec.split === undefined) {
    return split === undefined ? { [spec.x]: value } : 'the chart is split by nothing: a mark of it is named by its value across alone';
  }
  if (split === undefined) return `the chart is split by ${spec.split}: name the mark's ${spec.split} too`;
  return { [spec.x]: value, [spec.split]: split.value };
}

/**
 * The group-level columns a chart can plot as they are (`fn: 'value'`): the cube's (a margin, a share, a running
 * figure) and the chart's own group-stage calculations. A child aggregate is the grid's alone (it is computed from a
 * second query per level) and is not offered.
 */
export function groupLevelColumns(s: CubeSnapshot, spec?: ChartSpec): DerivedColumn[] {
  return [
    ...(s.groupDerived ?? []).filter((d) => !d.childAggregate),
    ...(spec?.calcs ?? []).filter((c) => c.stage === 'group').map((c) => c.column),
  ];
}

/** Every column name a calculated column reads: its expression's properties, or its window's columns. */
export function columnsRead(d: DerivedColumn): string[] {
  if (d.window) {
    return [
      ...(d.window.column !== undefined ? [d.window.column] : []),
      ...d.window.partition,
      ...d.window.order.map((o) => o.column),
    ];
  }
  if (d.childAggregate) return [d.childAggregate.of];
  const out = new Set<string>();
  const walk = (n: unknown): void => {
    if (Array.isArray(n)) { n.forEach(walk); return; }
    if (!n || typeof n !== 'object') return;
    const o = n as Record<string, unknown>;
    if (o['_type'] === 'property' && typeof o['property'] === 'string') out.add(o['property']);
    for (const v of Object.values(o)) walk(v);
  };
  walk(d.lambda);
  return [...out];
}

/** The dimensions a chart can use, and the columns it can measure. */
export function chartColumns(s: CubeSnapshot, spec?: ChartSpec): {
  dimensions: RowColumn[];
  measures: RowColumn[];
  /** Group-level columns, plotted as they are (`fn: 'value'`). */
  groupLevel: DerivedColumn[];
} {
  const all = [...rowColumns(s), ...chartRowCalcs(spec)];
  return {
    dimensions: all.filter((c) => c.kind === 'dimension'),
    measures: all.filter((c) => isNumeric(c.type)),
    groupLevel: groupLevelColumns(s, spec),
  };
}

/** A chart's own row-stage calculations, as the columns they add. */
function chartRowCalcs(spec: ChartSpec | undefined): RowColumn[] {
  return (spec?.calcs ?? []).filter((c) => c.stage === 'row').map((c): RowColumn => ({
    name: c.column.name,
    ...(c.column.type === undefined ? {} : { type: c.column.type }),
    kind: c.column.kind ?? defaultKind(c.column.type),
    derived: true,
  }));
}

/** The measure a new chart plots when it is not told one: a column it treats as a measure, a fraction first (money, a
 *  quantity) -- every number is a measure by default (D3), and an id or a year summed is the worst first chart -- on
 *  the grid's Aggregation of it (Column Properties), as the grid shows it; never one aggregated by a weighted average,
 *  which needs a weight a chart's value has not; else none. */
function firstMeasure(s: CubeSnapshot): ChartMeasure | undefined {
  const specOf = columnSpecs(s);
  const plotted = chartColumns(s).measures.filter((c) => c.kind === 'measure')
    .map((c) => ({ column: c, fn: defaultMeasure(c.name, specOf.get(c.name)).fn }))
    .filter((m) => m.fn !== 'wavg');
  const first = plotted.find((m) => isFractional(m.column.type)) ?? plotted[0];
  return first ? { column: first.column.name, fn: first.fn } : undefined;
}

/**
 * A CHART'S VALUES AS A GRID'S COLUMN PROPERTIES (Show Grid: chartGrid): each (column, aggregate) pair once -- two values of
 * one pair (a sum and its running total) carried by one column. A pair a column of the grid carries already (its own
 * column at that Aggregation, or a copy of it: a converted measures list's, an earlier Show Grid's) is that column's;
 * any other is made by grid-edits.ts measuresAsColumns, the one rule: the first of a column its Aggregation, unless the
 * column is the chart's across or split (a key the grid groups by) or carries another value already, and those a
 * calculated column named for the value, apart from every name the grid has. `carriedBy` says which column carries
 * each value, for the chart following that grid (followCube); a value no column carries -- a group-level column
 * plotted as it is, a pair left out -- has none, and the chart keeps it as it is.
 */
export function chartValuesAsColumns<D extends GridDraft>(draft: D, y: readonly ChartMeasure[]): {
  readonly draft: D;
  readonly notes: readonly string[];
  readonly carriedBy: (m: ChartMeasure) => string | undefined;
} {
  const pairKey = (m: { readonly column: string; readonly fn: string }): string => JSON.stringify([m.column, m.fn]);
  const keys = new Set([...draft.snapshot.rows, ...draft.snapshot.pivotOn]);
  const taken = new Set(allColumns(draft));
  // the columns whose Aggregation carries a value: another value of the column is a calculated column
  const claimed = new Set<string>();
  const carrier = new Map<string, string>();
  const asked: Measure[] = [];
  for (const m of y) {
    if (m.fn === 'value' || carrier.has(pairKey(m)) || asked.some((a) => pairKey(a) === pairKey(m))) continue;
    const had = carryingColumn(m, draft.snapshot);
    if (had !== undefined && !claimed.has(had)) {
      carrier.set(pairKey(m), had);
      claimed.add(had);
      continue;
    }
    const own = !keys.has(m.column) && !claimed.has(m.column);
    const name = own ? m.column : unusedName(m, taken);
    claimed.add(name);
    taken.add(name);
    asked.push({ name, column: m.column, fn: m.fn });
  }
  const made = measuresAsColumns(draft, asked);
  for (const x of made.made) carrier.set(pairKey(x.measure), x.column);
  return { draft: made.draft, notes: made.notes, carriedBy: (m) => carrier.get(pairKey(m)) };
}

/**
 * Why a chart has no grouping of its own to show in a grid (D7, Show Grid; Python's chart.grid()), in words -- or null:
 * it has one, its column across (and its split). A chart of rows (a scatter, a scatter matrix), of the grid as shown (a
 * treemap, a sunburst, a tree), of places (a map) or of one figure (a gauge) has none a grid's Rows would hold.
 */
export function ownGridRefusal(spec: ChartSpec): string | null {
  if (plotsRows(spec) || GRID_MARKS.has(spec.mark) || MAP_MARKS.has(spec.mark) || spec.mark === 'gauge') {
    return `a ${spec.mark} chart has no grouping of its own to show in a grid`;
  }
  return spec.x === undefined ? 'the chart has nothing across: no grouping of its own to show in a grid' : null;
}

/**
 * A CHART'S OWN GRID (D7: Show Grid on a frozen or a detached chart; Python's chart.grid()): the grid of the chart's own
 * grouping and query -- its cube's query (`draft`: as its grid has it, or as the chart kept it, detached) with the chart's
 * selection off its filter, grouped by its column across and its split, pivoted by nothing, its values the grid's Column
 * Properties (chartValuesAsColumns, each value read back from the column that carries it) -- and the chart following
 * that grid: the same chart, frozen no more. Or why it has none (ownGridRefusal).
 */
export function chartGrid(draft: GridDraft, spec: ChartSpec, selection: readonly FilterNode[]): {
  readonly snapshot: CubeSnapshot;
  readonly configuration: CubeConfiguration;
  readonly spec: ChartSpec;
  readonly notes: readonly string[];
} | string {
  const refused = ownGridRefusal(spec);
  if (refused !== null) return refused;
  const { filter: was, ...query } = draft.snapshot;
  const filter = withoutConditions(was, selection) ?? undefined;
  const rows = [spec.x!, ...(spec.split !== undefined ? [spec.split] : [])];
  const made = chartValuesAsColumns({ ...draft, snapshot: { ...query, ...(filter ? { filter } : {}), rows, pivotOn: [] } }, spec.y);
  // folded: the grid reads its query back over its configuration, whose Aggregations are the chart's values now
  const snapshot = applyToSnapshot(made.draft.snapshot, made.draft.config);
  const { frozen: _frozen, ...following } = spec;
  return { snapshot, configuration: made.draft.config, spec: followCube(following, snapshot, made.carriedBy), notes: made.notes };
}

/** A name for a calculated column carrying `m` that the grid has not: the value's own (`max_notional`), else by its column. */
function unusedName(m: ChartMeasure, taken: ReadonlySet<string>): string {
  const base = baseName(m);
  const named = taken.has(base) ? `${m.fn}_${m.column}` : base;
  if (!taken.has(named)) return named;
  let n = 2;
  while (taken.has(`${named}_${n}`)) n += 1;
  return `${named}_${n}`;
}

/**
 * The grid's column that carries a chart's value as it is, if one does: its own column at that aggregate (not a key the
 * grid groups by, which it does not aggregate), else a calculated column that copies it (grid-edits.ts copiedColumn,
 * as a converted measures list or Show Grid makes one) at that aggregate -- so a saved page's min and max of one
 * column, converted into the column and a copy, stay two values.
 */
function carryingColumn(m: ChartMeasure, s: CubeShape): string | undefined {
  if (m.fn === 'value') return m.column;
  const specs = columnSpecs(s);
  const carries = (name: string): boolean => defaultMeasure(name, specs.get(name)).fn === m.fn;
  if (!s.rows.includes(m.column) && !s.pivotOn.includes(m.column) && carries(m.column)) return m.column;
  return s.derived.find((d) => copiedColumn(d) === m.column && carries(d.name))?.name;
}

/**
 * A chart's value as a following chart plots it: its column at the grid's Aggregation of `column`, the grid column that
 * carries it (carryingColumn; a following chart's value no column carries follows its own column's). A row's value,
 * a value no column carries for Update, a value of a dimension (which the grid does not aggregate), one the grid
 * aggregates by a weighted average (which needs a weight a chart's value has not), and one whose followed pair another
 * of the chart's values plots already (the grid carries one of two values of a column: followed, the chart would plot
 * it twice, and lose the other for good, as a following chart keeps what it follows) keep the chart's own.
 */
function followedValue(m: ChartMeasure, s: CubeShape, column: string | undefined,
  plotted: readonly ChartMeasure[]): ChartMeasure {
  if (m.fn === 'value' || column === undefined) return m;
  if (rowColumns(s).find((c) => c.name === column)?.kind !== 'measure') return m;
  const fn = defaultMeasure(column, columnSpecs(s).get(column)).fn;
  if (fn === 'wavg' || fn === m.fn) return m;
  const next = { ...m, fn };
  return plotted.some((o) => o !== m && sameValue(o, next)) ? m : next;
}

/** Whether two of a chart's values plot the same: one column, one aggregate, shown alike. */
function sameValue(a: ChartMeasure, b: ChartMeasure): boolean {
  return a.column === b.column && a.fn === b.fn && JSON.stringify(a.showAs ?? null) === JSON.stringify(b.showAs ?? null);
}

/** A new chart's options with `x` across: time runs left to right, and long; categories lead with the largest. */
function optionsAcross(x: string | undefined, s: CubeShape): ChartOptions {
  const temporal = isTemporalAcross(x, s);
  return { ...DEFAULT_OPTIONS, ...(temporal ? { sort: { by: 'x', direction: 'asc' }, limit: TIME_LIMIT } : {}) };
}

/** Whether `x` is a date or a time the cube has (its rows' columns). */
function isTemporalAcross(x: string | undefined, s: CubeShape): boolean {
  return x !== undefined && isTemporal(rowColumns(s).find((c) => c.name === x)?.type);
}

/**
 * A chart with another column across (or none), as the cube's grouping moves it (followCube) and as a hand moves it
 * (editChart) -- one rule: what was chosen for the old column, its grain or bins and a drill down it, goes with it; the order is the new
 * column's -- a Pareto's from the largest always (its cumulative share runs along that order), a date's in time,
 * another column's from the largest -- and so is how many it draws, unless the chart was told: a time series' points
 * for a date, a category's default for anything else. A chart of rows (a scatter's X) asks no order and no such limit
 * (chartQuery): its options stay as they are.
 */
function acrossChanged(spec: ChartSpec, x: string | undefined, s: CubeShape): ChartSpec {
  const { x: _x, grain: _g, bins: _b, drill: _d, ...rest } = spec;
  const across = { ...rest, ...(x !== undefined ? { x } : {}) };
  if (plotsRows(spec)) return across;
  const inTime = spec.mark !== 'pareto' && isTemporalAcross(x, s);
  // the limit the old column was given by default, never one the chart was told; a map's is the world's, whatever is
  // across (the phase 6 audit: a map made from a date across lost it to the time default's change back)
  const limit = MAP_MARKS.has(spec.mark) ? spec.options.limit
    : inTime && spec.options.limit === DEFAULT_CHART_LIMIT ? TIME_LIMIT
    : !inTime && spec.options.limit === TIME_LIMIT ? DEFAULT_CHART_LIMIT : spec.options.limit;
  // a Marimekko runs in its order across and a Gantt from the earliest start, whatever is across (their own orders:
  // asMark's)
  const own = spec.mark === 'marimekko' || spec.mark === 'gantt';
  const sort = own ? spec.options.sort : inTime ? { by: 'x' as const, direction: 'asc' as const } : { by: 'y' as const, direction: 'desc' as const };
  return { ...across, options: { ...spec.options, sort, limit } };
}

/**
 * A first chart for a cube, from what the cube already says: its first row
 * dimension (else the first dimension) across, its first measure (else the
 * first number, summed) up, and its second row dimension as the split. A
 * date across is a line; anything else a bar.
 */
export function defaultChart(s: CubeSnapshot): ChartSpec | null {
  const x = s.rows[0] ?? chartColumns(s).dimensions[0]?.name;
  const split = s.rows[1];
  const y = firstMeasure(s);
  if (x === undefined) return null;
  const temporal = isTemporal(rowColumns(s).find((c) => c.name === x)?.type);
  return {
    version: CHART_VERSION,
    mark: temporal ? 'line' : 'bar',
    x,
    y: y ? [y] : [{ column: x, fn: 'count' }],
    ...(split !== undefined ? { split } : {}),
    options: optionsAcross(x, s),
  };
}

/**
 * A chart taking another kind, as the form's change of kind has always made it: a gauge and a scatter matrix drop the
 * column across (and its grain or bins with it, as a change of the column across takes them), a gauge and a kind of one
 * series the split, a Pareto runs from the largest, a box plot plots its first value's column's five figures
 * (boxMeasures; a group-level column, already one figure per group, has none: kept, for the rules to refuse), and a box
 * plot taking a kind of one value keeps its median, the figure it was ordered by (its four others would be drawn by no
 * kind of one value, the phase 5 audit); a map asks for the world's countries (MAP_LIMIT), and a bubble map's places
 * go with it, as a gauge's column across does. Only those: every other slot (a layer, a facet, a label, a size,
 * a hover's measures) stays, for the rules to judge on the new kind -- refused by name where it has no place, never
 * dropped without a word. Part of a change by hand (editChart).
 */
function asMark(spec: ChartSpec, mark: ChartMark, s: CubeSnapshot): ChartSpec {
  const { x, split, grain, bins, at: _at, ...rest } = spec;
  // a bubble map's places go with it: no other kind places anything by latitude and longitude
  const at = mark === 'bubblemap' ? spec.at : undefined;
  const keepX = mark !== 'gauge' && mark !== 'matrix' && x !== undefined;
  const keepSplit = !UNSPLIT.has(mark) && split !== undefined;
  // the options built once (the phase 7 audit: a kind's order was lost to a map's limit spread after it): a Pareto runs
  // from the largest, a Marimekko in its order across, a Gantt from the earliest start; a map asks for the world's
  // countries, and leaving one a chart asks for its own default again (a date across's, or the first fifty) -- the
  // default a map was given, never one it was told
  const sort: ChartOptions['sort'] | undefined = mark === 'pareto' ? { by: 'y', direction: 'desc' }
    : mark === 'marimekko' ? { by: 'x', direction: 'asc' } : mark === 'gantt' ? { by: 'y', direction: 'asc' } : undefined;
  const limit = MAP_MARKS.has(mark) && !MAP_MARKS.has(spec.mark) ? MAP_LIMIT
    : !MAP_MARKS.has(mark) && MAP_MARKS.has(spec.mark) && rest.options.limit === MAP_LIMIT
      ? (keepX && isTemporalAcross(x, s) ? TIME_LIMIT : DEFAULT_CHART_LIMIT) : undefined;
  return {
    ...rest,
    mark,
    // the column across's grain or bins go with it (acrossChanged's rule)
    ...(keepX ? { x, ...(grain !== undefined ? { grain } : {}), ...(bins !== undefined ? { bins } : {}) } : {}),
    ...(keepSplit ? { split } : {}),
    ...(sort !== undefined || limit !== undefined
      ? { options: { ...rest.options, ...(sort !== undefined ? { sort } : {}), ...(limit !== undefined ? { limit } : {}) } } : {}),
    ...(mark === 'boxplot' && rest.y[0] !== undefined && rest.y[0].fn !== 'value' ? { y: boxMeasures(rest.y[0].column) } : {}),
    ...(spec.mark === 'boxplot' && !SEVERAL_VALUES.has(mark) && rest.y[2] !== undefined ? { y: [rest.y[2]] } : {}),
    ...(at !== undefined ? { at } : {}),
  };
}

/** Whether a chart takes its grouping from the cube as the cube's grouping changes (followCube): a live chart of
 *  groups, not one of rows, nor one drawing the grid as shown. */
function live(spec: ChartSpec): boolean {
  return !spec.frozen && !plotsRows(spec) && !GRID_MARKS.has(spec.mark) && !MAP_MARKS.has(spec.mark);
}

/**
 * Whether a change by hand freezes a chart: it touches what the next change of the cube's grouping would set again
 * (followCube) -- a KPI's or a gauge's measure; another kind's column across, its split (but a kind of one series',
 * which has none) and its measure (but a kind of several values', which keeps its own). Frozen, the chart keeps the
 * hand's choice. Part of a change by hand (editChart).
 */
function freezesOn(spec: ChartSpec, changed: readonly string[]): boolean {
  if (!live(spec)) return false;
  const followed = spec.mark === 'kpi' || spec.mark === 'gauge' ? ['y']
    : ['x', ...(UNSPLIT.has(spec.mark) ? [] : ['split']), ...(SEVERAL_VALUES.has(spec.mark) ? [] : ['y'])];
  return changed.some((k) => followed.includes(k));
}

/** A change of a chart by hand, in its spec's own keys: a slot told as null is taken away, and an option likewise. */
export type ChartChange = {
  readonly [K in Exclude<keyof ChartSpec, 'version' | 'mark' | 'y' | 'options'>]?: ChartSpec[K] | null;
} & {
  readonly mark?: ChartMark;
  readonly y?: readonly ChartMeasure[];
  readonly options?: { readonly [K in keyof ChartOptions]?: ChartOptions[K] | null };
};

/**
 * A chart changed by hand -- each field of the charts' options form (ui/chart-panel.ts, ui/chart-form.ts), and a chart
 * Python makes or edits (chartFrom): one rule. What is told replaces what the chart had, and what is told as null is
 * taken away; a kind told takes what it needs and leaves what it has no place for (asMark); a new column across takes
 * its own order, and what was chosen for the old one goes with it (acrossChanged); told options are laid over the
 * chart's own; a hand on what the cube's grouping sets freezes a live chart (freezesOn), unless `frozen` is told.
 */
export function editChart(spec: ChartSpec, change: ChartChange, s: CubeSnapshot): ChartSpec {
  const told = (key: string): boolean => Object.hasOwn(change, key);
  const shaped = change.mark !== undefined && change.mark !== spec.mark ? asMark(spec, change.mark, s) : spec;
  const x = change.x ?? undefined;
  const moved = told('x') && x !== spec.x ? acrossChanged(shaped, x, s) : shaped;
  const { options, ...slots } = change;
  // options that are no object come only from a caller with no types (Python's, through chartFrom): kept as told, for
  // the reader to refuse by name
  const laid = options === undefined ? moved.options
    : isRecord(options) ? withoutNulls({ ...moved.options, ...options }) : options;
  const made = withoutNulls({ ...moved, ...slots, options: laid }) as ChartSpec;
  return !told('frozen') && freezesOn(made, Object.keys(change)) ? { ...made, frozen: true } : made;
}

/** What the charts' panel says of a cube it can make no chart of (defaultChart: no column to put across). */
export const NO_CHART = 'This cube has no column to chart.';

/**
 * The chart DataCube makes of what Python tells it (`given`: a saved spec's own keys, any of them): its own new chart
 * (defaultChart) changed by hand, or `before` changed by hand -- one rule, the panel's (editChart) -- then read as a
 * page reads a chart (readChart). The made chart, or what stops it, in words. Python's grid.chart(), Chart.plot() and
 * Chart.options() are made here (headless/grid.ts gridChart): never by rules of Python's own.
 */
export function chartFrom(
  given: Readonly<Record<string, unknown>>, s: CubeSnapshot, before?: ChartSpec,
): ChartSpec | string {
  const start = before ?? defaultChart(s);
  if (!start) return NO_CHART;
  // what Python told, untyped: whatever is not a chart's the reader refuses by name
  const read = readChart(editChart(start, given as ChartChange, s));
  return typeof read === 'string' ? `DataCube cannot read this chart: ${read}.` : read;
}

/** An object without the keys told as null: a slot or an option taken away. */
function withoutNulls<T extends object>(o: T): T {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== null)) as T;
}

/** Whether two values say the same, whatever the order of their objects' keys. */
function same(a: unknown, b: unknown): boolean {
  const canonical = (v: unknown): unknown => (Array.isArray(v) ? v.map(canonical)
    : isRecord(v) ? Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonical(v[k])])) : v);
  return JSON.stringify(canonical(a) ?? null) === JSON.stringify(canonical(b) ?? null);
}

/**
 * What a live chart was told that its grid's grouping replaces, in words, or null. A chart that follows its grid takes
 * its column across, its split and its values from the grid (followCube), so one told otherwise would be dropped
 * without a word. The panel never meets it -- a hand on them freezes the chart (freezesOn) -- but a chart told them
 * and told to follow (Python's frozen=False) does: refused, never quietly replaced.
 */
export function followRefusal(told: readonly string[], made: ChartSpec, followed: ChartSpec): string | null {
  const said = (v: unknown): string => (v === undefined ? 'none' : typeof v === 'string' ? `'${v}'`
    : (v as readonly ChartMeasure[]).map((m) => `${m.fn} of '${m.column}'`).join(', '));
  for (const [key, as] of [['x', 'across'], ['split', 'as its split'], ['y', 'as its values']] as const) {
    if (told.includes(key) && !same(made[key], followed[key])) {
      return `A chart that follows its grid takes ${said(followed[key])} ${as}, not ${said(made[key])}: leave ${key} `
        + 'out, or let the chart keep its own (frozen).';
    }
  }
  return null;
}

/**
 * A live chart, regrouped the way the cube is now: its first row group across; its second row group -- else its first
 * column label -- as the split; each value on the grid's Aggregation of the column carrying it (followedValue:
 * `carriedBy` -- Show Grid's, from the grid it makes (chartGrid); else carryingColumn, a value the grid carries as it is keeping its
 * aggregate, and one it does not following its own column's). The mark and the options are the chart's; the order follows what is across (acrossChanged). A frozen chart, a chart of rows (it plots
 * values, not groups), a chart of the grid as shown and a cube with nothing grouped are left as they are.
 */
export function followCube(spec: ChartSpec, s: CubeShape,
  carriedBy: (m: ChartMeasure) => string | undefined = (m) => carryingColumn(m, s) ?? m.column): ChartSpec {
  // a drilled chart stays where it was taken until it is taken back up
  if (!live(spec) || (spec.drill?.path.length ?? 0) > 0) return spec;
  // each value against the others as they are and as followed before it: none is followed onto another's pair
  const followed = (y: readonly ChartMeasure[]): readonly ChartMeasure[] => y.reduce<ChartMeasure[]>(
    (done, m, i) => [...done, followedValue(m, s, carriedBy(m), [...done, ...y.slice(i + 1)])], []);
  // a KPI or a gauge follows the grid's Aggregation of its value; a KPI's trend's column is its own
  if (spec.mark === 'kpi' || spec.mark === 'gauge') {
    const y = followed(spec.y);
    return same(y, spec.y) ? spec : { ...spec, y };
  }
  const x = s.rows[0] ?? s.pivotOn[0];
  if (x === undefined) return spec;
  // a kind of one series takes no split from the cube; a kind of several values (a dumbbell's two ends, a radar's
  // spokes) keeps its own, as the cube's one measure would leave it refused
  const split = UNSPLIT.has(spec.mark) ? undefined
    : s.rows[0] !== undefined ? (s.rows[1] ?? s.pivotOn[0]) : s.pivotOn[1];
  const y = SEVERAL_VALUES.has(spec.mark) ? spec.y : followed(spec.y);
  // a new column across takes its own order, and its grain or bins go with the old one (acrossChanged)
  const { split: _old, ...rest } = x === spec.x ? spec : acrossChanged(spec, x, s);
  const next: ChartSpec = { ...rest, y, ...(split !== undefined ? { split } : {}) };
  return same(next, spec) ? spec : next;
}

/** What stops a spec from being drawn, in words; empty when it can be. */
export function chartProblems(drilled: ChartSpec, s: CubeSnapshot): string[] {
  // a chart of the grid as shown (a treemap, a sunburst, a tree) is drawn from the grid's own rows and levels: the
  // column across, the values and the split it carries (kept for a change back to another kind) are not judged, as it
  // draws none of them; a slot it does not draw is refused as on any kind
  const gridKind = GRID_MARKS.has(drilled.mark);
  // a drilled chart is checked as it draws: the level after its path across (the levels themselves are checked too)
  const spec = drilledView(drilled).spec;
  const known = new Set([...rowColumns(s), ...chartRowCalcs(spec)].map((c) => c.name));
  const groupLevel = new Set(groupLevelColumns(s, spec).map((d) => d.name));
  const out: string[] = [];
  // a KPI's column across is its trend, and it may have none; a gauge is one value, with nothing across
  if (gridKind) {
    // nothing across, nothing to plot of its own
  } else if (spec.x === undefined && spec.mark !== 'kpi' && spec.mark !== 'gauge' && spec.mark !== 'matrix') {
    out.push('Pick a column across.');
  } else if (spec.x !== undefined && !known.has(spec.x)) out.push(`The cube has no column '${spec.x}'.`);
  if (spec.y.length === 0 && !gridKind) out.push('Pick something to plot.');
  const drawn = gridKind ? [] : spec.y;
  for (const m of [...drawn, ...(spec.tooltip ?? []), ...(spec.size ? [spec.size] : []), ...(spec.label ? [spec.label] : [])]) {
    if (m.fn === 'value') {
      if (plotsRows(spec)) out.push(`A ${markLabel(spec.mark)} plots rows, not groups: '${m.column}' is a group-level column.`);
      else if (!groupLevel.has(m.column)) out.push(`The cube has no group-level column '${m.column}'.`);
    } else if (m.fn !== 'count' && !known.has(m.column)) {
      out.push(`The cube has no column '${m.column}'.`);
    }
    // a weighted average is weighted by another column, which a chart's value names none of (the grid's has its weight)
    if (m.fn === 'wavg') out.push(`A chart plots no weighted average: '${m.column}' would need its weight.`);
  }
  if (spec.split !== undefined && !gridKind && !known.has(spec.split)) {
    out.push(`The cube has no column '${spec.split}'.`);
  }
  const needs = SPLIT_NEEDED[spec.mark];
  if (needs !== undefined && spec.split === undefined) out.push(needs);
  const kind = markLabel(spec.mark);
  const article = /^[aeiou]/i.test(kind) ? 'An' : 'A';
  if (UNSPLIT.has(spec.mark) && spec.split !== undefined) out.push(`${article} ${kind} draws one series: it has no split.`);
  if (spec.mark === 'gauge' && spec.x !== undefined) out.push('A gauge is one value: it has no column across.');
  if ((spec.mark === 'dumbbell' || spec.mark === 'range') && spec.y.length !== 2) {
    out.push(`${article} ${kind} plots two values per category: its low and its high (pick two).`);
  }
  if (spec.mark === 'boxplot') {
    const column = spec.y[0]?.column;
    const figures = spec.y.map((m) => ({ column: m.column, fn: m.fn }));
    const five = column === undefined ? [] : boxMeasures(column);
    if (column !== undefined && same(figures, five)) {
      // a chart's own calculated column is one of its columns (chartColumns offers it), as `xType` reads it below
      const type = [...rowColumns(s), ...chartRowCalcs(spec)].find((c) => c.name === column)?.type;
      if (known.has(column) && !isNumeric(type)) out.push(`'${column}' is not a number: it has no quartiles.`);
    } else if (figures.length === five.length && figures.every((m) => m.column === column)
      && same([...figures].map((m) => m.fn).sort(), [...BOX_FNS].sort())) {
      out.push(`A box plot's five figures go in order: ${BOX_FNS.join(', ')}.`);
    } else {
      out.push(`A box plot plots five figures of one column: its ${BOX_FNS.join(', ')}.`);
    }
    if (spec.y.some((m) => m.mark !== undefined || m.axis !== undefined)) {
      out.push('A box plot draws its five figures as one box: a value\'s own mark or axis has no place on it.');
    }
  }
  // A MAP finds each row's country by its code or its name (text); a bubble map places each by its latitude and
  // longitude, two number columns; nothing else places by them
  if (spec.mark === 'map' && spec.x !== undefined && known.has(spec.x)
    && familyOf([...rowColumns(s), ...chartRowCalcs(spec)].find((c) => c.name === spec.x)?.type) !== 'text') {
    out.push(`A map finds each row's country by its ISO code or its name: '${spec.x}' is not text.`);
  }
  if (spec.mark === 'bubblemap') {
    if (spec.at?.lat === undefined) out.push('A bubble map needs the column of each place\'s latitude.');
    if (spec.at?.lon === undefined) out.push('A bubble map needs the column of each place\'s longitude.');
    for (const c of [spec.at?.lat, spec.at?.lon].filter((p): p is string => p !== undefined)) {
      const type = [...rowColumns(s), ...chartRowCalcs(spec)].find((r) => r.name === c)?.type;
      if (!known.has(c)) out.push(`The cube has no column '${c}'.`);
      else if (!isNumeric(type)) out.push(`'${c}' is not a number: it places nothing.`);
    }
    // a place's name is one column and its position two others: grouped by each once (the phase 6 audit)
    if (spec.x !== undefined && (spec.x === spec.at?.lat || spec.x === spec.at?.lon)) {
      out.push(`'${spec.x}' is a place's ${spec.x === spec.at?.lat ? 'latitude' : 'longitude'}: name the places by another column.`);
    }
  } else if (spec.at !== undefined) {
    out.push(`${article} ${kind} places nothing by latitude and longitude: only a bubble map does.`);
  }
  // a map's places are countries or points, never periods or bins: a click on one would have no range to filter to
  if (MAP_MARKS.has(spec.mark) && (spec.grain !== undefined || spec.bins !== undefined)) {
    out.push(`${article} ${kind} places each row by what it names: it has no ${spec.grain !== undefined ? 'grain' : 'bins'}.`);
  }
  // A GANTT runs each row from its start to its end: two values, each a date column's least or greatest
  if (spec.mark === 'gantt') {
    // a date or an instant: a time of day alone has no place on a calendar's axis (the phase 7 audit)
    const dated = (m: ChartMeasure): boolean => {
      const type = [...rowColumns(s), ...chartRowCalcs(spec)].find((c) => c.name === m.column)?.type;
      return (m.fn === 'min' || m.fn === 'max') && isTemporal(type) && !isTimeOfDay(type);
    };
    if (spec.y.length !== 2 || !spec.y.every(dated)) {
      out.push('A Gantt runs each row from a start to an end: two values, each the min or max of a date column.');
    }
  }
  if (spec.mark === 'marimekko' && (spec.options.sort.by !== 'x')) {
    out.push('A Marimekko lays its columns out in their order across: order by category.');
  }
  if (spec.mark === 'radar' && spec.y.length < 3) out.push('A radar needs three values or more, one per spoke.');
  if (spec.mark === 'parallel' && spec.y.length < 2) out.push('Parallel coordinates need two values or more, one per axis.');
  // a pie, a lollipop, a KPI and a heatmap draw their first value alone: the others would be asked and never drawn
  if ((spec.mark === 'waterfall' || spec.mark === 'pareto' || spec.mark === 'gauge' || spec.mark === 'calendar'
    || spec.mark === 'funnel' || spec.mark === 'rose' || spec.mark === 'stream' || spec.mark === 'sankey'
    || spec.mark === 'chord' || spec.mark === 'pie' || spec.mark === 'lollipop' || spec.mark === 'kpi'
    || spec.mark === 'heatmap' || MAP_MARKS.has(spec.mark) || spec.mark === 'marimekko' || spec.mark === 'waffle')
    && spec.y.length > 1) {
    out.push(`${article} ${kind} plots one value.`);
  }
  // a Pareto runs from the largest: its cumulative share is along that order, so the chart's order is it
  if (spec.mark === 'pareto' && (spec.options.sort.by !== 'y' || spec.options.sort.direction !== 'desc')) {
    out.push('A Pareto runs from the largest value: order by value, largest first.');
  }
  if (spec.mark === 'gauge' && spec.options.target === undefined) {
    out.push('A gauge needs a target: its scale runs from zero to it.');
  }
  if ((spec.mark === 'waterfall' || spec.mark === 'pareto' || spec.mark === 'gauge' || spec.mark === 'boxplot'
    || spec.mark === 'marimekko' || spec.mark === 'waffle' || spec.mark === 'gantt') && spec.y.some((m) => m.showAs)) {
    out.push(`${article} ${kind} computes its own figures: a value shown as something else has no place on it.`);
  }
  // a grain takes a date to its period, bins a number to its bin: on any other column they mean nothing
  const xType = [...rowColumns(s), ...chartRowCalcs(spec)].find((c) => c.name === spec.x)?.type;
  // a date is a day or an instant: a time of day alone has no week, month or year, and no calendar (Python's _DATES)
  const isDate = (type: string | undefined): boolean => isTemporal(type) && !isTimeOfDay(type);
  if (spec.grain !== undefined && spec.x !== undefined && known.has(spec.x) && !isDate(xType)) {
    out.push(`'${spec.x}' is not a date: it has no ${spec.grain}.`);
  }
  if (spec.bins !== undefined && spec.x !== undefined && known.has(spec.x) && !isNumeric(xType)) {
    out.push(`'${spec.x}' is not a number: it has no bins.`);
  }
  if (spec.grain !== undefined && spec.bins !== undefined) out.push('A column across has a grain or bins, not both.');
  if (spec.x === undefined && !gridKind && (spec.grain !== undefined || spec.bins !== undefined)) {
    out.push(`No column is across: it has no ${spec.grain !== undefined ? 'grain' : 'bins'}.`);
  }
  if (gridKind && (spec.grain !== undefined || spec.bins !== undefined)) {
    out.push(`${article} ${kind} draws the grid as shown: it has no ${spec.grain !== undefined ? 'grain' : 'bins'}.`);
  }
  if (spec.split !== undefined && !gridKind && spec.split === spec.x) {
    out.push('Split by a different column than the one across.');
  }
  for (const level of drilled.drill?.levels ?? []) {
    if (!known.has(level)) out.push(`The cube has no column '${level}' to drill into.`);
  }
  if (DATE_ACROSS.has(spec.mark) && spec.x !== undefined && known.has(spec.x) && !isDate(xType)) {
    out.push(`${article} ${kind} runs along dates: '${spec.x}' is not one.`);
  }
  if (spec.mark === 'calendar' && spec.grain !== undefined && spec.grain !== 'day') {
    out.push('A calendar has a cell per day: its grain is the day.');
  }
  // only a StrictDate is a day: a date-time's rows are instants, and so may a Date's be (Pure's firstDayOfMonth and
  // adjust return one), so a cell per day needs them taken to their day first -- never drawn as days without a word
  if (spec.mark === 'calendar' && spec.grain === undefined && isDate(xType) && !isDay(xType)) {
    out.push(`A calendar has a cell per day: '${spec.x}' may hold a time of day, so give it the day grain.`);
  }
  // A SLOT NOTHING DRAWS IS REFUSED, never ignored (AGENTS.md invariant 4): small multiples are drawn by bars, lines
  // and areas; a mark's size by a scatter (a bubble); a layer reads a value the chart plots, on a kind that draws
  // layers; a scatter plots rows (no show-as); a 100% stack's axis is shares, which a layer of figures would be drawn
  // against
  if (spec.facet !== undefined) {
    // small multiples of bars, lines and areas: a panel per value, each its own query's rows; no brush or slider
    // across many panels, and no colour by value (one scale of colour across the page's panels is a design to make)
    if (!LAYERED.has(spec.mark)) out.push(`${article} ${kind} is not drawn as small multiples: bars, lines and areas are.`);
    for (const c of facetColumns(spec)) {
      if (!known.has(c)) out.push(`The cube has no column '${c}'.`);
      else if (c === spec.x || c === spec.split) out.push(`'${c}' is across or split already: a facet is another column.`);
    }
    if (spec.facet.row !== undefined && spec.facet.row === spec.facet.column) out.push('A facet\'s rows and columns are two columns.');
    if (spec.options.brush) out.push('Small multiples take no brush: brush one chart.');
    if (spec.options.zoom) out.push('Small multiples take no slider: zoom one chart.');
    if ((spec.layers ?? []).some((l) => l.kind === 'threshold')) out.push('Small multiples are not coloured by value yet.');
  }
  if (spec.size !== undefined && spec.mark !== 'scatter') out.push('Only a scatter sizes its marks (a bubble).');
  for (const l of spec.layers ?? []) {
    if (l.kind !== 'band' && (l.measure ?? 0) >= spec.y.length) out.push(`A ${l.kind} layer reads value ${(l.measure ?? 0) + 1}, which the chart does not plot.`);
  }
  if (!LAYERED.has(spec.mark) && (spec.layers ?? []).length > 0) out.push(`${article} ${kind} draws no reference lines yet.`);
  if (!LABELLED.has(spec.mark) && spec.label !== undefined) out.push(`${article} ${kind} writes no other measure on its marks.`);
  if (!HOVER_EXTRAS.has(spec.mark) && (spec.tooltip ?? []).length > 0 && spec.mark !== 'scatter') {
    out.push(`${article} ${kind}'s hover lists no further measures.`);
  }
  if (plotsRows(spec) && spec.y.some((m) => m.showAs)) out.push(`A ${markLabel(spec.mark)} plots rows: a value shown as a running total, a share or a rank needs groups.`);
  if (spec.mark === 'matrix') {
    if (spec.y.length < 2 || spec.y.length > MAX_MATRIX) out.push(`A scatter matrix plots each pair of two to ${MAX_MATRIX} values.`);
    if (spec.x !== undefined) out.push('A scatter matrix has no column across: it plots its values against each other.');
  }
  if (spec.options.stack === 'percent' && (spec.layers ?? []).some((l) => l.kind !== 'band')) {
    out.push('A 100% stack shows shares: a reference line, a threshold or a pinned extreme of figures has no place on it.');
  }
  // a group-level column of the cube's that reads a column of the grid's grouping the chart does not group by would be
  // computed over the chart's rows as if that column were there: refused, not answered wrongly
  // a bubble map's places are its grouping too (chartSnapshot's keys)
  const placeKeys = spec.mark === 'bubblemap' ? [spec.at?.lat, spec.at?.lon] : [];
  const keys = new Set([spec.x, spec.split, axisName(spec), ...placeKeys].filter((c): c is string => c !== undefined));
  const dimensions = new Set(rowColumns(s).filter((c) => c.kind === 'dimension').map((c) => c.name));
  const byName = new Map(groupLevelColumns(s, spec).map((d) => [d.name, d]));
  const reads = (name: string, seen = new Set<string>()): string[] => {
    if (seen.has(name)) return [];
    seen.add(name);
    const d = byName.get(name);
    return d ? columnsRead(d).flatMap((c) => [c, ...reads(c, seen)]) : [];
  };
  for (const m of drawn.filter((x) => x.fn === 'value')) {
    const missing = [...new Set(reads(m.column))].filter((c) => dimensions.has(c) && !keys.has(c));
    if (missing.length > 0) out.push(`'${m.column}' reads ${missing.map((c) => `'${c}'`).join(', ')}, which the chart does not group by.`);
  }
  return out;
}

/**
 * The cube's snapshot, regrouped for the chart.
 *
 * Kept from the cube: the source, the filter, the calculated columns.
 * Replaced: the grouping (x and split), the measures (the chart's), the
 * pivot (none), the sorts (the chart's order). Only the columns the chart
 * reads are kept, so a grouped query does not carry every other column
 * along as a "unique value".
 */
export function chartSnapshot(s: CubeSnapshot, drilled: ChartSpec): QuerySnapshot {
  // drilled, the level after the path across and the path as the chart's own conditions
  const view = drilledView(drilled);
  const conditions = view.conditions;
  // a gauge is one value: its query groups by nothing, as a KPI's total does
  const { x: _gx, split: _gs, ...ungrouped } = view.spec;
  const spec: ChartSpec = view.spec.mark === 'gauge' ? ungrouped : view.spec;
  const filter: FilterNode | undefined = conditions.length === 0 ? s.filter
    : { kind: 'and', children: [...(s.filter ? [s.filter] : []), ...conditions] };
  // a scatter, or a scatter matrix: rows as they are, not groups
  const scatter = plotsRows(spec);
  const across = axisName(spec);
  // SMALL MULTIPLES: a chart per value of the facet's columns -- grouped by them too, each panel a whole of its own
  const panels = facetColumns(spec);
  // a bubble map's places: grouped by them too, each place where its rows say it is (never an average of positions)
  const places = spec.mark === 'bubblemap' ? [spec.at?.lat, spec.at?.lon] : [];
  const keys = [scatter ? spec.x : across, spec.split, ...panels, ...places].filter((c): c is string => c !== undefined);
  // what the chart plots, and the measures a hover lists, a mark's size reads and its label says
  const extras = [...(spec.tooltip ?? []), ...(spec.size ? [spec.size] : []), ...(spec.label ? [spec.label] : [])];
  const plotted = [...spec.y, ...extras];
  const aggregated = plotted.filter((m): m is ChartMeasure & { fn: AggregateFn } => m.fn !== 'value');
  // a measure's name before any show-as: two measures shown two ways of one aggregate share it
  const measures: Measure[] = scatter ? [] : [...new Map(aggregated.map((m) => [baseName(m), {
    name: baseName(m),
    column: m.column,
    fn: m.fn,
  }] as const)).values()];
  // A GROUP-LEVEL COLUMN plotted as it is: kept, with every group-level column it reads (a share reads a window, a
  // window a measure) and every measure of the cube's they read, under the names they read them by -- the expression
  // was written against those names.
  const groupLevel = new Map(groupLevelColumns(s, spec).map((d) => [d.name, d]));
  // the grid's own aggregated columns, each on its Aggregation (Column Properties), as its group level writes them
  const specOf = columnSpecs(s);
  const gridColumns = new Set(rowColumns(s).map((c) => c.name));
  const keptGroup: DerivedColumn[] = [];
  const keptMeasures: Measure[] = [];
  const seen = new Set<string>(measures.map((m) => m.name));
  const need = (name: string): void => {
    if (seen.has(name)) return;
    seen.add(name);
    const d = groupLevel.get(name);
    if (d) {
      columnsRead(d).forEach(need);
      keptGroup.push(d);
      return;
    }
    // a column the chart groups by is a key of its rows, never aggregated (a window ordered by its column across)
    if (gridColumns.has(name) && !keys.includes(name)) keptMeasures.push(defaultMeasure(name, specOf.get(name)));
  };
  if (!scatter) plotted.filter((m) => m.fn === 'value').forEach((m) => need(m.column));
  // A SHOW-AS: group-level columns over the chart's rows, after what they read -- along the axis, restarting per series
  const shown: DerivedColumn[] = [];
  if (!scatter) {
    const named = new Set<string>();
    for (const m of plotted) {
      if (!m.showAs || named.has(measureName(m))) continue;
      named.add(measureName(m));
      shown.push(...showAsColumns(m.showAs, baseName(m), across !== undefined ? [across] : [],
        spec.split !== undefined ? [spec.split] : [], measureName(m), panels));
    }
  }
  // A REFERENCE LINE'S VALUE is the database's: a measure's average, minimum or maximum over every row of the chart,
  // a group-level window over them all (calc-presets.ts statColumn); pinned extremes are the maximum and the minimum
  if (!scatter) {
    const named = new Set(shown.map((d) => d.name));
    const stat = (st: LayerStat, m: ChartMeasure | undefined): void => {
      if (!m) return;
      const d = statColumn(st, measureName(m), panels);
      if (named.has(d.name)) return;
      named.add(d.name);
      shown.push(d);
    };
    for (const l of spec.layers ?? []) {
      if (l.kind === 'stat') stat(l.stat, spec.y[l.measure ?? 0]);
      if (l.kind === 'extremes') {
        stat('max', spec.y[l.measure ?? 0]);
        stat('min', spec.y[l.measure ?? 0]);
      }
    }
  }
  // A KIND'S OWN FIGURES, the database's: a waterfall's bars' starts and ends along the chart's order, a Pareto's
  // cumulative share, a two-ring pie's inner ring (calc-presets.ts)
  const lead = spec.y[0];
  if (!scatter && lead && across !== undefined) {
    const base = measureName(lead);
    if (spec.mark === 'waterfall') shown.push(...waterfallColumns(base, chartOrder(spec, across, base)));
    if (spec.mark === 'pareto') shown.push(...paretoColumns(base, across));
    if (spec.mark === 'pie' && spec.split !== undefined) shown.push(ringTotalColumn(base, across));
    if (spec.mark === 'marimekko' && spec.split !== undefined) shown.push(...marimekkoColumns(base, across, spec.split, spec.options.sort.direction));
    if (spec.mark === 'waffle') shown.push(...waffleColumns(base, chartOrder(spec, across, base)));
  }
  const rowCalcs = [...axisCalcs(spec), ...(spec.calcs ?? []).filter((c) => c.stage === 'row').map((c) => c.column)];
  // what the chart reads, and what the cube's filter reads (its literals are
  // written by their columns' types, so those columns must stay typed)
  const reads = new Set([
    ...keys,
    ...aggregated.map((m) => m.column),
    // a scatter plots rows: its columns as they are, a bubble's size and a hover's measures among them
    ...(scatter ? plotted.map((m) => m.column) : []),
    ...(spec.x !== undefined ? [spec.x] : []),
    ...keptMeasures.flatMap((m) => [m.column, ...(m.weight !== undefined ? [m.weight] : [])]),
    ...rowCalcs.flatMap(columnsRead),
    ...filterColumns(filter),
  ]);
  // a box plot's value is its median (BOX_FNS' third)
  const first = spec.mark === 'boxplot' ? spec.y[2] : spec.y[0];
  // the chart's order, ties broken by the category: a waterfall's running total runs along this very order; small
  // multiples panel by panel first
  const sorts = scatter ? [] : [...panels.map((column) => ({ column, direction: 'asc' as const })), ...(spec.options.sort.by === 'y' && first
    ? [{ column: measureName(first), direction: spec.options.sort.direction },
      ...(across !== undefined ? [{ column: across, direction: 'asc' as const }] : [])]
    : across !== undefined ? [{ column: across, direction: spec.options.sort.direction }] : [])];
  const {
    pivotValues: _pv, pivotTotal: _pt, groupDerived: _gd, window: _w,
    leafCount: _lc, childCount: _cc, ...rest
  } = s as CubeSnapshot & Record<string, unknown>;
  // a chart of periods or bins carries each category's end: the most of its rows' (AXIS_UPPER)
  if (!scatter && rangesAcross(spec) && across !== undefined) {
    measures.push({ name: AXIS_UPPER, column: upperName(across), fn: 'max' });
  }
  // every measure under its own name, two of one column too -- the chart's max beside the sum a margin reads: the
  // grouped level keeps each (query.ts groupedMeasures; it kept a column's last alone, and a copy of the column,
  // `__chart_<name>`, stood in for each other one until 2026-10-16)
  const allMeasures = [...measures, ...keptMeasures];
  return {
    ...(rest as CubeSnapshot),
    columns: s.columns.filter((c) => reads.has(c.name)),
    derived: [...s.derived, ...rowCalcs],
    ...(filter ? { filter } : {}),
    rows: scatter ? [] : keys,
    pivotOn: [],
    measures: allMeasures,
    ...(keptGroup.length + shown.length > 0 ? { groupDerived: [...keptGroup, ...shown] } : {}),
    sorts,
  };
}

/** The most small charts a facet draws: past it, too small to read. */
export const MAX_FACETS = 12;

/** A facet's columns, its rows' then its columns'. */
export function facetColumns(spec: ChartSpec): string[] {
  return [spec.facet?.row, spec.facet?.column].filter((c): c is string => c !== undefined);
}

/** The chart's order as a window's: by the category, or by the value with ties broken by the category. */
function chartOrder(spec: ChartSpec, across: string, value: string): SortSpec[] {
  const { by, direction } = spec.options.sort;
  return by === 'x' ? [{ column: across, direction }] : [{ column: value, direction }, { column: across, direction: 'asc' }];
}

/** Every column a filter tree names. */
function filterColumns(node: FilterNode | undefined): string[] {
  if (!node) return [];
  switch (node.kind) {
    case 'condition':
      return [node.column, ...(node.rightColumn !== undefined ? [node.rightColumn] : [])];
    case 'not':
      return filterColumns(node.child);
    default:
      return node.children.flatMap(filterColumns);
  }
}

/** The most points a KPI's trend asks for: more than a year of days. */
export const KPI_TREND_LIMIT = 500;

/**
 * A KPI's queries: its value -- the measure over every row of the cube's query, its filter applied, no grouping:
 * one row -- and, given a column across, its trend: the measure by that column, its LATEST values first (a long series
 * shows where it is now), one past the most it draws, so a trend cut short is known to be.
 */
export function kpiQueries(s: CubeSnapshot, spec: ChartSpec): {
  readonly total: ReturnType<typeof chartQuery>;
  readonly trend?: ReturnType<typeof chartQuery>;
} {
  const { x, split: _split, ...value } = spec;
  const total = chartQuery(s, value);
  if (x === undefined) return { total };
  const trend = chartQuery(s, {
    ...value, x, mark: 'line', options: { ...spec.options, sort: { by: 'x', direction: 'desc' }, limit: KPI_TREND_LIMIT + 1 },
  });
  return { total, trend };
}

/** The chart's query and the scope it runs at. */
export function chartQuery(s: CubeSnapshot, spec: ChartSpec): { query: Lambda; snapshot: QuerySnapshot; scope: LevelScope } {
  const snapshot = chartSnapshot(s, spec);
  // small multiples ask for each panel's rows: the most per panel, times the most panels drawn
  const limit = plotsRows(spec) ? SCATTER_LIMIT : spec.options.limit * (spec.facet ? MAX_FACETS : 1);
  const scope: LevelScope = { level: snapshot.rows.length, parent: [], limit };
  return { query: levelLambda(snapshot, scope), snapshot, scope };
}
