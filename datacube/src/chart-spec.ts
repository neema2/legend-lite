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

import { levelLambda, type LevelScope } from './query.ts';
import {
  rowColumns,
  type AggregateFn,
  type CubeSnapshot,
  type FilterNode,
  type Measure,
  type RowColumn,
} from './snapshot.ts';
import { isFractional, isNumeric, isTemporal } from '../../engine-client/src/types.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';

export type ChartMark = 'bar' | 'line' | 'area' | 'scatter' | 'pie' | 'heatmap' | 'treemap';

/** One measure the chart plots: a column and how its rows are aggregated. */
export interface ChartMeasure {
  readonly column: string;
  readonly fn: AggregateFn;
}

export interface ChartSpec {
  readonly version: 1;
  readonly mark: ChartMark;
  /** The category (bar, line, area, pie, heatmap columns) or the x value (scatter). */
  readonly x?: string;
  /** What is plotted. Scatter reads the first as its y; heatmap and pie the first as the value. */
  readonly y: readonly ChartMeasure[];
  /** A second dimension: one series per value (bar, line, area), the rows of a heatmap. */
  readonly split?: string;
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
}

export const CHART_MARKS: readonly { value: ChartMark; label: string }[] = [
  { value: 'bar', label: 'Bar' },
  { value: 'line', label: 'Line' },
  { value: 'area', label: 'Area' },
  { value: 'scatter', label: 'Scatter' },
  { value: 'pie', label: 'Pie' },
  { value: 'heatmap', label: 'Heatmap' },
  { value: 'treemap', label: 'Treemap' },
];

export const DEFAULT_CHART_LIMIT = 50;
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
  return m.fn === 'count' ? 'count' : `${m.fn}_${m.column}`;
}

/** The dimensions a chart can use, and the columns it can measure. */
export function chartColumns(s: CubeSnapshot): { dimensions: RowColumn[]; measures: RowColumn[] } {
  const all = rowColumns(s);
  return {
    dimensions: all.filter((c) => c.kind === 'dimension'),
    measures: all.filter((c) => isNumeric(c.type)),
  };
}

/**
 * A first chart for a cube, from what the cube already says: its first row
 * dimension (else the first dimension) across, its first measure (else the
 * first number, summed) up, and its second row dimension as the split. A
 * date across is a line; anything else a bar.
 */
export function defaultChart(s: CubeSnapshot): ChartSpec | null {
  const { dimensions, measures } = chartColumns(s);
  const byName = new Map(rowColumns(s).map((c) => [c.name, c]));
  const x = s.rows[0] ?? dimensions[0]?.name;
  const split = s.rows[1];
  // the cube's own measure; else a column it treats as a measure, a fraction
  // first (money, a quantity) -- every number is a measure by default (D3),
  // and an id or a year summed is the worst first chart; else a count
  const configured = s.measures.find((m) => m.fn !== 'wavg');
  const asMeasures = measures.filter((c) => c.kind === 'measure');
  const measureKind = asMeasures.find((c) => isFractional(c.type)) ?? asMeasures[0];
  const y: ChartMeasure | undefined = configured
    ? { column: configured.column, fn: configured.fn }
    : measureKind ? { column: measureKind.name, fn: 'sum' } : undefined;
  if (x === undefined) return null;
  const temporal = isTemporal(byName.get(x)?.type);
  return {
    version: 1,
    mark: temporal ? 'line' : 'bar',
    x,
    y: y ? [y] : [{ column: x, fn: 'count' }],
    ...(split !== undefined ? { split } : {}),
    options: {
      ...DEFAULT_OPTIONS,
      // time runs left to right; categories lead with the largest
      sort: temporal ? { by: 'x', direction: 'asc' } : DEFAULT_OPTIONS.sort,
    },
  };
}

/**
 * A live chart, regrouped the way the cube is now: its first row group
 * across; its second row group -- else its first column label -- as the
 * split; the cube's own measure plotted, when it has one. The mark and the
 * options are the chart's; the order follows what is across (a date runs
 * in time order, anything else leads with the largest). A frozen chart,
 * a scatter (it plots values, not groups) and a cube with nothing grouped
 * are left as they are.
 */
export function followCube(spec: ChartSpec, s: CubeSnapshot): ChartSpec {
  if (spec.frozen || spec.mark === 'scatter') return spec;
  const x = s.rows[0] ?? s.pivotOn[0];
  if (x === undefined) return spec;
  const split = s.rows[0] !== undefined ? (s.rows[1] ?? s.pivotOn[0]) : s.pivotOn[1];
  const configured = s.measures.find((m) => m.fn !== 'wavg');
  const y = configured ? [{ column: configured.column, fn: configured.fn }] : spec.y;
  const { split: _old, ...rest } = spec;
  const sort = x === spec.x ? spec.options.sort
    : isTemporal(rowColumns(s).find((c) => c.name === x)?.type)
      ? { by: 'x' as const, direction: 'asc' as const }
      : { by: 'y' as const, direction: 'desc' as const };
  const next: ChartSpec = {
    ...rest,
    x,
    y,
    ...(split !== undefined ? { split } : {}),
    options: { ...spec.options, sort },
  };
  return JSON.stringify(next) === JSON.stringify(spec) ? spec : next;
}

/** What stops a spec from being drawn, in words; empty when it can be. */
export function chartProblems(spec: ChartSpec, s: CubeSnapshot): string[] {
  const known = new Set(rowColumns(s).map((c) => c.name));
  const out: string[] = [];
  if (spec.x === undefined) out.push('Pick a column across.');
  else if (!known.has(spec.x)) out.push(`The cube has no column '${spec.x}'.`);
  if (spec.y.length === 0) out.push('Pick something to plot.');
  for (const m of spec.y) {
    if (m.fn !== 'count' && !known.has(m.column)) out.push(`The cube has no column '${m.column}'.`);
  }
  if (spec.split !== undefined && !known.has(spec.split)) {
    out.push(`The cube has no column '${spec.split}'.`);
  }
  if (spec.mark === 'heatmap' && spec.split === undefined) {
    out.push('A heatmap needs a second column, for its rows.');
  }
  if (spec.split !== undefined && spec.split === spec.x) {
    out.push('Split by a different column than the one across.');
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
export function chartSnapshot(s: CubeSnapshot, spec: ChartSpec): CubeSnapshot {
  const scatter = spec.mark === 'scatter';
  const keys = [spec.x, spec.split].filter((c): c is string => c !== undefined);
  const measures: Measure[] = scatter ? [] : spec.y.map((m) => ({
    name: measureName(m),
    column: m.column,
    fn: m.fn,
  }));
  // what the chart reads, and what the cube's filter reads (its literals are
  // written by their columns' types, so those columns must stay typed)
  const reads = new Set([...keys, ...spec.y.map((m) => m.column), ...filterColumns(s.filter)]);
  const first = measures[0];
  const sorts = scatter ? [] : spec.options.sort.by === 'y' && first
    ? [{ column: first.name, direction: spec.options.sort.direction }]
    : spec.x !== undefined ? [{ column: spec.x, direction: spec.options.sort.direction }] : [];
  const {
    pivotValues: _pv, pivotTotal: _pt, groupDerived: _gd, window: _w,
    leafCount: _lc, childCount: _cc, ...rest
  } = s as CubeSnapshot & Record<string, unknown>;
  return {
    ...(rest as CubeSnapshot),
    columns: s.columns.filter((c) => reads.has(c.name)),
    rows: scatter ? [] : keys,
    pivotOn: [],
    measures,
    sorts,
  };
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

/** The chart's query and the scope it runs at. */
export function chartQuery(s: CubeSnapshot, spec: ChartSpec): { query: Lambda; snapshot: CubeSnapshot; scope: LevelScope } {
  const snapshot = chartSnapshot(s, spec);
  const limit = spec.mark === 'scatter' ? SCATTER_LIMIT : spec.options.limit;
  const scope: LevelScope = { level: snapshot.rows.length, parent: [], limit };
  return { query: levelLambda(snapshot, scope), snapshot, scope };
}
