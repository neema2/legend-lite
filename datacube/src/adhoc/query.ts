// Ad Hoc Analysis mode's queries, and the grid they answer.
//
// An ad hoc grid mixes generations on one axis (2021, then 2021's
// quarters, then 2022), so it is not one groupBy. Every cell belongs to
// a SHAPE -- which generation each on-grid dimension's member is at --
// and each shape is one query: grouped by the columns down to those
// generations, filtered to the members shown and to the POV. The
// database aggregates, as every other query this product sends; this
// file only chooses the queries and places their answers.
//
// The queries go through the product's own serialiser, so the planner
// compiles them like any other cube query, and a shape the planner
// refuses is refused the same way.

import { PIVOT_SEPARATOR } from '../generated/lite-facts.ts';
import type { ResultColumn, ResultTable, Scalar } from '../result.ts';
import {
  memberConditions,
  NULL_GROUP,
  serialize,
  type LevelScope,
} from '../serialize.ts';
import type { CubeSnapshot, FilterNode, Measure } from '../snapshot.ts';
import { groupValue } from '../treeview.ts';
import {
  MEASURES,
  tuples,
  type AxisDimension,
  type AdHocGrid,
  type MemberPath,
  type Outline,
  type OutlineDimension,
} from './state.ts';

/** What the mode queries: the cube's source and measures, and its outline. */
export interface AdHocCube {
  /** Source, columns, calculated columns and the cube's own filter. */
  readonly snapshot: CubeSnapshot;
  readonly outline: Outline;
  /** The Measures dimension's members, as aggregates. */
  readonly measures: readonly Measure[];
}

export interface AdHocQuery {
  /** The shape: each on-grid dimension's generation, as a stable key. */
  readonly key: string;
  readonly groupColumns: readonly string[];
  readonly measures: readonly string[];
  /** The cube the query was serialised from, as the planner is handed it. */
  readonly snapshot: CubeSnapshot;
  /** The level it was serialised at; absent for a shape with no grouping. */
  readonly scope?: LevelScope;
  readonly pure: string;
}

const KEY_SEP = '\u0000';

function regularOnGrid(grid: AdHocGrid): AxisDimension[] {
  return [...grid.rows, ...grid.columns].filter((a) => a.dimension !== MEASURES);
}

function outlineOf(cube: AdHocCube, name: string): OutlineDimension {
  const d = cube.outline.dimensions.find((x) => x.name === name);
  if (!d) throw new Error(`no dimension '${name}' in the outline`);
  return d;
}

/** The measures a cell can ask for: the Measures members shown, or the POV's one. */
function measuresInPlay(cube: AdHocCube, grid: AdHocGrid): string[] {
  const onGrid = [...grid.rows, ...grid.columns].find((a) => a.dimension === MEASURES);
  if (onGrid) {
    return [...new Set(onGrid.members.filter((m) => m.length === 1).map((m) => m[0] as string))];
  }
  const pinned = grid.pov[MEASURES];
  const name = pinned?.[0] ?? cube.outline.measures[0];
  return name === undefined ? [] : [name];
}

/** One condition node from several, or none. */
function and(nodes: readonly FilterNode[]): FilterNode | undefined {
  if (nodes.length === 0) return undefined;
  return nodes.length === 1 ? nodes[0] : { kind: 'and', children: nodes };
}

/**
 * The queries a grid needs, one per shape. A shape where a measure has
 * nothing to aggregate (no measure in play) needs none.
 */
export function planQueries(cube: AdHocCube, grid: AdHocGrid): AdHocQuery[] {
  const measures = measuresInPlay(cube, grid);
  if (measures.length === 0) return [];
  const specs = measures.map((m) => {
    const spec = cube.measures.find((x) => x.name === m);
    if (!spec) throw new Error(`no measure '${m}' in the cube`);
    return spec;
  });
  const onGrid = regularOnGrid(grid);

  // The POV pins every off-grid dimension to its member, in every query.
  const pov: FilterNode[] = [];
  for (const [name, path] of Object.entries(grid.pov)) {
    if (name === MEASURES || path.length === 0) continue;
    pov.push(...memberConditions(cube.snapshot, outlineOf(cube, name).generations, path));
  }

  // Each on-grid dimension's generations in play, then every combination.
  const levels = onGrid.map((a) => [...new Set(a.members.map((m) => m.length))]
    .sort((x, y) => x - y));
  let shapes: number[][] = [[]];
  for (const options of levels) {
    shapes = shapes.flatMap((prefix) => options.map((l) => [...prefix, l]));
  }

  return shapes.map((shape) => {
    const groupColumns: string[] = [];
    const filters: FilterNode[] = [...(cube.snapshot.filter ? [cube.snapshot.filter] : []), ...pov];
    onGrid.forEach((a, i) => {
      const level = shape[i] as number;
      if (level === 0) return;
      const generations = outlineOf(cube, a.dimension).generations;
      groupColumns.push(...generations.slice(0, level));
      // Only the members shown at this generation.
      const shown = a.members.filter((m) => m.length === level);
      const each = shown.map((m) => and(memberConditions(cube.snapshot, generations, m)) as FilterNode);
      if (each.length > 0) filters.push(each.length === 1 ? each[0] as FilterNode : { kind: 'or', children: each });
    });
    const needed = new Set([...groupColumns, ...specs.map((m) => m.column),
      ...specs.flatMap((m) => (m.weight ? [m.weight] : []))]);
    const filter = and(filters);
    const snapshot: CubeSnapshot = {
      ...cube.snapshot,
      columns: cube.snapshot.columns.filter((c) => needed.has(c.name)),
      rows: groupColumns,
      pivotOn: [],
      measures: specs,
      sorts: [],
      groupDerived: [],
      leafCount: false,
      ...(filter ? { filter } : {}),
    };
    if (!filter) delete (snapshot as { filter?: FilterNode }).filter;
    const scope: LevelScope | undefined = groupColumns.length > 0
      ? { level: groupColumns.length, parent: [] } : undefined;
    const pure = serialize(snapshot, scope);
    return { key: shape.join(','), groupColumns, measures, snapshot,
      ...(scope ? { scope } : {}), pure };
  });
}

// -- the grid the answers make ----------------------------------------------

/** A member's own label: its last value, or its dimension's name at the top. */
export function memberLabel(dimension: string, path: MemberPath): string {
  if (path.length === 0) return dimension;
  const last = path[path.length - 1] as string;
  return last === NULL_GROUP ? '(blank)' : last;
}

/** A header segment: unique per member, so two Q1s never merge. */
const TOP = '\u0002';
function segment(dimension: string, path: MemberPath): string {
  return path.length === 0 ? `${TOP}${dimension}` : path.join('\u0001');
}

/** What a header segment shows. */
export function segmentLabel(s: string): string {
  if (s.startsWith(TOP)) return s.slice(TOP.length);
  const last = s.split('\u0001').pop() ?? s;
  return last === NULL_GROUP ? '(blank)' : last;
}

export interface AdHocView {
  /** Row label columns (one per row dimension), then one value column per column tuple. */
  readonly table: ResultTable;
  /** The row dimensions' names: the table's first columns. */
  readonly rowDimensions: readonly string[];
  /** Each displayed row's members, one per row dimension. */
  readonly rowTuples: readonly (readonly MemberPath[])[];
  /** Each value column's members, one per column dimension. */
  readonly columnTuples: readonly (readonly MemberPath[])[];
  /** The last column dimension's segments -- what the column model splits names on. */
  readonly lastSegments: readonly string[];
}

/**
 * Place the answers. Every (row, column) cell finds its shape's result
 * by its members' values, and reads its measure there; a cell no result
 * has is missing (null). Then the ad hoc suppression options drop
 * rows and columns that are all missing or all zero, and repeated outer
 * members are blanked.
 */
export function assembleGrid(
  cube: AdHocCube,
  grid: AdHocGrid,
  results: ReadonlyMap<string, ResultTable>,
  queries: readonly AdHocQuery[],
): AdHocView {
  const onGrid = regularOnGrid(grid).map((a) => a.dimension);
  const byKey = new Map(queries.map((q) => [q.key, q]));
  // Each result indexed by its group key.
  const index = new Map<string, Map<string, number>>();
  for (const q of queries) {
    const t = results.get(q.key);
    if (!t) continue;
    const at = new Map<string, number>();
    for (let r = 0; r < t.rowCount; r++) {
      at.set(q.groupColumns.map((_c, i) => groupValue(t.columns[i]?.values[r] ?? null)).join(KEY_SEP), r);
    }
    index.set(q.key, at);
  }
  const povMeasure = grid.pov[MEASURES]?.[0] ?? cube.outline.measures[0];

  const rowTuples = tuples(grid.rows);
  const columnTuples = tuples(grid.columns);
  const valueOf = (row: readonly MemberPath[], col: readonly MemberPath[]): Scalar => {
    const member = (dimension: string): MemberPath => {
      const r = grid.rows.findIndex((a) => a.dimension === dimension);
      if (r >= 0) return row[r] as MemberPath;
      const c = grid.columns.findIndex((a) => a.dimension === dimension);
      return c >= 0 ? col[c] as MemberPath : [];
    };
    const measurePath = [...grid.rows, ...grid.columns].some((a) => a.dimension === MEASURES)
      ? member(MEASURES) : povMeasure === undefined ? [] : [povMeasure];
    const measure = measurePath[0];
    if (measure === undefined) return null;
    const paths = onGrid.map(member);
    const shape = paths.map((p) => p.length).join(',');
    const q = byKey.get(shape);
    const t = results.get(shape);
    const r = index.get(shape)?.get(paths.flat().join(KEY_SEP));
    if (!q || !t || r === undefined) return null;
    const column = t.columns.find((c) => c.name === measure);
    return column?.values[r] ?? null;
  };

  // Every cell, then what suppression keeps.
  const cells = rowTuples.map((row) => columnTuples.map((col) => valueOf(row, col)));
  const o = grid.options;
  const empty = (v: Scalar): boolean => v === null;
  const zero = (v: Scalar): boolean => v === 0;
  const keepRow = (vals: readonly Scalar[]): boolean =>
    !(o.suppressMissingRows && vals.every(empty))
    && !(o.suppressZeroRows && vals.every((v) => empty(v) || zero(v)) && vals.some(zero));
  const colVals = (c: number): Scalar[] => cells.map((row) => row[c] ?? null);
  const keepCol = (c: number): boolean =>
    !(o.suppressMissingColumns && colVals(c).every(empty))
    && !(o.suppressZeroColumns && colVals(c).every((v) => empty(v) || zero(v)) && colVals(c).some(zero));
  const rowsKept = rowTuples.map((_r, i) => i).filter((i) => keepRow(cells[i] ?? []));
  const colsKept = columnTuples.map((_c, i) => i).filter(keepCol);

  // Row labels, indented by generation, repeated outer members blanked.
  const rowDimensions = grid.rows.map((a) => a.dimension);
  const labelColumns: ResultColumn[] = rowDimensions.map((dimension, d) => ({
    name: dimension,
    type: 'String',
    values: rowsKept.map((ri, k) => {
      const path = rowTuples[ri]?.[d] as MemberPath;
      if (o.suppressRepeatedMembers && d < rowDimensions.length - 1 && k > 0) {
        const prev = rowTuples[rowsKept[k - 1] as number] as readonly MemberPath[];
        const here = rowTuples[ri] as readonly MemberPath[];
        const sameOuter = here.slice(0, d + 1).every((p, j) =>
          (p as MemberPath).join(KEY_SEP) === (prev[j] as MemberPath).join(KEY_SEP));
        if (sameOuter) return null;
      }
      const depth = o.indentation === 'none' ? 0
        : o.indentation === 'subitems' ? path.length
          : Math.max(0, 3 - path.length);
      return `${' '.repeat(depth)}${memberLabel(dimension, path)}`;
    }),
  }));

  const columnDims = grid.columns.map((a) => a.dimension);
  const valueColumns: ResultColumn[] = colsKept.map((ci) => {
    const tuple = columnTuples[ci] as readonly MemberPath[];
    return {
      name: tuple.map((p, d) => segment(columnDims[d] as string, p)).join(PIVOT_SEPARATOR),
      type: 'Float',
      values: rowsKept.map((ri) => cells[ri]?.[ci] ?? null),
    };
  });
  const lastSegments = [...new Set(colsKept.map((ci) => {
    const tuple = columnTuples[ci] as readonly MemberPath[];
    const d = tuple.length - 1;
    return segment(columnDims[d] as string, tuple[d] as MemberPath);
  }))];

  return {
    table: {
      columns: [...labelColumns, ...valueColumns],
      rowCount: rowsKept.length,
      epoch: 0,
      elapsedMs: [...results.values()].reduce((n, t) => n + t.elapsedMs, 0),
    },
    rowDimensions,
    rowTuples: rowsKept.map((i) => rowTuples[i] as readonly MemberPath[]),
    columnTuples: colsKept.map((i) => columnTuples[i] as readonly MemberPath[]),
    lastSegments,
  };
}
