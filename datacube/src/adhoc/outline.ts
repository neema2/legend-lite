// Ad Hoc Analysis mode's outline: the dimensions and their members.
//
// The cube's named dimensions are the hierarchies (Time = year >
// quarter); every other groupable column is a one-generation dimension
// of its own, so any column can be placed on an axis or the POV; the
// Measures dimension's members are the cube's measures.
//
// A member's children, descendants and bottom level are what the
// SOURCE holds: a distinct-keys query per generation, filtered to the
// member, ordered by the database. The cube's own filter applies (a
// member no row carries is not a member); the POV does not (an
// outline does not change with the point of view).

import type { Dimension } from '../dimensions.ts';
import type { ResultTable } from '../result.ts';
import { memberConditions, serialize, type LevelScope } from '../serialize.ts';
import type { Scalar } from '../result.ts';
import {
  rowColumns,
  type CubeSnapshot,
  type FilterCondition,
  type FilterNode,
  type Measure,
} from '../snapshot.ts';
import { groupValue } from '../treeview.ts';
import type { AdHocCube } from './query.ts';
import {
  DEFAULT_OPTIONS,
  initialGrid,
  MEASURES,
  type AdHocGrid,
  type AxisDimension,
  type MemberPath,
  type Outline,
  type ZoomLevel,
} from './state.ts';

/**
 * The outline for a cube: its named dimensions whose columns it has,
 * then every other dimension column on its own, and the measures (the
 * configured ones, else each measure-kind column summed).
 */
export function buildCube(
  snapshot: CubeSnapshot,
  named: readonly Dimension[],
): AdHocCube {
  const columns = rowColumns(snapshot);
  const have = new Set(columns.map((c) => c.name));
  const hierarchies = named.filter((d) => d.columns.length > 0
    && d.columns.every((c) => have.has(c)) && d.name !== MEASURES);
  const used = new Set(hierarchies.flatMap((d) => d.columns));
  const singles = columns
    .filter((c) => c.kind === 'dimension' && !used.has(c.name))
    .map((c) => ({ name: c.name, generations: [c.name] }));
  const measures: Measure[] = snapshot.measures.length > 0
    ? [...snapshot.measures]
    : columns.filter((c) => c.kind === 'measure')
      .map((c) => ({ name: c.name, column: c.name, fn: 'sum' as const }));
  const outline: Outline = {
    dimensions: [
      ...hierarchies.map((d) => ({ name: d.name, generations: [...d.columns] })),
      ...singles,
    ],
    measures: measures.map((m) => m.name),
  };
  return { snapshot, outline, measures };
}

export interface MemberQuery {
  readonly snapshot: CubeSnapshot;
  readonly scope: LevelScope;
  readonly pure: string;
  readonly depth: number;
}

/**
 * The distinct members at generation `depth` under `under`: grouped by
 * the generations down to it, filtered to `under`, ordered by them.
 */
export function memberQuery(
  cube: AdHocCube,
  dimension: string,
  under: MemberPath,
  depth: number,
): MemberQuery {
  const d = cube.outline.dimensions.find((x) => x.name === dimension);
  if (!d) throw new Error(`no dimension '${dimension}' in the outline`);
  const keys = d.generations.slice(0, depth);
  const filters: FilterNode[] = [
    ...(cube.snapshot.filter ? [cube.snapshot.filter] : []),
    ...memberConditions(cube.snapshot, d.generations, under),
  ];
  const filter: FilterNode | undefined = filters.length === 0 ? undefined
    : filters.length === 1 ? filters[0] : { kind: 'and', children: filters };
  const { filter: _old, ...rest } = cube.snapshot;
  void _old;
  const snapshot: CubeSnapshot = {
    ...rest,
    ...(filter ? { filter } : {}),
    columns: cube.snapshot.columns.filter((c) => keys.includes(c.name)),
    rows: keys,
    pivotOn: [],
    measures: [],
    sorts: keys.map((column) => ({ column, direction: 'asc' as const })),
    groupDerived: [],
    leafCount: false,
  };
  const scope: LevelScope = { level: keys.length, parent: [] };
  return { snapshot, scope, pure: serialize(snapshot, scope), depth };
}

/** The member paths a member query answered. */
export function membersFrom(result: ResultTable, depth: number): MemberPath[] {
  const out: MemberPath[] = [];
  for (let r = 0; r < result.rowCount; r++) {
    const path: string[] = [];
    for (let g = 0; g < depth; g++) path.push(groupValue(result.columns[g]?.values[r] ?? null));
    out.push(path);
  }
  return out;
}

/** The generations a zoom level asks for, under a member at `from`. */
export function zoomDepths(level: ZoomLevel, from: number, deepest: number): number[] {
  if (from >= deepest) return [];
  if (level === 'next') return [from + 1];
  if (level === 'bottom') return [deepest];
  return Array.from({ length: deepest - from }, (_v, i) => from + 1 + i);
}

/**
 * Members from several generations in hierarchy order: each member
 * followed by its descendants (ancestor position TOP), or preceded by
 * them (BOTTOM).
 */
export function inHierarchyOrder(
  members: readonly MemberPath[],
  ancestor: 'top' | 'bottom',
): MemberPath[] {
  const byParent = new Map<string, MemberPath[]>();
  const key = (p: MemberPath): string => p.join('\u0000');
  const depths = members.map((m) => m.length);
  const top = Math.min(...depths);
  for (const m of members) {
    // A root is nobody's child -- and the top member's "parent" would
    // be itself, which walked forever.
    if (m.length === top) continue;
    const parent = key(m.slice(0, -1));
    byParent.set(parent, [...(byParent.get(parent) ?? []), m]);
  }
  const out: MemberPath[] = [];
  const walk = (m: MemberPath): void => {
    const kids = byParent.get(key(m)) ?? [];
    if (ancestor === 'top') out.push(m);
    for (const k of kids) walk(k);
    if (ancestor === 'bottom') out.push(m);
  };
  for (const m of members.filter((x) => x.length === top)) walk(m);
  return out;
}

/** The conditions a filter ANDs together at its top, or null when it is not a plain AND. */
function conjuncts(filter: FilterNode | undefined): FilterNode[] | null {
  if (!filter) return [];
  if (filter.kind === 'condition') return [filter];
  if (filter.kind === 'and') return [...filter.children];
  return null;
}

/**
 * Open the mode ON THE CUBE AS IT STANDS: its row groups down the rows,
 * its column pivots across (after the measures), and a filter that pins
 * a member -- `year == 2021`, or `year == 2021 && quarter == 'Q1'` down
 * a hierarchy -- as that dimension's member: on the POV, or the member
 * shown when the dimension is on an axis. Such a condition leaves the
 * cube's filter, so choosing another POV member is not ANDed against
 * the old one; everything else in the filter stays and applies to
 * every query, as it did.
 */
export function carryOver(
  snapshot: CubeSnapshot,
  named: readonly Dimension[],
  options = DEFAULT_OPTIONS,
): { cube: AdHocCube; grid: AdHocGrid } {
  const outline = buildCube(snapshot, named).outline;
  const all = conjuncts(snapshot.filter);
  const used = new Set<FilterNode>();
  const pinned = new Map<string, MemberPath>();
  for (const d of outline.dimensions) {
    const path: string[] = [];
    for (const column of d.generations) {
      const c = (all ?? []).find((x): x is FilterCondition => x.kind === 'condition'
        && x.column === column && x.operator === 'equal' && !used.has(x)
        && (typeof x.value === 'string' || typeof x.value === 'number'
          || typeof x.value === 'boolean' || x.value instanceof Date));
      if (!c) break;
      path.push(groupValue(c.value as Scalar));
      used.add(c);
    }
    if (path.length > 0) pinned.set(d.name, path);
  }
  const rest = (all ?? []).filter((x) => !used.has(x));
  const { filter: _old, ...unfiltered } = snapshot;
  void _old;
  const filter: FilterNode | undefined = all === null ? snapshot.filter
    : rest.length === 0 ? undefined
      : rest.length === 1 ? rest[0] : { kind: 'and', children: rest };
  const cube = buildCube({ ...unfiltered, ...(filter ? { filter } : {}) }, named);

  const dimensionOf = (column: string): string | undefined =>
    cube.outline.dimensions.find((d) => d.generations.includes(column))?.name;
  const onAxis = (columns: readonly string[], taken: ReadonlySet<string>): string[] => [
    ...new Set(columns.map(dimensionOf).filter((d): d is string => d !== undefined && !taken.has(d))),
  ];
  const rowDims = onAxis(snapshot.rows, new Set());
  const colDims = onAxis(snapshot.pivotOn, new Set(rowDims));
  const axis = (d: string): AxisDimension => ({ dimension: d, members: [pinned.get(d) ?? []] });
  const opening = initialGrid(cube.outline, options);
  const rows = (rowDims.length > 0 ? rowDims : opening.rows.map((a) => a.dimension)).map(axis);
  const columns = [...opening.columns, ...colDims.map(axis)];
  const placed = new Set([...rows, ...columns].map((a) => a.dimension));
  const pov: Record<string, MemberPath> = {};
  for (const d of cube.outline.dimensions) {
    if (!placed.has(d.name)) pov[d.name] = pinned.get(d.name) ?? [];
  }
  return { cube, grid: { rows, columns, pov, options } };
}
