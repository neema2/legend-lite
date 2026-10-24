// A GRID EDITED AT RANDOM, for the property tests (test/cube-round-trip.test.ts: written, read and opened again it comes
// back as it was; test/grid-between.test.ts: the commands between two of its states take one to the other): every kind
// of grid command the screen and Python build (grid-commands.ts), picked at random over a grid of every kind of column a
// person meets, seeded (mulberry32) so a failure names its seed and reproduces from it alone. A refused edit
// (CubeRefusal) is the screen's own refusal: skipped, and it must leave the grid as it was.

import assert from 'node:assert/strict';

import { element, toJson } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../src/config.ts';
import { difference } from '../src/cube-document.ts';
import { applyGridCommand, readGridCommand, type GridCommand, type SettingsPatch } from '../src/grid-commands.ts';
import { allColumns, firstSortDirection, groupableColumns, type GridDraft } from '../src/grid-edits.ts';
import { offeredAggregates } from '../src/grid-edits.ts';
import {
  CubeRefusal, pivotTotalColumn, rowColumns, type CubeSnapshot, type DerivedColumn, type FilterNode, type SortDirection,
} from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { stand } from './fake-planner.ts';

/** A grid of every kind of column a person meets: text, numbers of two types, a date, calculated ones at each level. */
export const CUBE: CubeSnapshot = {
  source: { query: element('t') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'traded', type: 'StrictDate' },
    { name: 'notional', type: 'Float' },
    { name: 'qty', type: 'Integer' },
  ],
  derived: [
    { name: 'big', lambda: stand('$x.notional > 1'), kind: 'dimension', type: 'Boolean' },
    { name: 'fee', lambda: stand('$x.notional * 2'), kind: 'measure', type: 'Float' },
  ],
  groupDerived: [{ name: 'share', lambda: stand('$x.notional / 2'), type: 'Float' }],
  rows: [],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

/** A seeded generator: the same seed, the same grid. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A grid as the screen holds one: its draft and its open groups (what every command edits). */
export type Walked = GridDraft & { readonly tree: TreeState };

/** The grid every walk starts from: the cube, the product's settings, nothing open. */
export const start = (): Walked => ({ snapshot: CUBE, config: DEFAULT_CONFIGURATION, tree: TreeState.empty() });

const FILTERS: readonly FilterNode[] = [
  { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
  { kind: 'condition', column: 'region', operator: 'notEqual', value: 'APAC' },
  { kind: 'condition', column: 'traded', operator: 'greaterThan', value: '2024-02-01' },
  { kind: 'condition', column: 'qty', operator: 'lessThanEqual', value: 5 },
];
const PATHS: readonly (readonly string[])[] = [['EMEA'], ['AMER'], ['EMEA', 'Rates'], ['AMER', 'FX']];

/** One edit a person (or Python) could make, picked at random, and its name for a failure's message. */
export function randomEdit(rand: () => number, d: Walked): [string, GridCommand] {
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)] as T;
  const columns = allColumns(d);
  const type = (name: string): string | undefined => rowColumns(d.snapshot).find((c) => c.name === name)?.type;
  const sortable = [...columns, ...(d.snapshot.pivotOn.length > 0 ? [pivotTotalColumn('notional')] : [])];
  const calculated = [...d.snapshot.derived, ...(d.snapshot.groupDerived ?? [])];
  const conditions = d.snapshot.filter === undefined ? [] : d.snapshot.filter.kind === 'and' ? d.snapshot.filter.children : [d.snapshot.filter];
  const choices: [string, () => [string, GridCommand]][] = [
    ['group', () => {
      const offered = groupableColumns(d);
      const rows = offered.filter(() => rand() < 0.35).slice(0, 2);
      return [`group ${rows.join(',')}`, { kind: 'group', rows }];
    }],
    ['pivot', () => {
      const offered = groupableColumns(d).filter((c) => !d.snapshot.rows.includes(c));
      const on = rand() < 0.4 ? [] : offered.filter(() => rand() < 0.3).slice(0, 1);
      return [`pivot ${on.join(',')}`, { kind: 'pivot', columns: on }];
    }],
    ['click a header', () => {
      const column = pick(sortable);
      const chained = rand() < 0.5;
      // the first direction as the screen reads it off the header's type
      return [`click ${column}${chained ? ' chained' : ''}`,
        { kind: 'clickSort', column, first: firstSortDirection(type(column)), chained }];
    }],
    ['click the tree', () => {
      const chained = rand() < 0.5;
      return [`click the tree${chained ? ' chained' : ''}`, { kind: 'clickTreeSort', chained }];
    }],
    ['add a sort', () => {
      const column = pick(sortable);
      const direction: SortDirection = rand() < 0.5 ? 'asc' : 'desc';
      return [`add sort ${column} ${direction}`, { kind: 'addSort', column, direction }];
    }],
    ['the sorts', () => {
      const sorts = [...new Set(sortable.filter(() => rand() < 0.3).slice(0, 3))]
        .map((column) => ({ column, direction: (rand() < 0.5 ? 'asc' : 'desc') as SortDirection }));
      return [`sorts ${sorts.map((s) => `${s.column} ${s.direction}`).join(',')}`, { kind: 'sorts', sorts }];
    }],
    ['clear a sort', () => {
      const column = d.snapshot.sorts.length > 0 ? pick(d.snapshot.sorts).column : pick(sortable);
      return [`clear sort ${column}`, { kind: 'clearSort', column }];
    }],
    ['an Aggregation', () => {
      const column = pick(columns);
      const fn = pick([...offeredAggregates(type(column)), null]);
      const weight = fn === 'wavg' ? { weight: 'qty' } : {};
      return [`aggregate ${column} ${String(fn)}`, { kind: 'column', name: column, change: { aggregate: fn, ...weight } }];
    }],
    ['a kind', () => {
      const column = pick(CUBE.columns.map((c) => c.name));
      const kind = rand() < 0.5 ? 'dimension' : 'measure';
      return [`kind ${column} ${kind}`, { kind: 'column', name: column, change: { kind } }];
    }],
    ['a filter', () => {
      const filter = rand() < 0.2 ? null : rand() < 0.5 ? pick(FILTERS)
        : { kind: 'and' as const, children: [pick(FILTERS), pick(FILTERS)] };
      return [`filter ${JSON.stringify(filter)}`, { kind: 'filter', filter }];
    }],
    ['a filter condition added', () => {
      const condition = pick(FILTERS);
      return [`add filter ${JSON.stringify(condition)}`, { kind: 'addFilter', condition }];
    }],
    ["a chart's mark", () => {
      // as a click on a chart's mark refilters its grid: the conditions it had off, the one clicked on
      const remove = conditions.filter(() => rand() < 0.5);
      const add = rand() < 0.8 ? [pick(FILTERS)] : [];
      return [`refilter -${remove.length} +${add.length}`, { kind: 'refilter', remove, add }];
    }],
    ['a calculated column', () => {
      // added, as the column editor adds one (a row-level measure, or a group-level column): a name the grid has is refused
      const name = `calc${Math.floor(rand() * 3)}`;
      return rand() < 0.75
        ? [`add calculated ${name}`,
          { kind: 'calculate', level: 'row', column: { name, lambda: stand('$x.qty * 3'), kind: 'measure', type: 'Integer' } }]
        : [`add group calculated ${name}`, { kind: 'calculate', level: 'group', column: { name, lambda: stand('$x.fee * 3') } }];
    }],
    ['a calculated column edited', () => {
      if (calculated.length === 0) return ['add calculated calc9', { kind: 'calculate', level: 'row', column: { name: 'calc9', lambda: stand('$x.qty'), kind: 'measure', type: 'Integer' } }];
      const column = pick(calculated);
      const level: 'row' | 'group' = rand() < 0.8 ? (d.snapshot.derived.includes(column) ? 'row' : 'group') : rand() < 0.5 ? 'row' : 'group';
      const renamed = rand() < 0.3;
      const name = renamed ? `${column.name}R` : column.name;
      const edited: DerivedColumn = level === 'group'
        ? { name, lambda: stand('$x.notional * 4') }
        : { name, lambda: stand('$x.qty * 5'), kind: rand() < 0.5 ? 'measure' : 'dimension', type: 'Integer' };
      return [`edit calculated ${column.name}${renamed ? ` as ${name}` : ''} at ${level}`,
        { kind: 'editCalculated', name: column.name, level, column: edited }];
    }],
    ['a calculated column removed', () => {
      if (calculated.length === 0) return ['add calculated calc8', { kind: 'calculate', level: 'row', column: { name: 'calc8', lambda: stand('$x.qty'), kind: 'measure', type: 'Integer' } }];
      // one nothing of the grid names (a group, a pivot, a sort or a filter by it would leave the grid naming a column
      // it has not, which no screen lets a person do: the column editor refuses the delete)
      const named = new Set([...d.snapshot.rows, ...d.snapshot.pivotOn, ...d.snapshot.sorts.map((s) => s.column),
        ...conditions.flatMap((c) => (c.kind === 'condition' ? [c.column] : []))]);
      const free = calculated.filter((c) => !named.has(c.name));
      const name = (free.length > 0 ? pick(free) : pick(calculated)).name;
      return [`remove calculated ${name}`, { kind: 'removeCalculated', name }];
    }],
    ['a group opened or closed', () => {
      const path = pick(PATHS);
      const open = rand() < 0.65;
      return [`${open ? 'open' : 'close'} ${path.join('/')}`, { kind: 'open', path, open }];
    }],
    ['collapse all', () => ['collapse all', { kind: 'collapseAll' }]],
    ['a setting', () => {
      const column = pick(columns);
      // as the configure command takes them: null clears a setting
      const patches: [string, SettingsPatch][] = [
        ['heatmap', { columns: { [column]: { heatmap: { from: '#ffffff', to: '#ff8a65' } } } }],
        ['hidden', { columns: { [column]: { hidden: rand() < 0.7 } } }],
        ['width', { columns: { [column]: { width: 40 + Math.floor(rand() * 200) } } }],
        ['totals', { pivotStatisticColumnPlacement: pick(['left', 'right'] as const) }],
        ['no totals', { pivotStatisticColumnPlacement: null }],
        ['row limit', { maxRows: pick([10, 100, 5000]) }],
        ['tree order', { treeColumnSort: pick(['asc', 'desc'] as const) }],
        ['leaf count', { showLeafCount: rand() < 0.5 }],
        ['root row', { showRootAggregation: rand() < 0.5 }],
        ['expand level', { initialExpandToLevel: pick([0, 1, 2]) }],
        ['total heatmap', { columns: { [pivotTotalColumn('notional')]: { heatmap: { from: '#ffffff', to: '#00ff00' } } } }],
        ['pivot exclusion', { columns: { [column]: { excludedFromPivot: rand() < 0.5 } } }],
        ['pivot sort', { columns: { [column]: { pivotSortDirection: pick(['asc', 'desc'] as const) } } }],
        ['leaf count mode', { leafCountMode: pick(['children', 'leaves'] as const) }],
        ['grouped columns kept', { showGroupedColumns: rand() < 0.5 }],
      ];
      const [name, patch] = pick(patches);
      return [`setting ${name}`, { kind: 'configure', patch }];
    }],
  ];
  return pick(choices)[1]();
}

/** A walk of `steps` random edits from `seed`: the grid after each (the first the start), and each edit's name, a
 *  refused one marked and its grid the one before. Each command is also read back from its JSON as the same command. */
export function randomWalk(seed: number, steps: number): { states: Walked[]; steps: string[] } {
  const rand = mulberry32(seed);
  let d = start();
  const states: Walked[] = [d];
  const names: string[] = [];
  for (let i = 0; i < steps; i += 1) {
    const [name, edit] = randomEdit(rand, d);
    // what Python sends is the same command (its JSON, read)
    assert.equal(toJson(readGridCommand(JSON.parse(toJson(edit)))), toJson(edit), `seed ${seed}, step ${i} (${name})`);
    const before = JSON.stringify([d.snapshot, d.config, d.tree.key]);
    try {
      d = applyGridCommand(d, edit);
      names.push(name);
    } catch (e) {
      if (!(e instanceof CubeRefusal)) throw new Error(`seed ${seed}, step ${i} (${name}): ${String(e)}`);
      // the screen's own refusal: said, and the grid left as it was
      assert.equal(JSON.stringify([d.snapshot, d.config, d.tree.key]), before, `seed ${seed}: a refused edit (${name}) changed the grid`);
      names.push(`${name} (refused)`);
    }
    states.push(d);
  }
  return { states, steps: names };
}

/** A grid after `steps` random edits from `seed` (the round-trip tests' shape): its draft, the edits made (the refused
 *  ones marked), and its open groups. */
export function editedGrid(seed: number, steps = 12): { draft: GridDraft; steps: string[]; tree: TreeState } {
  const walk = randomWalk(seed, steps);
  const last = walk.states[walk.states.length - 1]!;
  return { draft: { snapshot: last.snapshot, config: last.config }, steps: walk.steps, tree: last.tree };
}

/**
 * A grid's SAVED FORM, as content (cube-document.ts writeCube: the query without its relation, its epoch, its row
 * window and the types a run learned, which no command sets; the configuration as its differences from the product's;
 * the tree by its key), so two grids compare as the documents they would write -- but for two spellings of one grid:
 * an empty group-level list is none (the commands write `[]` where a fresh grid has nothing, snapshot.ts
 * withCalculated); and a source column's kind and pivot exclusion are read as the grid reads them (grid-edits.ts
 * columnKind: the configuration's, else the query's, else its type's default; an exclusion is a measure's), not as
 * written -- Column Properties writes a kind down only when it changes it (setColumn), so a kind set and set back is
 * written where one never set is not. Two editors applying one command write one text either way; an undo puts back
 * what a person sees.
 */
export function savedForm(d: GridDraft & { readonly tree?: TreeState }): unknown {
  const { source: _relation, epoch: _epoch, window: _window, pivotValues: _pinned, ...query } = d.snapshot;
  const untyped = (columns: readonly { readonly type?: string }[]): unknown[] =>
    columns.map(({ type: _learned, ...c }) => c);
  const { groupDerived, ...rest } = query;
  // each source column's kind and exclusion as the grid reads them, apart from the configuration's spelling
  const read = rowColumns(d.snapshot);
  const kinds = Object.fromEntries(d.snapshot.columns.map((c) => {
    const config = d.config.columns[c.name] ?? {};
    const kind = config.kind ?? read.find((r) => r.name === c.name)?.kind;
    const excluded = kind === 'dimension' ? undefined : config.excludedFromPivot ?? ('excludedFromPivot' in c ? c.excludedFromPivot : undefined) ?? false;
    return [c.name, { kind, ...(excluded !== undefined ? { excluded } : {}) }];
  }));
  const unkinded = (columns: readonly { readonly name: string }[]): unknown[] => columns.map((c) => {
    const { kind: _kind, excludedFromPivot: _excluded, ...spec } = c as { kind?: unknown; excludedFromPivot?: unknown };
    return spec;
  });
  const { columns: written, ...configuration } = (difference(d.config as CubeConfiguration, DEFAULT_CONFIGURATION) ?? {}) as {
    columns?: Record<string, Record<string, unknown>>;
  };
  const columns = Object.fromEntries(Object.entries(written ?? {}).flatMap(([name, c]) => {
    const { kind: _kind, excludedFromPivot: _excluded, ...settings } = c;
    return Object.keys(settings).length > 0 ? [[name, settings]] : [];
  }));
  return JSON.parse(JSON.stringify({
    query: {
      ...rest, columns: unkinded(untyped(query.columns) as { name: string }[]), derived: untyped(query.derived),
      ...(groupDerived !== undefined && groupDerived.length > 0 ? { groupDerived: untyped(groupDerived) } : {}),
    },
    kinds,
    configuration: { ...configuration, ...(Object.keys(columns).length > 0 ? { columns } : {}) },
    ...(d.tree !== undefined ? { tree: d.tree.key } : {}),
  }));
}
