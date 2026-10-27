// SELECT (docs/DATACUBE_FLOW_MODE_2026_10_24.md M1, Alteryx's Select): the columns a box hands on, the names it hands
// them on by, and the types -- said as the grid's own settings and columns: a column left out is hidden; a column
// renamed has a display name of its own, which a flow's box hands on as the relation's `rename` (box-columns.ts,
// join-open.ts inputRows); a column retyped is a calculated column of its conversion (`$x.price->toOne()->parseInteger()`,
// named `price__Integer`), its column hidden and it given the column's name to be handed on by -- so the box after it
// reads `price`, a whole number; the order it hands them on in is its column order (box-columns.ts boxOrder), a retyped
// column's conversion in the column's place. One step of commands; what a box says is read back off them (`retypesOf`,
// box-columns.ts boxRenamed, flow-boxes.ts).
//
// One rule for the box's window and Python's grid.select (headless gridPreset).
//
// PURE: no DOM, no planner.

import { fn, lambda, property, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, plainType, type TypeFamily } from '../../engine-client/src/types.ts';
import { handedOn } from './box-columns.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, type CubeShape, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** What a column can be made: text, a whole number, a fraction, an exact decimal, true or false, a date. */
export type Retype = 'String' | 'Integer' | 'Float' | 'Decimal' | 'Boolean' | 'Date';
export const RETYPES: readonly Retype[] = ['String', 'Integer', 'Float', 'Decimal', 'Boolean', 'Date'];

/** Each conversion's function, by the kind of value a column holds and what it is made: the compiler's own. */
const CONVERSIONS: Readonly<Partial<Record<TypeFamily, Readonly<Partial<Record<Retype, string>>>>>> = {
  text: { Integer: 'parseInteger', Float: 'parseFloat', Decimal: 'parseDecimal', Boolean: 'parseBoolean', Date: 'parseDate' },
  numeric: { String: 'toString', Integer: 'round', Float: 'toFloat', Decimal: 'toDecimal' },
  boolean: { String: 'toString' },
  temporal: { String: 'toString' },
};

/** What is made a number: summed when grouped, as a number column is. */
const NUMBERS: ReadonlySet<Retype> = new Set<Retype>(['Integer', 'Float', 'Decimal']);

/** What each conversion's function makes a column. */
const MADE: ReadonlyMap<string, Retype> = new Map(Object.values(CONVERSIONS).flatMap((by) => Object.entries(by!).map(([t, f]) => [f, t as Retype] as const)));

/** What a column of `type` can be made, its own type aside. */
export function retypesFor(type: string | undefined): Retype[] {
  const by = CONVERSIONS[familyOf(type) ?? 'any'] ?? {};
  const own = type === undefined ? undefined : plainType(type);
  return RETYPES.filter((t) => by[t] !== undefined && t !== own);
}

/** A retyped column's name: its column's, two underscores, and what it is made. */
const retypeName = (column: string, to: Retype): string => `${column}__${to}`;

/** The columns a grid's Select has retyped, read off its calculated columns: a conversion of one column, named for it. */
export function retypesOf(derived: readonly DerivedColumn[]): { readonly name: string; readonly column: string; readonly to: Retype }[] {
  return derived.flatMap((d) => {
    const param = d.lambda?.parameters[0]?.name;
    const body = d.lambda?.body;
    if (param === undefined || body?.length !== 1 || d.window !== undefined || d.unnest === true) return [];
    const call = body[0]!;
    const to = call._type === 'func' && call.parameters.length === 1 ? MADE.get(call.function) : undefined;
    const one = call._type === 'func' ? call.parameters[0] : undefined;
    const p = one?._type === 'func' && one.function === 'toOne' && one.parameters.length === 1 ? one.parameters[0] : undefined;
    const column = p?._type === 'property' && p.parameters[0]?._type === 'var' && (p.parameters[0] as { name: string }).name === param
      ? p.property : undefined;
    return to !== undefined && column !== undefined && d.name === retypeName(column, to) ? [{ name: d.name, column, to }] : [];
  });
}

/** A Select, as its window and Python set it: the columns kept, the new names of some, the new types of some. */
export interface Selection {
  readonly keep: readonly string[];
  readonly rename: Readonly<Record<string, string>>;
  readonly retype?: Readonly<Record<string, Retype>>;
  /** The order it hands its columns on in, by their own names (its window's rows', Alteryx's fields moved up and down);
   *  any it does not name after them. Absent, as they are. */
  readonly order?: readonly string[];
}

/** Each column of a grid as its Select window shows it: its name, its type, kept or not, its name and type handed on. */
export interface SelectedColumn {
  readonly name: string;
  readonly type: string;
  readonly kept: boolean;
  readonly as: string;
  readonly retype?: Retype;
  /** What it can be made. */
  readonly retypes: readonly Retype[];
}

/** A grid's columns as its Select has them now (`configuration`: its settings), in the order it hands them on: its own,
 *  its retypes read as theirs. */
export function selectionOf(s: Pick<CubeShape, 'columns' | 'derived' | 'rows' | 'pivotOn'>, configuration: Readonly<Record<string, unknown>>): SelectedColumn[] {
  const retypes = retypesOf(s.derived);
  const own = new Set(retypes.map((r) => r.name));
  const columns = rowColumns(s).filter((c) => !own.has(c.name));
  const on = new Map(handedOn(rowColumns(s).map((c) => c.name), s, configuration).map((c) => [c.name, c.as] as const));
  // in its column order: each where it -- or its conversion -- is placed, those it does not place after them
  const order = configuration['columnOrder'];
  const at = new Map((Array.isArray(order) ? order : []).map((c, i) => [c, i] as const));
  const rank = (name: string): number => at.get(retypes.find((r) => r.column === name)?.name ?? name) ?? at.size;
  return [...columns].sort((a, b) => rank(a.name) - rank(b.name)).map((c) => {
    const r = retypes.find((x) => x.column === c.name);
    const as = on.get(r?.name ?? c.name);
    return { name: c.name, type: c.type ?? '', kept: as !== undefined, as: as ?? c.name, ...(r !== undefined ? { retype: r.to } : {}),
      retypes: retypesFor(c.type) };
  });
}

/**
 * The commands that make a grid's columns a Select's (`s`: the grid as it is): each retype it no longer has taken off, each
 * new one added (a conversion of its column), then every column's settings -- kept shown, left out hidden, renamed given
 * its display name, a retyped one's column hidden and its conversion given the name. Refused in words: nothing kept, a
 * column the grid has not, a new name empty, a type its column cannot be made, two columns handed on by one name.
 */
export function selectCommands(s: CubeSnapshot, t: Selection): GridCommand[] {
  const was = retypesOf(s.derived);
  const internal = new Set(was.map((r) => r.name));
  const columns = rowColumns(s).filter((c) => !internal.has(c.name));
  const all = columns.map((c) => c.name);
  const retype = t.retype ?? {};
  const odd = [...t.keep, ...Object.keys(t.rename), ...Object.keys(retype)].filter((c) => !all.includes(c));
  if (odd.length > 0) throw new CubeRefusal(`the grid has no column ${[...new Set(odd)].map((c) => `'${c}'`).join(', ')}`);
  if (t.keep.length === 0) throw new CubeRefusal('keep a column: a box hands on at least one');
  const named = (c: string): string => (t.rename[c] ?? c).trim();
  const empty = Object.keys(t.rename).find((c) => named(c) === '');
  if (empty !== undefined) throw new CubeRefusal(`'${empty}' is renamed to nothing: give it a name, or keep its own`);
  const typeOf = new Map(columns.map((c) => [c.name, c.type] as const));
  for (const [c, to] of Object.entries(retype)) {
    if (!retypesFor(typeOf.get(c)).includes(to)) {
      throw new CubeRefusal(`'${c}' (${typeOf.get(c) ?? 'not typed yet'}) cannot be made ${to}: it can be made ${retypesFor(typeOf.get(c)).join(', ') || 'nothing else'}`);
    }
  }
  const out = new Map<string, string>();
  for (const c of all.filter((x) => t.keep.includes(x))) {
    const as = named(c);
    const other = out.get(as);
    if (other !== undefined) throw new CubeRefusal(`'${other}' and '${c}' would both be handed on as '${as}': name one otherwise`);
    out.set(as, c);
  }
  const wanted = (c: string): Retype | undefined => (t.keep.includes(c) ? retype[c] : undefined);
  // its retypes: those it keeps as they are, those it takes off, those it adds
  const commands: GridCommand[] = was.filter((r) => wanted(r.column) !== r.to).map((r) => ({ kind: 'removeCalculated', name: r.name }));
  const made = new Map<string, string>();
  for (const c of all) {
    const to = wanted(c);
    if (to === undefined) continue;
    const name = retypeName(c, to);
    made.set(c, name);
    if (was.some((r) => r.name === name)) continue;
    const fnName = CONVERSIONS[familyOf(typeOf.get(c)) ?? 'any']![to]!;
    const body: ValueSpecification = fn(fnName, fn('toOne', property(variable('x'), c)));
    commands.push({ kind: 'calculate', level: 'row', column: {
      name, lambda: lambda(['x'], body), kind: NUMBERS.has(to) ? 'measure' : 'dimension',
    } });
  }
  if (t.order !== undefined) {
    const strays = t.order.filter((c) => !all.includes(c));
    if (strays.length > 0) throw new CubeRefusal(`its order names no column ${strays.map((c) => `'${c}'`).join(', ')}`);
    const twice = t.order.find((c, i) => t.order!.indexOf(c) !== i);
    if (twice !== undefined) throw new CubeRefusal(`its order names '${twice}' twice`);
  }
  const settings: Record<string, { hidden: true | null; displayName: string | null }> = {};
  for (const c of all) {
    const kept = t.keep.includes(c);
    const as = named(c);
    const r = made.get(c);
    settings[c] = { hidden: kept && r === undefined ? null : true, displayName: kept && r === undefined && as !== c ? as : null };
    if (r !== undefined) settings[r] = { hidden: null, displayName: as };
  }
  // its order: each column where it is named, those it does not name after them as they are, a conversion in its
  // column's place -- none named, the grid's own: one order whoever sets it (the screen's window names every row,
  // Python's grid.select may name none: the review of 2026-10-26's B4)
  const listed = t.order ?? [];
  const order = [...listed, ...all.filter((c) => !listed.includes(c))].map((c) => made.get(c) ?? c);
  return [...commands, { kind: 'configure', patch: { columns: settings, columnOrder: order } }];
}

/** A Select read from JSON (Python's): `{kind: 'selectColumns', keep, rename?, retype?}` (a chart's `select` is a page
 *  command's kind) -- or refused in words. */
export function readSelection(raw: unknown): Selection {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'selectColumns') {
    throw new CubeRefusal(`a select is {kind: 'selectColumns', keep, rename, retype}, not ${JSON.stringify(raw)}`);
  }
  const odd = Object.keys(o).filter((k) => !['kind', 'keep', 'rename', 'retype', 'order'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a select takes keep, rename, retype and order, not ${odd.join(', ')}`);
  const order = o['order'];
  if (order !== undefined && !(Array.isArray(order) && order.every((c): c is string => typeof c === 'string'))) {
    throw new CubeRefusal(`its order is a list of column names, not ${JSON.stringify(order)}`);
  }
  const keep = o['keep'];
  if (!Array.isArray(keep) || !keep.every((c): c is string => typeof c === 'string')) {
    throw new CubeRefusal(`what it keeps is a list of column names, not ${JSON.stringify(keep)}`);
  }
  const names = (key: string, what: string, ok: (v: unknown) => boolean): Record<string, never> => {
    const v = o[key] ?? {};
    if (v === null || typeof v !== 'object' || Array.isArray(v) || !Object.values(v).every(ok)) {
      throw new CubeRefusal(`its ${key}s are {column: ${what}}, not ${JSON.stringify(v)}`);
    }
    return v as Record<string, never>;
  };
  const rename = names('rename', 'new name', (v) => typeof v === 'string');
  const retype = names('retype', RETYPES.join(' | '), (v) => (RETYPES as readonly unknown[]).includes(v));
  return { keep, rename, ...(Object.keys(retype).length > 0 ? { retype } : {}), ...(order !== undefined ? { order } : {}) };
}
