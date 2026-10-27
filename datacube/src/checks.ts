// A FLOW'S TEST (docs/DATACUBE_FLOW_MODE_2026_10_24.md FL8 and §4's Test, Alteryx's Test tool): checks of a box's rows --
// a column with no empty value, a column no value of which repeats, a number column within bounds, a count of rows
// within bounds -- each counted by the database as the rows that fail it, in one row: said as what a grid already does
// (Count Records' way, count-records.ts): a column of one value its rows are grouped by, a column of 1 for each row whose
// check fails, summed, every column it read hidden; a count of rows checked after grouping, a column over its group's
// count of 1 or 0. A box whose every count is 0 passes; its failing checks are what its row says. What a box checks is
// read back off its columns (`checksOf`): each rebuilt by the rule that made it and compared whole, never guessed at.
//
// One rule for the box's window and Python's grid.test (headless gridPreset).
//
// PURE: no DOM, no planner.

import { collection, fn, lambda, lit, not, property, toJson, variable, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { familyOf, isFractional } from '../../engine-client/src/types.ts';
import type { GridCommand } from './grid-commands.ts';
import { CubeRefusal, rowColumns, sameValue, type CubeShape, type CubeSnapshot, type DerivedColumn } from './snapshot.ts';

/** A check of a box's rows: no empty value; no value repeated; a number from `min` to `max` (either left out: no bound
 *  that side), an empty value no failure; its count of rows from `min` to `max` (whole numbers, a smallest 1 at least),
 *  of no column. */
export type Check =
  | { readonly kind: 'notEmpty'; readonly column: string }
  | { readonly kind: 'unique'; readonly column: string }
  | { readonly kind: 'between'; readonly column: string; readonly min?: number; readonly max?: number }
  | { readonly kind: 'rows'; readonly min?: number; readonly max?: number }
  /** Its values one of a list (dbt's accepted values): a text's or a number's, an empty value no failure. */
  | { readonly kind: 'oneOf'; readonly column: string; readonly values: readonly (string | number)[] }
  /** A text that matches a pattern (a regular expression, as RegEx's Match reads one), an empty value no failure. */
  | { readonly kind: 'matches'; readonly column: string; readonly pattern: string };

export const CHECK_KINDS: readonly Check['kind'][] = ['notEmpty', 'unique', 'between', 'rows', 'oneOf', 'matches'];

/** The column of one value its rows are grouped by, its value, and the count of its rows beside its checks. */
export const CHECKS_COLUMN = 'ChecksAll';
const ALL = 'checks';
export const ROWS_COLUMN = 'Rows';

/** What each check's count is called: its column's name, and what fails it (a count of rows: its count's). */
const SUFFIX: Readonly<Record<Check['kind'], string>> = {
  notEmpty: '_empty', unique: '_repeated', between: '_outside', rows: '_outside', oneOf: '_unexpected', matches: '_unmatched',
};
export const checkName = (c: Check): string => `${c.kind === 'rows' ? ROWS_COLUMN : c.column}${SUFFIX[c.kind]}`;
/** A unique check's row number among the rows of its value (a window column, hidden). */
const rowName = (column: string): string => `${column}__row`;

const x = (column: string): ValueSpecification => property(variable('x'), column);
/** `x | if(<failed>, |1, |0)`: 1 for each row the check fails. */
const failing = (failed: ValueSpecification): Lambda => lambda(['x'], fn('if', failed, lambda([], lit.integer(1)), lambda([], lit.integer(0))));
const bound = (v: number, whole: boolean): ValueSpecification => (whole ? lit.integer(v) : lit.float(v));

/** `v` outside `min` to `max`: below the one, above the other, or either. */
function outside(v: ValueSpecification, min: number | undefined, max: number | undefined, whole: boolean): ValueSpecification {
  const below = min === undefined ? undefined : fn('lessThan', v, bound(min, whole));
  const above = max === undefined ? undefined : fn('greaterThan', v, bound(max, whole));
  return below !== undefined && above !== undefined ? fn('or', below, above) : (below ?? above)!;
}

/** The calculated column that counts a check's failing rows, by the rule (`whole`: its column holds whole numbers) -- a
 *  count of rows's over its group's count, 1 when it is outside its bounds. */
function failingLambda(c: Check, whole: boolean): Lambda {
  switch (c.kind) {
    case 'notEmpty': return failing(fn('isEmpty', x(c.column)));
    // a value's rows after its first, an empty value repeating nothing (as no other check fails one: the correctness
    // audit's 8, 2026-10-26 -- the empties are one partition of the row number)
    case 'unique': return failing(fn('and', fn('isNotEmpty', x(c.column)), fn('greaterThan', fn('toOne', x(rowName(c.column))), lit.integer(1))));
    case 'between': return failing(fn('and', fn('isNotEmpty', x(c.column)), outside(fn('toOne', x(c.column)), c.min, c.max, whole)));
    case 'rows': return failing(outside(fn('toOne', x(ROWS_COLUMN)), c.min, c.max, true));
    case 'oneOf': return failing(fn('and', fn('isNotEmpty', x(c.column)),
      not(fn('in', fn('toOne', x(c.column)), collection(c.values.map((v) => (typeof v === 'string' ? lit.string(v) : bound(v, whole))))))));
    case 'matches': return failing(fn('and', fn('isNotEmpty', x(c.column)), not(fn('regexpLike', fn('toOne', x(c.column)), lit.string(c.pattern)))));
  }
}

/**
 * The commands that make a grid its checks' counts (`s`: the grid as it is): any grouping taken off; for each check a
 * column of 1 for each row it fails (a unique check's row number among its value's rows first, hidden); a count of its
 * rows; grouped by a column of one value, the counts summed, every other column hidden -- one step. Refused in words:
 * no check, a column the grid has not, a bound of a column that is no number or bounds the wrong way round, a check
 * twice, a name taken, a grid pivoted. A grid checking already is checked again: its own columns taken off first.
 */
export function checkCommands(s: CubeSnapshot, checks: readonly Check[]): GridCommand[] {
  if (checks.length === 0) throw new CubeRefusal('a test checks something: add a check');
  if (s.pivotOn.length > 0) throw new CubeRefusal('a pivoted grid\'s columns are its values: take its pivot off to check its rows');
  // a test set again: its own columns taken off first, in the same step, and the columns it read checked again
  const was = checksOf(s);
  const own = new Set(was === undefined ? [] : testColumns(was));
  // its count of rows's column (after grouping) first: it reads the others
  const off: GridCommand[] = was === undefined ? [] : [
    ...(s.groupDerived ?? []).filter((d) => own.has(d.name)).reverse().map((d): GridCommand => ({ kind: 'removeCalculated', name: d.name })),
    { kind: 'group', rows: [] },
    ...s.derived.filter((d) => own.has(d.name)).reverse().map((d): GridCommand => ({ kind: 'removeCalculated', name: d.name }))];
  const types = new Map(rowColumns(s).filter((c) => !own.has(c.name)).map((c) => [c.name, c.type] as const));
  const taken = new Set([...types.keys(), ...(s.groupDerived ?? []).map((d) => d.name).filter((n) => !own.has(n))]);
  const names = new Set<string>();
  const made: GridCommand[] = [];
  const counts: string[] = [];
  const after: GridCommand[] = [];
  for (const c of checks) {
    if (c.kind === 'rows') {
      if (c.min === undefined && c.max === undefined) throw new CubeRefusal('a count of rows is bounded: its smallest, its largest, or both');
      for (const v of [c.min, c.max]) {
        if (v !== undefined && !Number.isInteger(v)) throw new CubeRefusal(`a count of rows is a whole number, not ${v}`);
      }
      if (c.min !== undefined && c.min < 1) throw new CubeRefusal('every count of rows is 0 at least: a smallest is 1 or more, or left out');
      if (c.max !== undefined && c.max < 0) throw new CubeRefusal(`a count of rows is never ${c.max}`);
      if (c.min !== undefined && c.max !== undefined && c.min > c.max) throw new CubeRefusal(`rows from ${c.min} to ${c.max}: its smallest is above its largest`);
      if (names.has(checkName(c))) throw new CubeRefusal('its count of rows is checked twice');
      names.add(checkName(c));
      if (taken.has(checkName(c))) throw new CubeRefusal(`'${checkName(c)}' is already a column`);
      taken.add(checkName(c));
      after.push({ kind: 'calculate', level: 'group', column: { name: checkName(c), lambda: failingLambda(c, true) } });
      continue;
    }
    if (!types.has(c.column)) throw new CubeRefusal(`the grid has no column '${c.column}'`);
    const type = types.get(c.column);
    if (c.kind === 'between') {
      if (familyOf(type) !== 'numeric') throw new CubeRefusal(`'${c.column}' is ${type ?? 'not typed yet'}: only a number is checked within bounds`);
      if (c.min === undefined && c.max === undefined) throw new CubeRefusal(`a bound of '${c.column}': its smallest, its largest, or both`);
      for (const v of [c.min, c.max]) if (v !== undefined && !Number.isFinite(v)) throw new CubeRefusal(`a bound is a number, not ${v}`);
      if (c.min !== undefined && c.max !== undefined && c.min > c.max) throw new CubeRefusal(`'${c.column}' from ${c.min} to ${c.max}: its smallest is above its largest`);
      if (!isFractional(type) && [c.min, c.max].some((v) => v !== undefined && !Number.isInteger(v))) {
        throw new CubeRefusal(`'${c.column}' holds whole numbers: bound it by whole numbers`);
      }
    }
    if (c.kind === 'oneOf') {
      const family = familyOf(type);
      if (family !== 'text' && family !== 'numeric') throw new CubeRefusal(`'${c.column}' is ${type ?? 'not typed yet'}: a text's or a number's values are listed`);
      if (c.values.length === 0) throw new CubeRefusal(`list the values '${c.column}' may hold`);
      for (const v of c.values) {
        if (family === 'text' ? typeof v !== 'string' : typeof v !== 'number' || !Number.isFinite(v)) {
          throw new CubeRefusal(`'${c.column}' holds ${family === 'text' ? 'text' : 'numbers'}: ${JSON.stringify(v)} is none`);
        }
        if (family === 'numeric' && !isFractional(type) && !Number.isInteger(v)) throw new CubeRefusal(`'${c.column}' holds whole numbers, not ${v}`);
      }
      if (new Set(c.values.map(String)).size !== c.values.length) throw new CubeRefusal(`a value of '${c.column}' is listed twice`);
    }
    if (c.kind === 'matches') {
      if (familyOf(type) !== 'text') throw new CubeRefusal(`'${c.column}' is ${type ?? 'not typed yet'}: only a text matches a pattern`);
      if (c.pattern === '') throw new CubeRefusal(`give the pattern '${c.column}' matches`);
    }
    const name = checkName(c);
    if (names.has(name)) throw new CubeRefusal(`'${c.column}' is checked that way twice`);
    names.add(name);
    for (const n of [name, ...(c.kind === 'unique' ? [rowName(c.column)] : [])]) {
      if (taken.has(n)) throw new CubeRefusal(`'${n}' is already a column`);
      taken.add(n);
    }
    if (c.kind === 'unique') {
      made.push({ kind: 'calculate', level: 'row', column: {
        name: rowName(c.column), window: { fn: 'rowNumber', partition: [c.column], order: [{ column: c.column, direction: 'asc' }] },
      } });
    }
    made.push({ kind: 'calculate', level: 'row', column: { name, lambda: failingLambda(c, !isFractional(type)), kind: 'measure' } });
    counts.push(name);
  }
  for (const n of [ROWS_COLUMN, CHECKS_COLUMN]) if (taken.has(n)) throw new CubeRefusal(`'${n}' is already a column`);
  const hidden = [...types.keys(), CHECKS_COLUMN, ...checks.flatMap((c) => (c.kind === 'unique' ? [rowName(c.column)] : []))];
  return [
    ...off,
    ...(was === undefined && s.rows.length > 0 ? [{ kind: 'group', rows: [] } as const] : []),
    ...made,
    { kind: 'calculate', level: 'row', column: { name: ROWS_COLUMN, lambda: lambda(['x'], lit.integer(1)), kind: 'measure' } },
    { kind: 'calculate', level: 'row', column: { name: CHECKS_COLUMN, lambda: lambda(['x'], lit.string(ALL)), kind: 'dimension' } },
    { kind: 'group', rows: [CHECKS_COLUMN] },
    // its rows counted, not summed: 0 of no rows (the review of 2026-10-26's C4); each check's failing rows summed
    { kind: 'column', name: ROWS_COLUMN, change: { aggregate: 'count' } },
    ...counts.map((name): GridCommand => ({ kind: 'column', name, change: { aggregate: 'sum' } })),
    ...after,
    { kind: 'configure', patch: { columns: Object.fromEntries(hidden.map((c) => [c, { hidden: true }])) } },
  ];
}

/** The columns a test of `checks` makes: each check's count (a unique check's row number too), its rows' count and its
 *  column of one value -- none of the columns it reads. */
export function testColumns(checks: readonly Check[]): string[] {
  return [...checks.flatMap((c) => [checkName(c), ...(c.kind === 'unique' ? [rowName(c.column)] : [])]), ROWS_COLUMN, CHECKS_COLUMN];
}

/** The number a bound's literal is: an integer's, a float's, a negative's. */
function numberOf(v: ValueSpecification | undefined): number | undefined {
  if (v === undefined) return undefined;
  const o = v as { _type?: string; value?: unknown; function?: string; parameters?: ValueSpecification[] };
  if (o._type === 'integer' || o._type === 'float') return typeof o.value === 'number' ? o.value : Number(String((o.value as { text?: string })?.text ?? o.value));
  if (o._type === 'func' && o.function === 'minus' && o.parameters?.length === 1) {
    const n = numberOf(o.parameters[0]);
    return n === undefined ? undefined : -n;
  }
  return undefined;
}

/** The bounds an `outside` says, read off its comparisons: a lessThan's, a greaterThan's, or the two or-ed. */
function boundsOf(out: ValueSpecification | undefined): { min?: number; max?: number } {
  const o = out as { function?: string; parameters?: ValueSpecification[] } | undefined;
  const sides = o?.function === 'or' ? o.parameters ?? [] : o === undefined ? [] : [o];
  const side = (f: string): number | undefined => numberOf((sides.find((p) => (p as { function?: string }).function === f) as
    { parameters?: ValueSpecification[] } | undefined)?.parameters?.[1]);
  const min = side('lessThan');
  const max = side('greaterThan');
  return { ...(min !== undefined ? { min } : {}), ...(max !== undefined ? { max } : {}) };
}

/** A `failing` lambda's condition: what its `if` asks. */
const conditionOf = (l: Lambda): ValueSpecification | undefined => (l.body[0] as { parameters?: ValueSpecification[] }).parameters?.[0];

/** The count of rows a column over its group checks, read back: rebuilt and compared whole -- undefined for any other. */
function rowsCheckOf(d: DerivedColumn): Check | undefined {
  if (d.lambda === undefined || d.name !== checkName({ kind: 'rows' })) return undefined;
  const c: Check = { kind: 'rows', ...boundsOf(conditionOf(d.lambda)) };
  return (c.min !== undefined || c.max !== undefined) && toJson(d.lambda) === toJson(failingLambda(c, true)) ? c : undefined;
}

/** The check a calculated column counts, read back: its column's name from its own, the check rebuilt and compared whole
 *  (a between's bounds read off its comparisons) -- undefined for any other column. */
function checkOf(d: DerivedColumn, derived: readonly DerivedColumn[]): Check | undefined {
  if (d.lambda === undefined || d.kind !== 'measure') return undefined;
  for (const kind of CHECK_KINDS) {
    if (kind === 'rows' || !d.name.endsWith(SUFFIX[kind])) continue;
    const column = d.name.slice(0, -SUFFIX[kind].length);
    if (column === '') continue;
    let c: Check = { kind, column } as Check;
    if (kind === 'unique') {
      const w = derived.find((r) => r.name === rowName(column))?.window;
      if (w?.fn !== 'rowNumber' || !sameValue(w.partition, [column])) continue;
    }
    if (kind === 'between') {
      // `if(and(isNotEmpty($x.c), <out>), ...)`
      const bounds = boundsOf((conditionOf(d.lambda) as { parameters?: ValueSpecification[] } | undefined)?.parameters?.[1]);
      if (bounds.min === undefined && bounds.max === undefined) continue;
      c = { kind, column, ...bounds };
    }
    if (kind === 'oneOf' || kind === 'matches') {
      // `if(and(isNotEmpty($x.c), not(<in | regexpLike>(toOne($x.c), <its list | its pattern>))), ...)`
      const negated = (conditionOf(d.lambda) as { parameters?: ValueSpecification[] } | undefined)?.parameters?.[1] as
        { parameters?: ValueSpecification[] } | undefined;
      const asked = negated?.parameters?.[0] as { parameters?: ValueSpecification[] } | undefined;
      const given = asked?.parameters?.[1] as { _type?: string; value?: unknown; values?: ValueSpecification[] } | undefined;
      if (kind === 'matches') {
        if (given?._type !== 'string' || typeof given.value !== 'string') continue;
        c = { kind, column, pattern: given.value };
      } else {
        const values = (given?.values ?? []).map((v) => {
          const o = v as { _type?: string; value?: unknown };
          return o._type === 'string' && typeof o.value === 'string' ? o.value : numberOf(v);
        });
        if (values.length === 0 || values.some((v) => v === undefined)) continue;
        c = { kind, column, values: values as (string | number)[] };
      }
    }
    // its bounds whole numbers (a whole-number column's) or not: rebuilt each way they can be, compared as the protocol
    // writes them (a float read back from a page holds its double, one made here its exact digits: the same text)
    const wholeBounds = c.kind === 'oneOf' ? c.values.every((v) => typeof v === 'string' || Number.isInteger(v))
      : c.kind !== 'between' || [c.min, c.max].every((v) => v === undefined || Number.isInteger(v));
    const as = toJson(d.lambda);
    if ((wholeBounds && as === toJson(failingLambda(c, true))) || as === toJson(failingLambda(c, false))) return c;
  }
  return undefined;
}

/** A grid checking its rows, read off its query: its checks in order, its count of rows's last -- undefined for any
 *  other grid. */
export function checksOf(q: Pick<CubeShape, 'derived' | 'rows' | 'groupDerived'>): readonly Check[] | undefined {
  if (q.rows.length !== 1 || q.rows[0] !== CHECKS_COLUMN) return undefined;
  const all = q.derived.find((d) => d.name === CHECKS_COLUMN)?.lambda?.body[0] as { _type?: string; value?: unknown } | undefined;
  if (all?._type !== 'string' || all.value !== ALL) return undefined;
  const checks = [...q.derived.flatMap((d) => checkOf(d, q.derived) ?? []), ...(q.groupDerived ?? []).flatMap((d) => rowsCheckOf(d) ?? [])];
  return checks.length === 0 ? undefined : checks;
}

/** A check in words: "region has no empty value", "trade_id repeats no value", "notional from 0 to 1000000", "rows from
 *  1 to 500", "at least 1 row". */
export function checkWords(c: Check): string {
  const rows = (n: number | undefined): string => `${n} ${n === 1 ? 'row' : 'rows'}`;
  switch (c.kind) {
    case 'notEmpty': return `${c.column} has no empty value`;
    case 'unique': return `${c.column} repeats no value`;
    case 'between': return c.min !== undefined && c.max !== undefined ? `${c.column} from ${c.min} to ${c.max}`
      : c.min !== undefined ? `${c.column} at least ${c.min}` : `${c.column} at most ${c.max}`;
    case 'rows': return c.min !== undefined && c.max !== undefined ? `rows from ${c.min} to ${c.max}`
      : c.min !== undefined ? `at least ${rows(c.min)}` : `at most ${rows(c.max)}`;
    case 'oneOf': return `${c.column} is one of ${c.values.join(', ')}`;
    case 'matches': return `${c.column} matches ${c.pattern}`;
  }
}

/** A failing check: how many rows fail it -- a count of rows's, how many rows its input has. */
export interface Failing { readonly check: Check; readonly rows: number }

/**
 * The checks a test's row says fail (`value`: each check's count, by its column; undefined for every column when it has
 * no row), each with how many rows fail it -- a count of rows's, its rows. A test of no rows (its input has none) is
 * one row (query.ts: a grouping by one value is every row aggregated), its count of rows 0 and every other count none,
 * as a sum of no rows is: every check's 0, and a count of rows fails by the database's comparison (1 at least) -- or,
 * a test grouped into no row, its absence read so. A row of rows lacking a check's count is no row of this test: thrown,
 * never read as none failing (AGENTS.md invariant 4).
 */
export function failingChecks(checks: readonly Check[], value: (name: string) => unknown): Failing[] {
  const rows = value(ROWS_COLUMN);
  const row = rows !== undefined && rows !== null && Number(rows) > 0;
  return checks.flatMap((check): Failing[] => {
    const counted = value(checkName(check));
    const absent = counted === undefined || counted === null;
    if (row && absent) throw new Error(`a test's row has no count '${checkName(check)}' (${checkWords(check)}): it is no row of this test`);
    if (check.kind === 'rows') {
      const fails = absent ? check.min !== undefined : Number(counted) > 0;
      return fails ? [{ check, rows: row ? Number(rows) : 0 }] : [];
    }
    const n = absent ? 0 : Number(counted);
    return n > 0 ? [{ check, rows: n }] : [];
  });
}

/** A failing check in words: "region has no empty value: 2 rows fail", "rows from 1 to 500: it has 0 rows" (`count`:
 *  a number as its reader writes one). */
export function failingWords(f: Failing, count: (n: number) => string = String): string {
  return f.check.kind === 'rows' ? `${checkWords(f.check)}: it has ${count(f.rows)} ${f.rows === 1 ? 'row' : 'rows'}`
    : `${checkWords(f.check)}: ${count(f.rows)} ${f.rows === 1 ? 'row fails' : 'rows fail'}`;
}

/** Checks read from JSON (Python's): `{kind: 'test', checks: [{kind, column, min?, max?}, ...]}` (a `rows` check of no
 *  column) -- or refused in words. */
export function readChecks(raw: unknown): Check[] {
  const o = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw as Record<string, unknown> : undefined;
  if (o === undefined || o['kind'] !== 'test' || !Array.isArray(o['checks'])) {
    throw new CubeRefusal(`a test is {kind: 'test', checks: [...]}, not ${JSON.stringify(raw)}`);
  }
  return (o['checks'] as unknown[]).map((c) => {
    const k = c !== null && typeof c === 'object' && !Array.isArray(c) ? c as Record<string, unknown> : {};
    if (!CHECK_KINDS.includes(k['kind'] as Check['kind'])) throw new CubeRefusal(`a check is ${CHECK_KINDS.join(', ')}, not ${JSON.stringify(k['kind'])}`);
    const rows = k['kind'] === 'rows';
    if (!rows && typeof k['column'] !== 'string') throw new CubeRefusal(`a check names its column, not ${JSON.stringify(k['column'])}`);
    const own = k['kind'] === 'between' || rows ? ['min', 'max'] : k['kind'] === 'oneOf' ? ['values'] : k['kind'] === 'matches' ? ['pattern'] : [];
    const odd = Object.keys(k).filter((key) => !['kind', ...(rows ? [] : ['column']), ...own].includes(key));
    if (odd.length > 0) throw new CubeRefusal(`a ${String(k['kind'])} check takes no ${odd.join(', ')}`);
    for (const b of ['min', 'max']) if (k[b] !== undefined && typeof k[b] !== 'number') throw new CubeRefusal(`a bound is a number, not ${JSON.stringify(k[b])}`);
    if (k['kind'] === 'oneOf') {
      const values = k['values'];
      if (!Array.isArray(values) || !values.every((v) => typeof v === 'string' || typeof v === 'number')) {
        throw new CubeRefusal(`a oneOf check lists its values, texts or numbers, not ${JSON.stringify(values)}`);
      }
      return { kind: 'oneOf', column: k['column'] as string, values: values as (string | number)[] };
    }
    if (k['kind'] === 'matches') {
      if (typeof k['pattern'] !== 'string') throw new CubeRefusal(`a matches check gives its pattern, not ${JSON.stringify(k['pattern'])}`);
      return { kind: 'matches', column: k['column'] as string, pattern: k['pattern'] };
    }
    return { kind: k['kind'], ...(rows ? {} : { column: k['column'] }), ...(k['min'] !== undefined ? { min: k['min'] } : {}),
      ...(k['max'] !== undefined ? { max: k['max'] } : {}) } as Check;
  });
}
