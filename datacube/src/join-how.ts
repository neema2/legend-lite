// A JOIN'S HOW, AS A JOIN SOURCE KEEPS IT (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.2): the forms two inputs combine
// by and the reader of the saved document's shape -- all a cube document needs to read a join (cube-document.ts), so a
// page with no join loads no more of one (join.ts, what it becomes, is fetched with the first join: bundle-budget). What
// is wrong with a column's name or type is the compiler's to say, as it types what join.ts writes.

import { ProtocolError, readLambda, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { CubeRefusal } from './snapshot.ts';

/** A join's kind: matching rows only, all of mine, all of theirs, everything. */
export type JoinKindWord = 'inner' | 'left' | 'right' | 'full';
export const JOIN_KINDS: readonly JoinKindWord[] = ['inner', 'left', 'right', 'full'];

/** A row's comparison. */
export type JoinOp = '==' | '!=' | '<' | '<=' | '>' | '>=';
export const JOIN_OPS: readonly JoinOp[] = ['==', '!=', '<', '<=', '>', '>='];

/** Which way an as-of join's nearest row is: at or before mine (`>=`), strictly before (`>`), at or after, after. */
export type AsOfOp = '>=' | '<=' | '>' | '<';
export const AS_OF_OPS: readonly AsOfOp[] = ['>=', '<=', '>', '<'];

/** A row of a condition: my column, a comparison, their column -- each by the name its own input gives it. */
export interface JoinRow {
  readonly left: string;
  readonly op: JoinOp;
  readonly right: string;
}

/**
 * What pairs a row of mine with one of theirs: rows, ANDed (none: every pair), or a Pure `{a, b | ...}`, parsed first.
 * In a join or an as-of join a Pure condition names their columns as they come out of it (a name mine has, prefixed:
 * theirNames); in a semi- or anti-join, by their own names.
 */
export type JoinCondition = { readonly rows: readonly JoinRow[] } | { readonly lambda: Lambda };

/** How two inputs combine (§17.2). */
export type JoinHow =
  | {
    readonly form: 'join'; readonly kind: JoinKindWord; readonly on: JoinCondition; readonly prefix: string;
    /** Their columns brought over, by their own names, in the order given; null: all. */
    readonly keep: readonly string[] | null;
  }
  | {
    readonly form: 'asOf'; readonly match: JoinRow & { readonly op: AsOfOp }; readonly on: JoinCondition;
    readonly prefix: string; readonly keep: readonly string[] | null;
  }
  /** My rows that have a match (semi), or that have none (anti): my columns only, each row once. */
  | { readonly form: 'semi' | 'anti'; readonly on: JoinCondition }
  /** The rows of both, columns matched by name: a column one side lacks is empty on its rows. */
  | { readonly form: 'append' };

export type JoinForm = JoinHow['form'];
export const JOIN_FORMS: readonly JoinForm[] = ['join', 'asOf', 'semi', 'anti', 'append'];

/** An input as the join reads it: what it is called, its relation, and its columns as the compiler typed them. */
export interface JoinSide {
  readonly name: string;
  readonly relation: ValueSpecification;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/** The prefix a new join puts on their names that mine has. */
export const DEFAULT_PREFIX = 'their_';

// ---------------------------------------------------------------- reading, as a join source keeps it

function object(raw: unknown, what: string, allowed: readonly string[]): Record<string, unknown> {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw new CubeRefusal(`${what} is an object, not ${JSON.stringify(raw)}`);
  const o = raw as Record<string, unknown>;
  const odd = Object.keys(o).filter((k) => !allowed.includes(k));
  if (odd.length > 0) throw new CubeRefusal(`${what} takes ${allowed.join(', ')}, not ${odd.join(', ')}`);
  return o;
}

function name(raw: unknown, what: string): string {
  if (typeof raw !== 'string' || raw === '') throw new CubeRefusal(`${what} names a column, not ${JSON.stringify(raw)}`);
  return raw;
}

function readRow<Op extends string>(raw: unknown, what: string, ops: readonly Op[]): { left: string; op: Op; right: string } {
  const o = object(raw, what, ['left', 'op', 'right']);
  if (typeof o['op'] !== 'string' || !(ops as readonly string[]).includes(o['op'])) {
    throw new CubeRefusal(`${what}'s comparison is one of ${ops.join(' ')}, not ${JSON.stringify(o['op'])}`);
  }
  return { left: name(o['left'], `${what}'s left`), op: o['op'] as Op, right: name(o['right'], `${what}'s right`) };
}

function readCondition(raw: unknown, what: string): JoinCondition {
  const o = object(raw, what, ['rows', 'lambda']);
  if ('rows' in o === 'lambda' in o) throw new CubeRefusal(`${what} is rows or a lambda, one of them`);
  if ('rows' in o) {
    if (!Array.isArray(o['rows'])) throw new CubeRefusal(`${what}'s rows are a list`);
    return { rows: o['rows'].map((r, i) => readRow(r, `${what}'s row ${i + 1}`, JOIN_OPS)) };
  }
  let read: Lambda;
  try {
    read = readLambda(o['lambda']);
  } catch (e) {
    if (e instanceof ProtocolError) throw new CubeRefusal(`${what}'s lambda: ${e.message}`);
    throw e;
  }
  const params = read.parameters.map((p) => p.name);
  if (params.length !== 2 || params[0] === params[1]) {
    throw new CubeRefusal(`${what}'s lambda takes two rows, mine and theirs, by two names: {a, b | ...}`);
  }
  return { lambda: read };
}

function readKeep(raw: unknown): readonly string[] | null {
  if (raw === null) return null;
  if (!Array.isArray(raw)) throw new CubeRefusal(`a join's kept columns are a list, or null for all, not ${JSON.stringify(raw)}`);
  return raw.map((c) => name(c, 'a kept column'));
}

function readPrefix(raw: unknown): string {
  if (typeof raw !== 'string' || raw === '') throw new CubeRefusal(`a join's prefix starts a column's name (c_), not ${JSON.stringify(raw)}`);
  return raw;
}

/** How two inputs combine, read field by field as a join source keeps it, or why not (CubeRefusal). */
export function readJoinHow(raw: unknown): JoinHow {
  const form = (raw as { form?: unknown } | null)?.form;
  switch (form) {
    case 'join': {
      const o = object(raw, 'a join', ['form', 'kind', 'on', 'prefix', 'keep']);
      if (typeof o['kind'] !== 'string' || !(JOIN_KINDS as readonly string[]).includes(o['kind'])) {
        throw new CubeRefusal(`a join's kind is one of ${JOIN_KINDS.join(', ')}, not ${JSON.stringify(o['kind'])}`);
      }
      return {
        form, kind: o['kind'] as JoinKindWord, on: readCondition(o['on'], "a join's condition"), prefix: readPrefix(o['prefix']),
        keep: readKeep(o['keep']),
      };
    }
    case 'asOf': {
      const o = object(raw, 'an as-of join', ['form', 'match', 'on', 'prefix', 'keep']);
      return {
        form, match: readRow(o['match'], "an as-of join's match", AS_OF_OPS), on: readCondition(o['on'], "an as-of join's condition"),
        prefix: readPrefix(o['prefix']), keep: readKeep(o['keep']),
      };
    }
    case 'semi':
    case 'anti': {
      const o = object(raw, `a ${form}-join`, ['form', 'on']);
      return { form, on: readCondition(o['on'], `a ${form}-join's condition`) };
    }
    case 'append':
      object(raw, 'an append', ['form']);
      return { form };
    default:
      throw new CubeRefusal(`a join's form is one of ${JOIN_FORMS.join(', ')}, not ${JSON.stringify(form)}`);
  }
}

/** An input as a key is suggested over it: what it is called and its columns -- no relation needed (a menu's). */
export type JoinColumns = Pick<JoinSide, 'name' | 'columns'>;

/**
 * Keys to suggest, each with why: their name's `_id` among mine (`customers` → `customer_id`, `customers_id`) against
 * their `id`, then a name both inputs have, of one type -- but `id`, each table's own key far more often than the
 * other's. The row a new join starts with when exactly one is found.
 */
export function suggestedKeys(left: JoinColumns, right: JoinColumns): { readonly row: JoinRow; readonly why: string }[] {
  const mine = new Map(left.columns.map((c) => [c.name.toLowerCase(), c]));
  const out: { row: JoinRow; why: string }[] = [];
  const base = right.name.toLowerCase().replace(/\.[a-z0-9]+$/, '').replace(/[^a-z0-9_]/g, '_');
  const singular = base.endsWith('s') ? base.slice(0, -1) : base;
  const id = right.columns.find((c) => c.name.toLowerCase() === 'id');
  if (id) {
    for (const n of new Set([`${singular}_id`, `${base}_id`])) {
      const m = mine.get(n);
      if (m) out.push({ row: { left: m.name, op: '==', right: id.name }, why: `${right.name}'s id` });
    }
  }
  for (const c of right.columns) {
    const same = mine.get(c.name.toLowerCase());
    if (same && same.type === c.type && c.name.toLowerCase() !== 'id') out.push({ row: { left: same.name, op: '==', right: c.name }, why: 'the same name' });
  }
  return out;
}

/** A join a grid's right-click offers at once: their grid, by its id and its name, and the key it suggests (and why). */
export interface QuickJoin {
  readonly their: string;
  readonly name: string;
  readonly row: JoinRow;
  readonly why: string;
}

/**
 * A GRID'S QUICK JOINS ON THE COLUMN CLICKED (its right-click's Join ▸, as its Filter ▸ is the value clicked; the user,
 * 2026-10-23: "a fast option to join to another grid with the same column name and type ... only if clicked on a column"):
 * each other grid with a key on that column to suggest (suggestedKeys: a column of its of the same name and type, or its
 * id under its name), in the page's order, one each. A grid that joins as no fixed columns (a pivoted one) is its
 * caller's to leave out.
 */
export function quickJoins(mine: JoinColumns, others: readonly (JoinColumns & { readonly id: string })[], clicked: string): QuickJoin[] {
  return others.flatMap((o) => {
    const key = suggestedKeys(mine, o).find((k) => k.row.left === clicked);
    return key ? [{ their: o.id, name: o.name, row: key.row, why: key.why }] : [];
  });
}

/** The forms a quick join is made in: all of mine (a lookup), matching only, mine with a match, mine with none. */
export type QuickForm = 'left' | 'inner' | 'semi' | 'anti';

/** A quick join's `how`: on its one key, their columns brought whole (a name mine has prefixed, as the window's default). */
export function quickHow(row: JoinRow, form: QuickForm): JoinHow {
  const on = { rows: [row] };
  return form === 'semi' || form === 'anti' ? { form, on } : { form: 'join', kind: form, on, prefix: DEFAULT_PREFIX, keep: null };
}
