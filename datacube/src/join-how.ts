// A JOIN'S HOW, AS A JOIN SOURCE KEEPS IT (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.2): the forms two inputs combine
// by and the reader of the saved document's shape -- all a cube document needs to read a join (cube-document.ts), so a
// page with no join loads no more of one (join.ts, what it becomes, is fetched with the first join: bundle-budget). What
// is wrong with a column's name or type is the compiler's to say, as it types what join.ts writes.

import { ExactNumber, ProtocolError, readLambda, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { CubeRefusal } from './snapshot.ts';

/** A join's kind: matching rows only, all of mine, all of theirs, everything. */
export type JoinKindWord = 'inner' | 'left' | 'right' | 'full';
export const JOIN_KINDS: readonly JoinKindWord[] = ['inner', 'left', 'right', 'full'];

/** A row's comparison: `~` similar text, at most `within` edits apart (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2: a flow's
 *  Fuzzy Match, Alteryx's: `levenshteinDistance`). */
export type JoinOp = '==' | '!=' | '<' | '<=' | '>' | '>=' | '~';
export const JOIN_OPS: readonly JoinOp[] = ['==', '!=', '<', '<=', '>', '>=', '~'];

/** How many edits apart two similar texts may be, when a row does not say. */
export const SIMILAR_WITHIN = 2;

/** Which way an as-of join's nearest row is: at or before mine (`>=`), strictly before (`>`), at or after, after. */
export type AsOfOp = '>=' | '<=' | '>' | '<';
export const AS_OF_OPS: readonly AsOfOp[] = ['>=', '<=', '>', '<'];

/** A row of a condition: my column, a comparison, their column -- each by the name its own input gives it. */
export interface JoinRow {
  readonly left: string;
  readonly op: JoinOp;
  readonly right: string;
  /** A `~` row's edits apart at most (SIMILAR_WITHIN when not said); no other row's. */
  readonly within?: number;
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
    /** Each row of mine paired with one of theirs at most: their first row of each key -- their columns in the condition's
     *  rows, every one `==` -- each key's rows in an order of their every column that sorts, rising (a lookup's, Alteryx's
     *  Find Replace: a key theirs repeats matches once, C13). Absent: every match. */
    readonly once?: true;
    /** A Fuzzy Match's score (Alteryx's): each pair's likeness on its condition's first similar (`~`) row, 0 to 100 --
     *  100 less its edits apart for each 100 letters of the longer, rounded -- a column of this name, the database's.
     *  Matching rows only, so both texts have a value. Absent: none. */
    readonly score?: string;
  }
  | {
    readonly form: 'asOf'; readonly match: JoinRow & { readonly op: AsOfOp }; readonly on: JoinCondition;
    readonly prefix: string; readonly keep: readonly string[] | null;
  }
  /** My rows that have a match (semi), or that have none (anti): my columns only, each row once. */
  | { readonly form: 'semi' | 'anti'; readonly on: JoinCondition }
  /** The rows of both, columns matched by name: a column one side lacks is empty on its rows. `pick` (Dynamic Input,
   *  docs/DATACUBE_FLOW_MODE_2026_10_24.md M8): an expression in Pure of no parameter -- a page parameter, `|$source` --
   *  naming the one input whose rows it keeps, by its name: each input's rows filtered by it, the database keeping one. */
  | { readonly form: 'append'; readonly pick?: Lambda };

/** A join of every row of mine with every row of theirs: matching rows only, on no condition (Alteryx's Append Fields;
 *  docs/DATACUBE_FLOW_MODE_2026_10_24.md M2) -- the Join window's Every pair, a flow's Append Fields box. */
export const isEveryPair = (how: JoinHow): boolean =>
  how.form === 'join' && how.kind === 'inner' && 'rows' in how.on && how.on.rows.length === 0;

/** A join pairing rows by similar text: a row of its condition `~` (a flow's Fuzzy Match box). */
export const isFuzzy = (how: JoinHow): boolean =>
  how.form === 'join' && 'rows' in how.on && how.on.rows.some((r) => r.op === '~');

/** Why an append's pick is no expression naming an input, in words, if it is not: one expression of no parameter. */
export function pickProblem(pick: Lambda): string | undefined {
  return pick.parameters.length > 0 || pick.body.length !== 1
    ? "an append's pick is one expression of no parameter, as $source names a parameter of the page" : undefined;
}

/** A JOIN AS ITS PAGE HOLDS IT, OPENED (`held`, its source's own -- what it reads of the page's parameters `$name`,
 *  never their values now: page-parameters.ts boundIn binds only what is opened), with what opening reconciled
 *  (`opened`, join-open.ts reconciled: a kept column of theirs gone since left out) -- what a grid it opens writes. */
export function heldHow(held: JoinHow, opened: JoinHow): JoinHow {
  return (held.form === 'join' || held.form === 'asOf') && (opened.form === 'join' || opened.form === 'asOf') ? { ...held, keep: opened.keep } : held;
}

/** Why inputs called `names` cannot be picked among, in words, if they cannot: two called the same (a pick names one
 *  by its name). */
export function pickNamesProblem(names: readonly string[]): string | undefined {
  const twice = names.find((n, i) => names.indexOf(n) !== i);
  return twice === undefined ? undefined : `two of its inputs are named ${twice}: a Dynamic Input picks one by its name -- rename one`;
}

/** An append's pick in words: the parameter it reads (`$source`), or "an expression". */
export function pickWords(pick: Lambda): string {
  const e = pick.body[0];
  return e?._type === 'var' ? `$${e.name}` : 'an expression';
}

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

function readRow<Op extends string>(raw: unknown, what: string, ops: readonly Op[]): { left: string; op: Op; right: string; within?: number } {
  const o = object(raw, what, ['left', 'op', 'right', 'within']);
  if (typeof o['op'] !== 'string' || !(ops as readonly string[]).includes(o['op'])) {
    throw new CubeRefusal(`${what}'s comparison is one of ${ops.join(' ')}, not ${JSON.stringify(o['op'])}`);
  }
  // a similar row's edits apart: a whole number from 0, and no other row's -- read as a page's document holds a number
  // (an ExactNumber, written as Python's JSON is read) or as one
  const given = o['within'];
  const within = given instanceof ExactNumber ? Number(given.text) : given;
  if (within !== undefined && o['op'] !== '~') throw new CubeRefusal(`${what} compares by ${o['op']}: only a ~ row is within some edits`);
  if (within !== undefined && (typeof within !== 'number' || !Number.isInteger(within) || within < 0)) {
    throw new CubeRefusal(`${what}'s edits apart are a whole number from 0, not ${given instanceof ExactNumber ? given.text : JSON.stringify(given)}`);
  }
  return { left: name(o['left'], `${what}'s left`), op: o['op'] as Op, right: name(o['right'], `${what}'s right`),
    ...(within !== undefined ? { within: within as number } : {}) };
}

/** A condition read field by field as a join source keeps it (its `on`), or why not (CubeRefusal): a Join's L and R
 *  keep one too (cube-document.ts JoinInput.unmatched). */
export function readJoinCondition(raw: unknown, what: string): JoinCondition {
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
      const o = object(raw, 'a join', ['form', 'kind', 'on', 'prefix', 'keep', 'once', 'score']);
      if (typeof o['kind'] !== 'string' || !(JOIN_KINDS as readonly string[]).includes(o['kind'])) {
        throw new CubeRefusal(`a join's kind is one of ${JOIN_KINDS.join(', ')}, not ${JSON.stringify(o['kind'])}`);
      }
      if (o['once'] !== undefined && o['once'] !== true) throw new CubeRefusal(`a join's once is true or left out, not ${JSON.stringify(o['once'])}`);
      const score = o['score'];
      if (score !== undefined && (typeof score !== 'string' || score.trim() === '')) {
        throw new CubeRefusal(`a join's score is the name of its column, not ${JSON.stringify(score)}`);
      }
      const how: JoinHow = {
        form, kind: o['kind'] as JoinKindWord, on: readJoinCondition(o['on'], "a join's condition"), prefix: readPrefix(o['prefix']),
        keep: readKeep(o['keep']), ...(o['once'] === true ? { once: true as const } : {}), ...(score !== undefined ? { score } : {}),
      };
      const why = onceRefusal(how) ?? scoreRefusal(how);
      if (why !== undefined) throw new CubeRefusal(why);
      return how;
    }
    case 'asOf': {
      const o = object(raw, 'an as-of join', ['form', 'match', 'on', 'prefix', 'keep']);
      return {
        form, match: readRow(o['match'], "an as-of join's match", AS_OF_OPS), on: readJoinCondition(o['on'], "an as-of join's condition"),
        prefix: readPrefix(o['prefix']), keep: readKeep(o['keep']),
      };
    }
    case 'semi':
    case 'anti': {
      const o = object(raw, `a ${form}-join`, ['form', 'on']);
      return { form, on: readJoinCondition(o['on'], `a ${form}-join's condition`) };
    }
    case 'append': {
      const o = object(raw, 'an append', ['form', 'pick']);
      if (o['pick'] === undefined) return { form };
      let pick: Lambda;
      try {
        pick = readLambda(o['pick']);
      } catch (e) {
        if (!(e instanceof ProtocolError)) throw e;
        throw new CubeRefusal(`an append's pick is an expression in Pure: ${e.message}`);
      }
      const why = pickProblem(pick);
      if (why !== undefined) throw new CubeRefusal(why);
      return { form, pick };
    }
    default:
      throw new CubeRefusal(`a join's form is one of ${JOIN_FORMS.join(', ')}, not ${JSON.stringify(form)}`);
  }
}

/** A row's comparison turned round: mine `<` theirs is theirs `>` mine. */
const MIRRORED: Readonly<Record<JoinOp, JoinOp>> = { '==': '==', '!=': '!=', '<': '>', '<=': '>=', '>': '<', '>=': '<=', '~': '~' };

/**
 * WHAT PAIRS A SIDE'S ROW WITH NONE (a Join's L and R: docs/DATACUBE_FLOW_MODE_2026_10_24.md §3.3): the condition an
 * anti-join of that side against the other is on -- the join's own for its Left; for its Right, turned round (each row's
 * columns swapped and its comparison mirrored, a Pure condition's two rows named the other way). Refused in words: a
 * Union (it keeps every row of both), an as-of join (each row of mine has its nearest or none: a join's left kind says
 * it), a join's Pure condition (it names their columns as the join brings them out, prefixed; an anti-join, by their own).
 */
export function unmatchedOn(how: JoinHow, side: 'left' | 'right'): JoinCondition {
  if (how.form === 'append') throw new CubeRefusal('a Union keeps every row of both: none is left unmatched');
  if (how.form === 'asOf') throw new CubeRefusal("an as-of join's rows of mine with no nearest row are its left kind's: it has no Left and Right unmatched");
  const on = how.on;
  if ('rows' in on) {
    return side === 'left' ? on : { rows: on.rows.map((r) => ({ left: r.right, op: MIRRORED[r.op], right: r.left, ...(r.within !== undefined ? { within: r.within } : {}) })) };
  }
  if (how.form === 'join') {
    throw new CubeRefusal('its condition is Pure, naming their columns as the join brings them out: set it by rows to read its Left and Right unmatched');
  }
  const [mine, theirs] = on.lambda.parameters;
  return side === 'left' ? on : { lambda: { ...on.lambda, parameters: [theirs!, mine!] } };
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

/** Why a join cannot pair each of mine with one of theirs (`once`), or undefined: their key is the columns of its
 *  condition's rows, so the condition is rows, at least one, each `==`. */
export function onceRefusal(how: JoinHow): string | undefined {
  if (how.form !== 'join' || how.once !== true) return undefined;
  // a lookup keeps mine: theirs' rows each one of a key, those of no match a right or full join would keep are
  // theirs no more (the roadmap audit of 2026-10-26: theirs' rows of an empty key were one)
  if (how.kind === 'right' || how.kind === 'full') return `one of theirs for each of mine is a lookup of mine: a matching or a left join, not ${how.kind === 'right' ? 'all of theirs' : 'everything'}`;
  return 'rows' in how.on && how.on.rows.length > 0 && how.on.rows.every((r) => r.op === '==') ? undefined
    : 'one of theirs for each of mine takes a key: rows of equal (==) columns';
}

/** The similar row a join's score is of (`score`): its condition's first `~` row, or undefined. */
export function scoredRow(how: JoinHow): JoinRow | undefined {
  return how.form === 'join' && 'rows' in how.on ? how.on.rows.find((r) => r.op === '~') : undefined;
}

/** Why a join cannot score its pairs (`score`), or undefined: a score is of a similar (`~`) row's two texts, each a
 *  value -- matching rows only. */
export function scoreRefusal(how: JoinHow): string | undefined {
  if (how.form !== 'join' || how.score === undefined) return undefined;
  if (scoredRow(how) === undefined) return 'a score is a Fuzzy Match\'s: its condition needs a row of similar text (~)';
  return how.kind === 'inner' ? undefined : 'a score is each matched pair\'s: it takes matching rows only';
}

/** The forms a quick join is made in: all of mine (a lookup), matching only, mine with a match, mine with none. */
export type QuickForm = 'left' | 'inner' | 'semi' | 'anti';

/** A quick join's `how`: on its one key, their columns brought whole (a name mine has prefixed, as the window's default). */
export function quickHow(row: JoinRow, form: QuickForm): JoinHow {
  const on = { rows: [row] };
  return form === 'semi' || form === 'anti' ? { form, on } : { form: 'join', kind: form, on, prefix: DEFAULT_PREFIX, keep: null };
}
