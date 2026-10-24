// JOINING TWO GRIDS' RESULTS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): what a join -- on any condition (four
// kinds; every pair when it names none), as of, my rows that have a match or have none, the rows of both appended --
// becomes as one relation (joinRelation), the names it gives their columns (theirNames), the queries the Join dialog
// runs before it is applied (joinSummary) and their words. Its saved form and reader are join-how.ts's, re-exported.
// No DOM, no planner: tested alone, and planned on DuckDB (test/join-open.test.ts).

import {
  agg, and, cast, collection, derive, eq, fn, from, ge, gt, lambda, le, lit, lt, ne, property, type, variable,
  type JoinKind, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { rowCountLambda } from './query.ts';
import type { JoinCondition, JoinHow, JoinKindWord, JoinRow, JoinSide } from './join-how.ts';

export * from './join-how.ts';

// ---------------------------------------------------------------- what it becomes

const KIND: Readonly<Record<JoinKindWord, JoinKind>> = { inner: 'INNER', left: 'LEFT', right: 'RIGHT', full: 'FULL' };

/**
 * Their columns' names as a join or an as-of join brings them out: each one mine has, prefixed. Whether a name it makes
 * is taken is the compiler's to say, as it types the join.
 */
export function theirNames(mine: readonly string[], theirs: readonly string[], prefix: string): ReadonlyMap<string, string> {
  const taken = new Set(mine);
  return new Map(theirs.map((c) => [c, taken.has(c) ? `${prefix}${c}` : c]));
}

/** A row as a term over mine (`a`) and theirs (`b`, their column named `right`): an equal key that is empty matches no row. */
function rowTerm(row: JoinRow, right: string, [ma, tb]: readonly [string, string] = ['a', 'b']): ValueSpecification {
  const a = property(variable(ma), row.left);
  const b = property(variable(tb), right);
  switch (row.op) {
    // Pure's == matches an empty with an empty: a key matches only when it has a value (§17.2)
    case '==': return and(fn('isNotEmpty', a), eq(a, b));
    // and != pairs an empty with any value: a row pairs only when both have one
    case '!=': return and(fn('isNotEmpty', a), fn('isNotEmpty', b), ne(a, b));
    case '<': return lt(a, b);
    case '<=': return le(a, b);
    case '>': return gt(a, b);
    case '>=': return ge(a, b);
  }
}

/** A condition as a lambda of two rows, their columns read through `named` (their name → the name it has here). */
function conditionLambda(on: JoinCondition, named: (c: string) => string): Lambda {
  if ('lambda' in on) return on.lambda;
  return lambda(['a', 'b'], on.rows.length === 0 ? lit.boolean(true) : and(...on.rows.map((r) => rowTerm(r, named(r.right)))));
}

/** The names a condition gives its two rows. */
const rowsOf = (l: Lambda): [string, string] => l.parameters.map((p) => p.name) as [string, string];

/** Their relation with each name mine has renamed (theirNames). */
function renamed(right: JoinSide, names: ReadonlyMap<string, string>): Relation {
  let rel = from(right.relation);
  for (const [was, now] of names) if (was !== now) rel = rel.rename(was, now);
  return rel;
}

/** A semi- or anti-join: my rows that have (or have not) a row of theirs the condition pairs mine with. */
function semi(left: JoinSide, theirs: Relation, condition: Lambda, keep: 'isNotEmpty' | 'isEmpty'): Relation {
  const [a, b] = rowsOf(condition);
  return from(left.relation).filter(lambda([a], fn(keep, theirs.filter(lambda([b], ...condition.body)).node)));
}

/** The as-of join's match, as a lambda of two rows named as its condition names them (`a`, `b` when it has none). */
function matchLambda(how: Extract<JoinHow, { form: 'asOf' }>, named: (c: string) => string, rows: readonly [string, string]): Lambda {
  return lambda([...rows], rowTerm(how.match, named(how.match.right), rows));
}

/** An as-of join's condition, or none when it has no rows (the match alone pairs). */
function asOfCondition(how: Extract<JoinHow, { form: 'asOf' }>, named: (c: string) => string): Lambda | undefined {
  return 'rows' in how.on && how.on.rows.length === 0 ? undefined : conditionLambda(how.on, named);
}

/**
 * WHAT A JOIN BECOMES, as one relation (§17.2): their names mine has renamed with the prefix, then the join or as-of
 * join, then -- when it keeps some of theirs -- my columns and those selected; a semi- or anti-join a filter of mine by
 * theirs; an append each side extended by the columns only the other has, in one order, concatenated. Every name goes
 * in as written: a column an input lacks, a name taken twice, an append's column of two types are the compiler's to
 * refuse, as it types what this writes.
 */
export function joinRelation(left: JoinSide, right: JoinSide, how: JoinHow): ValueSpecification {
  const mine = left.columns.map((c) => c.name);
  switch (how.form) {
    case 'join':
    case 'asOf': {
      const names = theirNames(mine, right.columns.map((c) => c.name), how.prefix);
      const named = (c: string): string => names.get(c) ?? c;
      const theirs = renamed(right, names);
      let joined: Relation;
      if (how.form === 'join') {
        joined = from(left.relation).join(theirs, KIND[how.kind], conditionLambda(how.on, named));
      } else {
        const on = asOfCondition(how, named);
        joined = from(left.relation).asOfJoin(theirs, matchLambda(how, named, on ? rowsOf(on) : ['a', 'b']), on);
      }
      return (how.keep === null ? joined : joined.select([...mine, ...how.keep.map(named)])).node;
    }
    case 'semi':
    case 'anti':
      return semi(left, from(right.relation), conditionLambda(how.on, (c) => c), how.form === 'semi' ? 'isNotEmpty' : 'isEmpty').node;
    case 'append':
      return appended(left, right).node;
  }
}

/**
 * An append: mine in my order, then theirs I lack; each side extended by the columns only the other has -- `[]` of the
 * type the compiler gave that column -- and selected in that order when its own differs.
 */
function appended(left: JoinSide, right: JoinSide): Relation {
  const mine = new Set(left.columns.map((c) => c.name));
  const all = [...left.columns, ...right.columns.filter((c) => !mine.has(c.name))];
  const order = all.map((c) => c.name);
  const side = (s: JoinSide): Relation => {
    const own = new Set(s.columns.map((c) => c.name));
    const lacking = all.filter((c) => !own.has(c.name));
    let rel = from(s.relation);
    if (lacking.length > 0) rel = rel.extend(lacking.map((c) => derive(c.name, lambda(['x'], cast(collection([]), type(c.type))))));
    const now = [...s.columns.map((c) => c.name), ...lacking.map((c) => c.name)];
    return now.every((c, i) => c === order[i]) ? rel : rel.select(order);
  };
  return side(left).concatenate(side(right));
}

// ---------------------------------------------------------------- what it will do, before it is applied

/** The columns a summary's repeated-keys query answers in. */
export const REPEATED_KEYS = 'keys';
export const MOST_REPEATS = 'most';

/**
 * THE JOIN DIALOG'S QUESTIONS (§17.5), each a query the compiler plans: my rows; how many of them find a match (a join,
 * an as-of join, a semi- or anti-join); the rows after a join (more than mine: a key repeats on their side); for a join
 * on equal keys alone, how many of their keys repeat and the most times one does; an append's rows of theirs. Each
 * count answers in one row's `TOTAL_ROWS_COLUMN` (query.ts rowCountLambda); repeated keys in `keys` and `most`.
 */
export interface JoinSummaryQueries {
  readonly mine: Lambda;
  readonly matched?: Lambda;
  readonly joined?: Lambda;
  readonly repeated?: Lambda;
  readonly theirs?: Lambda;
}

export function joinSummary(left: JoinSide, right: JoinSide, how: JoinHow): JoinSummaryQueries {
  const mine = rowCountLambda(from(left.relation));
  switch (how.form) {
    case 'join':
    case 'asOf': {
      const names = theirNames(left.columns.map((c) => c.name), right.columns.map((c) => c.name), how.prefix);
      const named = (c: string): string => names.get(c) ?? c;
      const theirs = renamed(right, names);
      // an as-of join's match is part of what pairs a row: matched is "has a nearest row"
      let pairs: Lambda;
      if (how.form === 'join') {
        pairs = conditionLambda(how.on, named);
      } else {
        const on = asOfCondition(how, named);
        const match = matchLambda(how, named, on ? rowsOf(on) : ['a', 'b']);
        // the condition's statements kept (a `let` before its value), its value and the match both asked
        pairs = on ? lambda([...rowsOf(on)], ...on.body.slice(0, -1), and(...match.body, on.body[on.body.length - 1]!)) : match;
      }
      const matched = rowCountLambda(semi(left, theirs, pairs, 'isNotEmpty'));
      if (how.form === 'asOf') return { mine, matched };
      const joined = rowCountLambda(from(joinRelation(left, right, how)));
      const keys = 'rows' in how.on && how.on.rows.length > 0 && how.on.rows.every((r) => r.op === '==')
        ? [...new Set(how.on.rows.map((r) => r.right))] : [];
      return { mine, matched, joined, ...(keys.length > 0 ? { repeated: repeatedKeys(right, keys) } : {}) };
    }
    case 'semi':
    case 'anti':
      return { mine, matched: rowCountLambda(semi(left, from(right.relation), conditionLambda(how.on, (c) => c), 'isNotEmpty')) };
    case 'append':
      return { mine, theirs: rowCountLambda(from(right.relation)) };
  }
}

/** Their keys (each with a value: an empty one matches nothing) held by more than one row: how many, and the most rows one has. */
function repeatedKeys(right: JoinSide, keys: readonly string[]): Lambda {
  const counted = '__n__';
  const one = '__one__';
  return from(right.relation)
    .filter(lambda(['x'], and(...keys.map((k) => fn('isNotEmpty', property(variable('x'), k))))))
    .groupBy([...keys], [agg(counted, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y'))))])
    .filter(lambda(['x'], gt(property(variable('x'), counted), lit.integer(1))))
    .extend([derive(one, lambda(['x'], lit.string('1')))])
    .groupBy([one], [
      agg(REPEATED_KEYS, lambda(['x'], lit.integer(1)), lambda(['y'], fn('count', variable('y')))),
      agg(MOST_REPEATS, lambda(['x'], property(variable('x'), counted)), lambda(['y'], fn('max', variable('y')))),
    ])
    .lambda();
}

/** The dialog's questions answered: each count, and their keys that repeat (how many, the most rows one has). */
export interface JoinSummaryCounts {
  readonly mine: number;
  readonly matched?: number;
  readonly joined?: number;
  readonly theirs?: number;
  readonly repeated?: { readonly keys: number; readonly most: number };
}

const plural = (n: number, one: string, many = `${one}s`): string => `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`;

/** A count a form's questions always answer: its absence is a summary asked of another form. */
function asked(n: number | undefined, what: string): number {
  if (n === undefined) throw new Error(`a join's summary was not asked how many rows ${what}`);
  return n;
}

/** What a join will do, in words, a line each (§17.5): what the dialog shows before it is applied. */
export function summaryWords(how: JoinHow, c: JoinSummaryCounts): string[] {
  const out: string[] = [];
  if (how.form === 'append') {
    const theirs = asked(c.theirs, 'they have');
    return [`${plural(c.mine, 'row')} of mine and ${plural(theirs, 'row')} of theirs: ${plural(c.mine + theirs, 'row')}.`];
  }
  const matched = asked(c.matched, 'find a match');
  const unmatched = c.mine - matched;
  switch (how.form) {
    case 'semi':
    case 'anti':
      out.push(`${plural(matched, 'row')} of my ${plural(c.mine, 'row')} ${matched === 1 ? 'has' : 'have'} a match, ${plural(unmatched, 'row')} none.`);
      out.push(`It keeps ${plural(how.form === 'semi' ? matched : unmatched, 'row')}, each once.`);
      return out;
    case 'asOf':
      out.push(`${plural(matched, 'row')} of my ${plural(c.mine, 'row')} ${matched === 1 ? 'finds' : 'find'} a nearest row, ${plural(unmatched, 'row')} none (theirs left empty).`);
      out.push('Each of my rows stays once.');
      return out;
    case 'join': {
      out.push(`${plural(matched, 'row')} of my ${plural(c.mine, 'row')} ${matched === 1 ? 'finds' : 'find'} a match, ${plural(unmatched, 'row')} ${unmatched === 1 ? 'does' : 'do'} not.`);
      const joined = asked(c.joined, 'come out of it');
      // more rows than mine are a row of mine repeated -- where only mine are kept (a right or full join keeps theirs too)
      out.push(joined > c.mine && (how.kind === 'inner' || how.kind === 'left')
        ? `${plural(joined, 'row')} after it: more than mine -- a row of mine repeats once for each of theirs it matches.`
        : how.kind === 'right' || how.kind === 'full'
          ? `${plural(joined, 'row')} after it, theirs with no match of mine among them.`
          : `${plural(joined, 'row')} after it.`);
      if (c.repeated && c.repeated.keys > 0) {
        out.push(`${plural(c.repeated.keys, 'key')} of theirs ${c.repeated.keys === 1 ? 'appears' : 'appear'} more than once (at most ${c.repeated.most} times): `
          + 'the rows of mine with it repeat, and totals count them again.');
      }
      return out;
    }
  }
}

// ---------------------------------------------------------------- in words

const KIND_WORDS: Readonly<Record<JoinKindWord, string>> = {
  inner: 'matching rows only', left: 'all of mine', right: 'all of theirs', full: 'everything',
};

/** A row in words: `cust = id`. */
export function rowWords(row: JoinRow): string {
  return `${row.left} ${row.op === '==' ? '=' : row.op} ${row.right}`;
}

/** A join's grid's title: `trades ⋈ customers`, `trades + more`, `trades with customers`, `trades without customers`. */
export function joinTitle(how: JoinHow, left: string, right: string): string {
  switch (how.form) {
    case 'join': return `${left} ⋈ ${right}`;
    case 'asOf': return `${left} ⋈ ${right} as of`;
    case 'semi': return `${left} with ${right}`;
    case 'anti': return `${left} without ${right}`;
    case 'append': return `${left} + ${right}`;
  }
}

/** What a join does, in words: `all of mine, on cust = id`; `every pair`; `nearest at = at, on sym = sym`. */
export function joinWords(how: JoinHow): string {
  const on = (c: JoinCondition): string => ('lambda' in c ? 'a condition in Pure'
    : c.rows.length === 0 ? 'every pair' : c.rows.map(rowWords).join(' and '));
  switch (how.form) {
    case 'join': return `${KIND_WORDS[how.kind]}, on ${on(how.on)}`;
    case 'asOf': return `nearest ${rowWords(how.match)}${'rows' in how.on && how.on.rows.length === 0 ? '' : `, on ${on(how.on)}`}`;
    case 'semi': return `my rows with a match, on ${on(how.on)}`;
    case 'anti': return `my rows with no match, on ${on(how.on)}`;
    case 'append': return 'the rows of both, columns by name';
  }
}

/**
 * Whether a join would repeat rows of mine -- more rows after it than mine, where only mine are kept (all of mine, or
 * matching only): a quick join that would opens the window on it instead, its warning shown before anything is made.
 */
export function joinRepeats(how: JoinHow, c: JoinSummaryCounts): boolean {
  return how.form === 'join' && (how.kind === 'left' || how.kind === 'inner') && (c.joined ?? 0) > c.mine;
}
