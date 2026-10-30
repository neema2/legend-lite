// JOINING TWO GRIDS' RESULTS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): what a join -- on any condition (four
// kinds; every pair when it names none), as of, my rows that have a match or have none, the rows of both appended --
// becomes as one relation (joinRelation), the names it gives their columns (theirNames), the queries the Join dialog
// runs before it is applied (joinSummary) and their words. Its saved form and reader are join-how.ts's, re-exported.
// No DOM, no planner: tested alone, and planned on DuckDB (test/join-open.test.ts).

import {
  agg, and, asc, cast, collection, derive, divide, eq, fn, from, ge, gt, lambda, le, lit, lt, minus, ne, over, property, times, type, variable,
  type JoinKind, type Lambda, type Relation, type ValueSpecification,
} from '../../pure-protocol/src/index.ts';
import { familyOf, plainType } from '../../engine-client/src/types.ts';
import { rowCountLambda } from './query.ts';
import { CubeRefusal } from './snapshot.ts';
import { ORDERED } from './unique.ts';
import {
  isEveryPair, onceRefusal, pickNamesProblem, scoredRow, scoreRefusal, SIMILAR_WITHIN, type JoinCondition, type JoinHow, type JoinKindWord, type JoinRow,
  type JoinSide,
} from './join-how.ts';

export * from './join-how.ts';
export { joinWords, rowWords } from './join-words.ts';

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
    // similar text: both a value, at most so many edits apart -- the database's distance (a flow's Fuzzy Match)
    case '~': return and(fn('isNotEmpty', a), fn('isNotEmpty', b),
      le(fn('levenshteinDistance', fn('toOne', a), fn('toOne', b)), lit.integer(row.within ?? SIMILAR_WITHIN)));
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

/**
 * Their rows cut to the first of each key (a join's `once`, C13): numbered within each key by the database -- in an order of
 * their every other column that sorts, rising, so the first is the same every time (Unique's ties, C14) -- the first kept,
 * its number dropped. A key that is empty is kept as it is: it matches nothing (rowTerm).
 */
function firstOfEach(right: JoinSide, keys: readonly string[]): JoinSide {
  const names = new Set(right.columns.map((c) => c.name));
  let number = 'OnceRow';
  while (names.has(number)) number = `_${number}`;
  const ties = right.columns.filter((c) => !keys.includes(c.name) && ORDERED.has(familyOf(c.type) ?? '')).map((c) => asc(c.name));
  const relation = from(right.relation)
    .extend(derive(number, lambda(['p', 'w', 'r'], fn('rowNumber', variable('p'), variable('r')))), over(keys, ties))
    .filter(lambda(['x'], eq(property(variable('x'), number), lit.integer(1))))
    .select(right.columns.map((c) => c.name))
    .node;
  return { ...right, relation };
}

/** Their columns a join's one of theirs is keyed by (`once`): their side of its `==` rows, each once, in order. */
const onceKeys = (on: JoinCondition): string[] => ('rows' in on ? [...new Set(on.rows.map((r) => r.right))] : []);

/** A semi- or anti-join: my rows that have (or have not) a row of theirs the condition pairs mine with. */
function semi(left: JoinSide, theirs: Relation, condition: Lambda, keep: 'isNotEmpty' | 'isEmpty'): Relation {
  const [a, b] = rowsOf(condition);
  return from(left.relation).filter(lambda([a], fn(keep, theirs.filter(lambda([b], ...condition.body)).node)));
}

/** The as-of join's match, as a lambda of two rows named as its condition names them (`a`, `b` when it has none). */
function matchLambda(how: Extract<JoinHow, { form: 'asOf' }>, named: (c: string) => string, rows: readonly [string, string]): Lambda {
  return lambda([...rows], rowTerm(how.match, named(how.match.right), rows));
}

/**
 * A pair's likeness on a similar row (a join's `score`, a Fuzzy Match's): `round(100 * (1 - distance / longer))`, its
 * longer text's length at least 1 -- over the joined row `$x`, their column named as it comes out (`right`). The database
 * measures and rounds.
 */
function likeness(row: JoinRow, right: string): Lambda {
  const a = fn('toOne', property(variable('x'), row.left));
  const b = fn('toOne', property(variable('x'), right));
  const longer = fn('max', fn('max', fn('length', a), fn('length', b)), lit.integer(1));
  return lambda(['x'], fn('round', times(lit.integer(100), minus(lit.integer(1), divide(fn('levenshteinDistance', a, b), longer)))));
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
export function joinRelation(left: JoinSide, right: JoinSide, how: JoinHow, more: readonly JoinSide[] = []): ValueSpecification {
  if (more.length > 0 && how.form !== 'append') throw new Error(`a ${how.form} joins two inputs, not ${2 + more.length}: only an append takes more`);
  const mine = left.columns.map((c) => c.name);
  switch (how.form) {
    case 'join':
    case 'asOf': {
      const names = theirNames(mine, right.columns.map((c) => c.name), how.prefix);
      const named = (c: string): string => names.get(c) ?? c;
      // one of theirs for each of mine: their rows cut to the first of each key before they are named and joined
      const why = onceRefusal(how) ?? scoreRefusal(how);
      if (why !== undefined) throw new CubeRefusal(why);
      const theirs = renamed(how.form === 'join' && how.once === true ? firstOfEach(right, onceKeys(how.on)) : right, names);
      let joined: Relation;
      // a Fuzzy Match's score: each pair's likeness, after their columns
      const scored = how.form === 'join' && how.score !== undefined ? { name: how.score, row: scoredRow(how)! } : undefined;
      if (how.form === 'join') {
        joined = from(left.relation).join(theirs, KIND[how.kind], conditionLambda(how.on, named));
        if (scored !== undefined) joined = joined.extend([derive(scored.name, likeness(scored.row, named(scored.row.right)))]);
      } else {
        const on = asOfCondition(how, named);
        joined = from(left.relation).asOfJoin(theirs, matchLambda(how, named, on ? rowsOf(on) : ['a', 'b']), on);
      }
      return (how.keep === null ? joined : joined.select([...mine, ...how.keep.map(named), ...(scored !== undefined ? [scored.name] : [])])).node;
    }
    case 'semi':
    case 'anti':
      return semi(left, from(right.relation), conditionLambda(how.on, (c) => c), how.form === 'semi' ? 'isNotEmpty' : 'isEmpty').node;
    case 'append':
      return appended([left, right, ...more], how.pick).node;
  }
}

/**
 * An append of any number (a Union's, JoinSource.more): the first's columns in its order, then each next one's it lacks,
 * in turn; each side extended by the columns only the others have -- `[]` of the type the compiler gave that column (the
 * first side to have it) -- and selected in that order when its own differs; concatenated in the sides' order. A name
 * two sides type differently is refused in words: the compiler unites an Integer and a Float as the first's type, its
 * rows' fractions under it (the correctness audit's 6, 2026-10-26) -- a Select retypes one side first.
 */
function appended(sides: readonly JoinSide[], pick?: Lambda): Relation {
  // Dynamic Input: each side's rows those of the one its pick names -- `<pick> == '<its name>'`, its pick a parameter's
  // value bound where the source is resolved, so the database keeps one side's rows and reads none of the others'
  if (pick !== undefined) {
    const names = sides.map((s) => s.name);
    const twice = pickNamesProblem(names);
    if (twice !== undefined) throw new CubeRefusal(twice);
    // its pick a value now (its parameter's, bound): one of its inputs' names, compared as spelled
    const now = pick.body[0];
    if (now?._type === 'string' && !names.includes(now.value)) {
      // an input renamed or relinked since its parameter was made: said, with what to do
      throw new CubeRefusal(`it picks ${JSON.stringify(now.value)}, none of its inputs: ${names.join(', ')} -- set its parameter to one of them, or pick again in its settings (a new parameter of their names)`);
    }
    const picked = (s: JoinSide): JoinSide => ({ ...s, relation: from(s.relation).filter(lambda(['x'], eq(pick.body[0]!, lit.string(s.name)))).node });
    return appended(sides.map(picked));
  }
  const all: (JoinSide['columns'][number] & { readonly side: string })[] = [];
  for (const s of sides) {
    for (const c of s.columns) {
      const had = all.find((a) => a.name === c.name);
      if (had === undefined) all.push({ ...c, side: s.name });
      else if (plainType(had.type) !== plainType(c.type)) {
        throw new CubeRefusal(`'${c.name}' is ${plainType(had.type)} in ${had.side} and ${plainType(c.type)} in ${s.name}: `
          + "a Union's column is of one type -- retype one of them with a Select first");
      }
    }
  }
  const order = all.map((c) => c.name);
  const side = (s: JoinSide): Relation => {
    const own = new Set(s.columns.map((c) => c.name));
    const lacking = all.filter((c) => !own.has(c.name));
    let rel = from(s.relation);
    if (lacking.length > 0) rel = rel.extend(lacking.map((c) => derive(c.name, lambda(['x'], cast(collection([]), type(c.type))))));
    const now = [...s.columns.map((c) => c.name), ...lacking.map((c) => c.name)];
    return now.every((c, i) => c === order[i]) ? rel : rel.select(order);
  };
  return sides.slice(1).reduce((rel, s) => rel.concatenate(side(s)), side(sides[0]!));
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
      // every pair (an Append Fields): their rows counted too, by the database -- never mine's quotient
      if (isEveryPair(how)) return { mine, matched, joined, theirs: rowCountLambda(from(right.relation)) };
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
  // every pair (an Append Fields): each of mine beside each of theirs, no match asked of it
  if (isEveryPair(how)) {
    const joined = asked(c.joined, 'come out of it');
    const theirs = asked(c.theirs, 'they have');
    if (c.mine === 0) return ['I have no rows: none after it.'];
    return [`Each of my ${plural(c.mine, 'row')} beside ${theirs === 1 ? 'the one row' : `each of the ${plural(theirs, 'row')}`} of theirs: ${plural(joined, 'row')}.`];
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
        const more = `${plural(c.repeated.keys, 'key')} of theirs ${c.repeated.keys === 1 ? 'appears' : 'appear'} more than once (at most ${c.repeated.most} times): `;
        out.push(more + (how.once === true
          ? 'each row of mine with it takes the first of theirs, once.'
          : 'the rows of mine with it repeat, and totals count them again.'));
      }
      return out;
    }
  }
}

// ---------------------------------------------------------------- in words

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

/**
 * Whether a join would repeat rows of mine -- more rows after it than mine, where only mine are kept (all of mine, or
 * matching only): a quick join that would opens the window on it instead, its warning shown before anything is made.
 */
export function joinRepeats(how: JoinHow, c: JoinSummaryCounts): boolean {
  return how.form === 'join' && (how.kind === 'left' || how.kind === 'inner') && (c.joined ?? 0) > c.mine;
}
