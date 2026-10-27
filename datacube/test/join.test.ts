// Joining two grids' results (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17; src/join.ts): a join source's `how` read
// strictly, what each form becomes as one relation (their names mine has renamed first; an equal key with no value
// matching nothing; their columns not kept selected away; semi/anti a filter by theirs; an append's sides extended and
// ordered), every name written as given -- what is wrong with one the compiler's to say, as it types it
// (test/join-open.test.ts) -- the dialog's questions, and its words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { accessor, readLambda, toJson, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import {
  joinRelation, joinRepeats, joinSummary, joinTitle, joinWords, quickHow, quickJoins, readJoinHow, suggestedKeys, summaryWords, theirNames,
  type JoinHow, type JoinSide,
} from '../src/join.ts';
import { CubeRefusal } from '../src/snapshot.ts';

/** A tree as the Pure it is: enough of the grammar to read a join's relation in an assertion. */
function pure(n: ValueSpecification | undefined): string {
  if (n === undefined) return '?';
  const node = n as unknown as Record<string, unknown>;
  switch (node['_type']) {
    case 'func': {
      const name = node['function'] as string;
      const [first, ...rest] = node['parameters'] as ValueSpecification[];
      const op = { equal: '==', lessThan: '<', lessThanEqual: '<=', greaterThan: '>', greaterThanEqual: '>=', and: '&&' }[name];
      if (op && rest.length === 1) return `(${pure(first)} ${op} ${pure(rest[0])})`;
      if (name === 'not') return `!${pure(first)}`;
      return `${pure(first)}->${name}(${rest.map(pure).join(', ')})`;
    }
    case 'property': {
      const [owner] = node['parameters'] as ValueSpecification[];
      const o = owner as unknown as Record<string, unknown>;
      if (o['_type'] === 'packageableElementPtr') return `${String(o['fullPath']).split('::').pop()}.${String(node['property'])}`;
      return `${pure(owner)}.${String(node['property'])}`;
    }
    case 'var': return `$${String(node['name'])}`;
    case 'lambda': return `{${(node['parameters'] as { name: string }[]).map((p) => p.name).join(', ')} | ${(node['body'] as ValueSpecification[]).map(pure).join('; ')}}`;
    case 'string': return `'${String(node['value'])}'`;
    case 'integer': case 'boolean': return String(node['value']);
    case 'collection': return `[${(node['values'] as ValueSpecification[]).map(pure).join(', ')}]`;
    case 'genericTypeInstance': return `@${String(((node['genericType'] as Record<string, Record<string, unknown>>)['rawType']!)['fullPath'])}`;
    case 'classInstance': {
      const value = node['value'] as Record<string, unknown>;
      if (node['type'] === '>') return `#>{${(value['path'] as string[]).join('.')}}#`;
      if (node['type'] === 'colSpec') return `~${String(value['name'])}`;
      const specs = value['colSpecs'] as { name: string; function1?: ValueSpecification; function2?: ValueSpecification }[];
      return `~[${specs.map((s) => [s.name, s.function1 && pure(s.function1), s.function2 && pure(s.function2)].filter(Boolean).join(':')).join(', ')}]`;
    }
    default: return `<${String(node['_type'])}>`;
  }
}

const side = (name: string, columns: [string, string][]): JoinSide => ({
  name, relation: accessor('local::DB', name), columns: columns.map(([n, type]) => ({ name: n, type })),
});
const TRADES = side('trades', [['id', 'Integer'], ['cust', 'Integer'], ['sym', 'String'], ['at', 'DateTime']]);
const CUSTOMERS = side('customers', [['id', 'Integer'], ['region', 'String']]);
const PRICES = side('prices', [['sym', 'String'], ['at', 'DateTime'], ['px', 'Float']]);
const T = '#>{local::DB.trades}#';
const C = '#>{local::DB.customers}#';

const join = (over: Partial<Extract<JoinHow, { form: 'join' }>> = {}): JoinHow => ({
  form: 'join', kind: 'left', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] }, prefix: 'c_', keep: null, ...over,
});
const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

describe('a join source\'s how, read strictly', () => {
  it('reads each form, and what it writes reads back the same', () => {
    const forms: JoinHow[] = [
      join(),
      join({ kind: 'full', on: { rows: [] }, keep: ['region'] }),
      { form: 'asOf', match: { left: 'at', op: '>=', right: 'at' }, on: { rows: [{ left: 'sym', op: '==', right: 'sym' }] }, prefix: 'p_', keep: null },
      { form: 'semi', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] } },
      { form: 'anti', on: { rows: [{ left: 'cust', op: '<', right: 'id' }] } },
      { form: 'append' },
    ];
    for (const how of forms) assert.deepEqual(readJoinHow(JSON.parse(JSON.stringify(how))), how);
  });

  it('reads a condition in Pure, its two rows named apart', () => {
    const lambda = readLambda({ _type: 'lambda', parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 'c' }], body: [{ _type: 'boolean', value: true }] });
    const how = readJoinHow(JSON.parse(toJson(join({ on: { lambda } }))));
    assert.ok(how.form === 'join' && 'lambda' in how.on && how.on.lambda.parameters.length === 2);
    const one = { _type: 'lambda', parameters: [{ _type: 'var', name: 't' }], body: [{ _type: 'boolean', value: true }] };
    refused(() => readJoinHow({ ...join(), on: { lambda: one } }), /two rows, mine and theirs/);
    const same = { ...one, parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 't' }] };
    refused(() => readJoinHow({ ...join(), on: { lambda: same } }), /by two names/);
    refused(() => readJoinHow({ ...join(), on: { lambda: { _type: 'nope' } } }), /lambda:/);
  });

  it('refuses, in words, what it cannot read', () => {
    refused(() => readJoinHow({ form: 'cross' }), /one of join, asOf, semi, anti, append/);
    refused(() => readJoinHow({ ...join(), kind: 'outer' }), /kind is one of inner, left, right, full/);
    refused(() => readJoinHow({ ...join(), extra: 1 }), /not extra/);
    refused(() => readJoinHow({ ...join(), on: { rows: [{ left: 'a', op: '=', right: 'b' }] } }), /comparison is one of == != < <= > >= ~/);
    refused(() => readJoinHow({ ...join(), on: { rows: [{ left: 'a', op: '==', right: 'b', within: 1 }] } }), /only a ~ row is within some edits/);
    refused(() => readJoinHow({ ...join(), on: { rows: [{ left: 'a', op: '~', right: 'b', within: -1 }] } }), /a whole number from 0/);
    refused(() => readJoinHow({ ...join(), on: { rows: [], lambda: {} } }), /rows or a lambda, one of them/);
    refused(() => readJoinHow({ ...join(), on: {} }), /rows or a lambda, one of them/);
    refused(() => readJoinHow({ ...join(), prefix: '' }), /prefix starts a column's name/);
    refused(() => readJoinHow({ ...join(), keep: 'a' }), /a list, or null for all/);
    refused(() => readJoinHow({ form: 'asOf', match: { left: 'at', op: '==', right: 'at' }, on: { rows: [] }, prefix: 'p_', keep: null }), /one of >= <= > </);
    refused(() => readJoinHow({ form: 'append', on: { rows: [] } }), /an append takes form, not on/);
    refused(() => readJoinHow({ ...join(), on: { rows: [{ left: '', op: '==', right: 'b' }] } }), /names a column/);
  });
});

describe('what a join becomes', () => {
  it('renames their names mine has, then joins: an equal key with no value matches no row', () => {
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, join())),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.LEFT, {a, b | ($a.cust->isNotEmpty() && ($a.cust == $b.c_id))})`);
  });

  it('takes each kind, any comparison, rows ANDed; no rows is every pair', () => {
    const how = join({ kind: 'inner', on: { rows: [{ left: 'cust', op: '>=', right: 'id' }, { left: 'sym', op: '!=', right: 'region' }] } });
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, how)),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.INNER, {a, b | (($a.cust >= $b.c_id) && (($a.sym->isNotEmpty() && $b.region->isNotEmpty()) && !($a.sym == $b.region)))})`);
    for (const [kind, word] of [['right', 'RIGHT'], ['full', 'FULL']] as const) {
      assert.match(pure(joinRelation(TRADES, CUSTOMERS, join({ kind }))), new RegExp(`JoinKind\\.${word}`));
    }
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, join({ on: { rows: [] } }))),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.LEFT, {a, b | true})`);
  });

  it('similar text (~, a Fuzzy Match): both a value, at most so many edits apart by the database\'s distance', () => {
    const fuzzy = join({ kind: 'inner', on: { rows: [{ left: 'sym', op: '~', right: 'region', within: 1 }] } });
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, fuzzy)),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.INNER, {a, b | (($a.sym->isNotEmpty() && $b.region->isNotEmpty()) && ($a.sym->toOne()->levenshteinDistance($b.region->toOne()) <= 1))})`);
    assert.deepEqual(readJoinHow(JSON.parse(JSON.stringify(fuzzy))), fuzzy);
    assert.equal(joinWords(fuzzy), 'matching rows only, on sym ≈ region (within 1)');
    // not said: two edits
    assert.match(pure(joinRelation(TRADES, CUSTOMERS, join({ on: { rows: [{ left: 'sym', op: '~', right: 'region' }] } }))), /<= 2\)/);
  });

  it('selects away their columns not kept, after the join (a key it joined on included)', () => {
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, join({ keep: ['region'] }))),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.LEFT, {a, b | ($a.cust->isNotEmpty() && ($a.cust == $b.c_id))})->select(~[id, cust, sym, at, region])`);
  });

  it('takes a condition in Pure as written, their names as they come out', () => {
    const lambda = readLambda({
      _type: 'lambda', parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 'c' }],
      body: [{ _type: 'func', function: 'equal', parameters: [
        { _type: 'property', property: 'cust', parameters: [{ _type: 'var', name: 't' }] },
        { _type: 'property', property: 'c_id', parameters: [{ _type: 'var', name: 'c' }] },
      ] }],
    });
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, join({ on: { lambda } }))),
      `${T}->join(${C}->rename(~id, ~c_id), JoinKind.LEFT, {t, c | ($t.cust == $c.c_id)})`);
  });

  it('writes every name as given: one an input lacks, or a prefix that makes a name taken, is the compiler\'s to refuse', () => {
    assert.match(pure(joinRelation(TRADES, CUSTOMERS, join({ on: { rows: [{ left: 'nope', op: '==', right: 'gone' }] } }))),
      /\{a, b \| \(\$a\.nope->isNotEmpty\(\) && \(\$a\.nope == \$b\.gone\)\)\}/);
    assert.match(pure(joinRelation(TRADES, CUSTOMERS, join({ keep: ['gone', 'id'] }))), /->select\(~\[id, cust, sym, at, gone, c_id\]\)$/);
    // their id prefixed onto a c_id they have: both written, the rename the compiler refuses
    assert.deepEqual([...theirNames(['id'], ['id', 'c_id'], 'c_')], [['id', 'c_id'], ['c_id', 'c_id']]);
    assert.deepEqual([...theirNames(['id'], ['id', 'region'], 'their ')], [['id', 'their id'], ['region', 'region']]);
  });

  it('as of: the nearest of theirs by the match, among those the condition keeps', () => {
    const how: JoinHow = { form: 'asOf', match: { left: 'at', op: '>=', right: 'at' }, on: { rows: [{ left: 'sym', op: '==', right: 'sym' }] }, prefix: 'p_', keep: null };
    assert.equal(pure(joinRelation(TRADES, PRICES, how)),
      `${T}->asOfJoin(#>{local::DB.prices}#->rename(~sym, ~p_sym)->rename(~at, ~p_at), {a, b | ($a.at >= $b.p_at)}, {a, b | ($a.sym->isNotEmpty() && ($a.sym == $b.p_sym))})`);
    assert.equal(pure(joinRelation(TRADES, PRICES, { ...how, on: { rows: [] }, keep: ['px'] })),
      `${T}->asOfJoin(#>{local::DB.prices}#->rename(~sym, ~p_sym)->rename(~at, ~p_at), {a, b | ($a.at >= $b.p_at)})->select(~[id, cust, sym, at, px])`);
    // a condition in Pure names the two rows: the match is written over the same names
    const lambda = readLambda({ _type: 'lambda', parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 'p' }], body: [{ _type: 'boolean', value: true }] });
    assert.equal(pure(joinRelation(TRADES, PRICES, { ...how, on: { lambda } })),
      `${T}->asOfJoin(#>{local::DB.prices}#->rename(~sym, ~p_sym)->rename(~at, ~p_at), {t, p | ($t.at >= $p.p_at)}, {t, p | true})`);
    assert.match(pure(joinSummary(TRADES, PRICES, { ...how, on: { lambda } }).matched!.body[0]), /\{p \| \(\(\$t\.at >= \$p\.p_at\) && true\)\}/);
    // a condition of several statements (a let before its value): each kept, the match asked with its value
    const two = readLambda({ _type: 'lambda', parameters: [{ _type: 'var', name: 't' }, { _type: 'var', name: 'p' }], body: [
      { _type: 'func', function: 'letFunction', parameters: [{ _type: 'string', value: 'k' }, { _type: 'boolean', value: true }] },
      { _type: 'var', name: 'k' }] });
    const matched = pure(joinSummary(TRADES, PRICES, { ...how, on: { lambda: two } }).matched!.body[0]);
    assert.match(matched, /\{p \| 'k'->letFunction\(true\); \(\(\$t\.at >= \$p\.p_at\) && \$k\)\}/);
  });

  it('semi and anti: my rows filtered by theirs, by their own names', () => {
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, { form: 'semi', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] } })),
      `${T}->filter({a | ${C}->filter({b | ($a.cust->isNotEmpty() && ($a.cust == $b.id))})->isNotEmpty()})`);
    assert.equal(pure(joinRelation(TRADES, CUSTOMERS, { form: 'anti', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] } })),
      `${T}->filter({a | ${C}->filter({b | ($a.cust->isNotEmpty() && ($a.cust == $b.id))})->isEmpty()})`);
  });

  it('append: each side extended by the columns it lacks, in one order', () => {
    const more = side('more', [['sym', 'String'], ['id', 'Integer'], ['qty', 'Float']]);
    assert.equal(pure(joinRelation(TRADES, more, { form: 'append' })),
      `${T}->extend(~[qty:{x | []->cast(@Float)}])->concatenate(#>{local::DB.more}#->extend(~[cust:{x | []->cast(@Integer)}, at:{x | []->cast(@DateTime)}])->select(~[id, cust, sym, at, qty]))`);
    // the same columns in the same order: neither side touched
    assert.equal(pure(joinRelation(TRADES, TRADES, { form: 'append' })), `${T}->concatenate(${T})`);
    // a name of two types, refused in words: the compiler unites an Integer and a Float as the first's type, its rows'
    // fractions under it (the correctness audit's 6, 2026-10-26); a precise type's plain one is its type
    refused(() => joinRelation(TRADES, side('bad', [['id', 'String']]), { form: 'append' }),
      /^'id' is Integer in trades and String in bad: a Union's column is of one type -- retype one of them with a Select first$/);
    refused(() => joinRelation(TRADES, side('more', [['id', 'Integer']]), { form: 'append' }, [side('floats', [['id', 'Float']])]),
      /'id' is Integer in trades and Float in floats/);
    assert.equal(pure(joinRelation(side('a', [['s', 'meta::pure::precisePrimitives::Varchar']]), side('b', [['s', 'String']]), { form: 'append' })),
      '#>{local::DB.a}#->concatenate(#>{local::DB.b}#)');
  });
});

describe('what it will do, before it is applied', () => {
  it('asks my rows, those with a match, the rows after it, and their keys that repeat', () => {
    const q = joinSummary(TRADES, CUSTOMERS, join());
    assert.match(pure(q.mine.body[0]), /^#>\{local::DB\.trades\}#->extend\(~\[__ROOT__|^#>\{local::DB\.trades\}#->extend/);
    assert.match(pure(q.matched!.body[0]), /->filter\(\{a \| #>\{local::DB\.customers\}#->rename\(~id, ~c_id\)->filter\(\{b \| \(\$a\.cust->isNotEmpty\(\) && \(\$a\.cust == \$b\.c_id\)\)\}\)->isNotEmpty\(\)\}\)/);
    assert.match(pure(q.joined!.body[0]), /->join\(/);
    assert.match(pure(q.repeated!.body[0]), /^#>\{local::DB\.customers\}#->filter\(\{x \| \$x\.id->isNotEmpty\(\)\}\)->groupBy\(~\[id\]/);
    assert.match(pure(q.repeated!.body[0]), /keys:\{x \| 1\}:\{y \| \$y->count\(\)\}, most:\{x \| \$x\.__n__\}:\{y \| \$y->max\(\)\}/);
  });

  it('asks no repeated keys of a condition that is not equal keys alone', () => {
    assert.equal(joinSummary(TRADES, CUSTOMERS, join({ on: { rows: [{ left: 'cust', op: '<', right: 'id' }] } })).repeated, undefined);
    assert.equal(joinSummary(TRADES, CUSTOMERS, join({ on: { rows: [] } })).repeated, undefined);
    // every pair (an Append Fields): their rows asked of the database, never mine's quotient
    assert.deepEqual(Object.keys(joinSummary(TRADES, CUSTOMERS, join({ kind: 'inner', on: { rows: [] } }))), ['mine', 'matched', 'joined', 'theirs']);
  });

  it('as of: my rows, and those with a nearest row (the match and the condition both)', () => {
    const how: JoinHow = { form: 'asOf', match: { left: 'at', op: '>=', right: 'at' }, on: { rows: [{ left: 'sym', op: '==', right: 'sym' }] }, prefix: 'p_', keep: null };
    const q = joinSummary(TRADES, PRICES, how);
    assert.deepEqual(Object.keys(q), ['mine', 'matched']);
    assert.match(pure(q.matched!.body[0]), /\{b \| \(\(\$a\.at >= \$b\.p_at\) && \(\$a\.sym->isNotEmpty\(\) && \(\$a\.sym == \$b\.p_sym\)\)\)\}/);
  });

  it('semi, anti: those with a match; append: their rows', () => {
    assert.deepEqual(Object.keys(joinSummary(TRADES, CUSTOMERS, { form: 'anti', on: { rows: [{ left: 'cust', op: '==', right: 'id' }] } })), ['mine', 'matched']);
    assert.deepEqual(Object.keys(joinSummary(TRADES, TRADES, { form: 'append' })), ['mine', 'theirs']);
  });
});

describe('a join in words', () => {
  it('titles and says each form', () => {
    assert.equal(joinTitle(join(), 'trades', 'customers'), 'trades ⋈ customers');
    assert.equal(joinTitle({ form: 'append' }, 'trades', 'more'), 'trades + more');
    assert.equal(joinTitle({ form: 'anti', on: { rows: [] } }, 'trades', 'customers'), 'trades without customers');
    assert.equal(joinWords(join()), 'all of mine, on cust = id');
    // every row with every row (an Append Fields): said so; all of mine on no condition still a join's words
    assert.equal(joinWords(join({ kind: 'inner', on: { rows: [] } })), 'each row with every row of theirs');
    assert.equal(joinWords(join({ kind: 'left', on: { rows: [] } })), 'all of mine, on every pair');
    assert.deepEqual(summaryWords(join({ kind: 'inner', on: { rows: [] } }), { mine: 5, matched: 5, joined: 20, theirs: 4 }),
      ['Each of my 5 rows beside each of the 4 rows of theirs: 20 rows.']);
    assert.deepEqual(summaryWords(join({ kind: 'inner', on: { rows: [] } }), { mine: 0, matched: 0, joined: 0, theirs: 3 }), ['I have no rows: none after it.']);
    assert.throws(() => summaryWords(join({ kind: 'inner', on: { rows: [] } }), { mine: 5, matched: 5, joined: 20 }), /was not asked how many rows they have/);
    assert.equal(joinWords({ form: 'asOf', match: { left: 'at', op: '>=', right: 'at' }, on: { rows: [] }, prefix: 'p_', keep: null }), 'nearest at >= at');
  });

  it('says a right or full join\'s rows after it as theirs kept too, never a key of mine repeating', () => {
    const counts = { mine: 5, matched: 3, joined: 7 };
    assert.equal(summaryWords(join({ kind: 'left' }), counts)[1], '7 rows after it: more than mine -- a row of mine repeats once for each of theirs it matches.');
    for (const kind of ['right', 'full'] as const) {
      assert.equal(summaryWords(join({ kind }), counts)[1], '7 rows after it, theirs with no match of mine among them.');
    }
  });

  it('refuses words for a count its form always answers that it was not given', () => {
    assert.throws(() => summaryWords(join(), { mine: 5 }), /was not asked how many rows find a match/);
    assert.throws(() => summaryWords({ form: 'append' }, { mine: 5 }), /was not asked how many rows they have/);
  });

  it('suggests keys: their id under their name first, then a name both have (but id, each table\'s own key)', () => {
    assert.deepEqual(suggestedKeys(side('orders', [['id', 'Integer'], ['customer_id', 'Integer'], ['sym', 'String']]), side('customers', [['id', 'Integer'], ['sym', 'String']])),
      [{ row: { left: 'customer_id', op: '==', right: 'id' }, why: "customers's id" }, { row: { left: 'sym', op: '==', right: 'sym' }, why: 'the same name' }]);
    // a file's name: its extension is no part of it
    const file: JoinSide = { ...side('customers', [['id', 'Integer']]), name: 'customers.csv' };
    assert.deepEqual(suggestedKeys(side('orders', [['customer_id', 'Integer']]), file).map((k) => k.row.left), ['customer_id']);
    // the same name of another type is no key
    assert.deepEqual(suggestedKeys(side('a', [['k', 'Integer']]), side('b', [['k', 'String']])), []);
  });

  // the user, 2026-10-23: a right-click's Join ▸, a quick join to each grid sharing a column
  it('quick joins on the column clicked: each other grid with a key on it, one each, in the page\'s order', () => {
    const mine = side('orders', [['customer_id', 'Integer'], ['sym', 'String'], ['region', 'String']]);
    const others = [
      { ...side('customers', [['id', 'Integer'], ['region', 'String']]), id: 'grid-2' },
      { ...side('prices', [['sym', 'String'], ['px', 'Float']]), id: 'grid-3' },
      { ...side('labels', [['code', 'String']]), id: 'grid-4' },
    ];
    const say = (q: ReturnType<typeof quickJoins>) => q.map((j) => `${j.their} ${j.row.left}=${j.row.right}`);
    assert.deepEqual(say(quickJoins(mine, others, 'customer_id')), ['grid-2 customer_id=id'], 'their id under their name');
    assert.deepEqual(say(quickJoins(mine, others, 'sym')), ['grid-3 sym=sym']);
    assert.deepEqual(say(quickJoins(mine, others, 'region')), ['grid-2 region=region']);
    const both = [...others, { ...side('more', [['sym', 'String'], ['region', 'String']]), id: 'grid-5' }];
    assert.deepEqual(say(quickJoins(mine, both, 'region')), ['grid-2 region=region', 'grid-5 region=region'], 'each grid with it, in order');
    assert.deepEqual(quickJoins(mine, others, 'nope'), [], 'no grid has it: none');
  });

  it("a quick join's how: all of mine and matching only as a join, their columns whole; mine with or without a match", () => {
    const row = { left: 'customer_id', op: '==', right: 'id' } as const;
    assert.deepEqual(quickHow(row, 'left'), { form: 'join', kind: 'left', on: { rows: [row] }, prefix: 'their_', keep: null });
    assert.deepEqual(quickHow(row, 'inner'), { form: 'join', kind: 'inner', on: { rows: [row] }, prefix: 'their_', keep: null });
    assert.deepEqual(quickHow(row, 'anti'), { form: 'anti', on: { rows: [row] } });
    assert.deepEqual(readJoinHow(quickHow(row, 'semi')), quickHow(row, 'semi'), 'a how the saved form reads back');
  });

  it('a join repeats rows of mine when more come out than mine, keeping only mine', () => {
    const row = { left: 'k', op: '==', right: 'k' } as const;
    assert.equal(joinRepeats(quickHow(row, 'left'), { mine: 5, matched: 3, joined: 6 }), true);
    assert.equal(joinRepeats(quickHow(row, 'inner'), { mine: 5, matched: 3, joined: 4 }), false);
    assert.equal(joinRepeats(quickHow(row, 'left'), { mine: 5, matched: 3, joined: 5 }), false);
    assert.equal(joinRepeats(quickHow(row, 'semi'), { mine: 5, matched: 3 }), false, 'mine, each once');
    assert.equal(joinRepeats(join({ kind: 'full' }), { mine: 5, matched: 3, joined: 9 }), false, 'theirs kept too: more is no repeat');
  });
});
