// Each builder against legend-lite's own parser: the JSON a builder makes is BYTE-IDENTICAL to the
// JSON the grammar gives the same query (which is legend-engine's wire: lite's E1 is byte-exact
// with the engine's). And the compiler's print of it parses back to the same bytes.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  accessor, agg, and, asc, col, collection, derive, desc, divide, element, enumValue, eq, flatten, fn, from, ge, gt,
  lambda, lit, minus, ne, not, or, over, parameter, plus, relationType, times, to, toMany, type, variable,
  ExactNumber, ONE, MANY, ProtocolError, readLambda, toJson, type Lambda,
} from '../src/index.ts';
import { compose, parse, ready } from './lite.ts';

const T = accessor('a::DB', 's', 'T');
const x = (c: string) => col('x', c);

/** [what, built, the same query as Pure text] */
const TWINS: readonly (readonly [string, () => Lambda, string])[] = [
  ['an accessor', () => from(T).lambda(), '|#>{a::DB.s.T}#'],
  ['a default-schema accessor', () => from(accessor('a::DB', 'T')).lambda(), '|#>{a::DB.T}#'],
  ['select', () => from(T).select(['a', 'b c']).lambda(), "|#>{a::DB.s.T}#->select(~[a, 'b c'])"],
  ['select all', () => from(T).select().lambda(), '|#>{a::DB.s.T}#->select()'],
  ['extend one', () => from(T).extend(derive('z', lambda(['x'], plus(x('a'), lit.integer(1))))).lambda(),
    '|#>{a::DB.s.T}#->extend(~z: x|$x.a + 1)'],
  ['extend many', () => from(T).extend([derive('a', lambda(['x'], lit.integer(1))), derive('b', lambda(['x'], lit.integer(2)))]).lambda(),
    '|#>{a::DB.s.T}#->extend(~[a: x|1, b: x|2])'],
  ['a window', () => from(T).extend(derive('r', lambda(['p', 'w', 'r'], fn('rowNumber', variable('p'), variable('r')))),
    over(['a'], [asc('b')])).lambda(),
    '|#>{a::DB.s.T}#->extend(over(~[a], [~b->ascending()]), ~r: {p,w,r|$p->rowNumber($r)})'],
  ['filter', () => from(T).filter(lambda(['x'], and(gt(x('q'), lit.integer(0)), not(fn('isEmpty', x('b')))))).lambda(),
    '|#>{a::DB.s.T}#->filter(x|$x.q > 0 && !$x.b->isEmpty())'],
  ['groupBy', () => from(T).groupBy(['a'], [agg('n', lambda(['x'], x('b')), lambda(['y'], fn('sum', variable('y'))))]).lambda(),
    '|#>{a::DB.s.T}#->groupBy(~[a], ~[n: x|$x.b: y|$y->sum()])'],
  ['pivot and cast', () => from(T).pivot(['p'], [agg('n', lambda(['x'], x('b')), lambda(['y'], fn('sum', variable('y'))))])
    .castTo(relationType([{ name: 'a', type: type('String') }, { name: 'n', type: type('Integer') }])).lambda(),
    '|#>{a::DB.s.T}#->pivot(~[p], ~[n: x|$x.b: y|$y->sum()])->cast(@meta::pure::metamodel::relation::Relation<(a:String, n:Integer)>)'],
  ['sort, limit, distinct', () => from(T).sort([asc('a'), desc('b')]).limit(10).distinct().lambda(),
    '|#>{a::DB.s.T}#->sort([~a->ascending(), ~b->descending()])->limit(10)->distinct()'],
  ['rename', () => from(T).rename('a', 'b').lambda(), '|#>{a::DB.s.T}#->rename(~a, ~b)'],
  ['lateral flatten: each row once per element', () => from(T)
    .lateral(lambda(['x'], flatten(toMany(fn('get', col('x', 'doc'), lit.string('items')), type('meta::pure::metamodel::variant::Variant')), 'item')))
    .lambda(),
    "|#>{a::DB.s.T}#->lateral(x|$x.doc->get('items')->toMany(@meta::pure::metamodel::variant::Variant)->flatten(~item))"],
  ['join and concatenate', () => from(T).join(from(accessor('a::DB', 'U')), 'LEFT', lambda(['a', 'b'], eq(col('a', 'k'), col('b', 'k'))))
    .concatenate(from(accessor('a::DB', 'V'))).lambda(),
    '|#>{a::DB.s.T}#->join(#>{a::DB.U}#, meta::pure::functions::relation::JoinKind.LEFT, {a, b|$a.k == $b.k})->concatenate(#>{a::DB.V}#)'],
  ['in a list', () => lambda([], fn('in', variable('v'), collection([lit.string('x'), lit.string('y')]))),
    "|$v->in(['x', 'y'])"],
  ['!=, ||, <=, >=', () => lambda([], or(ne(variable('a'), lit.integer(1)), fn('lessThanEqual', variable('b'), lit.integer(2)),
    ge(variable('c'), lit.integer(3)))), '|($a != 1) || ($b <= 2) || ($c >= 3)'],
  ['arithmetic', () => lambda([], divide(times(variable('a'), variable('b')), minus(variable('c'), lit.integer(1)))),
    '|($a * $b) / ($c - 1)'],
  ['a Variant read', () => lambda([], toMany(to(fn('get', fn('get', x('v'), lit.string('a')), lit.integer(0)), type('Integer')), type('String'))),
    "|$x.v->get('a')->get(0)->to(@Integer)->toMany(@String)"],
  ['an enum value', () => lambda([], enumValue('meta::pure::functions::relation::JoinKind', 'INNER')),
    '|meta::pure::functions::relation::JoinKind.INNER'],
  ['an element', () => lambda([], fn('toOne', element('my::Thing'))), '|my::Thing->toOne()'],
  ['typed parameters', () => lambda([parameter('s', type('String'), ONE), parameter('n', type('Integer'), MANY)], variable('s')),
    '{s: String[1], n: Integer[*]|$s}'],
  ['every literal', () => lambda([], collection([
    lit.strictDate('2024-01-02'), lit.dateTime('2024-01-02T03:04:05.123456'), lit.decimal('12.5'), lit.float(1.5),
    lit.integer('9007199254740993'), lit.string("it's"), lit.boolean(true), lit.strictTime('10:11:12'),
    lit.integer(-5), lit.decimal('2.5'), lit.float(-1.5), lit.decimal('-0.5')])),
  "|[%2024-01-02, %2024-01-02T03:04:05.123456, 12.5D, 1.5, 9007199254740993, 'it\\'s', true, %10:11:12, -5, 2.5D, -1.5, -0.5D]"],
];

describe('each builder is the grammar, byte for byte', () => {
  for (const [what, built, text] of TWINS) {
    it(what, async () => {
      const json = toJson(built());
      assert.equal(json, await parse(text), text);
      // the compiler prints it, and the print parses back to the same bytes
      assert.equal(await parse(await compose(json, 'STANDARD')), json);
      assert.equal(await parse(await compose(json, 'PRETTY')), json);
      // and the library reads it back to the same bytes
      assert.equal(toJson(readLambda(json)), json);
    });
  }
});

describe('why queries are built as trees, not text', () => {
  it('Pure applies && and || left to right with the comparisons: text must parenthesize, a tree cannot get it wrong', async () => {
    const intended = toJson(lambda([], and(fn('lessThanEqual', col('x', 'a'), lit.integer(2)), ge(col('x', 'b'), lit.integer(3)))));
    assert.equal(await parse('|($x.a <= 2) && ($x.b >= 3)'), intended);
    // unparenthesized, the same text is ((a <= 2) && b) >= 3 -- lite's parser, byte-exact with upstream's
    assert.notEqual(await parse('|$x.a <= 2 && $x.b >= 3'), intended);
  });

  it('a decimal is exact on the wire, and the printer keeps its digits (not upstream\'s read through a double)', async () => {
    for (const [digits, printed] of [['12.30', '12.30D'], ['12345678901234567.89', '12345678901234567.89D'],
      ['0.0000001', '1E-7D'], ['-100.500', '-100.500D']] as const) {
      const json = toJson(lambda([], lit.decimal(digits)));
      assert.equal(json, await parse(`|${digits}D`));
      // printed as the wire spells its value (BigDecimal's layout), and back to the same bytes
      assert.equal(await compose(json), `|${printed}`);
      assert.equal(await parse(await compose(json)), json, `${digits} round-trips byte for byte`);
    }
  });
});

describe('numbers are spelled as the wire spells them: byte for byte with lite\'s parse', () => {
  // A deterministic spread (no Math.random: a failure must reproduce): every decade the double
  // range reaches, the plain/exponent boundaries (1e-3, 1e7), repeating fractions, the largest and
  // smallest (subnormal) doubles; decimals of every scale around BigDecimal's -6 boundary.
  let seed = 20260928;
  const next = (): number => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
  const floats: number[] = [5000, 0.1, 0.5, 1 / 3, 2 / 3, 1e-3, 0.00099999, 1e7, 9999999.999, 1234567.0,
    123456789012345678, Number.MAX_VALUE, Number.MIN_VALUE, 2 ** 53, 2 ** 53 + 2, 1e21, 1e-7, 100, 0.001, 1];
  for (let decade = -300; decade <= 300; decade += 7) {
    for (let i = 0; i < 4; i++) floats.push(Number(`${(next() * 9 + 1).toPrecision(1 + i * 5)}e${decade}`));
  }
  for (let i = 0; i < 1500; i++) floats.push(next() * 10 ** Math.floor(next() * 24 - 8));
  // and 20,000 doubles by their bits: every exponent, subnormals included
  for (let i = 0; i < 20000; i++) {
    const bits = BigInt(Math.floor(next() * 2 ** 31)) << 32n | BigInt(Math.floor(next() * 2 ** 31));
    floats.push(Math.abs(new Float64Array(new BigUint64Array([bits]).buffer)[0]!));
  }
  const decimals: string[] = ['0.0000001', '0.000001', '1.5', '12.30', '100', '0.00', '0', '123456789.000000001',
    '0.10', '10.10', '99999999999999999999.99', '0.0000000000001234'];
  for (let i = 0; i < 1000; i++) {
    const whole = Math.floor(next() * 3) === 0 ? '0' : String(Math.floor(next() * 10 ** Math.floor(next() * 12)));
    const scale = Math.floor(next() * 14);
    const fraction = Array.from({ length: scale }, () => String(Math.floor(next() * 10))).join('');
    decimals.push(scale === 0 ? whole : `${whole}.${fraction}`);
  }

  /*
   * The tab's build of lite once read and wrote some doubles with TeaVM's own conversions: 30 spelled differently
   * at the last digit and 2 read one unit in the last place off, accepted by the user on 2026-09-28 and pinned. Since
   * 2026-10-10 TeaVM's own conversions are the JDK's (//third_party/teavm_classlib, held by //wasm:conformance_test;
   * from 2026-10-09 lite had routed them through its own exact code), so the tab answers as the JVM does: every double
   * byte for byte.
   */
  it(`${floats.length} floats`, async () => {
    const lite = await ready();
    const differ: string[] = [];
    for (const v of floats) {
      if (!Number.isFinite(v) || v <= 0) continue;
      const ours = toJson(lambda([], lit.float(v)));
      const theirs = lite.parse(`|${v.toExponential()}`.replace('e+', 'e'));
      if (ours !== theirs) differ.push(`${v}: ours ${ours} lite ${theirs}`);
    }
    assert.deepEqual(differ.slice(0, 20), [], `${differ.length} of ${floats.length} differ:\n${differ.slice(0, 20).join("\n")}`);
  });

  it(`${decimals.length} decimals`, async () => {
    const lite = await ready();
    const differ: string[] = [];
    for (const d of decimals) {
      const ours = toJson(lambda([], lit.decimal(d)));
      const theirs = lite.parse(`|${d}D`);
      if (ours !== theirs) differ.push(`${d}: ours ${ours} lite ${theirs}`);
    }
    assert.deepEqual(differ.slice(0, 20), [], `${differ.length} of ${decimals.length} differ`);
  });
});

describe('a number is refused where it is made (it must be JSON); a date is the compiler\'s to judge', () => {
  it('carries a date, a timestamp and a time of day as given', () => {
    // the compiler's reader refuses these when the query is compiled ("Invalid month: 13")
    assert.deepEqual(lit.strictDate('2024-13-01'), { _type: 'strictDate', value: '2024-13-01' });
    assert.deepEqual(lit.dateTime('2024-01-02T25:00:00'), { _type: 'dateTime', value: '2024-01-02T25:00:00' });
    assert.deepEqual(lit.strictTime('24:00:00'), { _type: 'strictTime', value: '24:00:00' });
  });

  const refused: readonly (readonly [string, () => unknown])[] = [
    ['a decimal with an exponent', () => lit.decimal('1e5')],
    ['a decimal that is not a number', () => lit.decimal('12,30')],
    ['an unsafe JS integer', () => lit.integer(2 ** 53 + 2)],
    ['a non-finite float', () => lit.float(Number.POSITIVE_INFINITY)],
    ['a type with no literal', () => lit.of('meta::pure::metamodel::variant::Variant', '{}')],
  ];
  for (const [what, make] of refused) {
    it(what, () => assert.throws(make, ProtocolError));
  }

  it('a compiler type chooses the literal, and the value stays exact', () => {
    assert.equal(toJson(lit.of('Decimal', '12345678901234567.89')), '{"_type":"decimal","value":12345678901234567.89}');
    assert.equal(toJson(lit.of('Integer', 9007199254740993n)), '{"_type":"integer","value":9007199254740993}');
    assert.equal(toJson(lit.of('meta::pure::precisePrimitives::Varchar', 'a')), '{"_type":"string","value":"a"}');
    assert.equal(toJson(lit.of('StrictDate', '2024-01-02')), '{"_type":"strictDate","value":"2024-01-02"}');
    assert.equal(toJson(lit.of('DateTime', '2024-01-02T03:04:05')), '{"_type":"dateTime","value":"2024-01-02T03:04:05"}');
  });
});

describe('exact numbers', () => {
  it('survive a read and a write', () => {
    const text = '{"_type":"lambda","body":[{"_type":"decimal","value":12345678901234567.89}],"parameters":[]}';
    assert.equal(toJson(readLambda(text)), text);
  });

  it('refuse the lossy path', () => {
    assert.throws(() => JSON.stringify(ExactNumber.of('1.50')), ProtocolError);
  });

  it('a JavaScript number that cannot be exact is refused', () => {
    assert.throws(() => ExactNumber.ofInteger(2 ** 60), ProtocolError);
  });
});

describe('reading', () => {
  it('an unknown _type is refused, naming it', () => {
    assert.throws(() => readLambda('{"_type":"lambda","parameters":[],"body":[{"_type":"nope"}]}'), /nope/);
  });

  it('an older TDS shape is kept verbatim, for backwards compatibility', () => {
    const text = '{"_type":"lambda","body":[{"_type":"tdsOlapRank","function":{"_type":"lambda","body":[],"parameters":[]}}],"parameters":[]}';
    assert.equal(toJson(readLambda(text)), text);
  });
});
