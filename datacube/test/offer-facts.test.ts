// T5 (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md): what a column is offered comes from the
// COMPILER -- src/generated/offer-facts.ts, DataCube's own queries compiled per type -- and
// rules naming no type (src/offers.ts): an aggregate the compiler accepts whose result stays
// in the column's family; a filter operator whose condition compiles.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { OFFER_FACTS } from '../src/generated/offer-facts.ts';
import { typeColumns } from '../src/plan.ts';
import type { QueryRunner } from '../src/runner.ts';
import { AGGREGATE_FNS, FILTER_OPERATORS, type AggregateFn, type CubeSnapshot, type FilterOperator } from '../src/snapshot.ts';
import { takesOperator } from '../src/offers.ts';
import { filterOperatorsFor } from '../src/ui/menu.ts';
import { aggregatesFor } from '../src/ui/panel-column.ts';
import { accessor } from '../../pure-protocol/src/index.ts';
import { plannerFor } from './catalog-builder.ts';
import { liteParse, litePrint, row } from './lite-compiler.ts';

const offered = (type: string | undefined): AggregateFn[] => aggregatesFor(type).map((a) => a.value);

describe('aggregates offered by the compiler\'s answers', () => {
  it('a number takes every numeric aggregate, whatever numeric type the result is', () => {
    // an Integer's average is a Float and its deviation a Number: the same family
    const numeric: AggregateFn[] = ['sum', 'average', 'count', 'min', 'max', 'median', 'percentile25', 'percentile75', 'stdDevPopulation',
      'stdDevSample', 'variancePopulation', 'varianceSample', 'wavg', 'unique'];
    for (const type of ['Integer', 'Float', 'Decimal', 'Number']) assert.deepEqual(offered(type), numeric, type);
  });

  it('text: what keeps it text -- no count, whose result is a number', () => {
    assert.deepEqual(offered('String'), ['min', 'max', 'joinStrings', 'unique']);
  });

  it('dates, times, booleans and JSON: the ordered and the single value', () => {
    for (const type of ['StrictDate', 'DateTime', 'Date', 'StrictTime', 'Boolean', 'Variant']) {
      assert.deepEqual(offered(type), ['min', 'max', 'unique'], type);
    }
  });

  it('reads a type by its family, so a path or a precise primitive is the same column', () => {
    assert.deepEqual(offered('meta::pure::metamodel::variant::Variant'), offered('Variant'));
    assert.deepEqual(offered('meta::pure::precisePrimitives::BigInt'), offered('Integer'));
    assert.deepEqual(offered('meta::pure::precisePrimitives::Varchar'), offered('String'));
  });

  it('a type the compiler gave no facts for takes none; an untyped column all, until Apply', () => {
    assert.deepEqual(offered('Any'), []);
    assert.deepEqual(offered(undefined), [...AGGREGATE_FNS]);
  });
});

// The user, 2026-09-28: "if i want to make a calculated column that converts values to
// string it does let me do joinStrings".
describe('a calculated column that turns values into text offers joinStrings', () => {
  const MODEL = `###Relational
Database t::DB ( Table T ( qty INTEGER, desk VARCHAR(32) ) )
`;
  const planner = plannerFor(MODEL, '');
  const compiler: QueryRunner = {
    name: 'lite',
    stream: async () => { throw new Error('this test streams nothing'); },
    run: async () => { throw new Error('typing runs nothing'); },
    compile: async () => undefined,
    relationType: (query) => planner.relationType(query),
    parse: liteParse,
    print: litePrint,
  };
  const cube = async (body: string): Promise<CubeSnapshot> => ({
    source: { query: accessor('t::DB', 'T') },
    columns: [{ name: 'qty', type: 'Integer' }, { name: 'desk', type: 'String' }],
    derived: [{ name: 'qty_text', lambda: (await row(body)) }],
    rows: [],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  });

  it('typed String by the compiler before anything is offered, then joinStrings', async () => {
    const typed = await typeColumns((await cube('$x.qty->toOne()->toString()')), compiler);
    const type = typed.snapshot.derived[0]?.type;
    assert.equal(type, 'String');
    assert.ok(offered(type).includes('joinStrings'));
    assert.ok(!offered(type).includes('sum'));
  });

  it('the number it came from offers no joinStrings', () => {
    assert.ok(!offered('Integer').includes('joinStrings'));
  });

  it('toString takes ONE value: on a column that may be empty the compiler refuses it, as upstream', async () => {
    // Pure's toString(Any[1]); a table column is [0..1]. Upstream's engine refuses the
    // same way ("Can't find a match for function 'toLower(Varchar(32)[0..1])'").
    await assert.rejects(typeColumns((await cube('$x.qty->toString()')), compiler), /toString/);
  });
});

// PARITY with upstream DataCube (legend-studio packages/legend-data-cube, read 2026-09-28):
// each aggregate's `isCompatibleWithColumn`
// (src/stores/core/aggregation/DataCubeQueryAggregateOperation__*.tsx) over its column
// categories (DataCubeQueryEngine.ts `getDataType`). Copied here as the reference, never
// shipped; every difference below is named for a person to review.
describe('parity with upstream DataCube\'s aggregate offers', () => {
  type Category = 'Numeric' | 'Text' | 'Date' | 'Time' | 'Boolean';
  // getDataType: DateTime and StrictTime are its Time; anything unlisted, Variant too, is Text
  const UPSTREAM_CATEGORY: Readonly<Record<string, Category>> = {
    Integer: 'Numeric', Float: 'Numeric', Decimal: 'Numeric', Number: 'Numeric',
    StrictDate: 'Date', Date: 'Date', DateTime: 'Time', StrictTime: 'Time',
    Boolean: 'Boolean', String: 'Text', Variant: 'Text',
  };
  const NUMERIC: readonly Category[] = ['Numeric'];
  const UPSTREAM: Readonly<Partial<Record<AggregateFn, readonly Category[]>>> = {
    sum: NUMERIC, average: NUMERIC, count: NUMERIC, min: NUMERIC, max: NUMERIC,
    stdDevPopulation: NUMERIC, stdDevSample: NUMERIC, variancePopulation: NUMERIC, varianceSample: NUMERIC,
    joinStrings: ['Text'],
    unique: ['Text', 'Numeric', 'Date', 'Time'],
    // median, the quartiles and wavg: this DataCube's own; upstream has none of them
  };

  it('differs only where named', () => {
    const differences: string[] = [];
    for (const type of Object.keys(OFFER_FACTS)) {
      const ours = new Set(offered(type));
      const category = UPSTREAM_CATEGORY[type];
      assert.ok(category, `no upstream category for ${type}`);
      for (const fn of AGGREGATE_FNS) {
        const theirs = UPSTREAM[fn]?.includes(category) ?? false;
        if (ours.has(fn) !== theirs) differences.push(`${type} ${fn}: ${ours.has(fn) ? 'ours only' : 'upstream only'}`);
      }
    }
    assert.deepEqual(differences.sort(), [
      // min/max: Pure's collection::min<X>/max<X> take ANY type, on upstream's compiler as on
      // lite's (the user, 2026-09-28: keep them) -- "the latest trade date", the earliest time
      // of day, the first name alphabetically. JSON orders by its text in the database.
      'Date max: ours only', 'Date min: ours only',
      'DateTime max: ours only', 'DateTime min: ours only',
      'StrictDate max: ours only', 'StrictDate min: ours only',
      'StrictTime max: ours only', 'StrictTime min: ours only',
      'String max: ours only', 'String min: ours only',
      'Boolean max: ours only', 'Boolean min: ours only',
      'Variant max: ours only', 'Variant min: ours only',
      // uniqueValueOnly<T> takes any type; upstream DataCube leaves booleans out
      'Boolean unique: ours only',
      // the compiler refuses joinStrings of JSON (it takes String): upstream offers a query that fails
      'Variant joinStrings: upstream only',
      // this DataCube's own aggregates
      'Decimal median: ours only', 'Decimal wavg: ours only',
      'Float median: ours only', 'Float wavg: ours only',
      'Integer median: ours only', 'Integer wavg: ours only',
      'Number median: ours only', 'Number wavg: ours only',
      // the quartiles, a box plot's box (chart batch 4): Pure's percentile, a number of numbers
      'Decimal percentile25: ours only', 'Decimal percentile75: ours only',
      'Float percentile25: ours only', 'Float percentile75: ours only',
      'Integer percentile25: ours only', 'Integer percentile75: ours only',
      'Number percentile25: ours only', 'Number percentile75: ours only',
    ].sort());
  });
});

// Filter operators: each upstream DataCubeQueryFilterOperation__*.tsx `isCompatibleWithColumn`
// (read 2026-09-28) over the same categories. inCaseInsensitive / notInCaseInsensitive are
// this DataCube's own and not compared.
describe('parity with upstream DataCube\'s filter operators', () => {
  type Category = 'Numeric' | 'Text' | 'Date' | 'Time' | 'Boolean';
  const CATEGORY: Readonly<Record<string, Category>> = {
    Integer: 'Numeric', Float: 'Numeric', Decimal: 'Numeric', Number: 'Numeric',
    StrictDate: 'Date', Date: 'Date', DateTime: 'Time', StrictTime: 'Time',
    Boolean: 'Boolean', String: 'Text', Variant: 'Text',
  };
  const ALL: readonly Category[] = ['Text', 'Numeric', 'Date', 'Time', 'Boolean'];
  const TEXT: readonly Category[] = ['Text'];
  const ORDER: readonly Category[] = ['Numeric', 'Date', 'Time'];
  const VALUED: readonly Category[] = ['Text', 'Numeric', 'Date', 'Time'];
  const LISTED: readonly Category[] = ['Text', 'Numeric', 'Date'];
  const UPSTREAM: Readonly<Partial<Record<FilterOperator, readonly Category[]>>> = {
    equal: ALL, notEqual: ALL,
    lessThan: ORDER, lessThanEqual: ORDER, greaterThan: ORDER, greaterThanEqual: ORDER,
    isEmpty: VALUED, isNotEmpty: VALUED,
    contains: TEXT, notContains: TEXT, startsWith: TEXT, notStartsWith: TEXT, endsWith: TEXT, notEndsWith: TEXT,
    in: LISTED, notIn: LISTED,
    equalCaseInsensitive: TEXT, notEqualCaseInsensitive: TEXT, containsCaseInsensitive: TEXT,
    startsWithCaseInsensitive: TEXT, endsWithCaseInsensitive: TEXT,
    equalColumn: VALUED, notEqualColumn: VALUED, equalCaseInsensitiveColumn: TEXT, notEqualCaseInsensitiveColumn: TEXT,
    lessThanColumn: ORDER, lessThanEqualColumn: ORDER, greaterThanColumn: ORDER, greaterThanEqualColumn: ORDER,
  };

  it('differs only where named', () => {
    const differences: string[] = [];
    for (const type of Object.keys(OFFER_FACTS)) {
      const category = CATEGORY[type];
      assert.ok(category, `no upstream category for ${type}`);
      for (const op of FILTER_OPERATORS) {
        const theirs = UPSTREAM[op];
        if (theirs === undefined) continue;
        const ours = takesOperator(op, type);
        if (ours !== theirs.includes(category)) differences.push(`${type} ${op}: ${ours ? 'ours only' : 'upstream only'}`);
      }
    }
    const ordering = ['lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual',
      'lessThanColumn', 'lessThanEqualColumn', 'greaterThanColumn', 'greaterThanEqualColumn'];
    const text = ['contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith',
      'equalCaseInsensitive', 'notEqualCaseInsensitive', 'containsCaseInsensitive', 'startsWithCaseInsensitive',
      'endsWithCaseInsensitive', 'equalCaseInsensitiveColumn', 'notEqualCaseInsensitiveColumn'];
    assert.deepEqual(differences.sort(), [
      // TO REVIEW -- the compiler takes these and upstream DataCube does not offer them:
      // Pure orders text and booleans (boolean::lessThan(String|Boolean, ...)),
      ...ordering.map((op) => `String ${op}: ours only`),
      // a boolean orders against another column only: against a value, legend-engine refuses it
      // over a BIT column (ENGINE DEFECT S23, offers.ts), so it is not offered -- as upstream
      ...ordering.filter((op) => op.endsWith('Column')).map((op) => `Boolean ${op}: ours only`),
      // and a boolean or a time compares for membership, equality and presence like any value
      ...['isEmpty', 'isNotEmpty', 'in', 'notIn', 'equalColumn', 'notEqualColumn'].map((op) => `Boolean ${op}: ours only`),
      'DateTime in: ours only', 'DateTime notIn: ours only',
      'StrictTime in: ours only', 'StrictTime notIn: ours only',
      // upstream offers queries its own compiler refuses: Pure declares no order on a time of
      // day (StrictTime is not a Date; no lessThan takes it), and upstream files JSON under
      // text, whose operators take a String
      ...ordering.map((op) => `StrictTime ${op}: upstream only`),
      ...text.map((op) => `Variant ${op}: upstream only`),
    ].sort());
  });
});

// NOTHING A USER HAD IS LOST (the user, 2026-09-28: "do the right thing for users and don't
// lose functionality"). The offers before T5 -- the hand tables it deleted (61fa39c43:
// filter-editor.ts COMPATIBLE, panel-column.ts aggregatesFor, menu.ts filterOperatorsFor),
// by their own type groups -- are the reference: each must still be offered, unless the
// compiler refuses the query it builds, which a user could never run. Such an exception
// is named with its reason.
describe('nothing a user was offered before T5 is lost', () => {
  type Group = 'text' | 'number' | 'date' | 'time' | 'boolean' | 'variant';
  // the old dataTypeOf, by the compiler type's family
  const GROUP: Readonly<Record<string, Group>> = {
    String: 'text', Integer: 'number', Float: 'number', Decimal: 'number', Number: 'number',
    StrictDate: 'date', Date: 'date', DateTime: 'date', StrictTime: 'time', Boolean: 'boolean', Variant: 'variant',
  };
  const OLD_AGGREGATES: Readonly<Record<Group, readonly AggregateFn[]>> = {
    number: ['sum', 'average', 'count', 'min', 'max', 'median', 'stdDevPopulation', 'stdDevSample',
      'variancePopulation', 'varianceSample', 'wavg', 'unique'],
    text: ['joinStrings', 'min', 'max', 'unique'],
    date: ['min', 'max', 'unique'],
    time: ['min', 'max', 'unique'],
    boolean: ['unique'],
    variant: ['unique'],
  };
  const g = (...groups: Group[]): ReadonlySet<Group> => new Set(groups);
  const TEXT = g('text');
  const EQUALITY = g('text', 'number', 'date', 'time', 'boolean');
  const ORDERING = g('number', 'date', 'time');
  const LISTS = g('text', 'number', 'date');
  const NULLS = g('text', 'number', 'date', 'time', 'boolean', 'variant');
  const COLUMN_EQUALITY = g('text', 'number', 'date', 'time');
  const OLD_OPERATORS: Readonly<Record<FilterOperator, ReadonlySet<Group>>> = {
    equal: EQUALITY, notEqual: EQUALITY,
    lessThan: ORDERING, lessThanEqual: ORDERING, greaterThan: ORDERING, greaterThanEqual: ORDERING,
    isEmpty: NULLS, isNotEmpty: NULLS, in: LISTS, notIn: LISTS,
    contains: TEXT, notContains: TEXT, startsWith: TEXT, notStartsWith: TEXT, endsWith: TEXT, notEndsWith: TEXT,
    equalCaseInsensitive: TEXT, notEqualCaseInsensitive: TEXT, containsCaseInsensitive: TEXT,
    startsWithCaseInsensitive: TEXT, endsWithCaseInsensitive: TEXT, inCaseInsensitive: TEXT, notInCaseInsensitive: TEXT,
    equalColumn: COLUMN_EQUALITY, equalCaseInsensitiveColumn: TEXT, notEqualColumn: COLUMN_EQUALITY,
    notEqualCaseInsensitiveColumn: TEXT, lessThanColumn: ORDERING, lessThanEqualColumn: ORDERING,
    greaterThanColumn: ORDERING, greaterThanEqualColumn: ORDERING,
  };
  const COMPARISONS: FilterOperator[] = ['equal', 'notEqual', 'lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual'];
  const OLD_MENU: Readonly<Record<Group, readonly FilterOperator[]>> = {
    text: [...COMPARISONS, 'contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith'],
    number: COMPARISONS, date: COMPARISONS, time: COMPARISONS,
    boolean: ['equal', 'notEqual'],
    variant: [],
  };

  const ordering = ['lessThan', 'lessThanEqual', 'greaterThan', 'greaterThanEqual'];
  // Pure declares no order on a time of day -- StrictTime is not a Date and no
  // boolean::lessThan takes it, in legend-pure as in lite -- so these queries were always
  // refused: offered, never runnable. Ordering times needs that function first.
  const NEVER_RAN = [
    ...[...ordering, ...ordering.map((op) => `${op}Column`)].map((op) => `StrictTime ${op} (filter)`),
    ...ordering.map((op) => `StrictTime ${op} (menu)`),
  ];

  it('every old offer is still made, or its query never compiled', () => {
    const lost: string[] = [];
    for (const type of Object.keys(OFFER_FACTS)) {
      const group = GROUP[type];
      assert.ok(group, `no old group for ${type}`);
      const aggregates = new Set(offered(type));
      for (const fn of OLD_AGGREGATES[group]) if (!aggregates.has(fn)) lost.push(`${type} ${fn} (aggregate)`);
      for (const op of FILTER_OPERATORS) {
        if (OLD_OPERATORS[op].has(group) && !takesOperator(op, type)) lost.push(`${type} ${op} (filter)`);
      }
      const menu = new Set(filterOperatorsFor(type));
      for (const op of OLD_MENU[group]) if (!menu.has(op)) lost.push(`${type} ${op} (menu)`);
    }
    assert.deepEqual(lost.sort(), [...NEVER_RAN].sort());
  });
});
