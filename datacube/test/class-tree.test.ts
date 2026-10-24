// The class tree (src/class-tree.ts; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md §3.3): a model's classes as the compiler
// gives them -- own properties, supertypes', association ends -- and what a grid over a class may read through them: a
// related column, its to-many ends aggregated. And the class's relation with them projected (query.ts relatedSource),
// a snap of a grid with one refused, the command's reader.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { element, fn, property, variable, type AppliedFunction } from '../../pure-protocol/src/index.ts';
import { classRelation } from '../src/class-source.ts';
import { classTree, relatedLabel, relatedName, relatedOffers, relatedProblem, type ClassElement } from '../src/class-tree.ts';
import { readGridCommand } from '../src/grid-commands.ts';
import { relatedSource, relatedValue, snapPull } from '../src/query.ts';
import { CubeRefusal, type QuerySnapshot } from '../src/snapshot.ts';

type Property = NonNullable<ClassElement['properties']>[number];
const type = (fullPath: string): Property['genericType'] => ({ rawType: { _type: 'packageableType', fullPath } });
const ONE = { lowerBound: 1, upperBound: 1 };
const MANY = { lowerBound: 0 };
const prop = (name: string, of = 'String', multiplicity: Property['multiplicity'] = ONE): Property =>
  ({ name, genericType: type(of), multiplicity });

const ELEMENTS: ClassElement[] = [
  { _type: 'class', package: 'm', name: 'Party', properties: [prop('id', 'Integer')] },
  { _type: 'class', package: 'm', name: 'Customer', superTypes: [{ fullPath: 'm::Party' }], properties: [prop('name'), prop('region')] },
  {
    _type: 'class', package: 'm', name: 'Trade',
    properties: [prop('notional', 'Float'), prop('side', 'm::Side'), prop('tags', 'String', MANY), prop('book', 'm::Book')],
  },
  { _type: 'class', package: 'm', name: 'Book', properties: [prop('code')] },
  { _type: 'Enumeration', package: 'm', name: 'Side' },
  // each end the property its other end's class reaches it by
  { _type: 'association', package: 'm', name: 'TradeCustomer', properties: [prop('customer', 'm::Customer', { lowerBound: 0, upperBound: 1 }), prop('trades', 'm::Trade', MANY)] },
];
const TREE = classTree(ELEMENTS);

describe('the class tree: every class, its properties, its supertypes\', the association ends that reach it', () => {
  it('a class-typed property and an association end a class to go on to; a to-many one many', () => {
    assert.deepEqual(TREE['m::Trade'], [
      { name: 'notional', type: 'Float', kind: 'value', many: false },
      { name: 'side', type: 'm::Side', kind: 'enumeration', many: false },
      { name: 'tags', type: 'String', kind: 'value', many: true },
      { name: 'book', type: 'm::Book', kind: 'class', many: false },
      { name: 'customer', type: 'm::Customer', kind: 'class', many: false },
    ]);
    // the customer reaches its trades, and its supertype's id is its own
    assert.deepEqual(TREE['m::Customer']!.map((p) => `${p.name}${p.many ? '*' : ''}`), ['name', 'region', 'trades*', 'id']);
  });
});

describe("a related column: a path from the class, each to-many end's values aggregated", () => {
  it('names and words as a person reads them', () => {
    assert.equal(relatedName({ path: ['customer', 'region'] }), 'customer_region');
    assert.equal(relatedName({ path: ['trades', 'notional'], aggregate: 'sum' }), 'trades_notional_sum');
    assert.equal(relatedLabel({ path: ['trades'], aggregate: 'count' }), 'Count');
    assert.equal(relatedLabel({ path: ['trades', 'notional'], aggregate: 'average' }), 'Average of notional');
    assert.equal(relatedLabel({ path: ['customer', 'region'] }), 'region');
  });

  it('a to-one path a value, deeper through each class; a to-many end aggregated or counted', () => {
    assert.equal(relatedProblem(TREE, 'm::Trade', { path: ['customer', 'region'] }), null);
    assert.equal(relatedProblem(TREE, 'm::Trade', { path: ['customer', 'id'] }), null, "a supertype's property, through the hop");
    assert.equal(relatedProblem(TREE, 'm::Customer', { path: ['trades', 'notional'], aggregate: 'sum' }), null);
    assert.equal(relatedProblem(TREE, 'm::Customer', { path: ['trades'], aggregate: 'count' }), null);
    assert.equal(relatedProblem(TREE, 'm::Customer', { path: ['trades', 'book', 'code'], aggregate: 'max' }), null);
  });

  it('refused in words: no such property, a value gone through, a to-many end not aggregated, nothing to aggregate', () => {
    assert.equal(relatedProblem(TREE, 'm::Trade', { path: ['client', 'region'] }), 'Trade has no property client');
    assert.match(relatedProblem(TREE, 'm::Trade', { path: ['notional', 'x'] })!, /notional is a Float: a path goes on only through a class/);
    assert.match(relatedProblem(TREE, 'm::Customer', { path: ['trades', 'notional'] })!,
      /trades.notional holds many for each Customer: aggregate them \(count, sum, average, min, max\) -- a row for each would count/);
    assert.match(relatedProblem(TREE, 'm::Trade', { path: ['customer', 'region'], aggregate: 'count' })!, /is one for each Trade: nothing to aggregate/);
    assert.match(relatedProblem(TREE, 'm::Customer', { path: ['trades', 'customer'], aggregate: 'sum' })!, /a class at a path's end is counted/);
    assert.match(relatedProblem(TREE, 'm::Customer', { path: ['trades', 'book', 'code'], aggregate: 'sum' })!, /code is String: a sum is of numbers/);
    assert.match(relatedProblem(TREE, 'm::Trade', { path: ['side'] })!, /side is an enumeration \(Side\)/);
    assert.equal(relatedProblem(TREE, 'm::Nope', { path: ['x'] }), 'the project has no class m::Nope');
  });

  it("the menu's offers one hop away: a to-one end's values; a to-many end's count and its values' aggregates", () => {
    assert.deepEqual(relatedOffers(TREE, 'm::Trade').map((o) => [o.property, o.many, o.columns.map(relatedName)]), [
      ['book', false, ['book_code']],
      ['customer', false, ['customer_name', 'customer_region', 'customer_id']],
    ]);
    const [trades] = relatedOffers(TREE, 'm::Customer');
    assert.deepEqual(trades?.columns.map(relatedName), [
      'trades_count', 'trades_notional_sum', 'trades_notional_average', 'trades_notional_min', 'trades_notional_max',
    ]);
  });
});

describe("the class's relation with its related columns projected", () => {
  const s: QuerySnapshot = {
    source: { query: classRelation('m::Trade', ['notional']) },
    columns: [{ name: 'notional', type: 'Float' }],
    derived: [{ name: 'customer_region', related: { path: ['customer', 'region'] }, kind: 'dimension' }],
    rows: [], pivotOn: [], sorts: [], epoch: 1,
  };

  it("each a column of the class's own projection: its path, a to-many end's aggregate", () => {
    const specs = ((relatedSource(s) as AppliedFunction).parameters[1] as unknown as {
      readonly value: { readonly colSpecs: readonly { readonly name: string; readonly function1: { readonly body: readonly unknown[] } }[] };
    }).value.colSpecs;
    assert.deepEqual(specs.map((c) => c.name), ['notional', 'customer_region']);
    assert.deepEqual(specs[1]!.function1.body[0], relatedValue({ path: ['customer', 'region'] }));
    assert.deepEqual(relatedValue({ path: ['customer', 'region'] }), property(property(variable('x'), 'customer'), 'region'));
    assert.deepEqual(relatedValue({ path: ['trades', 'notional'], aggregate: 'sum' }),
      fn('sum', property(property(variable('x'), 'trades'), 'notional')));
    // none: the source as it is
    assert.equal(relatedSource({ ...s, derived: [] }), s.source.query);
  });

  it("refused on rows that are no class's projection, and a snap of a grid with one", () => {
    const file: QuerySnapshot = { ...s, source: { query: fn('getAll', element('m::Trade')) } };
    assert.throws(() => relatedSource(file), (e: unknown) => e instanceof CubeRefusal && /no class's projection/.test(e.message));
    assert.throws(() => snapPull(s), (e: unknown) => e instanceof CubeRefusal && /a copy of its rows has not: take the related columns off/.test(e.message));
  });

  it('read from JSON as a calculate command carries one, a row-level column; refused in words', () => {
    const calc = (related: unknown, level = 'row'): unknown => ({ kind: 'calculate', level, column: { name: 'r', related } });
    assert.deepEqual(readGridCommand(calc({ path: ['trades', 'notional'], aggregate: 'sum' })),
      { kind: 'calculate', level: 'row', column: { name: 'r', related: { path: ['trades', 'notional'], aggregate: 'sum' } } });
    assert.throws(() => readGridCommand(calc({ path: [] })), /at least one/);
    assert.throws(() => readGridCommand(calc({ path: ['trades'], aggregate: 'median' })), /aggregated by count, sum, average, min, max, not "median"/);
    assert.throws(() => readGridCommand(calc({ path: ['x'], by: 1 })), /a related column takes path, aggregate, not by/);
  });
});
