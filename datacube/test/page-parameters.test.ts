// A PAGE'S PARAMETERS (page-parameters.ts; docs/DATACUBE_FLOW_MODE_2026_10_24.md M8): named values any Pure on the page
// reads as `$name`, bound where a query is sent -- each read put in as its literal, but under a lambda of its own of that
// name -- and what a parameter may be, refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { fn, gt, lambda, lit, property, toJson, variable, type Lambda } from '../../pure-protocol/src/index.ts';
import {
  bothLenses, boundIn, parameterProblem, parametersLens, parametersRead, parameterWords, readParameters, type PageParameter,
} from '../src/page-parameters.ts';
import { CubeRefusal } from '../src/snapshot.ts';

const MIN: PageParameter = { name: 'min', type: 'Float', value: 90 };
const REGION: PageParameter = { name: 'region', type: 'String', value: 'EMEA', choices: ['EMEA', 'AMER'], label: 'Region' };

/** `rows->filter(x|$x.notional > $min)` over a table read as `$rows`, a lambda of no parameter. */
const filtered = (): Lambda => lambda([], fn('filter', variable('rows'), lambda(['x'], gt(property(variable('x'), 'notional'), variable('min')))));

describe("a page's parameters, bound where a query is sent", () => {
  it('puts each $name a query reads in as its literal -- nothing else changed', () => {
    const { lens, key } = parametersLens([MIN, REGION]);
    assert.ok(lens);
    const bound = lens(filtered());
    assert.equal(toJson(bound), toJson(lambda([], fn('filter', variable('rows'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.float(90)))))));
    assert.deepEqual(parametersRead(filtered(), [MIN, REGION]), ['min']);
    assert.equal(key, JSON.stringify([['min', 'Float', 90], ['region', 'String', 'EMEA']]), 'a value changed, its key changed');
    assert.deepEqual(parametersLens([]), { key: '' }, 'a page with none binds nothing');
  });

  it('reads its own under a lambda of a parameter of that name', () => {
    const shadowed = lambda([], fn('map', variable('min'), lambda(['min'], fn('plus', variable('min'), variable('region')))));
    const bound = parametersLens([MIN, REGION]).lens!(shadowed);
    assert.equal(toJson(bound), toJson(lambda([], fn('map', lit.float(90), lambda(['min'], fn('plus', variable('min'), lit.string('EMEA')))))));
    assert.deepEqual(parametersRead(shadowed, [MIN, REGION]), ['min', 'region']);
    assert.deepEqual(parametersRead(lambda([], lambda(['min'], variable('min'))), [MIN]), [], 'its own, no parameter of the page');
  });

  it('binds every lambda of a source resolved to be opened -- its inputs\' copies, a Pure box\'s function -- nothing else', () => {
    const exact = { notional: 90 };
    const source = { _type: 'rows', name: 'kept', pure: lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), variable('min'))))),
      input: { name: 'trades', query: { derived: [{ name: 'big', lambda: lambda(['x'], gt(property(variable('x'), 'notional'), variable('min'))) }], filter: exact } } };
    const bound = boundIn(source, [MIN]);
    assert.equal(toJson(bound.pure), toJson(lambda(['input'], fn('filter', variable('input'), lambda(['x'], gt(property(variable('x'), 'notional'), lit.float(90)))))));
    assert.equal(toJson(bound.input.query.derived[0]!.lambda), toJson(lambda(['x'], gt(property(variable('x'), 'notional'), lit.float(90)))));
    assert.deepEqual(bound.input.query.filter, exact, 'what is no lambda, as it is');
    assert.equal(boundIn(source, []), source, 'a page with none: the source itself');
  });

  it("never binds a lambda's own parameter: a stored one's own, by name, is its own (the roadmap audit of 2026-10-26)", () => {
    const ROWS: PageParameter = { name: 'rows', type: 'Integer', value: 5 };
    const R: PageParameter = { name: 'r', type: 'Integer', value: 7 };
    const pure = lambda(['rows'], fn('limit', variable('rows'), lit.integer(3)));
    const derived = lambda(['r'], gt(property(variable('r'), 'amount'), variable('min')));
    const bound = boundIn({ pure, derived }, [ROWS, R, MIN]);
    assert.equal(toJson(bound.pure), toJson(pure), 'its own $rows');
    assert.equal(toJson(bound.derived), toJson(lambda(['r'], gt(property(variable('r'), 'amount'), lit.float(90)))), 'its $min bound, its own $r not');
    assert.deepEqual(parametersRead(pure, [ROWS]), [], 'it reads no parameter of the page');
    assert.deepEqual(parametersRead(derived, [R, MIN]), ['min']);
  });

  it('two lenses one, either alone the other', () => {
    const a = (q: Lambda): Lambda => ({ ...q, body: [fn('a', ...q.body)] });
    const b = (q: Lambda): Lambda => ({ ...q, body: [fn('b', ...q.body)] });
    assert.equal(toJson(bothLenses(a, b)!(lambda([], lit.integer(1)))), toJson(lambda([], fn('b', fn('a', lit.integer(1))))));
    assert.equal(bothLenses(undefined, b), b);
    assert.equal(bothLenses(a, undefined), a);
    assert.equal(bothLenses(undefined, undefined), undefined);
  });

  it('refuses in words a parameter that is not one', () => {
    const problem = (p: Partial<PageParameter>, others: readonly PageParameter[] = []): string | undefined =>
      parameterProblem({ ...MIN, ...p } as PageParameter, others);
    assert.equal(problem({}), undefined);
    assert.match(problem({ name: '2nd' })!, /letters, digits and _/);
    assert.match(problem({ name: 'x' })!, /what DataCube's own expressions call what they read/);
    assert.match(problem({}, [MIN])!, /a parameter 'min' already/);
    assert.match(problem({ value: 'many' })!, /min's value is no Float: "many"/);
    assert.match(problem({ type: 'Integer', value: 1.5 })!, /is no Integer/);
    assert.match(problem({ type: 'Boolean', value: 'yes' })!, /is true or false, not "yes"/);
    assert.match(problem({ type: 'StrictDate', value: '31/01/2024' })!, /is a day, as 2024-01-31/);
    assert.match(problem({ type: 'String', value: 3 })!, /is a text, not 3/);
    assert.match(problem({ choices: [80, 100] })!, /value 90 is none of its choices/);
    assert.match(problem({ choices: [90, 90] })!, /choices name one twice/);
    assert.match(problem({ choices: [] })!, /choices are none/);
    assert.match(problem({ choices: [90, 'a'] })!, /choice 2 is no Float/);
    assert.equal(parameterWords(REGION), 'Region = EMEA');
  });

  it('reads them from JSON, each judged, a name once', () => {
    assert.deepEqual(readParameters([MIN, REGION]), [MIN, REGION]);
    assert.deepEqual(readParameters(undefined), []);
    assert.throws(() => readParameters([MIN, MIN]), (e: unknown) => e instanceof CubeRefusal && /already/.test(e.message));
    assert.throws(() => readParameters([{ ...MIN, extra: 1 }]), /takes no extra/);
    assert.throws(() => readParameters([{ name: 'n', type: 'Money', value: 1 }]), /n is a String, Integer/);
    assert.throws(() => readParameters({}), /a list/);
  });
});
