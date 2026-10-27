// The value list's rule (src/value-filter.ts) and the date ranges' (src/date-ranges.ts): the conditions a choice makes,
// the column's own conditions it replaces, and the question asked of the database (query.ts columnValuesLambda); and a
// top N's ranking (query.ts levelRelation) -- each as the compiler prints it.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DATE_RANGES, dateRangeConditions } from '../src/date-ranges.ts';
import { columnValuesLambda, levelLambda } from '../src/query.ts';
import { CubeRefusal, isRelativeDate, relativeDateWords, type FilterNode, type QuerySnapshot } from '../src/snapshot.ts';
import { cellFilterValue, filterWithout, keptCondition, ownConditions } from '../src/value-filter.ts';
import { variable } from '../../pure-protocol/src/index.ts';
import { print } from './lite-compiler.ts';

const region = (operator: 'equal' | 'in', value: string | readonly string[]): FilterNode =>
  ({ kind: 'condition', column: 'region', operator, value });
const big: FilterNode = { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 100 };

describe("a column's own conditions: what a value list replaces", () => {
  it('the whole filter when it is one of the column, the AND\'s children of the column, else none', () => {
    assert.deepEqual(ownConditions(region('equal', 'EMEA'), 'region'), [region('equal', 'EMEA')]);
    const both: FilterNode = { kind: 'and', children: [region('in', ['EMEA', 'AMER']), big] };
    assert.deepEqual(ownConditions(both, 'region'), [region('in', ['EMEA', 'AMER'])]);
    assert.deepEqual(ownConditions(both, 'desk'), []);
    assert.deepEqual(ownConditions(undefined, 'region'), []);
    // the value list's own OR -- its values or the blanks -- is the column's too
    const kept = keptCondition('region', ['EMEA', null]);
    assert.deepEqual(ownConditions({ kind: 'and', children: [kept, big] }, 'region'), [kept]);
    // an OR across columns is no one column's
    const across: FilterNode = { kind: 'or', children: [region('equal', 'EMEA'), big] };
    assert.deepEqual(ownConditions(across, 'region'), []);
  });

  it('the filter without them, what the values are read under', () => {
    assert.equal(filterWithout(region('equal', 'EMEA'), 'region'), undefined);
    assert.deepEqual(filterWithout({ kind: 'and', children: [region('equal', 'EMEA'), big] }, 'region'), big);
    assert.deepEqual(filterWithout(big, 'region'), big);
  });
});

describe('the condition a choice makes', () => {
  it('one value an equal, several an in, the blanks isEmpty, both an OR; nothing kept refused', () => {
    assert.deepEqual(keptCondition('region', ['EMEA']), region('equal', 'EMEA'));
    assert.deepEqual(keptCondition('region', ['EMEA', 'AMER']), region('in', ['EMEA', 'AMER']));
    assert.deepEqual(keptCondition('region', [null]), { kind: 'condition', column: 'region', operator: 'isEmpty' });
    assert.deepEqual(keptCondition('region', ['EMEA', null]),
      { kind: 'or', children: [region('equal', 'EMEA'), { kind: 'condition', column: 'region', operator: 'isEmpty' }] });
    assert.throws(() => keptCondition('region', []), (e: unknown) => e instanceof CubeRefusal && /at least one value/.test(e.message));
  });

  it("a cell's value as the menu's Add Filter takes it: a big integer's digits, a JSON column's document", () => {
    assert.equal(cellFilterValue(12n, 'Integer'), '12');
    assert.deepEqual(cellFilterValue('{"a":1}', 'Variant'), { json: '{"a":1}' });
    assert.equal(cellFilterValue(null, 'String'), null);
    assert.equal(cellFilterValue('EMEA', 'String'), 'EMEA');
  });
});

describe('the question a value list asks', () => {
  const s: QuerySnapshot = {
    source: { query: variable('trades') },
    columns: [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }],
    derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
    filter: { kind: 'and', children: [region('equal', 'EMEA'), big] },
  };

  it("the column's values under the other filters, each with its rows counted, in order, one past the cap", async () => {
    assert.equal(await print(columnValuesLambda(s, 'region')),
      '|$trades->filter(x|$x.notional > 100.0)->select(~[region])'
      + '->groupBy(~[region], ~[__rows__:x|1:y|$y->count()])->sort([~region->ascending()])->limit(1001)');
  });
});

describe('date ranges, relative to when the query runs', () => {
  it('each from its start (kept) to its end (left out)', () => {
    assert.deepEqual(dateRangeConditions('day', 'lastMonth'), [
      { kind: 'condition', column: 'day', operator: 'greaterThanEqual',
        value: { relative: 'today', startOf: 'month', offset: -1, unit: 'MONTHS' } },
      { kind: 'condition', column: 'day', operator: 'lessThan', value: { relative: 'today', startOf: 'month' } },
    ]);
    assert.deepEqual(dateRangeConditions('day', 'yearToDate').map((c) => (c as { value: unknown }).value), [
      { relative: 'today', startOf: 'year' }, { relative: 'today', offset: 1, unit: 'DAYS' },
    ]);
    for (const r of DATE_RANGES) assert.equal(dateRangeConditions('day', r).length, 2, r);
    assert.throws(() => dateRangeConditions('day', 'someday' as never), /a date range is today, yesterday/);
  });
});

describe("a date range's ends, as the compiler writes them: the database's date arithmetic", () => {
  it('today, taken to its period\'s start, then moved', async () => {
    const [from, to] = dateRangeConditions('trade_date', 'lastMonth');
    const s: QuerySnapshot = {
      source: { query: variable('trades') },
      columns: [{ name: 'trade_date', type: 'StrictDate' }],
      derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
      filter: { kind: 'and', children: [from!, to!] },
    };
    // the values a range keeps, its own conditions kept: the filter as the compiler prints it
    const text = await print(columnValuesLambda(s, 'trade_date', true));
    assert.match(text, /\(\$x\.trade_date >= today\(\)->firstDayOfMonth\(\)->adjust\(-1, meta::pure::functions::date::DurationUnit\.MONTHS\)\) && \(\$x\.trade_date < today\(\)->firstDayOfMonth\(\)\)/, text);
  });

  it('a range in words, as the filter bar and the menu say it', () => {
    assert.equal(relativeDateWords({ relative: 'today', startOf: 'month', offset: -1, unit: 'MONTHS' }),
      'the start of this month - 1 month');
    assert.equal(relativeDateWords({ relative: 'today', offset: -29, unit: 'DAYS' }), 'today - 29 days');
    assert.equal(relativeDateWords({ relative: 'now' }), 'now');
    // read strictly: a range's end is one of these shapes, nothing else
    assert.equal(isRelativeDate({ relative: 'today', offset: 1 }), false, 'an offset needs its unit');
    assert.equal(isRelativeDate({ relative: 'today', startOf: 'fortnight' }), false);
    assert.equal(isRelativeDate({ relative: 'today', offset: 1.5, unit: 'DAYS' }), false);
    assert.equal(isRelativeDate({ relative: 'today', extra: 1 }), false);
  });
});

describe('top or bottom N: ranked by the database after the grouping', () => {
  const s: QuerySnapshot = {
    source: { query: variable('trades') },
    columns: [{ name: 'region', type: 'String' }, { name: 'desk', type: 'String' }, { name: 'notional', type: 'Float' }],
    derived: [], rows: ['region', 'desk'], pivotOn: [], sorts: [], epoch: 1,
    measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    topN: { by: 'notional', n: 2 },
  };

  it('each level ranks its parent\'s groups, ties kept, and keeps N; the grand total is not ranked', async () => {
    const level = await print(levelLambda(s, { level: 2, parent: ['EMEA'] }));
    assert.ok(level.includes('->groupBy(~[region, desk], ~[notional:x|$x.notional:y|$y->sum()])'
      + '->extend(~[__all__topN_empty:x|$x.notional->isEmpty()])'
      + '->extend(over([~__all__topN_empty->ascending(), ~notional->descending()]), ~[__all__topN:{p, w, r|$p->rank($w, $r)}])'
      + '->filter(x|$x.__all__topN <= 2)->sort('), level);
    // the largest first, its empties last: led by whether it is empty (the correctness audit's 3, 2026-10-26); the smallest
    // first puts them last as it is
    assert.doesNotMatch(await print(levelLambda(s, { level: 0, parent: [] })), /topN/, 'the grand total');
    assert.match(await print(levelLambda({ ...s, topN: { by: 'notional', n: 3, bottom: true } }, { level: 1, parent: [] })),
      /over\(\[~notional->ascending\(\)\]\).*<= 3/, 'the bottom: the smallest first');
  });
});
