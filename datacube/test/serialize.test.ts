import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CubeSnapshot } from '../src/snapshot.ts';
import { totalOrderSorts } from '../src/snapshot.ts';
import {
  NULL_GROUP,
  filterExpression,
  ident,
  literal,
  serialize,
} from '../src/serialize.ts';

/** A minimal trades cube, overridable per test. */
function snap(over: Partial<CubeSnapshot> = {}): CubeSnapshot {
  return {
    source: { expression: '$trades' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'country', type: 'String' },
      { name: 'year', type: 'Integer' },
      { name: 'notional', type: 'Float' },
      { name: 'qty', type: 'Integer' },
    ],
    derived: [],
    rows: ['region', 'country'],
    pivotOn: ['year'],
    measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
    ...over,
  };
}

describe('serialize', () => {
  it('emits a pivot cube with implicit grouping', () => {
    assert.equal(
      serialize(snap()),
      '$trades->select(~[region, country, year, notional])' +
        '->pivot(~[year], ~[total:x|$x.notional:y|$y->sum()])' +
        '->sort([~region->ascending(), ~country->ascending()])',
    );
  });

  it('uses groupBy when there is no column dimension', () => {
    assert.equal(
      serialize(snap({ pivotOn: [] })),
      '$trades->select(~[region, country, notional])' +
        '->groupBy(~[region, country], ~[total:x|$x.notional:y|$y->sum()])' +
        '->sort([~region->ascending(), ~country->ascending()])',
    );
  });

  it('maps count to the constant 1, not to a column', () => {
    const s = serialize(
      snap({ measures: [{ name: 'n', column: 'notional', fn: 'count' }] }),
    );
    assert.match(s, /~\[n:x\|1:y\|\$y->count\(\)\]/);
    // 'notional' is not needed by count, so it must not be selected.
    assert.equal(s.includes('notional'), false);
  });

  it('carries the weight column through wavg', () => {
    const s = serialize(
      snap({
        measures: [
          { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' },
        ],
      }),
    );
    assert.match(s, /w:x\|\$x\.notional:y\|\$y->wavg\(\$y\.qty\)/);
    assert.match(s, /select\(~\[region, country, year, notional, qty\]\)/);
  });

  it('refuses wavg without a weight rather than degrading silently', () => {
    assert.throws(
      () =>
        serialize(
          snap({ measures: [{ name: 'w', column: 'notional', fn: 'wavg' }] }),
        ),
      /uses wavg but has no weight column/,
    );
  });

  it('appends row dimensions to make the sort a total order', () => {
    const s = snap({ sorts: [{ column: 'notional', direction: 'desc' }] });
    assert.deepEqual(totalOrderSorts(s), [
      { column: 'notional', direction: 'desc' },
      { column: 'region', direction: 'asc' },
      { column: 'country', direction: 'asc' },
    ]);
    assert.match(
      serialize(s),
      /sort\(\[~notional->descending\(\), ~region->ascending\(\), ~country->ascending\(\)\]\)/,
    );
  });

  it('does not duplicate a row dimension already sorted on', () => {
    const s = snap({ sorts: [{ column: 'region', direction: 'desc' }] });
    assert.deepEqual(totalOrderSorts(s), [
      { column: 'region', direction: 'desc' },
      { column: 'country', direction: 'asc' },
    ]);
  });

  it('emits the row window as slice(offset, end)', () => {
    assert.match(
      serialize(snap({ window: { offset: 100, limit: 50 } })),
      /->slice\(100, 150\)$/,
    );
  });

  it('emits extend before filter', () => {
    const s = serialize(
      snap({
        derived: [{ name: 'net', expression: '$x.notional * 0.98' }],
        filter: {
          kind: 'condition',
          column: 'region',
          operator: 'equal',
          value: 'EMEA',
        },
      }),
    );
    assert.match(
      s,
      /^\$trades->extend\(~\[net: x\|\$x\.notional \* 0\.98\]\)->filter\(/,
    );
  });

  it('pins pivot values when deliberately narrowed', () => {
    assert.match(
      serialize(snap({ pivotValues: [2023, 2024] })),
      /->pivot\(~\[year\], \[2023, 2024\], ~\[total:/,
    );
  });
});

describe('serialize with a level scope', () => {
  it('groups the grand total by nothing at all', () => {
    // level 0: every grouping column dropped, which is exactly what
    // makes the grand total the same expression as the detail.
    const out = serialize(snap(), { level: 0, parent: [] });
    assert.equal(
      out,
      '$trades->select(~[year, notional])' +
        '->pivot(~[year], ~[total:x|$x.notional:y|$y->sum()])',
    );
    // A single row needs no ordering or slicing.
    assert.equal(out.includes('sort('), false);
    assert.equal(out.includes('slice('), false);
  });

  it('groups the top level by the first dimension only', () => {
    assert.equal(
      serialize(snap(), { level: 1, parent: [] }),
      '$trades->select(~[region, year, notional])' +
        '->pivot(~[year], ~[total:x|$x.notional:y|$y->sum()])' +
        '->sort([~region->ascending()])',
    );
  });

  it('pins the parent branch when expanding', () => {
    const out = serialize(snap(), { level: 2, parent: ['EMEA'] });
    assert.match(out, /filter\(x\|\$x\.region == 'EMEA'\)/);
    assert.match(out, /select\(~\[region, country, year, notional\]\)/);
  });

  it('ands the parent branch onto the user filter', () => {
    const s = snap({
      filter: {
        kind: 'condition',
        column: 'notional',
        operator: 'greaterThan',
        value: 100,
      },
    });
    assert.match(
      serialize(s, { level: 2, parent: ['EMEA'] }),
      /filter\(x\|\(\$x\.notional > 100 && \$x\.region == 'EMEA'\)\)/,
    );
  });

  it('matches a NULL group key with isEmpty, not equals', () => {
    // '== null' matches nothing in SQL, so expanding a null group
    // would silently return no children.
    assert.match(
      serialize(snap(), { level: 2, parent: [NULL_GROUP] }),
      /filter\(x\|\$x\.region->isEmpty\(\)\)/,
    );
  });

  it('never orders by a dimension deeper than the level', () => {
    // 'country' does not exist in a level-1 result; naming it in the
    // ORDER BY would be a compile error at the engine.
    const out = serialize(snap(), { level: 1, parent: [] });
    assert.equal(out.includes('country'), false);
  });

  it('keeps a measure sort while dropping a deeper dimension sort', () => {
    const s = snap({
      sorts: [
        { column: 'total', direction: 'desc' },
        { column: 'country', direction: 'desc' },
      ],
    });
    assert.match(
      serialize(s, { level: 1, parent: [] }),
      /sort\(\[~total->descending\(\), ~region->ascending\(\)\]\)/,
    );
  });

  it('is unchanged without a scope', () => {
    assert.equal(serialize(snap()), serialize(snap(), undefined));
  });

  it('subtotal and detail differ only by a grouping column', () => {
    // The property the whole design rests on: strip the level-2
    // query of its second dimension and it IS the level-1 query.
    const detail = serialize(snap(), { level: 2, parent: [] });
    const subtotal = serialize(snap(), { level: 1, parent: [] });
    assert.equal(
      detail
        .replace('~[region, country, year, notional]', '~[region, year, notional]')
        .replace('sort([~region->ascending(), ~country->ascending()])',
                 'sort([~region->ascending()])'),
      subtotal,
    );
  });
});

describe('filterExpression', () => {
  it('renders comparisons', () => {
    assert.equal(
      filterExpression({
        kind: 'condition',
        column: 'year',
        operator: 'greaterThanEqual',
        value: 2020,
      }),
      '$x.year >= 2020',
    );
  });

  it('renders nested and/or with parentheses', () => {
    assert.equal(
      filterExpression({
        kind: 'and',
        children: [
          {
            kind: 'condition',
            column: 'region',
            operator: 'equal',
            value: 'EMEA',
          },
          {
            kind: 'or',
            children: [
              {
                kind: 'condition',
                column: 'year',
                operator: 'equal',
                value: 2023,
              },
              {
                kind: 'condition',
                column: 'year',
                operator: 'equal',
                value: 2024,
              },
            ],
          },
        ],
      }),
      "($x.region == 'EMEA' && ($x.year == 2023 || $x.year == 2024))",
    );
  });

  it('renders emptiness and membership', () => {
    assert.equal(
      filterExpression({
        kind: 'condition',
        column: 'country',
        operator: 'isEmpty',
      }),
      '$x.country->isEmpty()',
    );
    assert.equal(
      filterExpression({
        kind: 'not',
        child: {
          kind: 'condition',
          column: 'country',
          operator: 'in',
          value: ['US', 'GB'],
        },
      }),
      "!($x.country->in(['US', 'GB']))",
    );
  });

  it('treats an empty group as a no-op rather than an error', () => {
    assert.equal(filterExpression({ kind: 'and', children: [] }), 'true');
    assert.equal(filterExpression({ kind: 'or', children: [] }), 'false');
  });
});

describe('the full filter vocabulary', () => {
  const cond = (
    operator: string,
    extra: Record<string, unknown> = {},
  ): string =>
    filterExpression({
      kind: 'condition',
      column: 'region',
      operator,
      ...extra,
    } as never);

  it('renders negated string tests', () => {
    assert.equal(cond('notContains', { value: 'X' }), "!$x.region->contains('X')");
    assert.equal(
      cond('notStartsWith', { value: 'X' }),
      "!$x.region->startsWith('X')",
    );
    assert.equal(cond('notEndsWith', { value: 'X' }), "!$x.region->endsWith('X')");
    assert.equal(
      cond('notIn', { value: ['A', 'B'] }),
      "!$x.region->in(['A', 'B'])",
    );
  });

  it('uses isNotEmpty rather than negating isEmpty', () => {
    // The engine has the function; using its own vocabulary keeps a
    // generated query readable for whoever has to debug it.
    assert.equal(cond('isNotEmpty'), '$x.region->isNotEmpty()');
  });

  it('lowers BOTH sides for case-insensitive comparisons', () => {
    // Relying on collation would let the same cube answer differently
    // on two backends.
    assert.equal(
      cond('equalCaseInsensitive', { value: 'EMEA' }),
      "$x.region->toLower() == 'emea'",
    );
    assert.equal(
      cond('containsCaseInsensitive', { value: 'Em' }),
      "$x.region->toLower()->contains('em')",
    );
    assert.equal(
      cond('inCaseInsensitive', { value: ['EMEA', 'Amer'] }),
      "$x.region->toLower()->in(['emea', 'amer'])",
    );
    assert.equal(
      cond('notInCaseInsensitive', { value: ['EMEA'] }),
      "!$x.region->toLower()->in(['emea'])",
    );
  });

  it('compares two columns', () => {
    assert.equal(
      cond('greaterThanColumn', { rightColumn: 'country' }),
      '$x.region > $x.country',
    );
    assert.equal(
      cond('equalColumn', { rightColumn: 'country' }),
      '$x.region == $x.country',
    );
  });

  it('refuses a column comparison with no second column', () => {
    assert.throws(
      () => cond('equalColumn'),
      /needs a rightColumn/,
    );
  });

  it('quotes an awkward column name on both sides', () => {
    assert.equal(
      filterExpression({
        kind: 'condition',
        column: 'odd name',
        operator: 'equalColumn',
        rightColumn: '2023__|__total',
      }),
      "$x.'odd name' == $x.'2023__|__total'",
    );
  });
});

describe('ident and literal', () => {
  it('leaves plain identifiers alone and quotes the rest', () => {
    assert.equal(ident('region'), 'region');
    assert.equal(ident('_a1'), '_a1');
    // The pivot separator legend-lite and DataCube both use.
    assert.equal(ident('2011__|__total'), "'2011__|__total'");
    assert.equal(ident("it's"), "'it\\'s'");
  });

  it('escapes string literals and formats dates', () => {
    assert.equal(literal("O'Hara"), "'O\\'Hara'");
    assert.equal(literal(42), '42');
    assert.equal(literal(true), 'true');
    assert.equal(literal(new Date('2024-03-01T12:00:00Z')), '%2024-03-01');
  });
});
