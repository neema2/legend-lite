import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type {
  CubeSnapshot,
  FilterOperator,
} from '../src/snapshot.ts';
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

  it('pivots a cube with NO configured measures', () => {
    // This used to throw `CubeRefusal`. The refusal was thrown from
    // inside a floating refresh, so clicking "Horizontal Pivot on
    // region" produced an uncaught error, the grid kept the previous
    // answer with no explanation, and `pivotOn` stayed set -- so
    // every later query threw the same thing and one click wedged
    // the cube until a reload.
    //
    // Upstream never refuses: `_pivotAggCols` takes the selected
    // MEASURE columns and `_fixEmptyAggCols` substitutes a filler
    // count if there are none. The same cube already grouped happily,
    // because the groupBy path has always synthesised its aggregates.
    const s = serialize(snap({ measures: [] }));
    assert.match(s, /pivot\(~\[year\], ~\[/, 'it pivots at all');
    // `notional` is a Float, so it is a measure and it sums.
    assert.match(s, /notional:x\|\$x\.notional:y\|\$y->sum\(\)/);
    // And the DIMENSIONS are not aggregated: upstream excludes them
    // from a pivot deliberately, unlike a groupBy.
    assert.equal(/country:x\|/.test(s), false,
      'a dimension must not become a pivot aggregate');
  });

  it('falls back to a count when a pivot has nothing to aggregate', () => {
    // `_fixEmptyAggCols`: a pivot must aggregate something, so a cube
    // of nothing but dimensions still produces a query rather than
    // `pivot(~[year], ~[])`, which the compiler rejects far from the
    // cause.
    const s = serialize(snap({
      measures: [],
      columns: [
        { name: 'region', type: 'String' },
        { name: 'country', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
      ],
    }));
    assert.match(s, /~\[count:x\|/, 'the filler count is there');
  });

  it('maps count to the constant 1, not to a column', () => {
    const s = serialize(
      snap({ measures: [{ name: 'n', column: 'notional', fn: 'count' }] }),
    );
    assert.match(s, /~\[n:x\|1:y\|\$y->count\(\)\]/);
    // 'notional' is not needed by count, so it must not be selected.
    assert.equal(s.includes('notional'), false);
  });

  it('pairs value with weight in the MAP, where the row is in scope', () => {
    // This test previously asserted `y|$y->wavg($y.qty)` and passed,
    // while the engine rejected that query outright: the reduce sees a
    // collection of mapped NUMBERS, so `$y.qty` is an access on Float.
    // A green assertion on a string the engine will not accept is not
    // verification -- the string was well-formed and wrong. The torture
    // run against a real engine is what caught it, which is the whole
    // argument for having one.
    const s = serialize(
      snap({
        measures: [
          { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' },
        ],
      }),
    );
    assert.match(
      s,
      /w:x\|\$x\.notional->wavgRowMapper\(\$x\.qty\):y\|\$y->wavg\(\)/,
    );
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

  it('caps a level, AFTER the sort', () => {
    // A limit before the sort caps an arbitrary subset, so the first
    // page is not the first page.
    const out = serialize(snap(), { level: 1, parent: [], limit: 1001 });
    assert.match(out, /->sort\(\[~region->ascending\(\)\]\)->limit\(1001\)$/);
  });

  it('does not cap the grand total, which is one row', () => {
    const out = serialize(snap(), { level: 0, parent: [], limit: 1001 });
    assert.equal(out.includes('limit('), false);
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

  it('lowers both columns for a case-insensitive column comparison', () => {
    assert.equal(
      cond('equalCaseInsensitiveColumn', { rightColumn: 'country' }),
      '$x.region->toLower() == $x.country->toLower()',
    );
    assert.equal(
      cond('notEqualCaseInsensitiveColumn', { rightColumn: 'country' }),
      '$x.region->toLower() != $x.country->toLower()',
    );
  });

  it("covers all 31 of DataCube's operators", () => {
    // Counted from DataCubeQueryFilterOperator rather than
    // remembered: the first pass had 29 and was missing the
    // case-insensitive column-to-column pair.
    const operators: FilterOperator[] = [
      'equal',
      'notEqual',
      'lessThan',
      'lessThanEqual',
      'greaterThan',
      'greaterThanEqual',
      'isEmpty',
      'isNotEmpty',
      'contains',
      'notContains',
      'startsWith',
      'notStartsWith',
      'endsWith',
      'notEndsWith',
      'in',
      'notIn',
      'equalCaseInsensitive',
      'notEqualCaseInsensitive',
      'containsCaseInsensitive',
      'startsWithCaseInsensitive',
      'endsWithCaseInsensitive',
      'inCaseInsensitive',
      'notInCaseInsensitive',
      'equalColumn',
      'equalCaseInsensitiveColumn',
      'notEqualColumn',
      'notEqualCaseInsensitiveColumn',
      'lessThanColumn',
      'lessThanEqualColumn',
      'greaterThanColumn',
      'greaterThanEqualColumn',
    ];
    assert.equal(new Set(operators).size, 31);
    // Every one must RENDER rather than fall through to the throw.
    for (const operator of operators) {
      const out = filterExpression({
        kind: 'condition',
        column: 'region',
        operator,
        value: operator.toLowerCase().includes('in') ? ['A'] : 'A',
        rightColumn: 'country',
      });
      assert.ok(out.length > 0, operator);
    }
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
    // A TIMESTAMP keeps its time. This asserted `%2024-03-01` for a
    // Date carrying midday, i.e. every temporal was truncated to ten
    // characters -- so a group key for one minute of trading matched
    // the whole day and drilling into it returned every trade in it.
    // A wrong answer with no error is worse than an error.
    //
    // Read in LOCAL terms, because that is the frame these Dates are
    // built and displayed in; midnight still prints as a plain date.
    const noon = new Date('2024-03-01T12:00:00Z');
    const p = (n: number): string => String(n).padStart(2, '0');
    assert.equal(
      literal(noon),
      `%2024-03-01T${p(noon.getHours())}:${p(noon.getMinutes())}:00`,
    );
    const midnight = new Date(2024, 2, 1);
    assert.equal(literal(midnight), '%2024-03-01', 'midnight is a date');
  });
});

describe('drilling into a TEMPORAL group', () => {
  // Grouping by a date or timestamp column produced invalid SQL:
  //
  //   Conversion Error: invalid timestamp field format:
  //   "Fri Jan 01 2021 03:58:00 GMT-0500 (Eastern Standard Time)"
  //
  // A row path is TEXT, one string per level, and the key was written
  // with `String(date)` -- the locale form -- then fed back into the
  // next level's query as a filter value. So the group appeared and
  // opening it failed, while the grid kept the previous answer.
  const TEMPORAL: CubeSnapshot = {
    source: { expression: 't' },
    columns: [
      { name: 'booked_at', type: 'DateTime' },
      { name: 'trade_date', type: 'StrictDate' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: ['booked_at'],
    pivotOn: [],
    measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
    sorts: [],
    epoch: 1,
  };

  /** The key exactly as the tree writes it: local parts, no zone. */
  const key = (d: Date): string => {
    const p = (n: number): string => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`
      + `T${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  };

  it('filters on a TIMESTAMP literal, not on a locale string', () => {
    const at = new Date(2021, 0, 1, 3, 58, 0);
    const out = serialize(TEMPORAL, { level: 2, parent: [key(at)] });
    assert.match(out, /%2021-01-01T03:58:00/,
      'the drilldown must carry a datetime literal');
    // The shape of the old failure: the locale string, quoted.
    assert.equal(/GMT|Eastern|Standard Time/.test(out), false,
      'a JS Date toString reached the query');
    assert.equal(/'2021-01-01T03:58:00'/.test(out), false,
      'the timestamp went in as a STRING, which the engine refuses');
  });

  it('keeps a date-only group on its own day', () => {
    // The fourth timezone fault in this area would be here: a date
    // built at LOCAL midnight read back through UTC names the day
    // before in any zone ahead of it.
    const day = new Date(2021, 0, 1);
    const out = serialize(
      { ...TEMPORAL, rows: ['trade_date'] },
      { level: 2, parent: [key(day)] },
    );
    assert.match(out, /%2021-01-01/);
    assert.equal(out.includes('2020-12-31'), false, 'it slipped a day');
  });

  it('a non-temporal key is still compared as text', () => {
    const out = serialize(
      { ...TEMPORAL,
        columns: [...TEMPORAL.columns, { name: 'region', type: 'String' }],
        rows: ['region'] },
      { level: 2, parent: ['AMER'] },
    );
    assert.match(out, /'AMER'/);
  });
});

describe('the DETAIL cube: no grouping, no pivot, no measures', () => {
  // The plainest thing this product can show, and the least tested.
  // It referenced no columns, so nothing was projected; and it had
  // no grouping columns, so a guard meant for the grand total threw
  // away its sort and its row cap as well. The simplest grid was the
  // one that honoured neither.
  const DETAIL: CubeSnapshot = {
    source: { expression: 't' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };

  it('projects the columns the cube declares', () => {
    assert.equal(serialize(DETAIL), 't->select(~[region, notional])');
  });

  it('includes derived columns in the projection', () => {
    const withDerived: CubeSnapshot = {
      ...DETAIL,
      derived: [{ name: 'net', expression: '$x.notional * 2' }],
    };
    assert.match(serialize(withDerived), /select\(~\[region, notional, net\]\)/);
  });

  it('HONOURS its sort', () => {
    const sorted: CubeSnapshot = {
      ...DETAIL,
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    assert.match(serialize(sorted), /->sort\(\[~region->ascending\(\)\]\)$/);
  });

  it('HONOURS its row cap', () => {
    const pure = serialize(DETAIL, { level: 0, parent: [], limit: 100 });
    assert.match(pure, /->limit\(100\)$/);
  });

  it('caps AFTER sorting, so the first page is the first page', () => {
    const sorted: CubeSnapshot = {
      ...DETAIL,
      sorts: [{ column: 'notional', direction: 'desc' }],
    };
    const pure = serialize(sorted, { level: 0, parent: [], limit: 10 });
    assert.ok(
      pure.indexOf('->sort(') < pure.indexOf('->limit('),
      pure,
    );
  });

  it('still leaves the GRAND TOTAL unsorted and uncapped', () => {
    // One row: sorting and limiting it is noise. This is the case
    // the old guard was written for, and it must keep working.
    const total: CubeSnapshot = {
      ...DETAIL,
      measures: [{ name: 'n', column: 'notional', fn: 'sum' }],
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    const pure = serialize(total, { level: 0, parent: [], limit: 100 });
    assert.equal(pure.includes('->sort('), false, pure);
    assert.equal(pure.includes('->limit('), false, pure);
  });
});
