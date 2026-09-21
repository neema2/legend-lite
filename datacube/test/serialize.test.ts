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
    // AND KEEPS EVERY COLUMN IT SHOWS. This projected the keys and
    // the measure alone, so clearing a cube's column pivot left one
    // data column on screen and the rest gone -- from a grid whose
    // own columns panel still listed them. DataCube aggregates every
    // SELECTED column that is not a group key (`_groupByAggCols`),
    // measures by their function and the rest by `uniq`.
    assert.equal(
      serialize(snap({ pivotOn: [] })),
      '$trades->select(~[region, country, year, notional, qty])' +
        '->groupBy(~[region, country], ~[year:x|$x.year:y|$y->sum(),' +
        ' total:x|$x.notional:y|$y->sum(), qty:x|$x.qty:y|$y->sum()])' +
        '->sort([~region->ascending(), ~country->ascending()])',
    );
  });

  it('gives the GRAND TOTAL the same columns as the levels', () => {
    // A total row blank under a column where every row beneath it
    // carries a figure reads as "there is no total for this" rather
    // than as a projection that dropped it. The root query is a
    // groupBy with no keys, so it took the narrow path of its own.
    const total = serialize(snap({ rows: [], pivotOn: [] }));
    assert.match(total, /groupBy\(~\[\]/);
    assert.match(total, /qty:x\|\$x\.qty:y\|\$y->sum\(\)/, total);
    assert.match(total, /total:x\|\$x\.notional:y\|\$y->sum\(\)/, total);
  });

  it('carries a DIMENSION by its unique value rather than summing it', () => {
    // The reason the rule above is safe. Aggregating everything not
    // a key would sum a year and an id -- "2021 + 2022 + 2023" is
    // the kind of wrong that reads as a bug in the data -- so an
    // explicit kind beats the type the column is carried in, and the
    // column inference marks a key-like numeric a dimension.
    const pure = serialize(snap({
      pivotOn: [],
      columns: [
        { name: 'region', type: 'String' },
        { name: 'country', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
        { name: 'notional', type: 'Float' },
      ],
    }));
    assert.match(pure, /year:x\|\$x\.year:y\|\$y->uniqueValueOnly\(\)/);
    assert.equal(/year[^,\]]*->sum/.test(pure), false, pure);
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

  it('a measureless pivot KEEPS its row groups', () => {
    // A PIVOT TAKES ITS GROUPING FROM WHATEVER ELSE IS SELECTED, so
    // the projection decides the row groups. Widening it to every
    // column -- which is what letting a measureless pivot synthesise
    // its aggregates first did -- regrouped the cube BY every column.
    //
    // Reported from the product: grouped by region, desk and book,
    // then year across the top. The measures split across the years
    // correctly and the three row groups dissolved into a thousand
    // detail rows, while the row zone still listed region, desk and
    // book. The snapshot was right; the projection threw them away.
    const s = serialize({
      source: { expression: 't' },
      columns: [
        { name: 'region', type: 'String' },
        { name: 'desk', type: 'String' },
        { name: 'book', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
        { name: 'notional', type: 'Float' },
        { name: 'pnl', type: 'Float' },
      ],
      derived: [],
      rows: ['region', 'desk', 'book'],
      pivotOn: ['year'],
      measures: [],
      sorts: [],
      epoch: 1,
    }, { level: 1, parent: [] });

    // Level 1 groups by `region` alone, so THAT is what may be
    // selected beside the pivot key and the measures.
    const projected = /select\(~\[([^\]]*)\]/.exec(s)?.[1]
      ?.split(',').map((n) => n.trim()) ?? [];
    assert.deepEqual(projected, ['region', 'year', 'notional', 'pnl']);
    // The tell-tale of the fault: a detail column in the projection.
    assert.equal(projected.includes('desk'), false,
      'a deeper row dimension in the projection regroups the cube by it');
    assert.match(s, /pivot\(~\[year\]/);
    assert.match(s, /notional:x\|\$x\.notional:y\|\$y->sum\(\)/);
  });

  describe('grouped AND pivoted: the two-stage query', () => {
    // Measures spread across the pivot values; every other column
    // takes its unique value; one row per row dimension. Which needs
    // BOTH stages, in this order -- pivot, cast, groupBy -- because
    // the groupBy is the only thing that fixes the final row shape.
    // DataCubeQueryBuilder builds exactly that sequence.
    const CUBE = {
      source: { expression: 't' },
      columns: [
        { name: 'region', type: 'String' },
        { name: 'desk', type: 'String' },
        { name: 'trade_id', type: 'Integer', kind: 'dimension' as const },
        { name: 'quarter', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' as const },
        { name: 'notional', type: 'Float' },
        { name: 'pnl', type: 'Float' },
      ],
      derived: [],
      rows: ['region', 'desk'],
      pivotOn: ['year'],
      measures: [],
      sorts: [],
      epoch: 1,
    };
    const CAST = [
      { name: '2021__|__notional', measure: 'notional' },
      { name: '2021__|__pnl', measure: 'pnl' },
      { name: '2022__|__notional', measure: 'notional' },
      { name: '2022__|__pnl', measure: 'pnl' },
    ];

    it('CASTS between the stages, or the groupBy is illegal', () => {
      // A pivot's output columns exist in its data, not in the
      // relation's type, so naming one later is rejected outright:
      // "relation has no column '2021__|__notional'". That is what
      // the engine said when this shipped without a cast.
      const out = serialize({ ...CUBE, pivotCast: CAST },
        { level: 1, parent: [] });
      const cast = /cast\(@Relation<\(([^)]*)\)>\)/.exec(out)?.[1];
      assert.ok(cast, `no relation cast in ${out.slice(-200)}`);
      assert.ok(out.indexOf('pivot(') < out.indexOf('cast('));
      assert.ok(out.indexOf('cast(') < out.indexOf('groupBy('));
      // It declares the WHOLE post-pivot relation: a cast states the
      // type of the value it is applied to, not a difference from it.
      for (const name of ['2021__|__notional', '2022__|__pnl']) {
        assert.ok(cast.includes(`'${name}':Float`),
          `${name} must be declared, with its measure's type`);
      }
      assert.match(cast, /region:String/);
      assert.match(cast, /trade_id:Integer/, 'a carried column too');
      // And NOT what the pivot consumed.
      assert.equal(/\byear:/.test(cast), false,
        'the pivot key is gone from the relation it produced');
      assert.equal(/\bnotional:/.test(cast), false,
        'the measure survives only as its pivoted columns');
    });

    it('emits the pivot AND an outer groupBy', () => {
      const out = serialize({ ...CUBE, pivotCast: CAST },
        { level: 1, parent: [] });
      assert.match(out, /->pivot\(~\[year\]/);
      assert.match(out, /->groupBy\(~\[region\]/,
        'the outer groupBy is what collapses the intermediate');
      assert.ok(out.indexOf('pivot(') < out.indexOf('groupBy('),
        'the groupBy must come LAST: it fixes the final row shape');
    });

    it('carries every non-measure through as its unique value', () => {
      const out = serialize({ ...CUBE, pivotCast: CAST },
        { level: 1, parent: [] });
      // Projected, so the pivot passes them through...
      assert.match(out, /select\(~\[[^\]]*trade_id/);
      assert.match(out, /select\(~\[[^\]]*quarter/);
      // ...and aggregated by the OUTER groupBy, not by the pivot.
      const outer = out.slice(out.indexOf('groupBy('));
      assert.match(outer, /trade_id:x\|\$x\.trade_id:y\|\$y->uniqueValueOnly/);
      assert.match(outer, /quarter:x\|\$x\.quarter:y\|\$y->uniqueValueOnly/);
      // The PIVOT CALL alone, not everything before the groupBy: the
      // cast sits between them and names every post-pivot column,
      // `trade_id` included, which is its whole job.
      const inner = out.slice(out.indexOf('pivot('), out.indexOf('cast('));
      assert.equal(/trade_id/.test(inner), false,
        'a pivot aggregate on a dimension spreads it per value, which'
        + ' upstream calls "not helpful"');
    });

    it('keeps the pivot result columns, with the measure aggregate', () => {
      const out = serialize({ ...CUBE, pivotCast: CAST },
        { level: 1, parent: [] });
      const outer = out.slice(out.indexOf('groupBy('));
      for (const c of CAST) {
        assert.ok(outer.includes(`'${c.name}'`),
          `${c.name} must survive the outer groupBy`);
      }
      assert.match(outer, /->sum\(\)/);
    });

    it('groups by the LEVEL, not by every row dimension', () => {
      // Level 2 drills into a region, so desk becomes the key and
      // region is pinned by the parent filter.
      const out = serialize({ ...CUBE, pivotCast: CAST },
        { level: 2, parent: ['AMER'] });
      assert.match(out, /->groupBy\(~\[region, desk\]/);
      assert.match(out, /filter\(x\|\$x\.region == 'AMER'\)/);
    });

    it('falls back to the single stage until the cast is known', () => {
      // The pivot's column names come from a result, so the first
      // query cannot name them. Losing the carried columns for one
      // render beats breaking the grouping, which is what the wide
      // projection alone does.
      const out = serialize(CUBE, { level: 1, parent: [] });
      assert.match(out, /->pivot\(~\[year\]/);
      assert.equal(/->groupBy\(/.test(out), false);
      const projected = /select\(~\[([^\]]*)\]/.exec(out)?.[1] ?? '';
      assert.equal(projected.includes('trade_id'), false,
        'projecting it without an outer groupBy regroups the cube by it');
    });
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
