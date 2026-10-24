import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type {
  QuerySnapshot,
  FilterOperator,
} from '../src/snapshot.ts';
import { totalOrderSorts } from '../src/snapshot.ts';
import {
  detailSnapshot,
  literalNode,
  pinnedPivotFacts,
  type LevelScope,
  type PivotFacts,
  type TypeOf,
} from '../src/query.ts';
import { pivotTotalColumn } from '../src/snapshot.ts';
import { accessor, col, element, lambda, variable } from '../../pure-protocol/src/index.ts';
import type { FilterNode } from '../src/snapshot.ts';
import { print, printFilter, printLevel, printValues, row } from './lite-compiler.ts';

// The cube's queries are trees (src/query.ts); these tests read them as the COMPILER prints
// them (lite's printer, STANDARD: one line), the text a person would see.

/** A filter's condition as the compiler prints it, as the lambda `x|<condition>`. */
/** The compiler's types for the columns these filter tests name, as a cube has them. */
const FILTER_TYPES: Record<string, string> = {
  region: 'String', country: 'String', desk: 'String', 'odd name': 'String',
  year: 'Integer', qty: 'Integer', notional: 'Float', trade_date: 'StrictDate', payload: 'Variant',
};

function filterText(node: FilterNode, _param = 'x', typeOf: TypeOf = (c) => FILTER_TYPES[c]): Promise<string> {
  return printFilter(node, typeOf);
}



/** Step 1's answer for a cube pivoted on year: two years. */
const YEARS: PivotFacts = { tuples: [['2023'], ['2024']] };

/**
 * A level's query, printed, with the default cube's pivot values when a test
 * about something else does not give its own (the default cube pivots on
 * year). The one test of the refusal calls `printLevel` itself.
 */
function level(s: QuerySnapshot, scope?: LevelScope, facts?: PivotFacts): Promise<string> {
  return printLevel(s, scope, facts ?? (s.pivotOn.length > 0 ? YEARS : undefined));
}

/** A minimal trades cube, overridable per test. */
function snap(over: Partial<QuerySnapshot> = {}): QuerySnapshot {
  return {
    source: { query: variable('trades') },
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

describe('a level query', () => {
  it('refuses a measure named as another column where the level writes it under its name, and two of one name', async () => {
    // one rule at the level's top (calc.ts measureNameProblem): a grouped level writes every measure beside the keys
    const key = (over: Partial<QuerySnapshot> = {}) => snap({ measures: [{ name: 'region', column: 'qty', fn: 'sum' }], ...over });
    await assert.rejects(async () => level(key({ pivotOn: [] }), { level: 1, parent: [] }), /'region' is already a column/);
    await assert.rejects(async () => level(key({ pivotOn: [] }), { level: 1, parent: [] }),
      /the measure sum\(qty\) takes a name of its own/);
    // pivoted, a measure spread under each pivot value writes `<value>__|__region`: no column of the level is its name
    assert.match(await level(key(), undefined, YEARS), /2024__\|__region/);
    // kept out of the pivot (its column excluded), it is written beside the keys under its own name
    const keptOut = key({ columns: snap().columns.map((c) => (c.name === 'qty' ? { ...c, excludedFromPivot: true } : c)) });
    await assert.rejects(async () => level(keptOut, undefined, YEARS), /'region' is already a column/);
    // two measures of one name, pivoted or not
    const twice = (over: Partial<QuerySnapshot> = {}) => snap({
      measures: [{ name: 'total', column: 'notional', fn: 'sum' }, { name: 'total', column: 'qty', fn: 'sum' }], ...over });
    await assert.rejects(async () => level(twice(), undefined, YEARS), /'total' is already a measure's name: the measure sum\(qty\)/);
    await assert.rejects(async () => level(twice({ pivotOn: [] }), { level: 1, parent: [] }), /'total' is already a column/);
  });

  it('a grouped level sorts by what it writes; a sort on a column its measure renamed is the detail rows\' alone', async () => {
    const s = snap({ rows: ['region'], pivotOn: [], measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
      sorts: [{ column: 'notional', direction: 'desc' }] });
    const grouped = await level(s, { level: 1, parent: [] });
    assert.equal(/notional->descending/.test(grouped), false, grouped);
    // the detail rows beneath a group, read as the tree reads them (treeview.ts)
    const rows = detailSnapshot(s, ['EMEA']);
    const detail = await level(rows, { level: rows.rows.length, parent: [] });
    assert.match(detail, /~notional->descending\(\)/, detail);
  });

  it('keeps every measure of a grouped cube, two of one column and one of a grouping column too', async () => {
    // keyed by column, the grouped aggregates kept a column's last measure alone and skipped a grouping column's
    // (found 2026-10-16, PR neema2/legend-lite#47): each measure is a column of the level, under its own name
    const out = await level(snap({
      rows: ['region'], pivotOn: [],
      measures: [
        { name: 'total', column: 'notional', fn: 'sum' },
        { name: 'avg_n', column: 'notional', fn: 'average' },
        { name: 'trades', column: 'region', fn: 'count' },
      ],
    }), { level: 1, parent: [] });
    assert.match(out, /total:x\|\$x\.notional:y\|\$y->sum\(\)/, out);
    assert.match(out, /avg_n:x\|\$x\.notional:y\|\$y->average\(\)/, out);
    assert.match(out, /trades:x\|1:y\|\$y->count\(\)/, out);
  });

  it('writes a pivot as ONE groupBy, a conditional aggregate per value', async () => {
    // Each cell is its measure over exactly the rows of its value, and
    // the other columns are carried on their own aggregate in the same
    // groupBy (docs/DATACUBE_CUBE_PLAN_DESIGN_2026_09_27.md).
    assert.equal(
      (await level(snap(), undefined, YEARS)),
      '|$trades->select(~[region, country, year, notional, qty])' +
        "->groupBy(~[region, country], ~['2023__|__total':x|if($x.year == 2023, |$x.notional, |[]):y|$y->sum()," +
        " '2024__|__total':x|if($x.year == 2024, |$x.notional, |[]):y|$y->sum()," +
        ' qty:x|$x.qty:y|$y->sum()])' +
        '->sort([~region->ascending(), ~country->ascending()])',
    );
  });

  it('asks for the values first, over the filter but not the group keys', async () => {
    const q = (await printValues(snap({
      filter: { kind: 'condition', column: 'region', operator: 'equal', value: 'EMEA' },
      pivotSort: { year: 'desc' },
    })));
    assert.equal(q, "|$trades->filter(x|$x.region == 'EMEA')->select(~[year])->distinct()"
      + '->sort([~year->descending()])->limit(501)');
    assert.equal((await printValues(snap({ pivotOn: [] }))), null);
  });

  it('refuses to be written without its values', () => {
    assert.throws(() => printLevel(snap()), /pivot values/);
  });

  it('a NULL value has its own column, pinned by isEmpty', async () => {
    const s = (await level(snap(), { level: 1, parent: [] }, { tuples: [['2023'], [null]] }));
    assert.match(s, /'\(empty\)__\|__total':x\|if\(\$x\.year->isEmpty\(\), \|\$x\.notional, \|\[\]\)/);
  });

  it('uses groupBy when there is no column dimension', async () => {
    // AND KEEPS EVERY COLUMN IT SHOWS. This projected the keys and
    // the measure alone, so clearing a cube's column pivot left one
    // data column on screen and the rest gone -- from a grid whose
    // own columns panel still listed them. DataCube aggregates every
    // SELECTED column that is not a group key (`_groupByAggCols`),
    // measures by their function and the rest by `uniq`.
    assert.equal(
      (await level(snap({ pivotOn: [] }))),
      '|$trades->select(~[region, country, year, notional, qty])' +
        '->groupBy(~[region, country], ~[year:x|$x.year:y|$y->sum(),' +
        ' total:x|$x.notional:y|$y->sum(), qty:x|$x.qty:y|$y->sum()])' +
        '->sort([~region->ascending(), ~country->ascending()])',
    );
  });

  it('gives the GRAND TOTAL the same columns as the levels', async () => {
    // A total row blank under a column where every row beneath it
    // carries a figure reads as "there is no total for this" rather
    // than as a projection that dropped it. The root query is a
    // groupBy with no keys, so it took the narrow path of its own.
    const total = (await level(snap({ rows: [], pivotOn: [] })));
    // ONE GROUP, SAID AS A GROUP. `groupBy(~[], ~[...])` is the
    // obvious spelling and it crashes the real engine with a
    // NullPointerException out of the plan builder; upstream never
    // writes it either, extending a constant column and grouping by
    // that instead (`_extendRootAggregation`, value `[ROOT]`).
    assert.match(total, /extend\(~\[__root__:x\|'\[ROOT\]'\]\)/);
    assert.match(total, /groupBy\(~\[__root__\]/);
    assert.equal(/groupBy\(~\[\]/.test(total), false);
    assert.match(total, /qty:x\|\$x\.qty:y\|\$y->sum\(\)/, total);
    assert.match(total, /total:x\|\$x\.notional:y\|\$y->sum\(\)/, total);
  });

  it('carries a DIMENSION by its unique value rather than summing it', async () => {
    // The reason the rule above is safe. Aggregating everything not
    // a key would sum a year and an id -- "2021 + 2022 + 2023" is
    // the kind of wrong that reads as a bug in the data -- so an
    // explicit kind beats the type the column is carried in, and the
    // column inference marks a key-like numeric a dimension.
    const pure = (await level(snap({
      pivotOn: [],
      columns: [
        { name: 'region', type: 'String' },
        { name: 'country', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
        { name: 'notional', type: 'Float' },
      ],
    })));
    assert.match(pure, /year:x\|\$x\.year:y\|\$y->uniqueValueOnly\(\)/);
    assert.equal(/year[^,\]]*->sum/.test(pure), false, pure);
  });

  it('pivots a cube with NO configured measures', async () => {
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
    const s = (await level(snap({ measures: [] }), undefined, YEARS));
    // `notional` is a Float, so it is a measure and it sums.
    assert.match(s, /'2023__\|__notional':x\|if\(\$x\.year == 2023, \|\$x\.notional, \|\[\]\):y\|\$y->sum\(\)/);
    // And the DIMENSIONS are not spread: upstream excludes them from a
    // pivot deliberately, unlike a groupBy.
    assert.equal(/__\|__country/.test(s), false,
      'a dimension must not become a pivot aggregate');
  });

  it('a measureless pivot KEEPS its row groups', async () => {
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
    const s = (await level({
      source: { query: element('t') },
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
    }, { level: 1, parent: [] }, YEARS));

    // The groupBy names its keys, so a deeper row dimension in the
    // projection can no longer regroup the cube: it is carried, on
    // its unique value.
    assert.match(s, /->groupBy\(~\[region\], ~\[/);
    assert.match(s, /desk:x\|\$x\.desk:y\|\$y->uniqueValueOnly\(\)/);
    assert.match(s, /'2023__\|__notional':x\|if\(\$x\.year == 2023, \|\$x\.notional, \|\[\]\):y\|\$y->sum\(\)/);
  });

  describe('grouped AND pivoted: one groupBy', () => {
    // Measures spread across the pivot values, every other column on
    // its own aggregate, one row per row dimension -- in ONE groupBy.
    // It was pivot -> cast -> groupBy, which re-aggregated the pivot's
    // finer partial results: an average of averages (P2-4).
    const CUBE = {
      source: { query: element('t') },
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
    const VALUES: PivotFacts = { tuples: [['2021'], ['2022']] };

    it('has no pivot, no cast and no second stage', async () => {
      const out = (await level(CUBE, { level: 1, parent: [] }, VALUES));
      assert.equal(/pivot\(|cast\(/.test(out), false, out);
      assert.equal(out.split('groupBy(').length, 2, 'exactly one groupBy');
    });

    it('an AVERAGE cell is the average of its value\'s rows, not of partial averages', async () => {
      const out = (await level({ ...CUBE, measures: [{ name: 'avg_n', column: 'notional', fn: 'average' }] },
        { level: 1, parent: [] }, VALUES));
      assert.match(out, /'2021__\|__avg_n':x\|if\(\$x\.year == 2021, \|\$x\.notional, \|\[\]\):y\|\$y->average\(\)/);
    });

    it('carries every other column on its own aggregate', async () => {
      const out = (await level(CUBE, { level: 1, parent: [] }, VALUES));
      assert.match(out, /trade_id:x\|\$x\.trade_id:y\|\$y->uniqueValueOnly/);
      assert.match(out, /quarter:x\|\$x\.quarter:y\|\$y->uniqueValueOnly/);
      // A dimension is carried, never spread: "not helpful", upstream.
      assert.equal(/__\|__trade_id/.test(out), false);
    });

    it('groups by the LEVEL, not by every row dimension', async () => {
      const out = (await level(CUBE, { level: 2, parent: ['AMER'] }, VALUES));
      assert.match(out, /->groupBy\(~\[region, desk\]/);
      assert.match(out, /filter\(x\|\$x\.region == 'AMER'\)/);
    });

    it('a weighted average wraps both halves of its pair', async () => {
      const out = (await level({ ...CUBE,
        measures: [{ name: 'w', column: 'notional', fn: 'wavg', weight: 'pnl' }] },
      { level: 1, parent: [] }, VALUES));
      assert.match(out, /if\(\$x\.year == 2021, \|\$x\.notional, \|\[\]\)->wavgRowMapper\(if\(\$x\.year == 2021, \|\$x\.pnl, \|\[\]\)\)/);
    });

    it('the Total is a column of the same query, on the configured aggregate', async () => {
      const out = (await level({ ...CUBE,
        measures: [{ name: 'avg_n', column: 'notional', fn: 'average' }],
        pivotTotal: { placement: 'right', functions: { notional: 'max' } } },
      { level: 1, parent: [] }, VALUES));
      assert.ok(out.includes(`'${pivotTotalColumn('avg_n')}':x|$x.notional:y|$y->max()`), out);
    });

    it('drops a sort on a pivot column the values no longer make', async () => {
      const out = (await level({ ...CUBE,
        sorts: [{ column: '2019__|__notional', direction: 'desc' },
          { column: '2021__|__notional', direction: 'asc' }] },
      { level: 1, parent: [] }, VALUES));
      assert.equal(out.includes('2019'), false, out);
      assert.match(out, /sort\(\[~'2021__\|__notional'->ascending\(\)/);
    });
  });

  it('falls back to a count when a pivot has nothing to aggregate', async () => {
    // `_fixEmptyAggCols`: a pivot must aggregate something, so a cube
    // of nothing but dimensions still produces a query rather than
    // `pivot(~[year], ~[])`, which the compiler rejects far from the
    // cause.
    const s = (await level(snap({
      measures: [],
      columns: [
        { name: 'region', type: 'String' },
        { name: 'country', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
      ],
    }), undefined, YEARS));
    assert.match(s, /'2023__\|__count':x\|if\(\$x\.year == 2023, \|1, \|\[\]\):y\|\$y->count\(\)/,
      'the filler count is there');
  });

  it('maps count to the constant 1, not to a column', async () => {
    const s = (await level(
      snap({ measures: [{ name: 'n', column: 'notional', fn: 'count' }] }),
    ));
    // In a pivot cell, 1 on the value's rows and nothing elsewhere.
    assert.match(s, /'2023__\|__n':x\|if\(\$x\.year == 2023, \|1, \|\[\]\):y\|\$y->count\(\)/);
    // 'notional' is not needed by count, so it must not be selected.
    assert.equal(s.includes('notional'), false);
  });

  it('pairs value with weight in the MAP, where the row is in scope', async () => {
    // This test previously asserted `y|$y->wavg($y.qty)` and passed,
    // while the engine rejected that query outright: the reduce sees a
    // collection of mapped NUMBERS, so `$y.qty` is an access on Float.
    // A green assertion on a string the engine will not accept is not
    // verification -- the string was well-formed and wrong. The torture
    // run against a real engine is what caught it, which is the whole
    // argument for having one.
    const s = (await level(
      snap({
        measures: [
          { name: 'w', column: 'notional', fn: 'wavg', weight: 'qty' },
        ],
      }),
    ));
    assert.match(
      s,
      /'2023__\|__w':x\|if\(\$x\.year == 2023, \|\$x\.notional, \|\[\]\)->wavgRowMapper\(if\(\$x\.year == 2023, \|\$x\.qty, \|\[\]\)\):y\|\$y->wavg\(\)/,
    );
    assert.match(s, /select\(~\[region, country, year, notional, qty\]\)/);
  });

  it('refuses wavg without a weight rather than degrading silently', () => {
    assert.throws(
      () =>
        level(
          snap({ measures: [{ name: 'w', column: 'notional', fn: 'wavg' }] }),
        ),
      /uses wavg but has no weight column/,
    );
    // so every aggregate of two (snapshot.ts SECOND_COLUMN), its second column named by what it is
    assert.throws(() => level(snap({ measures: [{ name: 'c', column: 'notional', fn: 'corr' }] })), /uses corr but has no paired column/);
    assert.throws(() => level(snap({ measures: [{ name: 'm', column: 'notional', fn: 'maxBy' }] })), /uses maxBy but has no ordering column/);
  });

  it("writes an aggregate of two as one row of two values, each a pivot cell's own; a distinct count of its own column or the one it names", async () => {
    const s = await level(snap({
      rows: ['region'], pivotOn: [],
      measures: [
        { name: 'c', column: 'notional', fn: 'corr', weight: 'qty' },
        { name: 'top', column: 'country', fn: 'maxBy', weight: 'notional' },
        { name: 'n', column: 'qty', fn: 'distinctCount' },
        { name: 'k', column: 'qty', fn: 'distinctCount', weight: 'country' },
      ],
    }), { level: 1, parent: [] });
    assert.match(s, /c:x\|\$x\.notional->rowMapper\(\$x\.qty\):y\|\$y->corr\(\)/);
    assert.match(s, /top:x\|\$x\.country->rowMapper\(\$x\.notional\):y\|\$y->maxBy\(\)/);
    assert.match(s, /n:x\|\$x\.qty:y\|\$y->distinct\(\)->count\(\)/);
    assert.match(s, /k:x\|\$x\.country:y\|\$y->distinct\(\)->count\(\)/);
  });

  it('appends row dimensions to make the sort a total order', async () => {
    // Unpivoted: a pivot consumes `qty`, and a sort on a column the query does not make is dropped (P2-6), not sent.
    // On `qty`, which the grouped level writes on its own aggregate: this case sorted on `notional` once, which the
    // level writes as its measure `total` -- the printed query sorted on a column the groupBy did not keep (found
    // 2026-10-16: the level now sorts by what it writes, the detail rows by the rest).
    const s = snap({ pivotOn: [], sorts: [{ column: 'qty', direction: 'desc' }] });
    assert.deepEqual(totalOrderSorts(s), [
      { column: 'qty', direction: 'desc' },
      { column: 'region', direction: 'asc' },
      { column: 'country', direction: 'asc' },
    ]);
    assert.match(
      (await level(s)),
      /sort\(\[~qty->descending\(\), ~region->ascending\(\), ~country->ascending\(\)\]\)/,
    );
  });

  it('sorts by a pivot total while it exists, and drops the sort once totals or the pivot are off', async () => {
    // a total is a real column of the level's query; turned off, a sort left on it would
    // order by a column no query produces
    const total = pivotTotalColumn('total');
    const on = snap({ pivotTotal: { placement: 'right' }, sorts: [{ column: total, direction: 'desc' }] });
    assert.deepEqual(totalOrderSorts(on)[0], { column: total, direction: 'desc' });
    assert.ok((await level(on)).includes(`~'${total}'->descending()`), (await level(on)));
    const { pivotTotal: _off, ...noTotals } = on;
    const unpivoted = snap({ pivotOn: [], pivotTotal: { placement: 'right' }, sorts: on.sorts });
    for (const s of [noTotals, unpivoted]) {
      assert.ok(!totalOrderSorts(s).some((x) => x.column === total));
      assert.ok(!(await level(s)).includes(total), (await level(s)));
    }
  });

  it('does not duplicate a row dimension already sorted on', () => {
    const s = snap({ sorts: [{ column: 'region', direction: 'desc' }] });
    assert.deepEqual(totalOrderSorts(s), [
      { column: 'region', direction: 'desc' },
      { column: 'country', direction: 'asc' },
    ]);
  });

  it('emits the row window as slice(offset, end)', async () => {
    assert.match(
      (await level(snap({ window: { offset: 100, limit: 50 } }))),
      /->slice\(100, 150\)$/,
    );
  });

  it('emits extend before filter', async () => {
    const s = (await level(
      snap({
        derived: [{ name: 'net', lambda: (await row('$x.notional * 0.98')) }],
        filter: {
          kind: 'condition',
          column: 'region',
          operator: 'equal',
          value: 'EMEA',
        },
      }),
    ));
    assert.match(
      s,
      /^\|\$trades->extend\(~\[net:x\|\$x\.notional \* 0\.98\]\)->filter\(/,
    );
  });

  it('pins pivot values when deliberately narrowed, WITHOUT filtering the source', async () => {
    const pinned = snap({ pivotValues: [2023, 2024] });
    assert.equal((await printValues(pinned)), null, 'no values query: they are given');
    const facts = pinnedPivotFacts(pinned);
    assert.deepEqual(facts, YEARS);
    const s = (await level(pinned, undefined, facts ?? undefined));
    assert.match(s, /'2024__\|__total':x\|if\(\$x\.year == 2024,/);
    // A group whose rows all fall outside the list keeps its Total.
    assert.equal(/filter\(/.test(s), false, s);
  });
});

describe('an exploded calculated column', () => {
  it('is a lateral flatten: each row once per element, the element the column', async () => {
    const s = snap({ pivotOn: [], rows: [],
      derived: [{ name: 'tag', lambda: (await row("$x.doc->get('tags')->toMany(@meta::pure::metamodel::variant::Variant)")), unnest: true }] });
    assert.match((await level(s)),
      /->lateral\(x\|\$x\.doc->get\('tags'\)->toMany\(@meta::pure::metamodel::variant::Variant\)->flatten\(~tag\)\)/);
  });
});

describe('a level query at a level scope', () => {
  it('groups the grand total by nothing at all', async () => {
    // level 0: every grouping column dropped, which is exactly what
    // makes the grand total the same expression as the detail.
    const out = (await level(snap(), { level: 0, parent: [] }, YEARS));
    assert.equal(
      out,
      "|$trades->select(~[year, notional, region, country, qty])->extend(~[__root__:x|'[ROOT]'])" +
        "->groupBy(~[__root__], ~['2023__|__total':x|if($x.year == 2023, |$x.notional, |[]):y|$y->sum()," +
        " '2024__|__total':x|if($x.year == 2024, |$x.notional, |[]):y|$y->sum()," +
        ' region:x|$x.region:y|$y->uniqueValueOnly(), country:x|$x.country:y|$y->uniqueValueOnly(),' +
        ' qty:x|$x.qty:y|$y->sum()])',
    );
    // A single row needs no ordering or slicing.
    assert.equal(out.includes('sort('), false);
    assert.equal(out.includes('slice('), false);
  });

  it('groups the top level by the first dimension only', async () => {
    const out = (await level(snap(), { level: 1, parent: [] }, YEARS));
    assert.match(out, /->groupBy\(~\[region\], ~\['2023__\|__total'/);
    assert.match(out, /->sort\(\[~region->ascending\(\)\]\)$/);
  });

  it('pins the parent branch when expanding', async () => {
    const out = (await level(snap(), { level: 2, parent: ['EMEA'] }, YEARS));
    assert.match(out, /filter\(x\|\$x\.region == 'EMEA'\)/);
    assert.match(out, /->groupBy\(~\[region, country\]/);
  });

  it('ands the parent branch onto the user filter', async () => {
    const s = snap({
      filter: {
        kind: 'condition',
        column: 'notional',
        operator: 'greaterThan',
        value: 100,
      },
    });
    assert.match(
      (await level(s, { level: 2, parent: ['EMEA'] })),
      /filter\(x\|\(\$x\.notional > 100\.0\) && \(\$x\.region == 'EMEA'\)\)/,
    );
  });

  it('matches a NULL group key with isEmpty, not equals', async () => {
    // '== null' matches nothing in SQL, so expanding a null group
    // would silently return no children.
    assert.match(
      (await level(snap(), { level: 2, parent: [null] })),
      /filter\(x\|\$x\.region->isEmpty\(\)\)/,
    );
  });

  it('never orders by a dimension deeper than the level', async () => {
    // 'country' does not exist in a level-1 result; naming it in the
    // ORDER BY would be a compile error at the engine.
    const out = (await level(snap(), { level: 1, parent: [] }));
    // Carried beside the cells it may be; ordered by, never.
    assert.equal(/sort\(\[[^\]]*country/.test(out), false, out);
  });

  it('keeps a measure sort while dropping a deeper dimension sort', async () => {
    const s = snap({
      pivotOn: [],
      sorts: [
        { column: 'total', direction: 'desc' },
        { column: 'country', direction: 'desc' },
      ],
    });
    assert.match(
      (await level(s, { level: 1, parent: [] })),
      /sort\(\[~total->descending\(\), ~region->ascending\(\)\]\)/,
    );
  });

  it('caps a level, AFTER the sort', async () => {
    // A limit before the sort caps an arbitrary subset, so the first
    // page is not the first page.
    const out = (await level(snap(), { level: 1, parent: [], limit: 1001 }));
    assert.match(out, /->sort\(\[~region->ascending\(\)\]\)->limit\(1001\)$/);
  });

  it('does not cap the grand total, which is one row', async () => {
    const out = (await level(snap(), { level: 0, parent: [], limit: 1001 }));
    assert.equal(out.includes('limit('), false);
  });

  it('is unchanged without a scope', async () => {
    assert.equal((await level(snap())), (await level(snap(), undefined)));
  });

  it('a subtotal is the SAME cells over fewer keys', async () => {
    // The property the design rests on: a level's figures are the same
    // aggregates as the level below, grouped by one key fewer -- never a
    // second pass over the level below's results.
    const detail = (await level(snap(), { level: 2, parent: [] }));
    const subtotal = (await level(snap(), { level: 1, parent: [] }));
    const cells = (q: string): string[] => q.match(/'20\d\d__\|__total':[^,]*,[^,]*,[^)]*\):y\|\$y->sum\(\)/g) ?? [];
    assert.deepEqual(cells(detail), cells(subtotal));
    assert.equal(cells(detail).length, 2);
    assert.match(detail, /->groupBy\(~\[region, country\], /);
    assert.match(subtotal, /->groupBy\(~\[region\], /);
  });
});

describe('filterExpression', () => {
  it('renders comparisons', async () => {
    assert.equal(
      (await filterText({
        kind: 'condition',
        column: 'year',
        operator: 'greaterThanEqual',
        value: 2020,
      })),
      'x|$x.year >= 2020',
    );
  });

  it('renders nested and/or with parentheses', async () => {
    assert.equal(
      (await filterText({
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
      })),
      // each part parenthesized: the grammar applies < <= > >= left to right with && and ||
      "x|($x.region == 'EMEA') && (($x.year == 2023) || ($x.year == 2024))",
    );
  });

  it('renders emptiness and membership', async () => {
    assert.equal(
      (await filterText({
        kind: 'condition',
        column: 'country',
        operator: 'isEmpty',
      })),
      'x|$x.country->isEmpty()',
    );
    assert.equal(
      (await filterText({
        kind: 'not',
        child: {
          kind: 'condition',
          column: 'country',
          operator: 'in',
          value: ['US', 'GB'],
        },
      })),
      "x|!$x.country->in(['US', 'GB'])",
    );
  });

  it('treats an empty group as a no-op rather than an error', async () => {
    assert.equal((await filterText({ kind: 'and', children: [] })), 'x|true');
    assert.equal((await filterText({ kind: 'or', children: [] })), 'x|false');
  });
});

describe('the full filter vocabulary', () => {
  const cond = (
    operator: string,
    extra: Record<string, unknown> = {},
  ): Promise<string> =>
    filterText({
      kind: 'condition',
      column: 'region',
      operator,
      ...extra,
    } as never);

  it('renders negated string tests', async () => {
    assert.equal((await cond('notContains', { value: 'X' })), "x|!$x.region->meta::pure::functions::string::contains('X')");
    assert.equal(
      (await cond('notStartsWith', { value: 'X' })),
      "x|!$x.region->startsWith('X')",
    );
    assert.equal((await cond('notEndsWith', { value: 'X' })), "x|!$x.region->endsWith('X')");
    assert.equal(
      (await cond('notIn', { value: ['A', 'B'] })),
      "x|!$x.region->in(['A', 'B'])",
    );
  });

  it('uses isNotEmpty rather than negating isEmpty', async () => {
    // The engine has the function; using its own vocabulary keeps a
    // generated query readable for whoever has to debug it.
    assert.equal((await cond('isNotEmpty')), 'x|$x.region->isNotEmpty()');
  });

  it('lowers BOTH sides for case-insensitive comparisons', async () => {
    // Relying on collation would let the same cube answer differently
    // on two backends.
    //
    // TWO SHAPES THE REAL ENGINE FORCED. `toOne()` first, because a
    // relational column is `[0..1]` and `toLower` takes `String[1]`:
    // upstream legend-engine refuses `$x.region->toLower()` outright
    // ("Can't find a match for function 'toLower(Varchar(32)[0..1])'")
    // and nine of these operators were unusable there until this
    // call went in -- which is exactly where upstream puts it
    // (DataCubeQueryFilterOperation__EqualCaseInsensitive).
    //
    // And `toLower('EMEA')` rather than `'emea'` for EQUAL: the
    // lowering rule is then the engine's, not JavaScript's, and they
    // are not the same rule.
    assert.equal(
      (await cond('equalCaseInsensitive', { value: 'EMEA' })),
      "x|$x.region->toOne()->toLower() == toLower('EMEA')",
    );
    // BUT NOT for contains/startsWith/endsWith, which pre-lower.
    // Measured against a running legend-engine 4.138.5: with
    // `toLower(<literal>)` in this position `contains` dies with a
    // StackOverflowError inside sqlDialect.pure and the other two with
    // "Match failure: TypedFunction", while the same query with a
    // plain literal executes and returns the right rows. `equal`
    // accepts it and these three do not -- the engine's inconsistency,
    // and the differential (verify:engine:diff) is what found it.
    assert.equal(
      (await cond('containsCaseInsensitive', { value: 'Em' })),
      "x|$x.region->toOne()->toLower()->meta::pure::functions::string::contains('em')",
    );
    assert.equal(
      (await cond('startsWithCaseInsensitive', { value: 'LAT' })),
      "x|$x.region->toOne()->toLower()->startsWith('lat')",
    );
    assert.equal(
      (await cond('endsWithCaseInsensitive', { value: 'MEA' })),
      "x|$x.region->toOne()->toLower()->endsWith('mea')",
    );
    // EXCEPT IN AN `in` LIST, which takes literals and nothing else:
    // the engine asserts "IN is supported only for literal values or
    // negative numbers", so these two lower their values here. It is
    // also why upstream ships no builder for them at all.
    assert.equal(
      (await cond('inCaseInsensitive', { value: ['EMEA', 'Amer'] })),
      "x|$x.region->toOne()->toLower()->in(['emea', 'amer'])",
    );
    assert.equal(
      (await cond('notInCaseInsensitive', { value: ['EMEA'] })),
      "x|!$x.region->toOne()->toLower()->in(['emea'])",
    );
  });

  it('compares two columns', async () => {
    assert.equal(
      (await cond('greaterThanColumn', { rightColumn: 'country' })),
      'x|$x.region > $x.country',
    );
    assert.equal(
      (await cond('equalColumn', { rightColumn: 'country' })),
      'x|$x.region == $x.country',
    );
  });

  it('lowers both columns for a case-insensitive column comparison', async () => {
    // Both sides through `toOne()->toLower()`, for the multiplicity
    // reason above: two nullable columns, one function that takes
    // neither.
    assert.equal(
      (await cond('equalCaseInsensitiveColumn', { rightColumn: 'country' })),
      'x|$x.region->toOne()->toLower() == $x.country->toOne()->toLower()',
    );
    assert.equal(
      (await cond('notEqualCaseInsensitiveColumn', { rightColumn: 'country' })),
      'x|$x.region->toOne()->toLower() != $x.country->toOne()->toLower()',
    );
  });

  it("covers all 31 of DataCube's operators", async () => {
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
      const out = (await filterText({
        kind: 'condition',
        column: 'region',
        operator,
        // a list for the membership operators only (`contains` is not one)
        value: ['in', 'notIn', 'inCaseInsensitive', 'notInCaseInsensitive'].includes(operator) ? ['A'] : 'A',
        rightColumn: 'country',
      }));
      assert.ok(out.length > 0, operator);
    }
  });

  it('refuses a column comparison with no second column', () => {
    assert.throws(
      () => cond('equalColumn'),
      /needs a rightColumn/,
    );
  });

  it('quotes an awkward column name on both sides', async () => {
    assert.equal(
      (await filterText({
        kind: 'condition',
        column: 'odd name',
        operator: 'equalColumn',
        rightColumn: '2023__|__total',
      })),
      "x|$x.'odd name' == $x.'2023__|__total'",
    );
  });
});

describe('names and literals, as the compiler prints them', () => {
  it('leaves plain names alone and quotes the rest', async () => {
    const name = (n: string): Promise<string> => print(lambda(['x'], col('x', n)));
    assert.equal((await name('region')), 'x|$x.region');
    assert.equal((await name('_a1')), 'x|$x._a1');
    // The pivot separator legend-lite and DataCube both use.
    assert.equal((await name('2011__|__total')), "x|$x.'2011__|__total'");
    assert.equal((await name("it's")), "x|$x.'it\\'s'");
  });

  it('writes a value as its COLUMN\'s type writes it, exactly', async () => {
    const value = (v: Parameters<typeof literalNode>[0], type: string): Promise<string> =>
      print(lambda([], literalNode(v, type)));
    assert.equal((await value("O'Hara", 'String')), "|'O\\'Hara'");
    assert.equal((await value('42', 'Integer')), '|42');
    assert.equal((await value('9007199254740993', 'Integer')), '|9007199254740993');
    // a decimal keeps its digits, in the tree and in the print
    assert.equal((await value('12.30', 'Decimal')), '|12.30D');
    assert.equal((await value('2.5', 'Float')), '|2.5');
    assert.equal((await value(true, 'Boolean')), '|true');
    assert.equal((await value('true', 'Boolean')), '|true');
    // A TIMESTAMP keeps its time to the microsecond; a day is a day.
    assert.equal((await value('2024-03-01T12:58:07.123456', 'DateTime')), '|%2024-03-01T12:58:07.123456');
    assert.equal((await value('2024-03-01', 'StrictDate')), '|%2024-03-01');
    // the same text, a different column type: a different literal
    assert.equal((await value('2021', 'String')), "|'2021'");
  });

  it('refuses a value whose column the compiler has not typed, rather than guess', () => {
    assert.throws(() => literalNode('2021'), /has not typed the column/);
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
  const TEMPORAL: QuerySnapshot = {
    source: { query: element('t') },
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

  it('filters on a TIMESTAMP literal, not on a locale string', async () => {
    const at = new Date(2021, 0, 1, 3, 58, 0);
    const out = (await level(TEMPORAL, { level: 2, parent: [key(at)] }));
    assert.match(out, /%2021-01-01T03:58:00/,
      'the drilldown must carry a datetime literal');
    // The shape of the old failure: the locale string, quoted.
    assert.equal(/GMT|Eastern|Standard Time/.test(out), false,
      'a JS Date toString reached the query');
    assert.equal(/'2021-01-01T03:58:00'/.test(out), false,
      'the timestamp went in as a STRING, which the engine refuses');
  });

  it('keeps a date-only group on its own day', async () => {
    // A StrictDate cell's key is its calendar day as the database gave it
    // (values.ts): no zone to slip through, and a day literal back.
    const out = (await level(
      { ...TEMPORAL, rows: ['trade_date'] },
      { level: 2, parent: ['2021-01-01'] },
    ));
    assert.match(out, /%2021-01-01/);
    assert.equal(out.includes('2020-12-31'), false, 'it slipped a day');
  });

  it('a non-temporal key is still compared as text', async () => {
    const out = (await level(
      { ...TEMPORAL,
        columns: [...TEMPORAL.columns, { name: 'region', type: 'String' }],
        rows: ['region'] },
      { level: 2, parent: ['AMER'] },
    ));
    assert.match(out, /'AMER'/);
  });
});

describe('the DETAIL cube: no grouping, no pivot, no measures', () => {
  // The plainest thing this product can show, and the least tested.
  // It referenced no columns, so nothing was projected; and it had
  // no grouping columns, so a guard meant for the grand total threw
  // away its sort and its row cap as well. The simplest grid was the
  // one that honoured neither.
  const DETAIL: QuerySnapshot = {
    source: { query: element('t') },
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

  it('projects the columns the cube declares', async () => {
    assert.equal((await level(DETAIL)), '|t->select(~[region, notional])');
  });

  it('includes derived columns in the projection', async () => {
    const withDerived: QuerySnapshot = {
      ...DETAIL,
      derived: [{ name: 'net', lambda: (await row('$x.notional * 2')) }],
    };
    assert.match((await level(withDerived)), /select\(~\[region, notional, net\]\)/);
  });

  it('HONOURS its sort', async () => {
    const sorted: QuerySnapshot = {
      ...DETAIL,
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    assert.match((await level(sorted)), /->sort\(\[~region->ascending\(\)\]\)$/);
  });

  it('HONOURS its row cap', async () => {
    const pure = (await level(DETAIL, { level: 0, parent: [], limit: 100 }));
    assert.match(pure, /->limit\(100\)$/);
  });

  it('caps AFTER sorting, so the first page is the first page', async () => {
    const sorted: QuerySnapshot = {
      ...DETAIL,
      sorts: [{ column: 'notional', direction: 'desc' }],
    };
    const pure = (await level(sorted, { level: 0, parent: [], limit: 10 }));
    assert.ok(
      pure.indexOf('->sort(') < pure.indexOf('->limit('),
      pure,
    );
  });

  it('still leaves the GRAND TOTAL unsorted and uncapped', async () => {
    // One row: sorting and limiting it is noise. This is the case
    // the old guard was written for, and it must keep working.
    const total: QuerySnapshot = {
      ...DETAIL,
      measures: [{ name: 'n', column: 'notional', fn: 'sum' }],
      sorts: [{ column: 'region', direction: 'asc' }],
    };
    const pure = (await level(total, { level: 0, parent: [], limit: 100 }));
    assert.equal(pure.includes('->sort('), false, pure);
    assert.equal(pure.includes('->limit('), false, pure);
  });
});

describe('a calculated column aggregates as DECLARED', () => {
  const base = {
    source: { query: accessor('db', 'T') },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'notional', type: 'Float' },
    ],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  } as const;

  it('sums a numeric column the user called a measure', async () => {
    const out = (await level({
      ...base,
      derived: [{ name: 'uplift', lambda: (await row('$x.notional * 1.1')),
        kind: 'measure', type: 'Float' }],
      rows: ['region'],
    }));
    assert.match(out, /uplift:x\|\$x\.uplift:y\|\$y->sum\(\)/);
  });

  it('does NOT sum one the user called a dimension, whatever its type',
    async () => {
      // A declared kind beats the type, exactly as it does for a
      // source column: a numeric-looking calculated column the user
      // means as a key -- a year bucket, a banded id -- must not sum.
      const out = (await level({
        ...base,
        derived: [{ name: 'bucket', lambda: (await row('$x.notional->round()')),
          kind: 'dimension', type: 'Integer' }],
        rows: ['region'],
      }));
      assert.match(out, /bucket:x\|\$x\.bucket:y\|\$y->uniqueValueOnly\(\)/);
      assert.doesNotMatch(out, /bucket:x\|\$x\.bucket:y\|\$y->sum\(\)/);
    });

  it('does NOT sum a declared measure that is no number: its unique value (query.ts defaultAggregate)', async () => {
    // The column editor starts a new column a measure whatever its type,
    // and a saved cube can declare one: `sum` refused a Boolean or a
    // String measure (sum(Boolean)), so grouping broke the grid.
    const out = (await level({
      ...base,
      derived: [
        { name: 'big', lambda: (await row('$x.notional > 50')), kind: 'measure', type: 'Boolean' },
        { name: 'tag', lambda: (await row("$x.region + '!'")), kind: 'measure', type: 'String' },
      ],
      rows: ['region'],
    }));
    assert.match(out, /big:x\|\$x\.big:y\|\$y->uniqueValueOnly\(\)/);
    assert.match(out, /tag:x\|\$x\.tag:y\|\$y->uniqueValueOnly\(\)/);
  });

  it('does not sum an UNTYPED column, declared a measure or not: no number is guessed', async () => {
    // The default's documented contract. Every column is typed before a
    // query reads it -- a refresh's step 0 and a compile's (plan.ts
    // typeColumns) -- so only a runner that types nothing leaves one.
    const out = (await level({
      ...base,
      derived: [{ name: 'uplift', lambda: (await row('$x.notional * 1.1')),
        kind: 'measure' }],
      rows: ['region'],
    }));
    assert.match(out, /uplift:x\|\$x\.uplift:y\|\$y->uniqueValueOnly\(\)/);
  });

  it('falls back to the type when no kind was declared', async () => {
    // Snapshots saved before the field existed still behave as they
    // did: a numeric type defaults to a measure.
    const out = (await level({
      ...base,
      derived: [{ name: 'uplift', lambda: (await row('$x.notional * 1.1')),
        type: 'Float' }],
      rows: ['region'],
    }));
    assert.match(out, /uplift:x\|\$x\.uplift:y\|\$y->sum\(\)/);
  });

  it('keeps a group-stage column out of the pre-aggregation select', async () => {
    // The defect that made the whole group stage unusable: the name
    // went into `select(~[...])`, which runs before the column exists.
    const out = (await level({
      ...base,
      derived: [],
      groupDerived: [{ name: 'margin', lambda: (await row('$x.pnl / $x.notional')) }],
      rows: ['region'],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
    }));
    const select = /select\(~\[([^\]]*)\]/.exec(out)?.[1] ?? '';
    assert.ok(!select.includes('margin'),
      `margin leaked into the projection: ${select}`);
    const groupAt = out.indexOf('groupBy(~[');
    const extendAt = out.lastIndexOf('extend(~[margin');
    assert.ok(extendAt > groupAt,
      'the group-stage extend must come after the groupBy');
  });
});

describe('the GRAND TOTAL of a cube with no explicit measures', async () => {
  // Every uploaded file has no measures configured. Its level-0 query
  // was `t->limit(1001)` -- raw rows -- so "Show root aggregation"
  // put the FIRST TRADE's values in the Total row and raised a false
  // truncation warning (found by the 2026-09-25 controls sweep).
  const q = (await level(snap({ pivotOn: [], measures: [] }),
    { level: 0, parent: [], limit: 1001 }));

  it('aggregates, as one group over everything', () => {
    assert.match(q, /extend\(~\[__root__:x\|'\[ROOT\]'\]\)->groupBy\(~\[__root__\]/);
    assert.match(q, /notional:x\|\$x\.notional:y\|\$y->sum\(\)/);
  });

  it('is one row, so it carries no row cap', () => {
    assert.doesNotMatch(q, /limit\(/);
  });
});

describe('C3: a column pivot carries CALCULATED measures', () => {
  // The pivot's measure set read the source columns only, so a
  // calculated measure vanished from a pivoted cube with no error.
  it('aggregates a row-stage calculated measure in the pivot', async () => {
    const q = (await level(snap({
      rows: [], measures: [],
      derived: [{ name: 'uplift', lambda: (await row('$x.notional * 1.1')), kind: 'measure', type: 'Float' }],
    }), undefined, YEARS));
    assert.match(q, /'2023__\|__uplift':x\|if\(\$x\.year == 2023, \|\$x\.uplift, \|\[\]\):y\|\$y->sum\(\)/);
  });

  it('spreads a declared text measure as its unique value, never a sum (query.ts defaultAggregate)', async () => {
    const q = (await level(snap({
      rows: [], measures: [],
      derived: [{ name: 'tag', lambda: (await row("$x.region + '!'")), kind: 'measure', type: 'String' }],
    }), undefined, YEARS));
    assert.match(q, /'2023__\|__tag':x\|if\(\$x\.year == 2023, \|\$x\.tag, \|\[\]\):y\|\$y->uniqueValueOnly\(\)/);
    assert.doesNotMatch(q, /__tag':[^,]*,[^:]*:y\|\$y->sum\(\)/);
  });
});

describe('a Variant column', () => {
  const columns = [
    { name: 'region', type: 'String' },
    { name: 'payload', type: 'Variant' },
    { name: 'notional', type: 'Float' },
  ];

  it('refuses to be pivoted on, and says how to pivot on what is in it', () => {
    // It would run: every column would be named after a whole JSON
    // document. The question is always about a value inside one.
    assert.throws(
      () => level(snap({ columns, rows: ['region'], pivotOn: ['payload'] })),
      /cannot pivot on 'payload': it holds JSON.*\$x\.payload->get\('key'\)->to\(@String\)/,
    );
  });

  it('pivots on a value extracted from it', async () => {
    const s = (await level(snap({
      columns,
      derived: [{ name: 'sku', type: 'String', kind: 'dimension',
        lambda: (await row("$x.payload->get('sku')->to(@String)")) }],
      rows: ['region'],
      pivotOn: ['sku'],
    }), undefined, { tuples: [['A1']] }));
    assert.match(s, /extend\(~\[sku:x\|\$x\.payload->get\('sku'\)->to\(@String\)/);
    assert.match(s, /if\(\$x\.sku == 'A1'/);
  });

  it('can be filtered on its presence', async () => {
    assert.equal(
      (await filterText({
        kind: 'condition',
        column: 'payload',
        operator: 'isNotEmpty',
      })),
      'x|$x.payload->isNotEmpty()',
    );
  });
});

describe('drilling into a JSON group', () => {
  // Grouping by a Variant column worked and opening a group returned
  // nothing: the key came back as `$x.customer == '{...}'`, which
  // compares the document to a JSON STRING holding its text.
  const ORDERS: QuerySnapshot = {
    source: { query: element('t') },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'customer', type: 'Variant' },
      { name: 'tags', type: 'Variant' },
      { name: 'total', type: 'Float' },
    ],
    derived: [],
    rows: ['customer'],
    pivotOn: [],
    measures: [{ name: 'revenue', column: 'total', fn: 'sum' }],
    sorts: [],
    epoch: 1,
  };
  const doc = '{"name":"Acme","tier":"gold"}';

  it('compares an object key as a document', async () => {
    const out = (await level(ORDERS, { level: 2, parent: [doc] }));
    assert.ok(out.includes(`$x.customer == fromJson('${doc}')`), out);
  });

  it('compares an array key, and the empty array, as documents', async () => {
    const tags = { ...ORDERS, rows: ['tags'] };
    assert.ok((await level(tags, { level: 2, parent: ['["gift","b2b"]'] }))
      .includes(`$x.tags == fromJson('["gift","b2b"]')`));
    assert.ok((await level(tags, { level: 2, parent: ['[]'] }))
      .includes(`$x.tags == fromJson('[]')`));
  });

  it('opens the detail rows under a pivot with the same key', async () => {
    const pivoted = { ...ORDERS, pivotOn: ['region'] };
    const out = (await level(detailSnapshot(pivoted, [doc]), undefined, { tuples: [['EMEA']] }));
    assert.ok(out.includes(`fromJson('${doc}')`), out);
  });

  it('escapes a quote inside the document', async () => {
    const odd = '{"name":"O\'Brien"}';
    const out = (await level(ORDERS, { level: 2, parent: [odd] }));
    assert.ok(out.includes(`fromJson('{"name":"O\\'Brien"}')`), out);
  });
});

