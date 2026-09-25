import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_CONFIGURATION,
  DEFAULT_LINK_LABEL_PARAMETER,
  applyToSnapshot,
  columnConfig,
  fromSnapshot,
  labelFor,
  mergeColumnOrder,
  renameColumnConfig,
  resolvedWidths,
  toColumnAppearance,
  toColumnLayout,
  toFormats,
  withColumn,
  type CubeConfiguration,
} from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { serialize } from '../src/serialize.ts';
import { renameColumnReferences } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('withColumn', () => {
  it('returns the SAME configuration when nothing changed', () => {
    // Identity lets a caller skip a re-render rather than rebuilding
    // the grid because a dropdown was reopened on its current value.
    const c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    assert.equal(withColumn(c, 'region', { pinned: 'left' }), c);
  });

  it('merges into a column rather than replacing it', () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    c = withColumn(c, 'region', { hidden: true });
    assert.deepEqual(columnConfig(c, 'region'), {
      pinned: 'left',
      hidden: true,
    });
  });

  it('clearing a setting leaves NO trace of it', () => {
    // `{pinned: undefined}` serialises as `"pinned": null`, which a
    // reader cannot tell from a deliberate null.
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    c = withColumn(c, 'region', { pinned: undefined });
    assert.equal('pinned' in columnConfig(c, 'region'), false);
    assert.equal(JSON.stringify(c.columns), '{}', 'and the column goes too');
  });

  it('leaves other columns untouched', () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { hidden: true });
    c = withColumn(c, 'year', { hidden: true });
    assert.deepEqual(Object.keys(c.columns).sort(), ['region', 'year']);
  });
});

describe('width modes', () => {
  it('fixed and range are different intentions, not different numbers', () => {
    // A user switching to (Any) and back must not have lost what
    // they typed, so all three values are kept and the MODE decides.
    const c = { widthMode: 'any', width: 120, minWidth: 80 } as const;
    assert.deepEqual(resolvedWidths(c), {});
    assert.deepEqual(resolvedWidths({ ...c, widthMode: 'fixed' }), {
      width: 120,
    });
    assert.deepEqual(resolvedWidths({ ...c, widthMode: 'range' }), {
      minWidth: 80,
    });
  });

  it('infers fixed from a bare width, for a config written by hand', () => {
    assert.deepEqual(resolvedWidths({ width: 90 }), { width: 90 });
  });
});

describe('projections', () => {
  const config: CubeConfiguration = {
    ...DEFAULT_CONFIGURATION,
    columnOrder: ['year', 'region'],
    columns: {
      region: {
        hidden: true,
        blurred: true,
        pinned: 'left',
        displayName: 'Region',
        widthMode: 'range',
        minWidth: 80,
        maxWidth: 200,
        format: { kind: 'text' },
        appearance: { bold: true },
      },
      year: { widthMode: 'fixed', width: 60 },
    },
  };

  it('splits one column record into the four consumers', () => {
    const layout = toColumnLayout(config);
    assert.deepEqual(layout.order, ['year', 'region']);
    assert.deepEqual(layout.hidden, ['region']);
    assert.deepEqual(layout.blurred, ['region']);
    assert.deepEqual(layout.pinned, { region: 'left' });
    assert.deepEqual(layout.displayNames, { region: 'Region' });
    assert.deepEqual(layout.widths, { year: 60 });
    assert.deepEqual(layout.minWidths, { region: 80 });
    assert.deepEqual(layout.maxWidths, { region: 200 });
    assert.deepEqual(toFormats(config), { region: { kind: 'text' } });
    assert.deepEqual(toColumnAppearance(config), { region: { bold: true } });
  });

  it('emits nothing for settings nobody set', () => {
    // An empty object per consumer, not an object of empty objects:
    // a saved view must not record opinions nobody held.
    assert.deepEqual(toColumnLayout(DEFAULT_CONFIGURATION), {});
    assert.deepEqual(toFormats(DEFAULT_CONFIGURATION), {});
    assert.deepEqual(toColumnAppearance(DEFAULT_CONFIGURATION), {});
  });

  it('labels a column by its display name, else its own name', () => {
    assert.equal(labelFor(config, 'region'), 'Region');
    assert.equal(labelFor(config, 'year'), 'year');
  });
});

describe('the defaults are DataCube\'s, not assumptions', () => {
  // Two were wrong here because they were assumed rather than read
  // off DataCubeConfiguration: alternateRows and showRootAggregation
  // were both set to true when theirs are false. A default nobody
  // chose is still a decision made for the user.
  it('opens with no grand total', () => {
    assert.equal(DEFAULT_CONFIGURATION.showRootAggregation, false);
  });

  it('but the setting still works when asked for', () => {
    const on = { ...DEFAULT_CONFIGURATION, showRootAggregation: true };
    assert.equal(on.showRootAggregation, true);
  });

  it('matches their grid-line and colour defaults', () => {
    const a = DEFAULT_CONFIGURATION.appearance;
    assert.equal(a.showHorizontalGridLines, false);
    assert.equal(a.showVerticalGridLines, true);
    assert.equal(a.gridLineColor, '#d4d4d4', 'neutral-300');
    assert.equal(a.negativeForeground, '#ef4444', 'red-500');
    assert.equal(a.zeroForeground, '#a3a3a3', 'neutral-400');
    assert.equal(a.fontSize, 11, 'their DEFAULT_FONT_SIZE');
  });
});

describe('the snapshot boundary', () => {
  it('carries only the settings that change the SQL', () => {
    let config = withColumn(DEFAULT_CONFIGURATION, 'year', {
      kind: 'dimension',
      excludedFromPivot: true,
      // Colour cannot reach a query, and must not force a refetch.
      appearance: { bold: true },
    });
    config = { ...config, maxRows: 50, treeColumnSort: 'desc' };

    const s = applyToSnapshot(CUBE, config);
    assert.equal(s.maxRows, 50);
    assert.equal(s.treeColumnSort, 'desc');
    const year = s.columns.find((c) => c.name === 'year');
    assert.deepEqual(year, {
      name: 'year',
      type: 'Integer',
      kind: 'dimension',
      excludedFromPivot: true,
    });
    assert.equal('appearance' in (year as object), false);
  });

  it('does NOT bump the epoch', () => {
    // A caller that means to refetch bumps it; one that is only
    // reconciling must not, or every reconcile costs a round trip.
    assert.equal(applyToSnapshot(CUBE, DEFAULT_CONFIGURATION).epoch, 1);
  });

  it('reads a snapshot back, so the editor opens on what is running', () => {
    const running: CubeSnapshot = {
      ...CUBE,
      maxRows: 25,
      treeColumnSort: 'desc',
      columns: [
        { name: 'region', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
        { name: 'notional', type: 'Float' },
      ],
    };
    const config = fromSnapshot(running);
    assert.equal(config.maxRows, 25);
    assert.equal(config.treeColumnSort, 'desc');
    assert.deepEqual(config.columns, { year: { kind: 'dimension' } });
  });

  it('round-trips', () => {
    const config = { ...fromSnapshot(CUBE), maxRows: 77 } as CubeConfiguration;
    const back = fromSnapshot(applyToSnapshot(CUBE, config));
    assert.equal(back.maxRows, 77);
  });
});

describe('folding a reorder into the order of every column', () => {
  it('keeps the columns the grid never reported', () => {
    // The grid can only report what it is showing, and a cube hides
    // plenty: the row dimensions are the tree's, a pivot key is
    // spent on the header, an unticked column is off. Writing the
    // report straight in dropped all of them out of the order, and
    // the columns panel -- which sorts by it and puts anything
    // unlisted last -- threw them to the end of the list. One drag
    // and the grouped columns jumped.
    assert.deepEqual(
      mergeColumnOrder(
        ['region', 'desk', 'qtr', 'notional', 'pnl'],
        ['pnl', 'notional', 'qtr'],
      ),
      // region and desk keep their places; the three that moved take
      // the three slots they already had, in their new order.
      ['region', 'desk', 'pnl', 'notional', 'qtr'],
    );
  });

  it('is identity when nothing actually moved', () => {
    assert.deepEqual(
      mergeColumnOrder(['a', 'b', 'c', 'd'], ['b', 'd']),
      ['a', 'b', 'c', 'd'],
    );
  });

  it('places an ARRIVING column beside its new neighbour', () => {
    // A column dragged in from the panel is in the report and not in
    // the order, so it has no slot to take: its neighbours in the
    // new order say where it belongs.
    assert.deepEqual(
      mergeColumnOrder(['a', 'b', 'c'], ['a', 'x', 'b']),
      ['a', 'x', 'b', 'c'],
    );
    // Nothing before it: the column after it decides.
    assert.deepEqual(
      mergeColumnOrder(['a', 'b'], ['x', 'a']),
      ['x', 'a', 'b'],
    );
    // Nothing either side that the order knows: the end.
    assert.deepEqual(
      mergeColumnOrder(['a', 'b'], ['x']),
      ['a', 'b', 'x'],
    );
  });

  it('never drops or duplicates a column', () => {
    const full = ['a', 'b', 'c', 'd', 'e'];
    const out = mergeColumnOrder(full, ['e', 'c', 'a']);
    assert.deepEqual([...out].sort(), [...full].sort());
    assert.equal(new Set(out).size, out.length);
  });
});

describe('Column Properties > Aggregation reaches the query', () => {
  // The dropdown wrote `aggregateFn` into the configuration and nothing
  // between the configuration and the query read it: with notional set
  // to max, the query was byte-identical to the default and still
  // summed (census §2).
  const FLAT: CubeSnapshot = {
    source: { expression: 't' },
    columns: [
      { name: 'region', type: 'String' },
      { name: 'year', type: 'Integer', kind: 'dimension' },
      { name: 'notional', type: 'Float' },
      { name: 'qty', type: 'Integer', kind: 'measure' },
    ],
    derived: [],
    rows: ['region'],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
  };
  const withAgg = (
    name: string,
    aggregateFn: 'max' | 'wavg' | 'count',
    weight?: string,
  ): CubeConfiguration => withColumn(DEFAULT_CONFIGURATION, name, {
    aggregateFn,
    ...(weight ? { aggregationParameters: [weight] } : {}),
  });

  it('in a grouped cube', () => {
    const q = serialize(applyToSnapshot(FLAT, withAgg('notional', 'max')));
    assert.match(q, /notional:x\|\$x\.notional:y\|\$y->max\(\)/);
  });

  it('in a column pivot', () => {
    const q = serialize(applyToSnapshot({ ...FLAT, rows: [], pivotOn: ['year'] },
      withAgg('notional', 'max')));
    assert.match(q, /pivot\(~\[year\], ~\[[^\]]*notional:x\|\$x\.notional:y\|\$y->max\(\)/);
  });

  it('a weighted average carries its weight column', () => {
    const q = serialize(applyToSnapshot(FLAT, withAgg('notional', 'wavg', 'qty')));
    assert.match(q, /notional:x\|\$x\.notional->wavgRowMapper\(\$x\.qty\):y\|\$y->wavg\(\)/);
  });

  it('on a calculated column too', () => {
    const q = serialize(applyToSnapshot({
      ...FLAT,
      derived: [{ name: 'uplift', expression: '$x.notional * 1.1', kind: 'measure' }],
    }, withAgg('uplift', 'max')));
    assert.match(q, /uplift:x\|\$x\.uplift:y\|\$y->max\(\)/);
  });

  it('reads back, so the editor opens on what is running', () => {
    const running = applyToSnapshot(FLAT, withAgg('notional', 'wavg', 'qty'));
    const cfg = fromSnapshot(running);
    assert.equal(columnConfig(cfg, 'notional').aggregateFn, 'wavg');
    assert.deepEqual(columnConfig(cfg, 'notional').aggregationParameters, ['qty']);
  });
});

describe('Column Properties > Display as link reaches the grid', () => {
  it("names the label parameter, upstream's default when unset", () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'doc', { displayAsLink: true });
    c = withColumn(c, 'wiki', { displayAsLink: true, linkLabelParameter: 'title' });
    c = withColumn(c, 'plain', { linkLabelParameter: 'ignored' });
    assert.deepEqual(toColumnLayout(c).links, {
      doc: DEFAULT_LINK_LABEL_PARAMETER,
      wiki: 'title',
    });
    assert.equal(DEFAULT_LINK_LABEL_PARAMETER, 'dataCube.linkLabel');
  });
});

describe('renaming a calculated column that is in use', () => {
  // Renaming one the cube was grouped, pivoted, sorted or filtered by
  // left those naming a column that no longer existed, and the planner
  // refused the rename.
  const USING: CubeSnapshot = {
    source: { expression: 't' },
    columns: [{ name: 'region', type: 'String' }, { name: 'qty', type: 'Integer' }],
    derived: [{ name: 'big', expression: '$x.qty > 10', kind: 'dimension' }],
    rows: ['big'],
    pivotOn: ['big'],
    measures: [{ name: 'n', column: 'big', fn: 'count' }],
    sorts: [{ column: 'big', direction: 'desc' }],
    filter: { kind: 'not', child: { kind: 'or', children: [
      { kind: 'condition', column: 'big', operator: 'equal', value: true },
      { kind: 'condition', column: 'region', operator: 'equalColumn', rightColumn: 'big' },
    ] } },
    pivotCast: [{ name: 'true__|__n', measure: 'big' }],
    epoch: 1,
  };

  it('carries the new name into every reference', () => {
    const r = renameColumnReferences(USING, 'big', 'large');
    assert.deepEqual(r.rows, ['large']);
    assert.deepEqual(r.pivotOn, ['large']);
    assert.equal(r.sorts[0]?.column, 'large');
    assert.equal(r.measures[0]?.column, 'large');
    assert.match(JSON.stringify(r.filter), /"column":"large"/);
    assert.match(JSON.stringify(r.filter), /"rightColumn":"large"/);
    assert.doesNotMatch(JSON.stringify(r.filter), /"big"/);
    // A cast built on the old name is forgotten, to be learned again.
    assert.equal(r.pivotCast, undefined);
  });

  it("moves the column's settings and its place in the order", () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'big', { displayName: 'Big trade' });
    c = { ...c, columnOrder: ['region', 'big', 'qty'] };
    const r = renameColumnConfig(c, 'big', 'large');
    assert.equal(columnConfig(r, 'large').displayName, 'Big trade');
    assert.equal(r.columns['big'], undefined);
    assert.deepEqual(r.columnOrder, ['region', 'large', 'qty']);
  });
});
