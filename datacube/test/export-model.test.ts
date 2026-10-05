// What an export carries is the grid AS SHOWN (export-model.ts), built from the grid's own column
// model -- so these build real models with `buildColumnModel`, the function the grid itself uses.
// The 2026-09-29 audit found every format exporting hidden columns, blurred ones in the clear,
// the tree's own grouping columns, the query's order and internal names; each of those is here.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { buildColumnModel } from '../src/grid/columns.ts';
import { exportTable, flatHeader, REDACTED } from '../src/export-model.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { TreeRow } from '../src/tree.ts';

const flat: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'AMER'] },
    { name: 'desk', type: 'String', values: ['FX', 'Rates'] },
    { name: 'notional', type: 'Float', values: [1234.5, -20] },
    { name: 'secret', type: 'Float', values: [42, 7] },
  ],
  rowCount: 2,
  epoch: 1,
  elapsedMs: 0,
};

const base = { title: 't', treeRows: [] as TreeRow[], groupLabels: [], truncated: false };

describe('the export is the grid as shown', () => {
  it('leaves out a hidden column, keeps the user\'s order and labels', () => {
    const model = buildColumnModel(flat, [], ['notional'], {
      hidden: ['desk'],
      order: ['notional', 'region', 'secret'],
      displayNames: { notional: 'Notional USD' },
    });
    const t = exportTable({ ...base, rows: flat, model });
    assert.deepEqual(t.columns.map((c) => c.name), ['notional', 'region', 'secret']);
    assert.deepEqual(t.columns.map(flatHeader), ['Notional USD', 'region', 'secret']);
    assert.deepEqual(t.rows[0]?.cells, [1234.5, 'EMEA', 42]);
  });

  it('REDACTS a blurred column in every cell, and says so', () => {
    const model = buildColumnModel(flat, [], [], { blurred: ['secret'] });
    const t = exportTable({ ...base, rows: flat, model });
    const at = t.columns.findIndex((c) => c.name === 'secret');
    assert.equal(t.columns[at]?.redacted, true);
    assert.equal(t.columns[at]?.type, 'String', 'a redacted column is text, whatever it held');
    assert.deepEqual(t.rows.map((r) => r.cells[at]), [REDACTED, REDACTED]);
    assert.ok(!JSON.stringify(t.rows).includes('42'), 'the value is nowhere in the export');
    assert.match(t.notes.join(' '), /Blurred on screen.*secret/);
  });

  it('a grouped cube: the tree column under the dimensions\' names, each row with its depth and kind', () => {
    const grouped: ResultTable = {
      columns: [
        { name: '__tree', type: 'String', values: ['', 'EMEA', 'FX'] },
        { name: 'region', type: 'String', values: [null, 'EMEA', 'EMEA'] },
        { name: 'notional', type: 'Float', values: [100, 60, 60] },
      ],
      rowCount: 3,
      epoch: 1,
      elapsedMs: 0,
    };
    const tree: TreeRow[] = [
      { path: [], level: 0, depth: 1, isGroup: true, expanded: true, isTotal: true },
      { path: ['EMEA'], level: 1, depth: 2, isGroup: true, expanded: true, isTotal: false },
      { path: ['EMEA', 'FX'], level: 2, depth: 3, isGroup: false, expanded: false, isTotal: false },
    ];
    const model = buildColumnModel(grouped, ['region'], ['notional']);
    const t = exportTable({ ...base, rows: grouped, model, treeRows: tree, groupLabels: ['Region', 'Desk'] });
    assert.equal(t.grouped, true);
    assert.equal(flatHeader(t.columns[0]!), 'Region / Desk', 'not "__tree"');
    assert.ok(!t.columns.some((c) => c.name === 'region'), 'the grouping column the tree hides stays hidden');
    assert.deepEqual(t.rows.map((r) => [r.cells[0], r.depth, r.kind]),
      [['Total', 1, 'total'], ['EMEA', 2, 'group'], ['FX', 3, 'row']]);
  });

  it('a pivoted cube: each leaf headed by its levels, as the grid draws them', () => {
    const pivoted: ResultTable = {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: '2021__|__notional', type: 'Float', values: [10] },
        { name: '2022__|__notional', type: 'Float', values: [20] },
      ],
      rowCount: 1,
      epoch: 1,
      elapsedMs: 0,
    };
    const model = buildColumnModel(pivoted, [], ['notional'], {}, 1);
    const t = exportTable({ ...base, rows: pivoted, model });
    assert.deepEqual(t.columns.map((c) => c.path), [['region'], ['2021', 'notional'], ['2022', 'notional']]);
    assert.ok(!t.columns.some((c) => flatHeader(c).includes('__|__')), 'no internal names');
    assert.equal(t.headerRows.length, 2, 'the grid\'s two header levels travel with the table');
  });

  it('says when the grid was truncated at the row limit', () => {
    const model = buildColumnModel(flat);
    const t = exportTable({ ...base, rows: flat, model, truncated: true, maxRows: 1000 });
    assert.match(t.notes.join(' '), /Truncated: showing the first 1,000 rows/);
  });
});

describe('the export LOOKS like the grid: each cell styled as the grid styles it', () => {
  const money: ResultTable = {
    columns: [
      { name: 'region', type: 'String', values: ['EMEA', 'AMER', 'APAC'] },
      { name: 'pnl', type: 'Float', values: [10, -5, 0] },
    ],
    rowCount: 3, epoch: 1, elapsedMs: 0,
  };
  const model = buildColumnModel(money);
  const appearance = { negativeForeground: '#ef4444', zeroForeground: '#a3a3a3', alternateRowsStandardMode: true,
    fontFamily: 'Roboto', fontSize: 11, showVerticalGridLines: true, gridLineColor: '#d4d4d4' };
  const t = exportTable({ ...base, rows: money, model, appearance,
    columnAppearance: { region: { bold: true, textAlign: 'center' } },
    cellBackground: (leaf, row) => (leaf.name === 'pnl' && row === 0 ? '#ff8a65' : null) });

  it('colours a value by what it is: negative red, zero gray', () => {
    assert.equal(t.rows[1]?.styles[1]?.color, '#ef4444');
    assert.equal(t.rows[2]?.styles[1]?.color, '#a3a3a3');
  });
  it('a heatmap wins, the band shades every other row, the column\'s own look applies', () => {
    assert.equal(t.rows[0]?.styles[1]?.background, '#ff8a65');
    assert.equal(t.rows[1]?.styles[0]?.background, '#d7e0eb', 'the band, on the second row');
    assert.equal(t.rows[0]?.styles[0]?.background, undefined);
    assert.deepEqual([t.rows[0]?.styles[0]?.bold, t.rows[0]?.styles[0]?.align], [true, 'center']);
  });
  it('carries the grid\'s font and lines, and a plain gray header', () => {
    assert.deepEqual([t.look.fontFamily, t.look.fontSize, t.look.verticalLines, t.look.horizontalLines, t.look.headerBackground],
      ['Roboto', 11, true, false, '#f5f5f5']);
  });
  it('a total row is shaded and bold, as the grid draws it', () => {
    const tree: TreeRow[] = [{ path: [], level: 0, depth: 1, isGroup: true, expanded: true, isTotal: true }];
    const one: ResultTable = { ...money, rowCount: 1, columns: money.columns.map((c) => ({ ...c, values: c.values.slice(0, 1) })) };
    const totals = exportTable({ ...base, rows: one, model: buildColumnModel(one), treeRows: tree, appearance });
    assert.deepEqual([totals.rows[0]?.styles[0]?.background, totals.rows[0]?.styles[0]?.bold], ['#fafafa', true]);
  });
});
