// When DATA becomes a COLUMN NAME.
//
// A horizontal pivot builds its column names out of the VALUES in the
// pivoted column, so anything a user can store in a cell becomes part
// of an identifier that the grid then has to take apart again. That
// round trip -- value -> generated name -> header path -- is the one
// place in this product where hostile data turns directly into
// structure, and a wrong answer there is not a crash but something
// worse: a header that silently says the wrong thing about correct
// numbers.
//
// So these assert the round trip rather than the absence of an
// exception. The properties that matter:
//
//   RECOVERY   the measure comes back, and the value comes back
//              byte for byte
//   UNIFORMITY every column of a one-dimension pivot has a
//              two-level header -- a value level and a measure
//              level. A column that parses deeper or shallower than
//              its siblings is what misaligns a pivot header.
//   DISTINCTNESS  two different values never merge into one header
//              cell. Merging is how a pivot shows one label over two
//              unrelated columns of numbers.
//
// The hostile values are not invented freely: each one is a thing a
// real column contains. An empty string, a NULL, a value that happens
// to equal a measure name, a product code with the separator in it, a
// value with trailing underscores from a legacy feed.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  buildColumnModel,
  splitPath,
  PIVOT_SEPARATOR,
  TREE_COLUMN,
} from '../src/grid/columns.ts';
import type { ResultTable } from '../src/result.ts';

/** How legend-lite spells a pivot column: value(s), then the measure. */
function pivotName(values: readonly string[], measure: string): string {
  return [...values, measure].join(PIVOT_SEPARATOR);
}

function tableOf(names: readonly string[]): ResultTable {
  return {
    columns: names.map((name) => ({ name, type: 'String', values: [null] })),
    rowCount: 1,
    epoch: 1,
    elapsedMs: 0,
  };
}

/**
 * Values a real pivoted column can hold, each chosen for the way it
 * attacks the name round trip rather than for looking unusual.
 */
const HOSTILE_VALUES: readonly (readonly [string, string])[] = [
  ['ordinary', 'AMER'],
  ['empty string', ''],
  ['a single space', ' '],
  ['trailing underscore', 'legacy_'],
  ['many trailing underscores', 'legacy___'],
  ['leading underscore', '_legacy'],
  ['equal to the measure name', 'notional'],
  ['ending with the measure name', 'gross notional'],
  ['ending with the measure, no space', 'grossnotional'],
  ['containing the separator', `a${PIVOT_SEPARATOR}b`],
  ['exactly the separator', PIVOT_SEPARATOR],
  ['a lone pipe', '|'],
  ['underscores either side', '__x__'],
  ['numeric', '2021'],
  ['negative number', '-1'],
  ['unicode', 'Ünïcødé'],
  ['an emoji', '🙂'],
  ['an apostrophe', "it's"],
  ['a double quote', 'say "hi"'],
  ['a backslash', 'back\\slash'],
  ['a newline', 'two\nlines'],
  ['a tab', 'two\tcols'],
  ['the word null', 'null'],
  ['a very long value', 'x'.repeat(300)],
  ['dots', 'a.b.c'],
  ['dashes', 'a-b-c'],
  ['sql-ish', "'; DROP TABLE t; --"],
  ['html-ish', '<script>x</script>'],
  ['brace', '${x}'],
  ['just a dot', '.'],
];

const MEASURE = 'notional';

describe('a pivot value that becomes a column name', () => {
  for (const [label, value] of HOSTILE_VALUES) {
    it(`recovers the measure from a value that is ${label}`, () => {
      const name = pivotName([value], MEASURE);
      const path = splitPath(name, [MEASURE], 1);
      assert.equal(
        path[path.length - 1],
        MEASURE,
        `measure lost from ${JSON.stringify(name)} -> ${JSON.stringify(path)}`,
      );
    });

    it(`recovers the value itself when it is ${label}`, () => {
      const name = pivotName([value], MEASURE);
      const path = splitPath(name, [MEASURE], 1);
      assert.equal(
        path.slice(0, -1).join(PIVOT_SEPARATOR),
        value,
        `value corrupted: ${JSON.stringify(value)} -> ${JSON.stringify(path)}`,
      );
    });

    it(`gives a two-level header for a value that is ${label}`, () => {
      // UNIFORMITY. One pivot dimension and one measure is a
      // two-level header for EVERY column, whatever the value holds.
      // A value that parses to a different depth than its siblings is
      // exactly what leaves a pivot header misaligned.
      const name = pivotName([value], MEASURE);
      const path = splitPath(name, [MEASURE], 1);
      assert.equal(
        path.length,
        2,
        `depth ${path.length}, not 2: ${JSON.stringify(value)} -> ${JSON.stringify(path)}`,
      );
    });
  }
});

describe('a pivot whose values are all hostile at once', () => {
  const values = HOSTILE_VALUES.map(([, v]) => v);
  // Distinct inputs only: the grid cannot be asked to tell apart two
  // columns the ENGINE gave the same name, and duplicate values are
  // not a thing a pivot produces.
  const distinct = [...new Set(values)];
  const names = distinct.map((v) => pivotName([v], MEASURE));

  it('produces one leaf per column and loses none', () => {
    const model = buildColumnModel(tableOf(names), [], [MEASURE], {}, 1);
    assert.equal(model.leaves.length, names.length);
  });

  it('keeps every header cell inside the leaves', () => {
    const model = buildColumnModel(tableOf(names), [], [MEASURE], {}, 1);
    for (const row of model.headerRows) {
      for (const cell of row) {
        assert.ok(cell.colStart >= 0, `negative colStart ${cell.colStart}`);
        assert.ok(cell.colSpan >= 1, `non-positive colSpan ${cell.colSpan}`);
        assert.ok(
          cell.colStart + cell.colSpan <= model.leaves.length,
          `cell ${JSON.stringify(cell.label)} overruns: ${cell.colStart}+${cell.colSpan} > ${model.leaves.length}`,
        );
      }
    }
  });

  it('covers each header row exactly once, with no gaps or overlaps', () => {
    // A gap renders as a blank slot the columns slide under; an
    // overlap renders as two labels fighting for one column. Both
    // look like "the header is misaligned" and neither throws.
    const model = buildColumnModel(tableOf(names), [], [MEASURE], {}, 1);
    for (const [level, row] of model.headerRows.entries()) {
      const covered = new Array<number>(model.leaves.length).fill(0);
      for (const cell of row) {
        for (let c = cell.colStart; c < cell.colStart + cell.colSpan; c++) {
          covered[c] = (covered[c] ?? 0) + 1;
        }
      }
      // A leaf shallower than this level is covered by a rowSpan from
      // a row above, so it is legitimately absent here.
      model.leaves.forEach((leaf, i) => {
        const expected = leaf.path.length > level ? 1 : 0;
        assert.equal(
          covered[i],
          expected,
          `level ${level}, leaf ${i} (${JSON.stringify(leaf.name)}) covered ${covered[i]} times, expected ${expected}`,
        );
      });
    }
  });

  it('never merges two different values into one header cell', () => {
    // DISTINCTNESS. Each column here comes from a DIFFERENT pivot
    // value, so at the value level every cell must span exactly one
    // column. A wider cell means two unrelated columns of numbers are
    // sitting under a single label.
    const model = buildColumnModel(tableOf(names), [], [MEASURE], {}, 1);
    const valueLevel = model.headerRows[0] ?? [];
    for (const cell of valueLevel) {
      assert.equal(
        cell.colSpan,
        1,
        `header ${JSON.stringify(cell.label)} spans ${cell.colSpan} columns, merging distinct values`,
      );
    }
  });
});

describe('two pivot dimensions with hostile values', () => {
  const pairs: readonly (readonly [string, string])[] = [
    ['AMER', 'NY'],
    ['', 'NY'],
    ['AMER', ''],
    ['', ''],
    [`a${PIVOT_SEPARATOR}b`, 'NY'],
    ['notional', 'notional'],
    ['legacy_', 'x_'],
  ];

  for (const [a, b] of pairs) {
    it(`recovers both dimensions from (${JSON.stringify(a)}, ${JSON.stringify(b)})`, () => {
      const name = pivotName([a, b], MEASURE);
      const path = splitPath(name, [MEASURE], 2);
      assert.equal(path[path.length - 1], MEASURE, 'measure lost');
      assert.equal(
        path.length,
        3,
        `depth ${path.length}, not 3: ${JSON.stringify(path)}`,
      );
      assert.equal(path[0], a, 'first dimension corrupted');
      assert.equal(path[1], b, 'second dimension corrupted');
    });
  }
});

describe('a measure name that collides with the data', () => {
  it('prefers the longest measure so one cannot shadow another', () => {
    const measures = ['pnl', 'net_pnl'];
    assert.deepEqual(splitPath(pivotName(['2021'], 'net_pnl'), measures), [
      '2021',
      'net_pnl',
    ]);
    assert.deepEqual(splitPath(pivotName(['2021'], 'pnl'), measures), [
      '2021',
      'pnl',
    ]);
  });

  it('does not split a row dimension that ends with a measure name', () => {
    // 'grosspnl' is a DIMENSION, not a pivot column, even though it
    // ends with the measure 'pnl'. Passing the dimension list is what
    // keeps this exact -- without it the header would read
    // 'gross' / 'pnl'.
    const table = tableOf([TREE_COLUMN, 'grosspnl', pivotName(['2021'], 'pnl')]);
    const model = buildColumnModel(table, ['grosspnl'], ['pnl']);
    const dim = model.leaves.find((l) => l.name === 'grosspnl');
    assert.ok(dim, 'dimension leaf missing');
    assert.deepEqual(dim.path, ['grosspnl']);
    assert.equal(dim.isDimension, true);
  });
});
