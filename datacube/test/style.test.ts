import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  cellStyle,
  coloursFor,
  gridVariables,
  isAlternateRow,
  mergeAppearance,
  valueState,
} from '../src/style.ts';
import type { CellAppearance } from '../src/style.ts';

describe('valueState', () => {
  it('separates negative, zero and normal', () => {
    assert.equal(valueState(-1), 'negative');
    assert.equal(valueState(0), 'zero');
    assert.equal(valueState(1), 'normal');
  });

  it('treats NaN as an error, not a number', () => {
    // A failed computation must not be coloured as though it held a
    // value; that is how a broken measure goes unnoticed.
    assert.equal(valueState(Number.NaN), 'error');
    assert.equal(valueState(1, true), 'error');
  });

  it('treats null as ordinary, because an empty cell is ordinary', () => {
    // In a pivot a null usually means "no rows in this combination",
    // which is information rather than a fault.
    assert.equal(valueState(null), 'normal');
  });

  it('leaves non-numbers alone', () => {
    assert.equal(valueState('EMEA'), 'normal');
    assert.equal(valueState(false), 'normal');
  });

  it('does not call -0 negative', () => {
    // -0 === 0 is true, so the zero slot wins, which is what a reader
    // expects to see.
    assert.equal(valueState(-0), 'zero');
  });
});

describe('coloursFor', () => {
  const set = {
    normalForeground: '#111',
    negativeForeground: 'crimson',
    zeroForeground: '#999',
    errorForeground: 'orange',
    normalBackground: '#fff',
  };

  it('picks the slot for the state', () => {
    assert.equal(coloursFor(set, 'negative').foreground, 'crimson');
    assert.equal(coloursFor(set, 'zero').foreground, '#999');
    assert.equal(coloursFor(set, 'error').foreground, 'orange');
    assert.equal(coloursFor(set, 'normal').foreground, '#111');
  });

  it('falls back to normal for an unset slot', () => {
    // A cube that sets only a negative colour must still render
    // everything else rather than blanking it.
    const partial = { normalForeground: '#111', negativeForeground: 'red' };
    assert.equal(coloursFor(partial, 'zero').foreground, '#111');
    assert.equal(coloursFor(partial, 'negative').foreground, 'red');
  });

  it('says nothing when nothing is configured', () => {
    assert.deepEqual(coloursFor({}, 'negative'), {});
  });

  it('falls back on the background independently of the foreground', () => {
    assert.equal(coloursFor(set, 'negative').background, '#fff');
  });
});

describe('mergeAppearance', () => {
  const cube: CellAppearance = {
    fontFamily: 'Inter',
    fontSize: 12,
    bold: true,
    normalForeground: '#111',
  };

  it('lets a column override field by field', () => {
    // A column setting only its alignment must keep the cube's fonts
    // rather than resetting them.
    const merged = mergeAppearance(cube, { textAlign: 'right' });
    assert.equal(merged.textAlign, 'right');
    assert.equal(merged.fontFamily, 'Inter');
    assert.equal(merged.bold, true);
  });

  it('lets a column turn something off', () => {
    assert.equal(mergeAppearance(cube, { bold: false }).bold, false);
  });

  it("returns the cube's appearance when there is no column one", () => {
    assert.equal(mergeAppearance(cube, undefined), cube);
  });
});

describe('cellStyle', () => {
  it('colours by the value, not by the column', () => {
    const a: CellAppearance = {
      normalForeground: '#111',
      negativeForeground: 'crimson',
    };
    assert.equal(cellStyle(a, 42)['color'], '#111');
    assert.equal(cellStyle(a, -42)['color'], 'crimson');
  });

  it('combines underline and strikethrough rather than replacing', () => {
    const s = cellStyle({ underline: true, strikethrough: true }, 1);
    assert.equal(s['text-decoration'], 'underline line-through');
  });

  it('maps alignment onto the flex axis the cells use', () => {
    assert.equal(cellStyle({ textAlign: 'left' }, 1)['justify-content'], 'flex-start');
    assert.equal(cellStyle({ textAlign: 'center' }, 1)['justify-content'], 'center');
    assert.equal(cellStyle({ textAlign: 'right' }, 1)['justify-content'], 'flex-end');
  });

  it('says nothing when nothing is configured', () => {
    assert.deepEqual(cellStyle({}, 1), {});
  });
});

describe('isAlternateRow', () => {
  it('shades every other row by default', () => {
    assert.deepEqual(
      [0, 1, 2, 3].map((i) => isAlternateRow(i)),
      [false, true, false, true],
    );
  });

  it('treats the count as a BAND SIZE, not a modulus', () => {
    // "alternate every 2" means two shaded then two clear, which is
    // what a printed report looks like.
    assert.deepEqual(
      [0, 1, 2, 3, 4, 5].map((i) => isAlternateRow(i, 2)),
      [false, false, true, true, false, false],
    );
  });

  it('never divides by zero', () => {
    assert.equal(isAlternateRow(1, 0), true);
  });
});

describe('gridVariables', () => {
  it('turns grid lines off explicitly rather than by omission', () => {
    // An omitted variable inherits the stylesheet's default, so "off"
    // has to be said rather than left unsaid.
    const v = gridVariables({ showHorizontalGridLines: false });
    assert.equal(v['--dc-hgrid'], '0');
    assert.equal(v['--dc-vgrid'], '1', 'the other stays on');
  });

  it('carries colours and fonts through', () => {
    const v = gridVariables({
      gridLineColor: '#ddd',
      alternateRowsColor: '#fafafa',
      fontFamily: 'Inter',
      fontSize: 13,
    });
    assert.equal(v['--dc-border'], '#ddd');
    assert.equal(v['--dc-alt-row'], '#fafafa');
    assert.equal(v['--dc-font'], 'Inter');
    assert.equal(v['--dc-font-size'], '13px');
  });
});
