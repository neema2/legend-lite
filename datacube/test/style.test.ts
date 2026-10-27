import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  autoTextAlign,
  cellStyle,
  columnRange,
  heatColour,
  heatPosition,
  mixHex,
  coloursFor,
  fontStack,
  gridVariables,
  highlightBand,
  isAlternateRow,
  mergeAppearance,
  valueState,
} from '../src/style.ts';
import type { CellAppearance } from '../src/style.ts';

describe('valueState', () => {
  it('separates negative, zero and normal', () => {
    assert.equal(valueState(-1, 'Float'), 'negative');
    assert.equal(valueState(0, 'Float'), 'zero');
    assert.equal(valueState(1, 'Float'), 'normal');
  });

  it('treats NaN as an error, not a number', () => {
    // A failed computation must not be coloured as though it held a
    // value; that is how a broken measure goes unnoticed.
    assert.equal(valueState(Number.NaN, 'Float'), 'error');
    assert.equal(valueState(1, 'Float', true), 'error');
  });

  it('treats null as ordinary, because an empty cell is ordinary', () => {
    // In a pivot a null usually means "no rows in this combination",
    // which is information rather than a fault.
    assert.equal(valueState(null, 'Float'), 'normal');
  });

  it('leaves a column the compiler does not type numeric alone', () => {
    assert.equal(valueState('EMEA', 'String'), 'normal');
    assert.equal(valueState(false, 'Boolean'), 'normal');
    // text that looks like a number is still text: a String column of '-5' is not negative
    assert.equal(valueState('-5', 'String'), 'normal');
    assert.equal(valueState(-5, undefined), 'normal');
  });

  it('reads a decimal\'s exact text and a big integer by their column\'s type', () => {
    assert.equal(valueState('-12.50', 'Decimal'), 'negative');
    assert.equal(valueState('0.00', 'Decimal'), 'zero');
    assert.equal(valueState('1234.56', 'Decimal'), 'normal');
    assert.equal(valueState(-9007199254740993n, 'Integer'), 'negative');
  });

  it('does not call -0 negative', () => {
    // -0 === 0 is true, so the zero slot wins, which is what a reader
    // expects to see.
    assert.equal(valueState(-0, 'Float'), 'zero');
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
    assert.equal(cellStyle(a, 42, 'Float')['color'], '#111');
    assert.equal(cellStyle(a, -42, 'Float')['color'], 'crimson');
  });

  it('combines underline and strikethrough rather than replacing', () => {
    // The controls exclude them; a cube saved elsewhere can hold both.
    const s = cellStyle({ underline: 'solid', strikethrough: true }, 1, 'Float');
    assert.equal(s['text-decoration'], 'underline line-through');
  });

  it("draws the underline's variant", () => {
    assert.equal(cellStyle({ underline: 'wavy' }, 1, 'Float')['text-decoration'], 'underline wavy');
    assert.equal(cellStyle({ underline: 'solid' }, 1, 'Float')['text-decoration'], 'underline');
  });

  it("renders upstream's family names as stacks, anything else as given", () => {
    assert.equal(cellStyle({ fontFamily: 'Roboto Mono' }, 1, 'Float')['font-family'],
      '"Roboto Mono", ui-monospace, monospace');
    assert.equal(fontStack('Inter'), 'Inter');
  });

  it('maps alignment onto the flex axis the cells use', () => {
    assert.equal(cellStyle({ textAlign: 'left' }, 1, 'Float')['justify-content'], 'flex-start');
    assert.equal(cellStyle({ textAlign: 'center' }, 1, 'Float')['justify-content'], 'center');
    assert.equal(cellStyle({ textAlign: 'right' }, 1, 'Float')['justify-content'], 'flex-end');
  });

  it('says nothing when nothing is configured', () => {
    assert.deepEqual(cellStyle({}, 1, 'Float'), {});
  });
});

describe('a column\'s alignment when none is set (the user, 2026-10-25: DataCube\'s rule)', () => {
  it('a measure\'s figures right, so they line up by place value', () => {
    for (const type of ['Float', 'Integer', 'Decimal', 'Number']) assert.equal(autoTextAlign(true, type), 'right', type);
  });
  it('everything else left: a dimension of numbers, a text or date measure, a column not yet typed', () => {
    assert.equal(autoTextAlign(false, 'Integer'), 'left', 'a year grouped by is a label');
    assert.equal(autoTextAlign(false, 'String'), 'left');
    assert.equal(autoTextAlign(true, 'String'), 'left', 'a text measure (a unique) reads as text');
    assert.equal(autoTextAlign(true, 'StrictDate'), 'left');
    assert.equal(autoTextAlign(true, undefined), 'left');
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

describe('highlight rows, as upstream', () => {
  it('Standard (the default) bands every other row; off is off', () => {
    assert.equal(highlightBand({}), 1);
    assert.equal(highlightBand({ alternateRowsStandardMode: false }), 0);
  });

  it('Custom bands by its count, in its colour, and wins over Standard', () => {
    const custom = { alternateRows: true, alternateRowsCount: 3, alternateRowsColor: '#abcdef' };
    assert.equal(highlightBand({ ...custom, alternateRowsStandardMode: true }), 3);
    assert.equal(gridVariables(custom)['--dc-alt-row'], '#abcdef');
  });

  it("Standard uses the DEFAULT colour, not Custom's", () => {
    const v = gridVariables({ alternateRowsStandardMode: true, alternateRowsColor: '#abcdef' });
    assert.equal('--dc-alt-row' in v, false);
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
      alternateRows: true,
      alternateRowsColor: '#fafafa',
      fontFamily: 'Inter',
      fontSize: 13,
    });
    // The user's grid lines, NOT the structural frame. DataCube
    // keeps them apart -- --ag-border-color is neutral-200 while
    // DEFAULT_GRID_LINE_COLOR is neutral-300 -- and folding them
    // together makes the frame move when a user recolours the lines.
    assert.equal(v['--dc-grid-line'], '#ddd');
    assert.equal('--dc-border' in v, false);
    assert.equal(v['--dc-alt-row'], '#fafafa');
    assert.equal(v['--dc-font'], 'Inter');
    assert.equal(v['--dc-font-size'], '13px');
  });
});

describe('heatmap', () => {
  it('ignores blanks and non-numbers when measuring a column', () => {
    assert.deepEqual(columnRange([10, null, 'x', -5, 30], 'Float'), { min: -5, max: 30 });
    // a decimal's exact text places on the scale by its column's type
    assert.deepEqual(columnRange(['-1.50', '2.25'], 'Decimal'), { min: -1.5, max: 2.25 });
  });

  it('measures no range over a column the compiler does not type numeric', () => {
    assert.equal(columnRange(['10', '20'], 'String'), null);
  });

  it('reports no range when there is nothing to scale', () => {
    // A caller must render no heatmap rather than a uniform block of
    // the low colour.
    assert.equal(columnRange([null, 'x'], 'Float'), null);
  });

  it('places a value in its range', () => {
    const r = { min: 0, max: 100 };
    assert.equal(heatPosition(0, r), 0);
    assert.equal(heatPosition(50, r), 0.5);
    assert.equal(heatPosition(100, r), 1);
  });

  it('clamps a value outside a fixed range', () => {
    const r = { min: 0, max: 10 };
    assert.equal(heatPosition(-5, r), 0);
    assert.equal(heatPosition(99, r), 1);
  });

  it('maps a flat column to the TOP, not the bottom', () => {
    // When every value is identical they are all the maximum;
    // rendering them all as the minimum reads as "all low", which is
    // the opposite of true.
    assert.equal(heatPosition(7, { min: 7, max: 7 }), 1);
  });

  it('mixes colours across the scale', () => {
    assert.equal(mixHex('#000000', '#ffffff', 0), '#000000');
    assert.equal(mixHex('#000000', '#ffffff', 1), '#ffffff');
    assert.equal(mixHex('#000000', '#ffffff', 0.5), '#808080');
  });

  it('accepts short hex', () => {
    assert.equal(mixHex('#000', '#fff', 1), '#ffffff');
  });

  it('colours a cell, or declines to', () => {
    const spec = { from: '#ffffff', to: '#ff0000' };
    const range = { min: 0, max: 10 };
    assert.equal(heatColour(10, spec, range, 'Float'), '#ff0000');
    assert.equal(heatColour(null, spec, range, 'Float'), null);
    assert.equal(heatColour('x', spec, range, 'Float'), null);
    assert.equal(heatColour(5, spec, null, 'Float'), null, 'no range, no colour');
  });

  it('prefers a fixed range over the measured one', () => {
    // Deriving the scale from the visible rows makes the colours
    // change as the user scrolls a windowed grid.
    const spec = { from: '#000000', to: '#ffffff', range: { min: 0, max: 100 } };
    assert.equal(heatColour(50, spec, { min: 49, max: 51 }, 'Float'), '#808080');
  });
});

describe('cube-wide Case (General Properties)', () => {
  // A disabled control ("Per column") until 2026-09-25; upstream has
  // DataCubeConfiguration.fontCase for the whole cube.
  it('renders as text-transform on every cell', () => {
    assert.equal(cellStyle({ fontCase: 'uppercase' }, 'emea', 'String')['text-transform'], 'uppercase');
    assert.equal(cellStyle({}, 'emea', 'String')['text-transform'], undefined);
  });
});
