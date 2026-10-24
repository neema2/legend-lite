// Conditional formats (src/conditional.ts; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P3): a data bar's extent from the
// zero line, an icon by the third of the range, a rule's test against the column's figures at a depth, each read from
// JSON or refused in words.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  barExtent, columnStats, iconFor, readDataBar, readIconSet, readRule, ruleHolds, ruleLabel, ruleStyle, RULE_STYLES,
  type RuleWhen,
} from '../src/conditional.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { applyGridCommand, type GridCommand } from '../src/grid-commands.ts';
import { CubeRefusal, type CubeSnapshot } from '../src/snapshot.ts';

const refused = (f: () => unknown, words: RegExp): void => {
  assert.throws(f, (e: unknown) => e instanceof CubeRefusal && words.test(e.message), String(words));
};

describe("a column's figures at a depth: its range, its average, its values largest first", () => {
  it('blanks and text left out; none when it has no number', () => {
    assert.deepEqual(columnStats([3, null, -1, 10, 'x'], 'Float'), { range: { min: -1, max: 10 }, mean: 4, sorted: [10, 3, -1] });
    assert.equal(columnStats([null, null], 'Float'), null);
    // a decimal's exact text placed as a number (presentation)
    assert.deepEqual(columnStats(['1.50', '2.50'], 'Decimal')?.range, { min: 1.5, max: 2.5 });
  });
});

describe('a data bar: from the zero line to the value', () => {
  it('all positive: from the left, the largest the whole cell', () => {
    assert.deepEqual(barExtent(5, 'Float', { min: 2, max: 10 }), { start: 0, end: 0.5, negative: false });
    assert.deepEqual(barExtent(10, 'Float', { min: 2, max: 10 }), { start: 0, end: 1, negative: false });
  });

  it('negatives and positives: the zero line where zero falls, a negative bar left of it', () => {
    assert.deepEqual(barExtent(-5, 'Float', { min: -5, max: 15 }), { start: 0, end: 0.25, negative: true });
    assert.deepEqual(barExtent(15, 'Float', { min: -5, max: 15 }), { start: 0.25, end: 1, negative: false });
  });

  it('all negative: from the right; a blank none; one value everywhere no bar', () => {
    assert.deepEqual(barExtent(-2, 'Float', { min: -8, max: -2 }), { start: 0.75, end: 1, negative: true });
    assert.equal(barExtent(null, 'Float', { min: 0, max: 1 }), null);
    assert.deepEqual(barExtent(0, 'Float', { min: 0, max: 0 }), { start: 0, end: 0, negative: false });
  });
});

describe('an icon by the third of the range the value falls in', () => {
  const range = { min: 0, max: 90 };
  it('low, middle, high; reversed, a cost\'s', () => {
    assert.deepEqual([10, 40, 80].map((v) => iconFor(v, 'Float', { set: 'arrows' }, range)?.label), ['low', 'middle', 'high']);
    assert.deepEqual([10, 80].map((v) => iconFor(v, 'Float', { set: 'arrows', reverse: true }, range)?.glyph), ['▲', '▼']);
    assert.equal(iconFor('x', 'String', { set: 'flags' }, range), null);
  });
});

describe("a rule's test, against the column's figures at the value's depth", () => {
  const stats = columnStats([1, 2, 3, 4, 5, 5], 'Integer');
  const holds = (when: RuleWhen, values: readonly unknown[]): boolean[] =>
    values.map((v) => ruleHolds(when, v as never, 'Integer', stats));

  it('comparisons, between either way round, equality of a number or a text', () => {
    assert.deepEqual(holds({ op: 'greaterThan', value: 3 }, [3, 4]), [false, true]);
    assert.deepEqual(holds({ op: 'atMost', value: 3 }, [3, 4, null]), [true, false, false]);
    assert.deepEqual(holds({ op: 'between', from: 4, to: 2 }, [1, 2, 4, 5]), [false, true, true, false]);
    assert.deepEqual(['EMEA', 'AMER', null].map((v) => ruleHolds({ op: 'equal', value: 'EMEA' }, v, 'String', null)), [true, false, false]);
    assert.deepEqual(['EMEA', null].map((v) => ruleHolds({ op: 'notEqual', value: 'EMEA' }, v, 'String', null)), [false, false]);
  });

  it('text contains or starts with, either case; blank and not blank', () => {
    assert.deepEqual(['Emea', 'xem', null].map((v) => ruleHolds({ op: 'contains', text: 'EM' }, v, 'String', null)), [true, true, false]);
    assert.deepEqual(['Emea', 'xem'].map((v) => ruleHolds({ op: 'startsWith', text: 'em' }, v, 'String', null)), [true, false]);
    assert.deepEqual(holds({ op: 'blank' }, [null, '', 0]), [true, true, false]);
  });

  it('the top and bottom N, ties at the Nth in; above and below the average', () => {
    assert.deepEqual(holds({ op: 'top', count: 2 }, [3, 4, 5]), [false, false, true], 'the two largest are both 5');
    assert.deepEqual(holds({ op: 'top', count: 3 }, [3, 4]), [false, true]);
    assert.deepEqual(holds({ op: 'bottom', count: 2 }, [1, 2, 3]), [true, true, false]);
    assert.deepEqual(holds({ op: 'top', count: 50 }, [1]), [true], 'more asked than there are: every one');
    assert.deepEqual(holds({ op: 'aboveAverage' }, [3, 4]), [false, true]);
    assert.deepEqual(holds({ op: 'belowAverage' }, [3, 4]), [true, false]);
  });

  it('the first rule that holds draws its style', () => {
    const rules = [{ when: { op: 'lessThan', value: 0 } as const, style: RULE_STYLES.redText },
      { when: { op: 'lessThan', value: 10 } as const, style: RULE_STYLES.yellow }];
    assert.deepEqual(ruleStyle(rules, -1, 'Float', null), RULE_STYLES.redText);
    assert.deepEqual(ruleStyle(rules, 5, 'Float', null), RULE_STYLES.yellow);
    assert.equal(ruleStyle(rules, 50, 'Float', null), null);
    assert.equal(ruleLabel({ op: 'between', from: 1, to: 2 }), 'Between 1 and 2');
    assert.equal(ruleLabel({ op: 'contains', text: 'EM' }), 'Contains "EM"');
  });
});

describe('read from JSON, as a configure command or a saved page carries them, or refused in words', () => {
  it('a data bar, an icon set, a rule', () => {
    assert.deepEqual(readDataBar({ color: '#638ec6', range: { min: 0, max: 10 } }), { color: '#638ec6', range: { min: 0, max: 10 } });
    assert.deepEqual(readIconSet({ set: 'trafficLights', reverse: true }), { set: 'trafficLights', reverse: true });
    assert.deepEqual(readRule({ when: { op: 'top', count: 10 }, style: { background: '#ffc7ce', bold: true } }),
      { when: { op: 'top', count: 10 }, style: { background: '#ffc7ce', bold: true } });
    // one of Excel's styles by its name: kept as its colours
    assert.deepEqual(readRule({ when: { op: 'blank' }, style: 'yellow' }), { when: { op: 'blank' }, style: RULE_STYLES.yellow });
  });

  it('refused: a colour that is none, an unknown set, a rule missing its value, a count of none, a style drawing nothing', () => {
    refused(() => readDataBar({ color: 'blue' }), /a data bar's color is a colour as #rrggbb, not "blue"/);
    refused(() => readDataBar({ color: '#fff', width: 3 }), /a data bar takes color, negativeColor, range, not width/);
    refused(() => readDataBar({ color: '#fff', range: { min: 5, max: 1 } }), /5 is over 1/);
    refused(() => readIconSet({ set: 'stars' }), /one of arrows, trafficLights, symbols, flags, not "stars"/);
    refused(() => readRule({ when: { op: 'greaterThan' }, style: { bold: true } }), /greaterThan takes value: value is missing/);
    refused(() => readRule({ when: { op: 'top', count: 0 }, style: { bold: true } }), /a whole number from 1, not 0/);
    refused(() => readRule({ when: { op: 'odd' }, style: { bold: true } }), /op one of greaterThan/);
    refused(() => readRule({ when: { op: 'blank' }, style: {} }), /draws something/);
    refused(() => readRule({ when: { op: 'blank' }, style: 'purple' }), /one of lightRed, yellow, green, redText, bold or/);
    refused(() => readRule({ when: { op: 'contains', text: '' }, style: { bold: true } }), /some text, not ""/);
  });
});

describe("configure judges a column's conditional formats as their readers do", () => {
  const draft = { snapshot: { source: { query: { _type: 'packageableElementPtr', fullPath: 't' } }, columns: [{ name: 'qty', type: 'Integer', kind: 'measure' }],
    derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1 } as unknown as CubeSnapshot, config: DEFAULT_CONFIGURATION };
  const configure = (columns: unknown): unknown => applyGridCommand(draft, { kind: 'configure', patch: { columns } } as GridCommand);
  it('kept as given; refused in words, naming the column', () => {
    const kept = configure({ qty: { dataBar: { color: '#9fc3ea' }, rules: [{ when: { op: 'blank' }, style: { bold: true } }] } }) as typeof draft;
    assert.deepEqual(kept.config.columns['qty']?.dataBar, { color: '#9fc3ea' });
    refused(() => configure({ qty: { dataBar: { color: 'blue' } } }), /^qty: a data bar's color is a colour as #rrggbb/);
    refused(() => configure({ qty: { rules: { when: { op: 'blank' } } } }), /qty: rules are a list/);
    refused(() => configure({ qty: { heatmap: { from: '#fff' } } }), /qty: a colour scale's to is a colour/);
    // cleared by null, as every setting is
    assert.equal((configure({ qty: { iconSet: null } }) as typeof draft).config.columns['qty']?.iconSet, undefined);
  });
});
