// Excel's number format codes (src/format-code.ts; docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P3): each section's
// digits, grouping, scaling commas, percent, literals and colour; a date's and a time's fields; refused in words where
// a code asks what DataCube does not draw. Each case is what Excel shows for the same code and value.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache } from '../src/format.ts';
import { formatByCode, readFormatCode } from '../src/format-code.ts';
import { CubeRefusal } from '../src/snapshot.ts';

const text = (code: string, value: unknown, numeric = true): string | undefined =>
  formatByCode(readFormatCode(code), value as never, numeric)?.text;
const colour = (code: string, value: unknown): string | undefined => formatByCode(readFormatCode(code), value as never, true)?.colour;

describe('a number by its code', () => {
  it('digits, places, grouping', () => {
    assert.equal(text('#,##0', 1234567.8), '1,234,568');
    assert.equal(text('0.00', 3.14159), '3.14');
    assert.equal(text('#,##0.0#', 1234.5), '1,234.5');
    assert.equal(text('#,##0.0#', 1234.567), '1,234.57');
    assert.equal(text('000', 7), '007');
    assert.equal(text('#.##', 0.5), '.5');
    // an exact decimal's text read as its number
    assert.equal(text('#,##0.00', '1234.5'), '1,234.50');
  });

  it('scaling commas, a percent, literals, currency', () => {
    assert.equal(text('#,##0.0,,"M"', 12_345_678), '12.3M');
    assert.equal(text('#,##0,"k"', 45_600), '46k');
    assert.equal(text('0.0%', 0.1234), '12.3%');
    assert.equal(text('$#,##0', 1500), '$1,500');
    assert.equal(text('[$€-407]#,##0.00', 9.5), '€9.50');
    assert.equal(text('#,##0 "units"', 3), '3 units');
    assert.equal(text('0\\h', 4), '4h');
  });

  it('a negative without its own section: a minus, never on what rounds to zero', () => {
    assert.equal(text('$#,##0', -1500), '-$1,500');
    assert.equal(text('0.0', -0.01), '0.0');
  });

  it("the negative's, the zero's and the text's own sections, and a section's colour", () => {
    const code = '#,##0;[Red](#,##0);"-";"n/a: "@';
    assert.equal(text(code, 1200), '1,200');
    assert.equal(text(code, -1200), '(1,200)');
    assert.equal(colour(code, -1200), '#ff0000');
    assert.equal(colour(code, 1200), undefined);
    assert.equal(text(code, 0), '-');
    assert.equal(text(code, 'EMEA', false), 'n/a: EMEA');
    // no text section: the grid writes the text as it is
    assert.equal(formatByCode(readFormatCode('0.00'), 'EMEA', false), null);
  });
});

describe('a date or a timestamp by its code (UTC, as stored)', () => {
  it('years, months by name and number, days, weekdays', () => {
    assert.equal(text('yyyy-mm-dd', '2026-10-24', false), '2026-10-24');
    assert.equal(text('dd mmm yyyy', '2026-10-24', false), '24 Oct 2026');
    assert.equal(text('mmmm d, yyyy', '2026-03-05', false), 'March 5, 2026');
    assert.equal(text('ddd dd/mm/yy', '2026-10-24', false), 'Sat 24/10/26');
  });

  it("m is a minute after an hour or before a second, else a month; a 12-hour clock's AM/PM", () => {
    assert.equal(text('yyyy-mm-dd hh:mm', '2026-10-24T09:05:00', false), '2026-10-24 09:05');
    assert.equal(text('h:mm AM/PM', '2026-10-24 15:30:00', false), '3:30 PM');
    assert.equal(text('mm:ss', '2026-10-24T09:05:07', false), '05:07');
  });
});

describe('refused in words where Excel asks what DataCube does not draw', () => {
  const refused = (code: string, words: RegExp): void => {
    assert.throws(() => readFormatCode(code), (e: unknown) => e instanceof CubeRefusal && words.test(e.message), code);
  };
  it('a condition, an exponent, a fraction, five sections, an open quote, nothing', () => {
    refused('[>100]0;0', /\[>100\] \(a condition or an elapsed time\)/);
    refused('0.00E+00', /an exponent/);
    refused('# ?/?', /a fraction/);
    refused('0;0;0;@;0', /at most four sections/);
    refused('0 "units', /a quote that is never closed/);
    refused(' ', /not nothing/);
    refused('yyyy 0', /a date and a number in one section/);
  });
});

describe('the grid writes a code its column carries, before its own number format', () => {
  it('a number, a date column, a text left as it is', () => {
    const f = new FormatterCache();
    assert.equal(f.format(-1500, { kind: 'number', code: '$#,##0;[Red]($#,##0)' }, 'Float'), '($1,500)');
    assert.equal(f.format('2026-10-24', { kind: 'auto', code: 'dd mmm yyyy' }, 'StrictDate'), '24 Oct 2026');
    assert.equal(f.format('EMEA', { kind: 'auto', code: '#,##0' }, 'String'), 'EMEA');
    assert.equal(f.format(null, { kind: 'number', code: '#,##0', nullText: '—' }, 'Float'), '—');
  });
});
