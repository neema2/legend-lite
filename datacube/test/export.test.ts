import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache } from '../src/format.ts';
import { escapeField, toClipboard, toCsv } from '../src/export.ts';
import type { ResultTable } from '../src/result.ts';

const TABLE: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'AMER', null] },
    { name: 'total', type: 'Float', values: [1234.5, -42, null] },
  ],
  rowCount: 3,
  epoch: 1,
  elapsedMs: 0,
};

describe('escapeField', () => {
  it('leaves an ordinary value alone', () => {
    assert.equal(escapeField('EMEA', ','), 'EMEA');
  });

  it('quotes when the field contains the delimiter or a newline', () => {
    assert.equal(escapeField('a,b', ','), '"a,b"');
    assert.equal(escapeField('a\nb', ','), '"a\nb"');
    assert.equal(escapeField('a,b', '\t'), 'a,b', 'not a TSV delimiter');
  });

  it('doubles embedded quotes', () => {
    assert.equal(escapeField('say "hi"', ','), '"say ""hi"""');
  });

  it('defuses a value a spreadsheet would run as a formula', () => {
    // CSV injection: a cell whose value came from a database must
    // never execute when the file is opened.
    assert.equal(escapeField('=1+1', ','), "'=1+1");
    assert.equal(escapeField('+A1', ','), "'+A1");
    // ...but a negative NUMBER is data, not a formula: prefixing it
    // would make Excel read it as text and a summed column would be
    // wrong, which is exactly what raw export exists to avoid.
    assert.equal(escapeField('-2', ','), '-2');
    assert.equal(escapeField('-2.5e3', ','), '-2.5e3');
    assert.equal(escapeField('-A1', ','), "'-A1");
    assert.equal(escapeField('@SUM(A1)', ','), "'@SUM(A1)");
    // And one that only becomes dangerous once quoted.
    assert.equal(escapeField('=cmd|"x"', ','), '"\'=cmd|""x"""');
  });
});

describe('toCsv', () => {
  it('writes a header and raw values by default', () => {
    const csv = toCsv(TABLE, { bom: false });
    assert.equal(
      csv,
      'region,total\r\nEMEA,1234.5\r\nAMER,-42\r\n,\r\n',
    );
  });

  it('defaults to raw, because formatted numbers do not re-sum', () => {
    // Exporting "$1,234.50" into a spreadsheet and summing the column
    // is a classic way to get a wrong total.
    assert.match(toCsv(TABLE, { bom: false }), /1234\.5/);
    assert.equal(toCsv(TABLE, { bom: false }).includes('$'), false);
  });

  it('writes what the screen shows when asked', () => {
    const csv = toCsv(TABLE, {
      bom: false,
      formatted: true,
      formatters: new FormatterCache(),
      formats: {
        total: { kind: 'currency', currency: 'USD', locale: 'en-US' },
      },
    });
    assert.match(csv, /"\$1,234\.50"/);
    // A negative currency contains no comma here, so it stays bare --
    // but it starts with '-', which is defused.
    assert.match(csv, /'-\$42\.00/);
  });

  it('refuses formatted export with no formatter cache', () => {
    assert.throws(
      () => toCsv(TABLE, { formatted: true }),
      /needs a FormatterCache/,
    );
  });

  it('renders null as empty, never as the word null', () => {
    assert.match(toCsv(TABLE, { bom: false }), /\r\n,\r\n$/);
  });

  it('restricts and orders columns when asked', () => {
    const csv = toCsv(TABLE, { bom: false, columns: ['total', 'region'] });
    assert.match(csv, /^total,region\r\n/);
  });

  it('prepends a BOM by default so Excel reads UTF-8', () => {
    assert.equal(toCsv(TABLE).charCodeAt(0), 0xfeff);
    assert.notEqual(toCsv(TABLE, { bom: false }).charCodeAt(0), 0xfeff);
  });
});

describe('toClipboard', () => {
  it('uses tabs, LF and no BOM', () => {
    const tsv = toClipboard(TABLE);
    assert.equal(tsv.charCodeAt(0), 'r'.charCodeAt(0), 'no BOM in a paste');
    assert.match(tsv, /^region\ttotal\n/);
    assert.equal(tsv.includes('\r'), false);
  });
});
