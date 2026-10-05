import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { FormatterCache } from '../src/format.ts';
import { encodedWords, escapeField, exportCsv, toClipboard, toCsv, toEml } from '../src/export.ts';
import { exportTable, REDACTED } from '../src/export-model.ts';
import { buildColumnModel } from '../src/grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

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

describe('exportCsv: the grid as shown', () => {
  it('writes the grid\'s headers in its order, raw values, blurred columns REDACTED', () => {
    const model = buildColumnModel(TABLE, [], ['total'], { order: ['total', 'region'], blurred: ['region'],
      displayNames: { total: 'Total USD' } });
    const csv = exportCsv(exportTable({ title: 't', rows: TABLE, model, treeRows: [], groupLabels: [], truncated: false }));
    assert.equal(csv, `\ufeffTotal USD,region\r\n1234.5,${REDACTED}\r\n-42,${REDACTED}\r\n,${REDACTED}\r\n`);
  });
  it('a grouped cube leads with its Level', () => {
    const t = exportTable({ title: 't', rows: TABLE, model: buildColumnModel(TABLE), treeRows: [], groupLabels: [], truncated: false });
    const csv = exportCsv({ ...t, grouped: true, rows: t.rows.map((r, i) => ({ ...r, depth: i + 1 })) });
    assert.match(csv, /^\ufeffLevel,region,total\r\n1,EMEA,1234\.5\r\n2,AMER/);
  });
});

/** A minimal MIME reader, strict about the parts this test relies on. */
function mimeParts(eml: string, boundary: string): string[] {
  return eml.split(`--${boundary}`).slice(1, -1).map((p) => p.replace(/^\r\n/, ''));
}

describe('toEml: an unsent draft, to the letter', () => {
  const bytes = Uint8Array.from([0x25, 0x50, 0x44, 0x46, 0x00, 0xff, 0xe9, 0x0a]);
  const eml = toEml({
    subject: 'Café P&L — Q1',
    text: 'Café: 2 rows.',
    attachment: { name: 'Café report.pdf', mime: 'application/pdf', content: bytes },
  });

  it('declares MIME, ends every line CRLF, carries its subject RFC 2047-encoded', () => {
    assert.ok(eml.startsWith('MIME-Version: 1.0\r\n'));
    assert.ok(!/[^\r]\n/.test(eml), 'no bare LF');
    const subject = /\r\nSubject: (.*?)\r\nX-Unsent/s.exec(eml)?.[1] ?? '';
    const decoded = [...subject.matchAll(/=\?UTF-8\?B\?([^?]+)\?=/g)]
      .map((m) => Buffer.from(m[1]!, 'base64').toString('utf8')).join('');
    assert.equal(decoded, 'Café P&L — Q1');
    assert.equal(encodedWords('plain'), 'plain');
  });
  it('has a text and an HTML body saying what is attached', () => {
    const body = Buffer.from(/text\/plain; charset="UTF-8"\r\nContent-Transfer-Encoding: base64\r\n\r\n([A-Za-z0-9+/=\r\n]+?)\r\n--/.exec(eml)?.[1] ?? '', 'base64').toString('utf8');
    assert.equal(body, 'Café: 2 rows.');
    assert.match(eml, /Content-Type: text\/html; charset="UTF-8"/);
  });
  it('attaches the real BYTES, with the filename in RFC 2231 form beside an ASCII fallback', () => {
    const [, attachment] = mimeParts(eml, '=_mixed_datacube');
    assert.match(attachment ?? '', /filename="Caf_ report\.pdf"; filename\*=UTF-8''Caf%C3%A9%20report\.pdf/);
    const b64 = (attachment ?? '').split('\r\n\r\n')[1]?.replace(/\r\n/g, '') ?? '';
    assert.deepEqual([...Buffer.from(b64, 'base64')], [...bytes], 'byte for byte');
    assert.ok((attachment ?? '').split('\r\n').every((l) => l.length <= 998), 'no line past RFC 5322\'s limit');
  });
});
