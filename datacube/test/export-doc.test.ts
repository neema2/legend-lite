// Plain text and PDF export, from the export model.
//
// The PDF tests assert STRUCTURE in BYTES: a PDF's /Length and xref are byte counts into the file,
// and the first version counted JavaScript characters -- every é or £ then broke the file for a
// strict reader, and its tests passed because they measured the same wrong thing (2026-09-29).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { toPdf, toPlainText, winAnsiByte } from '../src/export-doc.ts';
import { exportTable, type ExportTable } from '../src/export-model.ts';
import { buildColumnModel } from '../src/grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

function result(rows: number, region = (i: number) => `Region ${i}`): ResultTable {
  const r: (string | null)[] = [];
  const amount: (number | null)[] = [];
  for (let i = 0; i < rows; i++) {
    r.push(region(i));
    amount.push(i % 3 === 0 ? null : i * 1000.5);
  }
  return {
    columns: [
      { name: 'region', type: 'String', values: r },
      { name: 'amount', type: 'Float', values: amount },
    ],
    rowCount: rows,
    epoch: 1,
    elapsedMs: 0,
  };
}

const shown = (rt: ResultTable, title = ''): ExportTable =>
  exportTable({ title, rows: rt, model: buildColumnModel(rt), treeRows: [], groupLabels: [], truncated: false });

const latin1 = (b: Uint8Array): string => Buffer.from(b).toString('latin1');

describe('plain text export', () => {
  it('lines the columns up: header, rule, rows; numbers right, text left', () => {
    const lines = toPlainText(shown(result(3))).trimEnd().split('\n');
    assert.equal(lines.length, 5);
    assert.match(lines[1] ?? '', /^-+ {2}-+$/);
    const [a, b] = [lines[3] ?? '', lines[4] ?? ''];
    assert.equal(a.length, b.length, 'numbers end on the same column');
  });
  it('leaves no trailing whitespace, and a title and notes head it', () => {
    const out = toPlainText({ ...shown(result(2), 'Trades'), notes: ['Truncated.'] });
    assert.ok(out.split('\n').every((l) => !/\s$/.test(l)));
    assert.match(out, /^Trades\n\nTruncated\.\n\n/);
  });
  it('indents the tree by depth', () => {
    const t = shown(result(2));
    const tree: ExportTable = {
      ...t,
      grouped: true,
      columns: [{ ...t.columns[0]!, tree: true }, t.columns[1]!],
      rows: [{ ...t.rows[0]!, depth: 1, kind: 'group' }, { ...t.rows[1]!, depth: 3, kind: 'row' }],
    };
    const lines = toPlainText(tree).split('\n');
    assert.match(lines[3] ?? '', /^ {4}Region 1/, 'depth 3: two levels of two spaces');
  });
});

describe('PDF is bytes, and its bytes are consistent', () => {
  const pdf = toPdf(shown(result(80, (i) => (i % 2 ? `Zürich £${i}` : `Paris €${i} — ok…`)), 'Café report'));
  const text = latin1(pdf);

  it('is a PDF: header, binary comment, trailer', () => {
    assert.ok(text.startsWith('%PDF-1.4\n%'));
    assert.ok(text.trimEnd().endsWith('%%EOF'));
  });
  it('points startxref at the xref table, in bytes', () => {
    const at = Number(/startxref\n(\d+)\n%%EOF/.exec(text)?.[1]);
    assert.equal(text.slice(at, at + 4), 'xref');
  });
  it('gives every xref offset the byte where its object begins', () => {
    const xref = text.slice(text.lastIndexOf('\nxref\n'));
    const offsets = [...xref.matchAll(/^(\d{10}) 00000 n $/gm)].map((m) => Number(m[1]));
    assert.ok(offsets.length > 4);
    offsets.forEach((off, i) => assert.ok(text.startsWith(`${i + 1} 0 obj`, off), `object ${i + 1} at ${off}`));
  });
  it('declares each stream Length as its bytes', () => {
    for (const m of text.matchAll(/<< \/Length (\d+) >>\nstream\n/g)) {
      const start = (m.index ?? 0) + m[0].length;
      assert.equal(text.slice(start + Number(m[1]), start + Number(m[1]) + 10), '\nendstream');
    }
  });
  it('draws Latin-1 and WinAnsi characters as themselves: é ü £ € — …', () => {
    assert.ok(text.includes(`Caf${String.fromCharCode(0xe9)} report`), 'é as one WinAnsi byte');
    assert.ok(text.includes(`Z${String.fromCharCode(0xfc)}rich ${String.fromCharCode(0xa3)}`), 'ü and £');
    assert.ok(text.includes(String.fromCharCode(0x80)), '€ at 0x80');
    assert.ok(text.includes(String.fromCharCode(0x97)), '— at 0x97');
    assert.ok(text.includes(String.fromCharCode(0x85)), '… at 0x85');
    assert.equal(winAnsiByte('中'), undefined);
  });
  it('names what its font cannot draw, instead of a silent "?"', () => {
    const cjk = latin1(toPdf(shown(result(2, () => '北京'))));
    assert.match(cjk, /cannot be drawn.*U\+5317 U\+4EAC/);
  });
  it('paginates a long table and repeats the header on every page', () => {
    const pages = (text.match(/\/Type \/Page /g) ?? []).length;
    assert.ok(pages >= 2);
    assert.equal((text.match(/\(region\) Tj/g) ?? []).length, pages);
  });
  it('turns landscape for a wide table, and never overlaps: columns continue on another page', () => {
    const wide: ResultTable = {
      columns: Array.from({ length: 40 }, (_, i) => ({ name: `column_${i}`, type: 'String', values: [`value ${i} long text`] })),
      rowCount: 1, epoch: 1, elapsedMs: 0,
    };
    const w = latin1(toPdf(shown(wide)));
    assert.match(w, /\/MediaBox \[0 0 842 595\]/, 'landscape A4');
    assert.ok((w.match(/\/Type \/Page /g) ?? []).length > 1, 'the columns went on to another page');
  });
  it('escapes a bracket or backslash in a cell so it cannot end the string', () => {
    const b = latin1(toPdf(shown(result(1, () => 'a) b\\ (c'))));
    assert.ok(b.includes('(a\\) b\\\\ \\(c) Tj'));
  });
  it('an empty table is still a valid file', () => {
    const e = latin1(toPdf(shown(result(0))));
    assert.match(e, /\/Count 1 /);
  });
});

describe('the PDF draws each cell\'s look', () => {
  it('fills a coloured cell, draws its text in its colour, and a gray header', () => {
    const t = shown(result(2));
    const styled: ExportTable = { ...t, rows: t.rows.map((r, i) => ({ ...r, styles: r.styles.map((st, c) =>
      (c === 1 && i === 1 ? { ...st, background: '#ff8a65', color: '#ef4444', bold: true } : st)) })) };
    const pdf = latin1(toPdf(styled));
    assert.match(pdf, /1\.000 0\.541 0\.396 rg [\d.]+ [\d.]+ [\d.]+ [\d.]+ re f/, 'the heat fill');
    assert.match(pdf, /0\.937 0\.267 0\.267 rg BT \/F2 /, 'red text, in the bold face');
    assert.match(pdf, /0\.961 0\.961 0\.961 rg /, 'the gray header');
    assert.match(pdf, /\/BaseFont \/Times-Roman/, 'every base-14 face is on offer');
  });
});

describe('the whole page in a PDF: the board as laid out, the table in its tile', () => {
  const jpeg = Uint8Array.from([0xff, 0xd8, 0xff, 0xe0, 1, 2, 3, 0xff, 0xd9]);
  const board = (gridH: number) => ({ cols: 12, tiles: [
    { id: 'grid', kind: 'grid' as const, title: 'Grid', x: 0, y: 0, w: 12, h: gridH },
    { id: 'c', kind: 'chart' as const, title: 'By region', x: 0, y: gridH, w: 6, h: 10,
      picture: { width: 400, height: 200, pixelWidth: 800, pixelHeight: 400, png: new Uint8Array([1]), jpeg } },
  ] });
  const consistent = (pdf: string): void => {
    const xref = pdf.slice(pdf.lastIndexOf('\nxref\n'));
    const offsets = [...xref.matchAll(/^(\d{10}) 00000 n $/gm)].map((m) => Number(m[1]));
    offsets.forEach((off, i) => assert.ok(pdf.startsWith(`${i + 1} 0 obj`, off), `object ${i + 1}`));
  };

  it('draws each chart as an embedded JPEG, and the TABLE in the grid\'s tile: one page when it all fits', () => {
    const pdf = latin1(toPdf(shown(result(3), 'Trades'), { page: board(14) }));
    assert.match(pdf, /\/Subtype \/Image \/Width 800 \/Height 400 \/ColorSpace \/DeviceRGB \/BitsPerComponent 8 \/Filter \/DCTDecode \/Length 9 >>/);
    assert.match(pdf, /\/XObject << \/Im1 \d+ 0 R >>/);
    assert.match(pdf, /cm \/Im1 Do Q/);
    assert.match(pdf, /\/Count 1 /, 'the whole table fitted its tile: the dashboard is the whole file');
    assert.match(pdf, /\/MediaBox \[0 0 842 595\]/);
    assert.ok(pdf.includes('(Region 1) Tj'), 'the table\'s cells are on the dashboard');
    consistent(pdf);
  });

  it('a table too long for its tile: still one page, the rows that fit, and a line saying so', () => {
    const pdf = latin1(toPdf(shown(result(200), 'Trades'), { page: board(8) }));
    assert.match(pdf, /Showing the first \d+ of 200 rows; the whole table is in the Excel and CSV exports/);
    assert.match(pdf, /\/Count 1 /, 'the dashboard is the whole file');
    assert.ok(!pdf.includes('/MediaBox [0 0 595 842]'), 'no table pages after it');
    consistent(pdf);
  });
});
