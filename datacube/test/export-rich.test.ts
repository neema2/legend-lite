// HTML and Excel export, from the export model. The workbook is unzipped and every part parsed
// as XML: the file this replaced was not a real workbook and could be malformed (2026-09-29).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { strFromU8, unzipSync } from 'fflate';
import { JSDOM } from 'jsdom';

import { cssOf, escapeXml, toHtml } from '../src/export-rich.ts';
import { exportTable, REDACTED, type ExportTable } from '../src/export-model.ts';
import { argb, columnLetters, formatCode, sheetName, toXlsx } from '../src/export-xlsx.ts';
import { buildColumnModel } from '../src/grid/columns.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';

const rt: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'A&B <x>'] },
    { name: 'notional', type: 'Float', values: [1234.5, -20] },
    { name: 'qty', type: 'Integer', values: [3, null] },
    { name: 'day', type: 'StrictDate', values: ['2024-02-29', null] },
    { name: 'at', type: 'DateTime', values: ['2024-02-29T13:14:15', null] },
    { name: 'ok', type: 'Boolean', values: [true, false] },
    { name: 'big', type: 'Integer', values: [9007199254740993n, 1n] },
    { name: 'secret', type: 'Float', values: [42, 7] },
  ],
  rowCount: 2,
  epoch: 1,
  elapsedMs: 0,
};
const table = (title = 'Profit and loss by region, Q1 & Q2 [draft]'): ExportTable => exportTable({
  title, rows: rt, model: buildColumnModel(rt, [], ['notional'], { blurred: ['secret'] }),
  treeRows: [], groupLabels: [], truncated: false,
});

const xml = (text: string): Document => {
  const doc = new new JSDOM('').window.DOMParser().parseFromString(text, 'application/xml');
  assert.equal(doc.getElementsByTagName('parsererror').length, 0, `not well-formed:\n${text.slice(0, 300)}`);
  return doc;
};

describe('escapeXml', () => {
  it('escapes everything that can break a document, the ampersand first', () => {
    assert.equal(escapeXml(`<a href="x">&'</a>`), '&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;');
    assert.equal(escapeXml('&lt;'), '&amp;lt;');
  });
});

describe('HTML', () => {
  it('is a standalone page of the grid as shown, blurred columns redacted and said so', () => {
    const html = toHtml(table());
    assert.match(html, /^<!doctype html>/);
    assert.ok(html.includes('A&amp;B &lt;x&gt;'), 'data escaped');
    assert.ok(!html.includes('>42<'), 'the blurred value is not in the page');
    assert.ok(html.includes(REDACTED));
    assert.match(html, /Blurred on screen/);
  });
  it('carries a pivot\'s header levels with their spans', () => {
    const pivoted: ResultTable = {
      columns: [
        { name: 'region', type: 'String', values: ['EMEA'] },
        { name: '2021__|__notional', type: 'Float', values: [1] },
        { name: '2021__|__pnl', type: 'Float', values: [2] },
      ],
      rowCount: 1, epoch: 1, elapsedMs: 0,
    };
    const t = exportTable({ title: 'p', rows: pivoted, model: buildColumnModel(pivoted, [], ['notional', 'pnl'], {}, 1),
      treeRows: [], groupLabels: [], truncated: false });
    assert.match(toHtml(t), /<th scope="col" colspan="2">2021<\/th>/);
  });
});

describe('Excel: a real .xlsx', () => {
  const files = unzipSync(toXlsx(table(), { formats: { notional: { kind: 'currency', currency: 'USD', decimals: 0, negativeParens: true } } }));
  const part = (name: string): string => {
    const f = files[name];
    assert.ok(f, `the workbook has ${name}`);
    return strFromU8(f!);
  };

  it('has the OOXML parts, each well-formed XML', () => {
    for (const name of ['[Content_Types].xml', '_rels/.rels', 'xl/workbook.xml', 'xl/_rels/workbook.xml.rels',
      'xl/styles.xml', 'xl/worksheets/sheet1.xml', 'xl/worksheets/sheet2.xml']) xml(part(name));
  });
  it('names the sheet as Excel allows: no [ ] : * ? / \\, at most 31 characters, cut before escaping', () => {
    const sheet = xml(part('xl/workbook.xml')).getElementsByTagName('sheet')[0]!.getAttribute('name')!;
    assert.ok(sheet.length <= 31 && !/[[\]:*?/\\]/.test(sheet), sheet);
    assert.equal(sheetName('a'.repeat(28) + ' & b'), `${'a'.repeat(28)} &`);
    assert.equal(sheetName('[]'), 'Sheet1');
  });
  it('types its cells: numbers, a date serial, a boolean, text; an unsafe big integer as text', () => {
    const sheet = part('xl/worksheets/sheet1.xml');
    assert.match(sheet, /<c r="B2"[^>]*><v>1234\.5<\/v><\/c>/, 'a number is a number');
    assert.match(sheet, /<c r="D2" s="\d+"><v>45351<\/v><\/c>/, '2024-02-29 is serial 45351');
    assert.match(sheet, /<c r="E2" s="\d+"><v>45351\.55156/, 'with its time of day');
    assert.match(sheet, /<c r="F2"[^>]* t="b"><v>1<\/v><\/c>/);
    assert.match(sheet, /<c r="G2"[^>]* t="inlineStr"><is><t xml:space="preserve">9007199254740993<\/t>/, 'kept exact, as text');
  });
  it('carries the column\'s format as an Excel format code', () => {
    assert.match(part('xl/styles.xml'), /formatCode="&quot;\$&quot;#,##0;\(&quot;\$&quot;#,##0\)"/);
    assert.equal(formatCode({ kind: 'number', decimals: 2, numberScale: 'millions' }, 'Float'), '#,##0.00,,"m"');
    assert.equal(formatCode({ kind: 'number', displayCommas: false }, 'Integer'), '0');
    assert.equal(formatCode(undefined, 'Float'), null);
  });
  it('redacts a blurred column, and the About sheet says why', () => {
    const sheet = part('xl/worksheets/sheet1.xml');
    assert.ok(!/<v>42<\/v>/.test(sheet));
    assert.ok(sheet.includes(REDACTED));
    assert.match(part('xl/worksheets/sheet2.xml'), /Blurred on screen/);
  });
  it('outlines a tree and freezes the header', () => {
    const t = table();
    const tree: ExportTable = { ...t, grouped: true, columns: [{ ...t.columns[0]!, tree: true }, ...t.columns.slice(1)],
      rows: [{ ...t.rows[0]!, depth: 1, kind: 'group' }, { ...t.rows[1]!, depth: 2, kind: 'row' }] };
    const sheet = strFromU8(unzipSync(toXlsx(tree))['xl/worksheets/sheet1.xml']!);
    assert.match(sheet, /<row r="3" outlineLevel="1">/);
    assert.match(sheet, /state="frozen"/);
  });
  it('names its columns A..Z, AA..', () => {
    assert.deepEqual([0, 25, 26, 701, 702].map(columnLetters), ['A', 'Z', 'AA', 'ZZ', 'AAA']);
  });
});

describe('the rich formats draw each cell\'s look', () => {
  const styled = (): ExportTable => {
    const t = table('Styled');
    return { ...t, rows: t.rows.map((r, i) => ({ ...r, styles: r.styles.map((st, c) => (c === 1
      ? { ...st, ...(i === 1 ? { color: '#ef4444' } : {}), ...(i === 0 ? { background: '#ff8a65', bold: true } : {}) }
      : st)) })) };
  };
  it('HTML: inline colours and weight', () => {
    const html = toHtml(styled());
    const rule = (cls: string): string => new RegExp(`td\\.${cls} \\{ ([^}]*) \\}`).exec(html)?.[1] ?? '';
    const cls = (i: number): string => [...html.matchAll(/<tr>(.*?)<\/tr>/g)][i + 1]?.[1]?.match(/<td(?: class="(s\d+)")?>/g)?.[1]
      ?.match(/s\d+/)?.[0] ?? '';
    assert.match(rule(cls(0)), /background:#ff8a65.*font-weight:600/, 'the first row\'s notional: its heat and weight');
    assert.match(rule(cls(1)), /color:#ef4444/, 'the second row\'s: red');
    assert.ok(!/<td style=/.test(html), 'no inline style: one class per look');
    assert.match(html, /th \{ background: #f5f5f5/, 'a plain gray header');
    assert.equal(cssOf({ align: 'right', underline: 'dashed' }), 'text-decoration:underline dashed;text-align:right');
  });
  it('the workbook: fills and font colours in its styles, a double as a NUMBER with its format', () => {
    const files = unzipSync(toXlsx(styled(), { formats: { notional: { kind: 'currency', currency: 'USD', decimals: 0 } } }));
    const styles = strFromU8(files['xl/styles.xml']!);
    assert.match(styles, /<fgColor rgb="FFFF8A65"\/>/);
    assert.match(styles, /<color rgb="FFEF4444"\/>/);
    assert.match(styles, /<fgColor rgb="FFF5F5F5"\/>/, 'the gray header');
    const precise = unzipSync(toXlsx({ ...styled(), rows: styled().rows.map((r) => ({ ...r, cells: r.cells.map((v, c) => (c === 1 ? 66677024.29998732 : v)) })) }));
    assert.match(strFromU8(precise['xl/worksheets/sheet1.xml']!), /<c r="B2" s="\d+"><v>66677024\.29998732<\/v><\/c>/,
      'a Float of 16 digits is a number, not text');
    assert.equal(argb('#abc'), 'FFAABBCC');
    assert.equal(argb('red'), null);
  });
});

describe('the whole page: charts where the board puts them, and the whole grid', () => {
  const picture = { width: 400, height: 200, pixelWidth: 800, pixelHeight: 400,
    png: Uint8Array.from([0x89, 0x50, 0x4e, 0x47, 1, 2, 3]), jpeg: Uint8Array.from([0xff, 0xd8, 0xff, 4, 5, 6]) };
  const page = { cols: 12, tiles: [
    { id: 'grid', kind: 'grid' as const, title: 'Grid', x: 0, y: 0, w: 12, h: 14 },
    { id: 'chart-1', kind: 'chart' as const, title: 'By region', x: 0, y: 14, w: 6, h: 10, picture },
    { id: 'chart-2', kind: 'chart' as const, title: 'Not drawn', x: 6, y: 14, w: 6, h: 10 },
  ] };
  it('HTML lays the page out as the board: each tile at its place, a chart as its picture, the grid whole in its tile', () => {
    const html = toHtml(table(), { page });
    assert.match(html, /<section class="tile" style="grid-column:1 \/ span 6;grid-row:15 \/ span 10">\n<h2>By region<\/h2>\n<img alt="By region" width="400" height="200" src="data:image\/png;base64,iVBORwECAw=="/);
    assert.match(html, /<h2>Grid<\/h2>\n<table>/, 'the table inside the grid\'s tile');
    assert.match(html, /\(this chart had not drawn\)/);
  });
  it('the workbook: a Dashboard sheet first, the chart as a picture, the table on its own sheet', () => {
    const files = unzipSync(toXlsx(table('Trades'), { page }));
    assert.match(strFromU8(files['xl/workbook.xml']!), /<sheet name="Dashboard" sheetId="1" r:id="rId4"\/><sheet name="Trades"/);
    assert.deepEqual([...files['xl/media/image1.png']!], [...picture.png]);
    const drawing = strFromU8(files['xl/drawings/drawing1.xml']!);
    xml(drawing);
    assert.match(drawing, /<xdr:from><xdr:col>0<\/xdr:col><xdr:colOff>0<\/xdr:colOff><xdr:row>29<\/xdr:row>/, 'below its title, at the board\'s place');
    const dash = strFromU8(files['xl/worksheets/sheet3.xml']!);
    const sheet1 = strFromU8(files['xl/worksheets/sheet1.xml']!);
    // THE TABLE ITSELF in the grid's tile, one row down (under its title), styled as on its own sheet
    assert.match(dash, /<c r="A2" s="(\d+)" t="inlineStr"><is><t xml:space="preserve">region<\/t>/);
    const cell = (sheet: string, ref: string): string | undefined => new RegExp(`<c r="${ref}"[^>]*>(?:<v>[^<]*</v>|<is>.*?</is>)?`).exec(sheet)?.[0];
    assert.equal(cell(dash, 'H3')?.replace('H3', 'X'), cell(sheet1, 'H2')?.replace('H2', 'X'), 'the same cell, the same style');
    for (const part of ['xl/worksheets/sheet3.xml', 'xl/drawings/_rels/drawing1.xml.rels', '[Content_Types].xml']) xml(strFromU8(files[part]!));
  });
  it('the workbook: a table longer than its tile moves what is below it down; one wider moves what is beside it right', () => {
    const long = table('Trades');
    const rows = Array.from({ length: 60 }, () => long.rows[0]!);
    const tall = { ...long, rows };
    const beside = { cols: 12, tiles: [
      { id: 'grid', kind: 'grid' as const, title: 'Grid', x: 0, y: 0, w: 2, h: 14 },
      { id: 'side', kind: 'chart' as const, title: 'Beside', x: 2, y: 0, w: 6, h: 10, picture },
      { id: 'under', kind: 'chart' as const, title: 'Under', x: 0, y: 14, w: 6, h: 10, picture },
    ] };
    const files = unzipSync(toXlsx(tall, { page: beside }));
    const drawing = strFromU8(files['xl/drawings/drawing1.xml']!);
    const froms = [...drawing.matchAll(/<xdr:col>(\d+)<\/xdr:col><xdr:colOff>\d+<\/xdr:colOff><xdr:row>(\d+)<\/xdr:row>/g)].map((m) => [Number(m[1]), Number(m[2])] as const);
    assert.ok(froms[0]![0] >= 8, `beside the table, past its ${long.columns.length} columns (at column ${froms[0]![0]})`);
    assert.equal(froms[0]![1], 1, 'beside it: on the board\'s row');
    assert.equal(froms[1]![0], 0, "under it: on the board's column");
    assert.ok(froms[1]![1] > 1 + 1 + 60, `under it: past the table's last row (at row ${froms[1]![1]})`);
  });
  it('no charts, no dashboard: the workbook is the grid and its About sheet', () => {
    const files = unzipSync(toXlsx(table('Trades')));
    assert.equal(files['xl/drawings/drawing1.xml'], undefined);
  });
});
