import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { escapeXml, toHtml, toSpreadsheetML } from '../src/export-rich.ts';
import { FormatterCache } from '../src/format.ts';
import type { ResultTable } from '../src/result.ts';

const TABLE: ResultTable = {
  columns: [
    { name: 'region', type: 'String', values: ['EMEA', 'A & B', null] },
    { name: 'total', type: 'Float', values: [1234.5, -42, null] },
  ],
  rowCount: 3,
  epoch: 1,
  elapsedMs: 0,
};

describe('escapeXml', () => {
  it('escapes everything that can break a document', () => {
    assert.equal(
      escapeXml(`<a href="x">A & B's</a>`),
      '&lt;a href=&quot;x&quot;&gt;A &amp; B&#39;s&lt;/a&gt;',
    );
  });

  it('escapes the ampersand first, not last', () => {
    // Escaping & after < would double-escape the &lt; it just made.
    assert.equal(escapeXml('<'), '&lt;');
  });
});

describe('toHtml', () => {
  it('produces a standalone document with inline styles', () => {
    // It is opened from disk or pasted into an email, where no
    // stylesheet follows it.
    const html = toHtml(TABLE, { title: 'P&L' });
    assert.match(html, /^<!doctype html>/);
    assert.match(html, /<style>/);
    assert.match(html, /<title>P&amp;L<\/title>/);
  });

  it('escapes data, not just the title', () => {
    assert.match(toHtml(TABLE), /<td>A &amp; B<\/td>/);
  });

  it('right-aligns numbers per CELL, not per column', () => {
    // A mixed column stays readable that way.
    const html = toHtml(TABLE);
    assert.match(html, /<td class="n">1234\.5<\/td>/);
    assert.match(html, /<td>EMEA<\/td>/);
  });

  it('renders through the formatter when given one', () => {
    const html = toHtml(TABLE, {
      formatters: new FormatterCache(),
      formats: { total: { kind: 'currency', currency: 'USD', locale: 'en-US' } },
    });
    assert.match(html, /\$1,234\.50/);
  });

  it('uses display labels for headers', () => {
    assert.match(
      toHtml(TABLE, { labels: { total: 'Total (USD)' } }),
      /<th scope="col">Total \(USD\)<\/th>/,
    );
  });

  it('renders null as empty rather than the word null', () => {
    assert.match(toHtml(TABLE), /<td><\/td>/);
  });
});

describe('toSpreadsheetML', () => {
  it('writes numbers as numbers, with their RAW value', () => {
    // This is the whole reason Excel export is not just CSV: a
    // spreadsheet must be able to sum the column.
    const xml = toSpreadsheetML(TABLE, {
      formatters: new FormatterCache(),
      formats: { total: { kind: 'currency', currency: 'USD', locale: 'en-US' } },
    });
    assert.match(xml, /<Data ss:Type="Number">1234\.5<\/Data>/);
    assert.equal(
      xml.includes('$1,234.50'),
      false,
      'the formatted text must not replace the number',
    );
  });

  it('writes labels as formatted strings', () => {
    const xml = toSpreadsheetML(TABLE);
    assert.match(xml, /<Data ss:Type="String">EMEA<\/Data>/);
  });

  it('announces itself to Excel', () => {
    const xml = toSpreadsheetML(TABLE);
    assert.match(xml, /<\?mso-application progid="Excel\.Sheet"\?>/);
    assert.match(xml, /urn:schemas-microsoft-com:office:spreadsheet/);
  });

  it('emits an empty cell for null, not a zero', () => {
    // A null summed as zero is a wrong total.
    assert.match(toSpreadsheetML(TABLE), /<Cell\/>/);
  });

  it('escapes data in cells', () => {
    assert.match(toSpreadsheetML(TABLE), /A &amp; B/);
  });

  it('truncates the sheet name to what Excel allows', () => {
    const xml = toSpreadsheetML(TABLE, { title: 'x'.repeat(60) });
    const name = /ss:Name="([^"]*)"/.exec(xml)?.[1] ?? '';
    assert.equal(name.length, 31);
  });

  it('writes dates as dates', () => {
    const dated: ResultTable = {
      columns: [
        {
          name: 'd',
          type: 'Date',
          values: [new Date('2026-03-01T00:00:00Z')],
        },
      ],
      rowCount: 1,
      epoch: 1,
      elapsedMs: 0,
    };
    assert.match(
      toSpreadsheetML(dated),
      /<Data ss:Type="DateTime">2026-03-01T00:00:00<\/Data>/,
    );
  });
});
