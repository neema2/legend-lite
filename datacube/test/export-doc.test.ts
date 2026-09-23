// Plain text and PDF export.
//
// The PDF tests assert STRUCTURE rather than appearance, because a
// PDF that looks right in one reader and is rejected by another is
// the failure mode that matters, and it is always structural: a byte
// offset in the xref table, an object count, an unescaped character
// ending a string early. Those are checkable; "does it look nice" is
// not, and pretending otherwise would be a green check over nothing.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { escapePdfText, toPdf, toPlainText } from '../src/export-doc.ts';
import type { ResultTable } from '../src/result.ts';

function table(rows: number): ResultTable {
  const region: (string | null)[] = [];
  const amount: (number | null)[] = [];
  for (let i = 0; i < rows; i++) {
    region.push(`Region ${i}`);
    amount.push(i % 3 === 0 ? null : i * 1000.5);
  }
  return {
    columns: [
      { name: 'region', type: 'String', values: region },
      { name: 'amount', type: 'Float', values: amount },
    ],
    rowCount: rows,
    epoch: 1,
    elapsedMs: 0,
  };
}

describe('plain text export', () => {
  it('lines the columns up in a fixed width', () => {
    const out = toPlainText(table(3));
    const lines = out.trimEnd().split('\n');
    // Header, rule, three rows.
    assert.equal(lines.length, 5);

    // Every row's SECOND column ends at the same offset. Measured
    // from the right, because a row whose last cell is blank has its
    // padding trimmed -- which is correct, and which makes measuring
    // from the left meaningless for that row.
    const ends = lines
      .filter((l) => !/^-+/.test(l) && /\S\s\s+\S/.test(l))
      .map((l) => l.length);
    assert.equal(new Set(ends).size, 1, lines.join('\n'));
  });

  it('right-aligns numbers and left-aligns text', () => {
    const t: ResultTable = {
      columns: [
        { name: 'k', type: 'String', values: ['a', 'bbbb'] },
        { name: 'n', type: 'Float', values: [1, 1000] },
      ],
      rowCount: 2,
      epoch: 1,
      elapsedMs: 0,
    };
    const lines = toPlainText(t).trimEnd().split('\n');
    const first = lines[2] ?? '';
    const second = lines[3] ?? '';
    // '1' is padded left to sit under the last digit of '1000'.
    assert.match(first, /a\s+1$/);
    assert.match(second, /bbbb\s+1000$/);
  });

  it('leaves no trailing whitespace on a row', () => {
    // Invisible on screen, and it survives a paste looking like
    // corruption.
    for (const line of toPlainText(table(4)).split('\n')) {
      assert.equal(line, line.replace(/\s+$/, ''), JSON.stringify(line));
    }
  });

  it('renders an empty table without throwing', () => {
    const empty: ResultTable = {
      columns: [{ name: 'a', type: 'String', values: [] }],
      rowCount: 0,
      epoch: 1,
      elapsedMs: 0,
    };
    assert.match(toPlainText(empty), /^a\n-\n$/);
  });

  it('includes a title when given one', () => {
    assert.match(toPlainText(table(1), { title: 'Q3' }), /^Q3\n\n/);
  });
});

describe('PDF escaping', () => {
  it('escapes the characters that end a string object', () => {
    assert.equal(escapePdfText('a(b)c'), 'a\\(b\\)c');
    assert.equal(escapePdfText('a\\b'), 'a\\\\b');
  });

  it('flattens newlines rather than emitting them raw', () => {
    // A raw newline inside a literal would terminate it and shift
    // every byte offset in the xref after it.
    assert.equal(escapePdfText('a\nb'), 'a b');
    assert.equal(escapePdfText('a\r\nb'), 'a  b');
  });

  it('replaces characters a base-14 font cannot draw', () => {
    assert.equal(escapePdfText('a🙂b'), 'a??b');
  });
});

describe('PDF structure', () => {
  const parse = (pdf: string) => ({
    objects: [...pdf.matchAll(/^(\d+) 0 obj$/gm)].map((m) => Number(m[1])),
    startxref: Number(/startxref\n(\d+)/.exec(pdf)?.[1] ?? -1),
    size: Number(/\/Size (\d+)/.exec(pdf)?.[1] ?? -1),
    pages: Number(/\/Count (\d+)/.exec(pdf)?.[1] ?? -1),
  });

  it('writes a well-formed header and trailer', () => {
    const pdf = toPdf(table(3));
    assert.match(pdf, /^%PDF-1\.4\n/);
    assert.match(pdf, /%%EOF\n$/);
    assert.match(pdf, /\/Type \/Catalog/);
  });

  it('numbers every object once, in order', () => {
    const { objects } = parse(toPdf(table(3)));
    assert.deepEqual(objects, [...objects].sort((a, b) => a - b));
    assert.equal(new Set(objects).size, objects.length);
    assert.equal(objects[0], 1);
  });

  it('declares a Size that matches the objects written', () => {
    const pdf = toPdf(table(3));
    const { objects, size } = parse(pdf);
    assert.equal(size, objects.length + 1, 'Size counts the free object');
  });

  it('points startxref at the actual xref table', () => {
    // The failure this catches produces a file every reader rejects,
    // with no indication of where the problem is.
    const pdf = toPdf(table(3));
    const { startxref } = parse(pdf);
    assert.ok(startxref > 0);
    assert.equal(pdf.slice(startxref, startxref + 4), 'xref');
  });

  it('gives every xref offset the byte where its object begins', () => {
    const pdf = toPdf(table(5));
    const { startxref } = parse(pdf);
    const lines = pdf.slice(startxref).split('\n');
    // lines[0] 'xref', lines[1] '0 N', lines[2] the free entry.
    const entries = lines.slice(3).filter((l) => /^\d{10} \d{5} n/.test(l));
    assert.ok(entries.length > 0, 'there are xref entries at all');
    entries.forEach((entry, i) => {
      const offset = Number(entry.slice(0, 10));
      assert.equal(
        pdf.slice(offset, offset + `${i + 1} 0 obj`.length),
        `${i + 1} 0 obj`,
        `entry ${i} points at the wrong byte`,
      );
    });
  });

  it('declares a stream Length matching its content', () => {
    // A wrong Length truncates the page or runs past its end.
    const pdf = toPdf(table(4));
    for (const m of pdf.matchAll(/<< \/Length (\d+) >>\nstream\n/g)) {
      const declared = Number(m[1]);
      const start = m.index + m[0].length;
      const end = pdf.indexOf('\nendstream', start);
      assert.equal(end - start, declared, 'stream length disagrees');
    }
  });

  it('paginates a long table and repeats the header', () => {
    const pdf = toPdf(table(300));
    const { pages } = parse(pdf);
    assert.ok(pages > 1, `expected several pages, got ${pages}`);
    // One /Type /Page object per page.
    const pageObjects = [...pdf.matchAll(/\/Type \/Page[^s]/g)].length;
    assert.equal(pageObjects, pages);
    // The heading appears once per page.
    const headings = [...pdf.matchAll(/\(region\) Tj/g)].length;
    assert.equal(headings, pages, 'the header repeats on every page');
  });

  it('produces a valid file for an empty table', () => {
    const empty: ResultTable = {
      columns: [{ name: 'a', type: 'String', values: [] }],
      rowCount: 0,
      epoch: 1,
      elapsedMs: 0,
    };
    const pdf = toPdf(empty);
    const { startxref, pages } = parse(pdf);
    assert.equal(pages, 1, 'an empty result is still one page');
    assert.equal(pdf.slice(startxref, startxref + 4), 'xref');
  });

  it('does not let a cell containing a bracket corrupt the file', () => {
    const t: ResultTable = {
      columns: [
        { name: 'k', type: 'String', values: ['a)b(c\\d', 'plain'] },
      ],
      rowCount: 2,
      epoch: 1,
      elapsedMs: 0,
    };
    const pdf = toPdf(t);
    const { startxref } = parse(pdf);
    assert.equal(pdf.slice(startxref, startxref + 4), 'xref');
    assert.match(pdf, /\(a\\\)b\\\(c\\\\d\) Tj/);
  });
});
