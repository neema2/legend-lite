// Plain text and PDF: the two DataCube export formats this did not have.
//
// PLAIN TEXT is a fixed-width table, not another delimited file. CSV
// already covers "machine reads it next"; plain text exists for the
// other case -- pasting a small result into a chat message, a ticket
// or an email body -- where what matters is that the columns line up
// in a monospaced font. So it pads to the widest cell, right-aligns
// numbers the way a reader expects, and draws one rule under the
// header.
//
// PDF is written by hand, and the reason is worth stating because the
// obvious objection is "use a library". A PDF of a table is a very
// small subset of the format: a catalogue, a page tree, one font
// resource, and a content stream of positioned text. That is a few
// hundred lines with no dependency, it runs identically in a browser
// and in node, and -- the part that decided it -- it is TESTABLE:
// the output is inspectable text, so the tests can assert the
// structure rather than trusting a black box to have drawn something.
//
// What this PDF deliberately is NOT: a layout engine. One font
// (Helvetica, a PDF base-14 face so nothing is embedded), no wrapping
// -- long cells are truncated with an ellipsis -- and a fixed page
// size. A grid export wants the numbers on a page, and anything more
// ambitious is a rendering pipeline out of proportion to that.

import type { ColumnFormat, FormatterCache } from './format.ts';
import type { ResultTable, Scalar } from './result.ts';

export interface DocExportOptions {
  readonly title?: string;
  readonly formatters?: FormatterCache;
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** Restrict and order columns; defaults to all, in table order. */
  readonly columns?: readonly string[];
  /** Header labels, where they differ from the column names. */
  readonly labels?: Readonly<Record<string, string>>;
  /** Cap on a rendered cell before it is truncated. */
  readonly maxCellWidth?: number;
}

interface Cell {
  readonly text: string;
  readonly numeric: boolean;
}

function chosen(table: ResultTable, options: DocExportOptions) {
  return options.columns
    ? options.columns
        .map((n) => table.columns.find((c) => c.name === n))
        .filter((c): c is NonNullable<typeof c> => c !== undefined)
    : table.columns;
}

/**
 * The grid as text, once, for both exporters.
 *
 * Shared so the PDF and the plain text can never disagree about what
 * a cell says -- the formatter, the label and the truncation are
 * decided here and both renderers lay out the same strings.
 */
function grid(
  table: ResultTable,
  options: DocExportOptions,
): { header: Cell[]; rows: Cell[][] } {
  const cols = chosen(table, options);
  const cap = options.maxCellWidth ?? 60;
  const clip = (s: string): string =>
    s.length <= cap ? s : `${s.slice(0, Math.max(1, cap - 1))}…`;

  const header: Cell[] = cols.map((c) => ({
    text: clip(options.labels?.[c.name] ?? c.name),
    numeric: false,
  }));

  const rows: Cell[][] = [];
  for (let r = 0; r < table.rowCount; r++) {
    rows.push(
      cols.map((c) => {
        const v: Scalar = c.values[r] ?? null;
        const text = options.formatters
          ? options.formatters.format(v, options.formats?.[c.name])
          : v === null
            ? ''
            : String(v);
        return { text: clip(text), numeric: typeof v === 'number' };
      }),
    );
  }
  return { header, rows };
}

/** Widest rendered cell per column, header included. */
function widths(header: Cell[], rows: Cell[][]): number[] {
  const w = header.map((h) => h.text.length);
  for (const row of rows) {
    row.forEach((cell, i) => {
      w[i] = Math.max(w[i] ?? 0, cell.text.length);
    });
  }
  return w;
}

/**
 * A fixed-width text table.
 *
 * Numbers are right-aligned and text left-aligned, which is the same
 * rule the grid uses on screen -- a column of figures that does not
 * line up on its last digit is unreadable, and that is the entire
 * reason to choose this format over CSV.
 */
export function toPlainText(
  table: ResultTable,
  options: DocExportOptions = {},
): string {
  const { header, rows } = grid(table, options);
  if (header.length === 0) return '';
  const w = widths(header, rows);

  const line = (cells: Cell[]): string =>
    cells
      .map((cell, i) => {
        const width = w[i] ?? 0;
        return cell.numeric
          ? cell.text.padStart(width)
          : cell.text.padEnd(width);
      })
      .join('  ')
      // A row of left-aligned cells ends in padding nobody can see;
      // trailing spaces survive a paste and look like corruption.
      .replace(/\s+$/, '');

  const out: string[] = [];
  if (options.title) out.push(options.title, '');
  out.push(line(header));
  out.push(w.map((n) => '-'.repeat(n)).join('  '));
  for (const row of rows) out.push(line(row));
  return `${out.join('\n')}\n`;
}

// -- PDF ---------------------------------------------------------------

/** Points. A4 portrait, which is the less surprising default outside the US. */
const PAGE_WIDTH = 595;
const PAGE_HEIGHT = 842;
const MARGIN = 36;
const FONT_SIZE = 9;
const HEADER_SIZE = 10;
const LINE_HEIGHT = 13;
const TITLE_SIZE = 14;

/**
 * Width of a string in Helvetica at size 1, approximated.
 *
 * The real metrics are a per-glyph table; this is the average
 * advance, which is enough to decide column positions for a table.
 * Getting it slightly wrong makes columns a little wide, never
 * overlapping -- the failure direction that still reads.
 */
function textWidth(s: string, size: number): number {
  return s.length * size * 0.52;
}

/** Escape a string for a PDF literal string object. */
export function escapePdfText(s: string): string {
  return s
    .replace(/\\/g, '\\\\')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)')
    // A PDF literal cannot contain a raw newline or control byte, and
    // a cell CAN -- a value with a line break would otherwise end the
    // string object and corrupt every byte offset after it.
    .replace(/[\r\n\t]/g, ' ')
    // WinAnsi is a single-byte encoding; anything outside it has no
    // glyph in a base-14 font, so it is replaced rather than emitted
    // as a byte the reader will misdraw.
    .replace(/[^\x20-\x7E\xA0-\xFF]/g, '?');
}

interface Placed {
  readonly x: number;
  readonly text: string;
}

/**
 * The grid as a PDF document.
 *
 * Pages are filled top to bottom and the header row is repeated on
 * each, because a table whose headings appear only on page one is not
 * readable on page four.
 */
export function toPdf(
  table: ResultTable,
  options: DocExportOptions = {},
): string {
  const { header, rows } = grid(table, options);
  const w = widths(header, rows);

  // Column x positions from the widest cell, scaled to fit the page.
  const gap = 8;
  const natural = w.map((n) => textWidth('M'.repeat(n), FONT_SIZE));
  const total = natural.reduce((a, b) => a + b, 0) + gap * (w.length - 1 || 0);
  const available = PAGE_WIDTH - MARGIN * 2;
  const scale = total > available && total > 0 ? available / total : 1;
  const colWidth = natural.map((n) => n * scale);
  const xs: number[] = [];
  let x = MARGIN;
  for (const cw of colWidth) {
    xs.push(x);
    x += cw + gap;
  }

  const place = (cells: Cell[]): Placed[] =>
    cells.map((cell, i) => {
      const left = xs[i] ?? MARGIN;
      const width = colWidth[i] ?? 0;
      // Right-aligned numbers are positioned from the column's right
      // edge, the same rule as the text export.
      const offset = cell.numeric
        ? Math.max(0, width - textWidth(cell.text, FONT_SIZE))
        : 0;
      return { x: left + offset, text: cell.text };
    });

  const titleHeight = options.title ? TITLE_SIZE + LINE_HEIGHT : 0;
  const firstTop = PAGE_HEIGHT - MARGIN - titleHeight;
  const perPage = Math.max(
    1,
    Math.floor((firstTop - MARGIN - LINE_HEIGHT * 2) / LINE_HEIGHT),
  );

  const pages: string[] = [];
  for (let start = 0; start < Math.max(rows.length, 1); start += perPage) {
    const slice = rows.slice(start, start + perPage);
    const parts: string[] = [];
    let y = PAGE_HEIGHT - MARGIN;

    if (options.title && start === 0) {
      parts.push(
        `BT /F2 ${TITLE_SIZE} Tf 1 0 0 1 ${MARGIN} ${y} Tm`
        + ` (${escapePdfText(options.title)}) Tj ET`,
      );
      y -= TITLE_SIZE + LINE_HEIGHT;
    }

    for (const cell of place(header)) {
      parts.push(
        `BT /F2 ${HEADER_SIZE} Tf 1 0 0 1 ${cell.x.toFixed(2)} ${y.toFixed(2)} Tm`
        + ` (${escapePdfText(cell.text)}) Tj ET`,
      );
    }
    // The rule under the header.
    parts.push(
      `0.5 w ${MARGIN} ${(y - 4).toFixed(2)} m`
      + ` ${(PAGE_WIDTH - MARGIN).toFixed(2)} ${(y - 4).toFixed(2)} l S`,
    );
    y -= LINE_HEIGHT + 2;

    for (const row of slice) {
      for (const cell of place(row)) {
        parts.push(
          `BT /F1 ${FONT_SIZE} Tf 1 0 0 1 ${cell.x.toFixed(2)} ${y.toFixed(2)} Tm`
          + ` (${escapePdfText(cell.text)}) Tj ET`,
        );
      }
      y -= LINE_HEIGHT;
    }
    pages.push(parts.join('\n'));
  }

  return assemble(pages);
}

/**
 * Wrap content streams into a PDF file.
 *
 * The xref table is byte offsets into the file, so the objects have
 * to be serialised before the table can be written -- which is why
 * this measures as it goes rather than templating the whole thing.
 * An xref that is wrong by one byte produces a file every reader
 * rejects, with no clue as to where.
 */
function assemble(pages: readonly string[]): string {
  const count = Math.max(pages.length, 1);
  const streams = pages.length > 0 ? pages : [''];

  // 1 catalogue, 2 page tree, 3 + 4 fonts, then per page: a page
  // object and its content stream.
  const kids = streams
    .map((_, i) => `${5 + i * 2} 0 R`)
    .join(' ');

  const objects: string[] = [
    '<< /Type /Catalog /Pages 2 0 R >>',
    `<< /Type /Pages /Count ${count} /Kids [${kids}] >>`,
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica'
    + ' /Encoding /WinAnsiEncoding >>',
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold'
    + ' /Encoding /WinAnsiEncoding >>',
  ];

  streams.forEach((content, i) => {
    objects.push(
      `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${PAGE_WIDTH} ${PAGE_HEIGHT}]`
      + ` /Resources << /Font << /F1 3 0 R /F2 4 0 R >> >>`
      + ` /Contents ${6 + i * 2} 0 R >>`,
    );
    objects.push(
      `<< /Length ${content.length} >>\nstream\n${content}\nendstream`,
    );
  });

  let out = '%PDF-1.4\n';
  const offsets: number[] = [];
  objects.forEach((body, i) => {
    offsets.push(out.length);
    out += `${i + 1} 0 obj\n${body}\nendobj\n`;
  });

  const xref = out.length;
  out += `xref\n0 ${objects.length + 1}\n`;
  out += '0000000000 65535 f \n';
  for (const off of offsets) {
    out += `${String(off).padStart(10, '0')} 00000 n \n`;
  }
  out += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\n`;
  out += `startxref\n${xref}\n%%EOF\n`;
  return out;
}
