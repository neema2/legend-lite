// Plain text and PDF, from the export model (export-model.ts): the grid as shown.
//
// PLAIN TEXT is a fixed-width table, not another delimited file. CSV covers "a machine reads
// it next"; plain text is for pasting a result into a message or a ticket, where what matters
// is that the columns line up in a monospaced font. It pads to the widest cell, right-aligns
// numbers, indents the tree by depth and draws one rule under the header.
//
// PDF is written by hand, and it is BYTES. A PDF's `/Length` and its xref table are byte
// counts into the file; the first version measured JavaScript characters and wrote UTF-8, so
// any é or £ made every offset after it wrong and a strict reader refused the file (the
// 2026-09-29 audit). The text is encoded as WinAnsi (Windows-1252), the encoding the base-14
// fonts carry, so € — … and every Latin-1 letter draw as themselves; a character outside it
// cannot be drawn by a base-14 font at all, and is named in a note on the page rather than
// silently becoming "?". (Embedding a Unicode font is the plan's next step for this format.)
//
// Layout: A4, portrait when the table fits and landscape when it does not; the font scales
// down to a floor, and past it the columns continue on further pages with the first column
// repeated, so nothing overlaps. The header repeats on every page; the title and the notes
// head the first.

import { UI_LOCALE } from '../../engine-client/src/locale.ts';
import type { ColumnFormat, FormatterCache } from './format.ts';
import { cased, flatHeader, type ExportPage, type ExportStyle, type ExportTable } from './export-model.ts';
import type { Scalar } from '../../engine-client/src/result.ts';
import { fontStack } from './style.ts';
import { isNumeric } from '../../engine-client/src/types.ts';

export interface DocExportOptions {
  readonly formatters?: FormatterCache;
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** Cap on a rendered cell before it is truncated. */
  readonly maxCellWidth?: number;
  /** The board, when it holds charts: the file is one page, laid out as the board is. */
  readonly page?: ExportPage;
}

interface Cell {
  readonly text: string;
  readonly numeric: boolean;
  readonly bold: boolean;
}

/** The table as text, once, so the PDF and the plain text can never disagree about a cell. */
function grid(table: ExportTable, options: DocExportOptions): { header: Cell[]; rows: Cell[][] } {
  const cap = options.maxCellWidth ?? 60;
  const clip = (s: string): string => (s.length <= cap ? s : `${s.slice(0, Math.max(1, cap - 1))}…`);
  const text = (v: Scalar, name: string, type: string | undefined): string =>
    options.formatters ? options.formatters.format(v, options.formats?.[name], type)
      : v === null ? '' : String(v);

  const header: Cell[] = table.columns.map((c) => ({ text: clip(flatHeader(c)), numeric: false, bold: false }));
  const rows = table.rows.map((row) => table.columns.map((c, i): Cell => {
    const v = row.cells[i] ?? null;
    const shown = c.redacted ? String(v) : text(v, c.name, c.type);
    // the tree reads as a tree: two spaces a level
    const indented = c.tree ? `${'  '.repeat(Math.max(0, row.depth - 1))}${shown}` : shown;
    return {
      text: clip(indented),
      numeric: !c.redacted && !c.tree && v !== null && isNumeric(c.type),
      // weight comes from the cell's look (the grid bolds totals, and what the user set)
      bold: false,
    };
  }));
  return { header, rows };
}

function widths(header: Cell[], rows: Cell[][]): number[] {
  const w = header.map((h) => [...h.text].length);
  for (const row of rows) row.forEach((cell, i) => { w[i] = Math.max(w[i] ?? 0, [...cell.text].length); });
  return w;
}

/**
 * A fixed-width text table: numbers right-aligned on their last digit, text left, the tree
 * indented, the title and any notes above it.
 */
export function toPlainText(table: ExportTable, options: DocExportOptions = {}): string {
  const { header, rows } = grid(table, options);
  if (header.length === 0) return '';
  const w = widths(header, rows);
  const pad = (cell: Cell, width: number): string => {
    const fill = ' '.repeat(Math.max(0, width - [...cell.text].length));
    return cell.numeric ? fill + cell.text : cell.text + fill;
  };
  const line = (cells: Cell[]): string =>
    cells.map((cell, i) => pad(cell, w[i] ?? 0)).join('  ')
      // trailing spaces survive a paste and look like corruption
      .replace(/\s+$/, '');
  const out: string[] = [];
  if (table.title) out.push(table.title, '');
  for (const note of table.notes) out.push(note);
  if (table.notes.length > 0) out.push('');
  out.push(line(header));
  out.push(w.map((n) => '-'.repeat(n)).join('  '));
  for (const row of rows) out.push(line(row));
  return `${out.join('\n')}\n`;
}

// -- PDF ---------------------------------------------------------------------------------

/** Points: A4. */
const A4_SHORT = 595;
const A4_LONG = 842;
const MARGIN = 36;
/** The dashboard's margin: a quarter inch, inside what office printers can print. */
const DASH_MARGIN = 18;
const FONT_MAX = 9;
const FONT_MIN = 6;
const TITLE_SIZE = 14;

/** Windows-1252's 0x80-0x9F: the characters WinAnsi places where Latin-1 has controls. */
const CP1252_HIGH: Readonly<Record<string, number>> = {
  '€': 0x80, '‚': 0x82, 'ƒ': 0x83, '„': 0x84, '…': 0x85, '†': 0x86, '‡': 0x87, 'ˆ': 0x88,
  '‰': 0x89, 'Š': 0x8a, '‹': 0x8b, 'Œ': 0x8c, 'Ž': 0x8e, '‘': 0x91, '’': 0x92, '“': 0x93,
  '”': 0x94, '•': 0x95, '–': 0x96, '—': 0x97, '˜': 0x98, '™': 0x99, 'š': 0x9a, '›': 0x9b,
  'œ': 0x9c, 'ž': 0x9e, 'Ÿ': 0x9f,
};

/** The WinAnsi byte for a character, or undefined when a base-14 font has no glyph for it. */
export function winAnsiByte(ch: string): number | undefined {
  const code = ch.codePointAt(0) ?? 0;
  if (code >= 0x20 && code <= 0x7e) return code;
  if (code >= 0xa0 && code <= 0xff) return code;
  return CP1252_HIGH[ch];
}

/** A PDF literal string's body, as bytes: escaped, WinAnsi-encoded; `missing` collects what cannot be drawn. */
function pdfString(s: string, missing: Set<string>): number[] {
  const out: number[] = [];
  for (const ch of s.replace(/[\r\n\t]/g, ' ')) {
    if (ch === '\\' || ch === '(' || ch === ')') {
      out.push(0x5c, ch.charCodeAt(0));
      continue;
    }
    const b = winAnsiByte(ch);
    if (b === undefined) {
      missing.add(ch);
      out.push(0x3f); // '?', and the note says which characters
    } else {
      out.push(b);
    }
  }
  return out;
}

/** The PDF base-14 family nearest a font stack: serif, monospace, or sans. */
type Family = 'sans' | 'serif' | 'mono';
function familyOf(fontFamily: string | undefined): Family {
  const stack = fontStack(fontFamily ?? 'Roboto').toLowerCase();
  if (/monospace|courier|consolas|menlo|mono/.test(stack)) return 'mono';
  if (/(^|[^-])serif|times|georgia|garamond/.test(stack)) return 'serif';
  return 'sans';
}

/** Font resource names: family x bold x italic, twelve base-14 faces. */
const FACES: Readonly<Record<Family, readonly [string, string, string, string]>> = {
  sans: ['Helvetica', 'Helvetica-Bold', 'Helvetica-Oblique', 'Helvetica-BoldOblique'],
  serif: ['Times-Roman', 'Times-Bold', 'Times-Italic', 'Times-BoldItalic'],
  mono: ['Courier', 'Courier-Bold', 'Courier-Oblique', 'Courier-BoldOblique'],
};
const FAMILIES: readonly Family[] = ['sans', 'serif', 'mono'];
/** `/F<n>`: 1-12, in FAMILIES x (regular, bold, italic, bold italic) order. */
function fontRef(family: Family, bold: boolean, italic: boolean): string {
  return `F${FAMILIES.indexOf(family) * 4 + (bold ? 1 : 0) + (italic ? 2 : 0) + 1}`;
}

/** Average advance at size 1, per family: enough to place a table's columns without overlap. */
function textWidth(chars: number, size: number, family: Family = 'sans', bold = false): number {
  const em = family === 'mono' ? 0.6 : family === 'serif' ? 0.5 : bold ? 0.58 : 0.55;
  return chars * size * em;
}

/** A colour as PDF operands 'r g b' in 0..1, or null. */
function rgb(colour: string | undefined): string | null {
  const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(colour?.trim() ?? '');
  if (!m) return null;
  const hex = m[1]!.length === 3 ? [...m[1]!].map((c) => c + c).join('') : m[1]!;
  return [0, 2, 4].map((i) => (parseInt(hex.slice(i, i + 2), 16) / 255).toFixed(3)).join(' ');
}

/** A content stream built as bytes. */
class Stream {
  readonly #bytes: number[] = [];
  text(s: string): void {
    for (let i = 0; i < s.length; i++) this.#bytes.push(s.charCodeAt(i) & 0xff);
  }
  string(s: string, missing: Set<string>): void {
    this.#bytes.push(0x28, ...pdfString(s, missing), 0x29);
  }
  get bytes(): number[] {
    return this.#bytes;
  }
}

/** The table, prepared once for drawing: its cells as text, its column widths, its header levels. */
interface Prepared {
  readonly rows: Cell[][];
  readonly widths: number[];
  readonly levels: readonly (readonly { readonly label: string; readonly colStart: number; readonly colSpan: number; readonly rowSpan: number }[])[];
  readonly family: Family;
  /** The grid's font size, in points: the ceiling a table is drawn at. */
  readonly nominal: number;
}

const PAD = 3;

function prepare(table: ExportTable, options: DocExportOptions): Prepared {
  const { rows } = grid(table, options);
  const levels = table.headerRows.length > 0 ? table.headerRows : [table.columns.map((c, i) => ({
    label: flatHeader(c), colStart: i, colSpan: 1, rowSpan: 1,
  }))];
  // a column is as wide as its cells and its OWN header labels (a spanning label is shared)
  const widths_ = widths(table.columns.map((c) => ({ text: c.path[c.path.length - 1] ?? '', numeric: false, bold: false })), rows);
  const nominal = Math.max(FONT_MIN, Math.min(FONT_MAX + 3, table.look.fontSize * 0.75));
  return { rows, widths: widths_, levels, family: familyOf(table.look.fontFamily), nominal };
}

/** The width `cols` take at `size`. */
function needWidth(p: Prepared, cols: readonly number[], size: number): number {
  return cols.reduce((sum, i) => sum + textWidth(p.widths[i] ?? 0, size, p.family) + PAD * 2, 0);
}

/**
 * Draw the table -- its header levels, `count` rows from `start`, the grid's lines and a frame --
 * with its left edge at `left` and its top at `top`, at `size`. Returns the bottom it reached.
 * The one drawing of a table: the table's own pages and the grid's tile on the dashboard both
 * call it, so the two can never look different.
 */
function drawTable(s: Stream, missing: Set<string>, table: ExportTable, p: Prepared, block: {
  readonly cols: readonly number[]; readonly left: number; readonly top: number; readonly size: number;
  readonly start: number; readonly count: number;
}): number {
  const { cols, size } = block;
  const look = table.look;
  const scale = size / p.nominal;
  const lineHeight = size * 1.6;
  const textColour = rgb(look.color) ?? '0 0 0';
  const lineColour = rgb(look.lineColor) ?? '0.83 0.83 0.83';
  const xs: number[] = [];
  const cw: number[] = [];
  let x = block.left;
  for (const i of cols) {
    xs.push(x);
    const width = textWidth(p.widths[i] ?? 0, size, p.family) + PAD * 2;
    cw.push(width);
    x += width;
  }
  const right = x;
  /** One cell: its background box, its text where its alignment puts it, its decoration. */
  const put = (cell: Cell, style: ExportStyle | undefined, col: number, top: number): void => {
    const left = xs[col]!;
    const width = cw[col]!;
    const bg = rgb(style?.background);
    if (bg) s.text(`${bg} rg ${left.toFixed(2)} ${(top - lineHeight).toFixed(2)} ${width.toFixed(2)} ${lineHeight.toFixed(2)} re f\n`);
    const family = style?.fontFamily ? familyOf(style.fontFamily) : p.family;
    const bold = cell.bold || style?.bold === true;
    const italic = style?.italic === true;
    const px = style?.fontSize !== undefined ? Math.max(FONT_MIN, style.fontSize * 0.75 * scale) : size;
    const text = cased(cell.text, style?.fontCase);
    const tw = textWidth([...text].length, px, family, bold);
    const align = style?.align ?? 'left';
    const tx = align === 'right' ? left + width - PAD - tw : align === 'center' ? left + (width - tw) / 2 : left + PAD;
    const ty = top - lineHeight + (lineHeight - px) / 2 + px * 0.22;
    const colour = rgb(style?.color) ?? textColour;
    s.text(`${colour} rg BT /${fontRef(family, bold, italic)} ${px.toFixed(2)} Tf 1 0 0 1 ${Math.max(left + 1, tx).toFixed(2)} ${ty.toFixed(2)} Tm `);
    s.string(text, missing);
    s.text(' Tj ET\n');
    if (style?.underline || style?.strike) {
      const ly = style.strike ? ty + px * 0.3 : ty - px * 0.12;
      s.text(`${colour} RG 0.5 w ${tx.toFixed(2)} ${ly.toFixed(2)} m ${(tx + tw).toFixed(2)} ${ly.toFixed(2)} l S\n`);
    }
  };
  const headerTop = block.top;
  // THE GRID'S HEADER LEVELS, each cell over the columns it spans that are drawn here
  p.levels.forEach((level, depth) => {
    for (const cell of level) {
      const on = cols.map((leaf, pos) => [leaf, pos] as const)
        .filter(([leaf]) => leaf >= cell.colStart && leaf < cell.colStart + cell.colSpan).map(([, pos]) => pos);
      if (on.length === 0) continue;
      const first = on[0]!;
      const last = on[on.length - 1]!;
      const left = xs[first]!;
      const width = xs[last]! + cw[last]! - left;
      const top = headerTop - depth * lineHeight;
      const height = cell.rowSpan * lineHeight;
      s.text(`${rgb(look.headerBackground) ?? '0.96 0.96 0.96'} rg ${left.toFixed(2)} ${(top - height).toFixed(2)} ${width.toFixed(2)} ${height.toFixed(2)} re f\n`);
      s.text(`0.9 0.9 0.9 RG 0.5 w ${left.toFixed(2)} ${(top - height).toFixed(2)} ${width.toFixed(2)} ${height.toFixed(2)} re S\n`);
      const tw = textWidth([...cell.label].length, size, p.family);
      const tx = left + Math.max(1, (width - tw) / 2);
      const ty = top - height + (height - size) / 2 + size * 0.22;
      s.text(`${rgb(look.headerColor) ?? '0 0 0'} rg BT /${fontRef(p.family, false, false)} ${size.toFixed(2)} Tf 1 0 0 1 ${tx.toFixed(2)} ${ty.toFixed(2)} Tm `);
      s.string(cell.label, missing);
      s.text(' Tj ET\n');
    }
  });
  let y = headerTop - p.levels.length * lineHeight;
  const bodyTop = y;
  p.rows.slice(block.start, block.start + block.count).forEach((row, k) => {
    const styles = table.rows[block.start + k]?.styles;
    cols.forEach((i, col) => put(row[i]!, styles?.[i], col, y));
    if (look.horizontalLines) {
      s.text(`${lineColour} RG 0.5 w ${block.left.toFixed(2)} ${(y - lineHeight).toFixed(2)} m ${right.toFixed(2)} ${(y - lineHeight).toFixed(2)} l S\n`);
    }
    y -= lineHeight;
  });
  // the frame and the header rule; the grid's vertical lines when it shows them
  s.text(`0.9 0.9 0.9 RG 0.5 w ${block.left.toFixed(2)} ${y.toFixed(2)} ${(right - block.left).toFixed(2)} ${(headerTop - y).toFixed(2)} re S\n`);
  s.text(`0.9 0.9 0.9 RG 0.5 w ${block.left.toFixed(2)} ${bodyTop.toFixed(2)} m ${right.toFixed(2)} ${bodyTop.toFixed(2)} l S\n`);
  if (look.verticalLines) {
    for (let col = 1; col < cols.length; col++) {
      s.text(`${lineColour} RG 0.5 w ${xs[col]!.toFixed(2)} ${y.toFixed(2)} m ${xs[col]!.toFixed(2)} ${bodyTop.toFixed(2)} l S\n`);
    }
  }
  return y;
}

/**
 * The grid as a PDF file, as bytes, that LOOKS like the grid: each cell's background (bands,
 * totals, value colours, heatmaps), its text colour, weight, slant, alignment and decoration as
 * the grid resolves them (export-model.ts), the grid's lines, and a plain gray header.
 *
 * With charts on the board, the file is ONE page: the board as laid out, the table drawn in the
 * grid's tile (the rows that fit it, and a line saying so when that is not all of them).
 * Without charts, columns that fit the page at the font floor share a page; past it they continue on the next
 * page set, the first column repeated so every page is readable on its own.
 */
export function toPdf(table: ExportTable, options: DocExportOptions = {}): Uint8Array {
  const p = prepare(table, options);
  const missing = new Set<string>();
  const board = dashboard(table, options.page, p, missing);
  const all = p.widths.map((_, i) => i);

  // The grid's font size is the ceiling; portrait if it fits, else landscape, scaled down to a floor.
  const portraitRoom = A4_SHORT - MARGIN * 2;
  const landscape = needWidth(p, all, p.nominal) > portraitRoom;
  const pageWidth = landscape ? A4_LONG : A4_SHORT;
  const pageHeight = landscape ? A4_SHORT : A4_LONG;
  const room = pageWidth - MARGIN * 2;
  const natural = needWidth(p, all, p.nominal);
  const size = Math.max(FONT_MIN, natural > room ? p.nominal * room / natural : p.nominal);

  const pages: PdfPage[] = [];
  // with charts, the dashboard IS the file: the board as laid out, on one page
  if (board.pages.length === 0) {
    const groups: number[][] = [];
    let current: number[] = [];
    for (const i of all) {
      const trial = [...current, i];
      if (current.length > 0 && needWidth(p, trial, size) > room) {
        groups.push(current);
        current = all.length > 1 ? [0, i] : [i];
      } else {
        current = trial;
      }
    }
    if (current.length > 0) groups.push(current);

    const lineHeight = size * 1.6;
    const notes = board.pages.length > 0 ? [] : [...table.notes];
    const textColour = rgb(table.look.color) ?? '0 0 0';
    for (let g = 0; g < groups.length; g++) {
      const cols = groups[g]!;
      const titled = g === 0 && board.pages.length === 0;
      const firstPageTop = pageHeight - MARGIN - (titled && table.title ? TITLE_SIZE * 1.6 : 0)
        - (titled ? notes.length * lineHeight : 0);
      const headerRows = p.levels.length;
      const perFirst = Math.max(1, Math.floor((firstPageTop - MARGIN - lineHeight * (headerRows + 1)) / lineHeight));
      const perPage = Math.max(1, Math.floor((pageHeight - MARGIN * 2 - lineHeight * (headerRows + 1)) / lineHeight));
      let start = 0;
      let firstOfGroup = true;
      do {
        const take = titled && firstOfGroup ? perFirst : perPage;
        const s = new Stream();
        let y = pageHeight - MARGIN;
        if (titled && firstOfGroup) {
          if (table.title) {
            s.text(`${textColour} rg BT /${fontRef(p.family, true, false)} ${TITLE_SIZE} Tf 1 0 0 1 ${MARGIN} ${(y - TITLE_SIZE).toFixed(2)} Tm `);
            s.string(table.title, missing);
            s.text(' Tj ET\n');
            y -= TITLE_SIZE * 1.6;
          }
          for (const note of notes) {
            s.text(`0.33 0.33 0.33 rg BT /${fontRef(p.family, false, false)} ${size.toFixed(2)} Tf 1 0 0 1 ${MARGIN} ${(y - size).toFixed(2)} Tm `);
            s.string(note, missing);
            s.text(' Tj ET\n');
            y -= lineHeight;
          }
        }
        drawTable(s, missing, table, p, { cols, left: MARGIN, top: y, size, start, count: take });
        pages.push({ bytes: s.bytes, width: pageWidth, height: pageHeight });
        start += take;
        firstOfGroup = false;
      } while (start < p.rows.length);
    }
  }
  if (missing.size > 0) {
    // said on the page, not swallowed: which characters this font could not draw
    const s = new Stream();
    s.text(`0 0 0 rg BT /F1 ${size.toFixed(2)} Tf 1 0 0 1 ${MARGIN} ${(pageHeight - MARGIN - size).toFixed(2)} Tm `);
    s.string(`Some characters cannot be drawn in this PDF's font and show as "?": ${[...missing].join(' ')}`
      .replace(/[^\x20-\x7e]/g, (ch) => `U+${(ch.codePointAt(0) ?? 0).toString(16).toUpperCase().padStart(4, '0')}`), new Set());
    s.text(' Tj ET\n');
    pages.push({ bytes: s.bytes, width: pageWidth, height: pageHeight });
  }
  return assemble([...board.pages, ...pages]);
}

/**
 * THE DASHBOARD PAGE, when the board holds charts: landscape, quarter-inch margins, the title
 * centred across the top, each
 * tile where the board puts it (its columns across the page, its rows down it) -- each chart as
 * its picture fitted into its tile without distortion, the TABLE drawn in the grid's tile, fitted
 * to its width (down to the font floor) and centred in it; when not every row fits, the tile says
 * how many it shows.
 */
function dashboard(table: ExportTable, page: ExportPage | undefined, p: Prepared, missing: Set<string>):
  { readonly pages: PdfPage[] } {
  if (!page || !page.tiles.some((t) => t.kind === 'chart')) return { pages: [] };
  const width = A4_LONG;
  const height = A4_SHORT;
  const s = new Stream();
  let top = height - DASH_MARGIN;
  if (table.title) {
    const tw = textWidth([...table.title].length, TITLE_SIZE, 'sans', true);
    s.text(`0 0 0 rg BT /F2 ${TITLE_SIZE} Tf 1 0 0 1 ${((width - tw) / 2).toFixed(2)} ${(top - TITLE_SIZE).toFixed(2)} Tm `);
    s.string(table.title, missing);
    s.text(' Tj ET\n');
    top -= TITLE_SIZE * 1.8;
  }
  const rows = Math.max(1, ...page.tiles.map((t) => t.y + t.h));
  const unitW = (width - DASH_MARGIN * 2) / page.cols;
  const unitH = (top - DASH_MARGIN) / rows;
  const GUTTER = 6;
  const images: { name: string; jpeg: Uint8Array; width: number; height: number }[] = [];
  for (const t of page.tiles) {
    const x = DASH_MARGIN + t.x * unitW + GUTTER / 2;
    const w = t.w * unitW - GUTTER;
    const yTop = top - t.y * unitH - GUTTER / 2;
    const h = t.h * unitH - GUTTER;
    s.text(`0.9 0.9 0.9 RG 0.75 w ${x.toFixed(2)} ${(yTop - h).toFixed(2)} ${w.toFixed(2)} ${h.toFixed(2)} re S\n`);
    s.text(`0 0 0 rg BT /F2 10 Tf 1 0 0 1 ${(x + 6).toFixed(2)} ${(yTop - 14).toFixed(2)} Tm `);
    s.string(t.title, missing);
    s.text(' Tj ET\n');
    const innerTop = yTop - 20;
    const innerH = h - 26;
    const innerW = w - 12;
    if (t.kind === 'chart' && t.picture) {
      const aspect = t.picture.width / t.picture.height;
      const drawW = Math.min(innerW, innerH * aspect);
      const drawH = drawW / aspect;
      const dx = x + 6 + (innerW - drawW) / 2;
      const dy = innerTop - drawH - (innerH - drawH) / 2;
      const name = `Im${images.length + 1}`;
      images.push({ name, jpeg: t.picture.jpeg, width: t.picture.pixelWidth, height: t.picture.pixelHeight });
      s.text(`q ${drawW.toFixed(2)} 0 0 ${drawH.toFixed(2)} ${dx.toFixed(2)} ${dy.toFixed(2)} cm /${name} Do Q\n`);
    } else if (t.kind === 'grid') {
      // THE TABLE IN ITS TILE: fitted to the tile's width, as many rows as its height holds
      const all = p.widths.map((_, i) => i);
      const natural = needWidth(p, all, p.nominal);
      const size = Math.max(FONT_MIN, natural > innerW ? p.nominal * innerW / natural : p.nominal);
      const cols: number[] = [];
      for (const i of all) {
        if (cols.length > 0 && needWidth(p, [...cols, i], size) > innerW) break;
        cols.push(i);
      }
      const lineHeight = size * 1.6;
      const room = innerH - p.levels.length * lineHeight;
      const everything = cols.length === all.length && p.rows.length * lineHeight <= room;
      const fit = Math.max(0, Math.floor((room - (everything ? 0 : lineHeight)) / lineHeight));
      const count = everything ? p.rows.length : Math.min(p.rows.length, fit);
      // centred across its tile, as the charts are in theirs
      const left = x + 6 + Math.max(0, (innerW - needWidth(p, cols, size)) / 2);
      const bottom = drawTable(s, missing, table, p, { cols, left, top: innerTop, size, start: 0, count });
      if (!everything) {
        const shown = `${count.toLocaleString(UI_LOCALE)} of ${p.rows.length.toLocaleString(UI_LOCALE)} rows`
          + (cols.length < all.length ? `, ${cols.length} of ${all.length} columns` : '');
        s.text(`0.33 0.33 0.33 rg BT /F1 ${size.toFixed(2)} Tf 1 0 0 1 ${left.toFixed(2)} ${(bottom - size - 3).toFixed(2)} Tm `);
        s.string(`Showing the first ${shown}; the whole table is in the Excel and CSV exports.`, missing);
        s.text(' Tj ET\n');
      }
    } else {
      s.text(`0.33 0.33 0.33 rg BT /F1 9 Tf 1 0 0 1 ${(x + 6).toFixed(2)} ${(innerTop - 12).toFixed(2)} Tm `);
      s.string('(this chart had not drawn)', missing);
      s.text(' Tj ET\n');
    }
  }
  return { pages: [{ bytes: s.bytes, width, height, images }] };
}

/** One page of a PDF: its drawing, its size, and the images it draws (a chart's JPEG). */
interface PdfPage {
  readonly bytes: number[];
  readonly width: number;
  readonly height: number;
  readonly images?: readonly { readonly name: string; readonly jpeg: Uint8Array; readonly width: number; readonly height: number }[];
}

/**
 * Wrap pages into a PDF file, measuring bytes as it goes: the xref is byte offsets. Objects are
 * numbered in the order they are written -- the catalogue, the page tree, twelve fonts, then each
 * page, its content and its images -- so the xref lists them in order.
 */
function assemble(given: readonly PdfPage[]): Uint8Array {
  const pages = given.length > 0 ? given : [{ bytes: [], width: A4_SHORT, height: A4_LONG }];
  // object ids: 1 catalogue, 2 page tree, 3-14 fonts, then per page: page, content, its images
  const ids: { page: number; content: number; images: number[] }[] = [];
  let next = 15;
  for (const p of pages) {
    const page = next++;
    const content = next++;
    ids.push({ page, content, images: (p.images ?? []).map(() => next++) });
  }
  const out: number[] = [];
  const ascii = (s: string): void => { for (let i = 0; i < s.length; i++) out.push(s.charCodeAt(i) & 0xff); };
  const offsets: number[] = [];
  const object = (body: () => void): void => {
    offsets.push(out.length);
    ascii(`${offsets.length} 0 obj\n`);
    body();
    ascii('\nendobj\n');
  };
  const stream = (dict: string, bytes: ArrayLike<number>): void => {
    ascii(`<< ${dict}/Length ${bytes.length} >>\nstream\n`);
    for (let i = 0; i < bytes.length; i++) out.push(bytes[i]!);
    ascii('\nendstream');
  };
  // a binary comment line: tells transfer tools this file is not 7-bit text
  ascii('%PDF-1.4\n%');
  out.push(0xe2, 0xe3, 0xcf, 0xd3);
  ascii('\n');
  object(() => ascii('<< /Type /Catalog /Pages 2 0 R >>'));
  object(() => ascii(`<< /Type /Pages /Count ${pages.length} /Kids [${ids.map((i) => `${i.page} 0 R`).join(' ')}] >>`));
  // twelve base-14 faces, objects 3-14: /F1../F12 (fontRef's order)
  const faces = FAMILIES.flatMap((f) => FACES[f]);
  for (const face of faces) {
    object(() => ascii(`<< /Type /Font /Subtype /Type1 /BaseFont /${face} /Encoding /WinAnsiEncoding >>`));
  }
  const fonts = faces.map((_, i) => `/F${i + 1} ${i + 3} 0 R`).join(' ');
  pages.forEach((p, i) => {
    const own = ids[i]!;
    const xobjects = (p.images ?? []).map((img, k) => `/${img.name} ${own.images[k]} 0 R`).join(' ');
    object(() => ascii(`<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${p.width} ${p.height}]`
      + ` /Resources << /Font << ${fonts} >>${xobjects ? ` /XObject << ${xobjects} >>` : ''} >> /Contents ${own.content} 0 R >>`));
    object(() => stream('', p.bytes));
    for (const img of p.images ?? []) {
      // a JPEG is what DCTDecode reads: embedded as it is, no pixel ever decoded here
      object(() => stream(`/Type /XObject /Subtype /Image /Width ${img.width} /Height ${img.height}`
        + ' /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode ', img.jpeg));
    }
  });
  const xref = out.length;
  ascii(`xref\n0 ${offsets.length + 1}\n0000000000 65535 f \n`);
  for (const off of offsets) ascii(`${String(off).padStart(10, '0')} 00000 n \n`);
  ascii(`trailer\n<< /Size ${offsets.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`);
  return Uint8Array.from(out);
}
