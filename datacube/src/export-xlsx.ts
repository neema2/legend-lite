// Excel export: a real OOXML workbook (.xlsx), from the export model (export-model.ts).
//
// The file this replaces was SpreadsheetML 2003 named .xls: desktop Excel warns that its format
// and extension disagree, Excel Online and Google Sheets refuse it, and a title with an `&`
// near its 31st character produced an XML parse error (the 2026-09-29 audit). An .xlsx is a zip
// of a few XML parts; `fflate`, already the page's dependency, writes the zip.
//
// What a workbook carries that the other formats cannot:
// - numbers as numbers, dates and times as dates (serial days), booleans as booleans, so the
//   sheet can compute on them; a number Excel cannot hold exactly (a BIGINT past 2^53, a
//   decimal of more than 15 significant digits) goes in as text rather than silently changing;
// - each column's own format as an Excel format code (currency, fixed places, commas,
//   negatives in parentheses, k/m/b scales, a unit), so it reads as the grid does;
// - the grid's header rows, merged where the grid spans them, and frozen;
// - the tree as Excel's outline levels (groups collapse in Excel), totals in bold;
// - an "About" sheet with the title, the moment and the notes (truncation, redaction).

import { strToU8, zipSync } from 'fflate';

import type { ColumnFormat } from './format.ts';
import { cased, type ExportColumn, type ExportPage, type ExportTable } from './export-model.ts';
import type { Scalar } from '../../engine-client/src/result.ts';
import { fontStack } from './style.ts';
import { isNumeric, isTemporal, isTimeOfDay } from '../../engine-client/src/types.ts';

export interface XlsxOptions {
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** When the file was made, for the About sheet. */
  readonly at?: Date;
  /** The board, when it holds charts: a Dashboard sheet of its charts, first. */
  readonly page?: ExportPage;
}

const esc = (s: string): string => s
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
  // characters XML 1.0 cannot carry at all
  .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f￾￿]/g, '');

/** Column letters: 0 -> A, 25 -> Z, 26 -> AA. */
export function columnLetters(index: number): string {
  let n = index + 1;
  let out = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    out = String.fromCharCode(65 + r) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

/** A sheet name Excel accepts: no `[]:*?/\`, not blank, at most 31 characters (cut BEFORE escaping). */
export function sheetName(title: string): string {
  const clean = [...title.replace(/[[\]:*?/\\]/g, ' ').replace(/\s+/g, ' ').trim()].slice(0, 31).join('').trim();
  return clean.replace(/^'+|'+$/g, '') || 'Sheet1';
}

const CURRENCY_SYMBOLS: Readonly<Record<string, string>> = { USD: '$', EUR: '€', GBP: '£', JPY: '¥', CNY: '¥', INR: '₹', CHF: 'CHF ' };
const quote = (s: string): string => `"${s.replace(/"/g, '""')}"`;

/** An Excel number format code for a column's format, or null for Excel's General. */
export function formatCode(format: ColumnFormat | undefined, type: string | undefined): string | null {
  if (!format || !isNumeric(type)) return null;
  const min = format.decimals ?? format.minimumFractionDigits ?? 0;
  const max = Math.max(min, format.decimals ?? format.maximumFractionDigits ?? min);
  const fraction = max > 0 ? `.${'0'.repeat(min)}${'#'.repeat(max - min)}` : '';
  let body = `${format.displayCommas === false ? '0' : '#,##0'}${fraction}`;
  const scale = format.numberScale;
  if (scale === 'thousands') body += ',"k"';
  else if (scale === 'millions') body += ',,"m"';
  else if (scale === 'billions') body += ',,,"b"';
  else if (scale === 'trillions') body += ',,,,"t"';
  if (format.kind === 'percent') body += '%';
  if (format.kind === 'currency' && format.currency) {
    body = `${quote(CURRENCY_SYMBOLS[format.currency] ?? `${format.currency} `)}${body}`;
  }
  if (format.unit) {
    body = format.unit.startsWith('_') ? `${quote(format.unit.slice(1))}${body}` : `${body}${quote(format.unit)}`;
  }
  return format.negativeParens ? `${body};(${body})` : body;
}

/** Excel's serial day for a calendar date and time as written (no zone moved): day 0 is 1899-12-30. */
function serial(y: number, mo: number, d: number, h = 0, mi = 0, s = 0, frac = 0): number {
  const days = (Date.UTC(y, mo - 1, d) - Date.UTC(1899, 11, 30)) / 86_400_000;
  return days + (h * 3600 + mi * 60 + s + frac) / 86_400;
}

const DATE = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?)?/;
const TIME = /^(\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?$/;

/** Whether a number's text is one Excel's double holds exactly (15 significant digits). */
function exactInExcel(text: string): boolean {
  const digits = text.replace(/^[+-]/, '').replace(/e.*$/i, '').replace('.', '').replace(/^0+/, '');
  return digits.replace(/0+$/, '').length <= 15;
}

/** '#rrggbb' (or '#rgb') as Excel's ARGB, or null for anything else. */
export function argb(colour: string | undefined): string | null {
  if (!colour) return null;
  const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(colour.trim());
  if (!m) return null;
  const hex = m[1]!.length === 3 ? [...m[1]!].map((c) => c + c).join('') : m[1]!;
  return `FF${hex.toUpperCase()}`;
}

/** A font family's first face, as Excel names a font ('Roboto', 'Arial'). */
function faceOf(family: string): string {
  return (fontStack(family).split(',')[0] ?? family).trim().replace(/^['"]|['"]$/g, '');
}

/** What one cell looks like in the workbook: its font, fill, border, number format and alignment. */
interface XfSpec {
  readonly numFmt: number;
  readonly font: string;
  readonly fill: string;
  readonly border: string;
  readonly align: string;
}

/** Styles, registered as cells need them: fonts, fills, borders, number formats, alignments. */
class Styles {
  readonly #fmts = new Map<string, number>();
  readonly #fonts = new Map<string, number>();
  readonly #fills = new Map<string, number>();
  readonly #borders = new Map<string, number>();
  readonly #xfs = new Map<string, number>();
  readonly #xfList: string[] = [];

  constructor(defaultFont: string) {
    this.#fonts.set(defaultFont, 0);
    this.#fills.set('<fill><patternFill patternType="none"/></fill>', 0);
    this.#fills.set('<fill><patternFill patternType="gray125"/></fill>', 1);
    this.#borders.set('<border><left/><right/><top/><bottom/><diagonal/></border>', 0);
    this.#xfList.push('<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>');
  }

  /** A number format's id: 0 for General, else a custom format (ids from 164, Excel's first free one). */
  fmt(code: string | null): number {
    if (code === null) return 0;
    let id = this.#fmts.get(code);
    if (id === undefined) {
      id = 164 + this.#fmts.size;
      this.#fmts.set(code, id);
    }
    return id;
  }
  static #id(map: Map<string, number>, xml: string): number {
    let id = map.get(xml);
    if (id === undefined) {
      id = map.size;
      map.set(xml, id);
    }
    return id;
  }
  xf(spec: XfSpec): number {
    const key = JSON.stringify(spec);
    let id = this.#xfs.get(key);
    if (id === undefined) {
      id = this.#xfList.length;
      this.#xfs.set(key, id);
      const font = Styles.#id(this.#fonts, spec.font);
      const fill = Styles.#id(this.#fills, spec.fill);
      const border = Styles.#id(this.#borders, spec.border);
      this.#xfList.push(`<xf numFmtId="${spec.numFmt}" fontId="${font}" fillId="${fill}" borderId="${border}" xfId="0"`
        + ' applyNumberFormat="1" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1">'
        + `${spec.align}</xf>`);
    }
    return id;
  }
  xml(): string {
    const list = (m: Map<string, number>): string => [...m].sort((a, b) => a[1] - b[1]).map(([x]) => x).join('');
    const fmts = [...this.#fmts].map(([code, id]) => `<numFmt numFmtId="${id}" formatCode="${esc(code)}"/>`).join('');
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
      + '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
      + (fmts ? `<numFmts count="${this.#fmts.size}">${fmts}</numFmts>` : '')
      + `<fonts count="${this.#fonts.size}">${list(this.#fonts)}</fonts>`
      + `<fills count="${this.#fills.size}">${list(this.#fills)}</fills>`
      + `<borders count="${this.#borders.size}">${list(this.#borders)}</borders>`
      + '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
      + `<cellXfs count="${this.#xfList.length}">${this.#xfList.join('')}</cellXfs>`
      + '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
      + '</styleSheet>';
  }
}

/** A font element for a look: face, size in points (CSS px x 0.75), weight, slant, lines, colour. */
function fontXml(look: { readonly face: string; readonly px: number; readonly bold?: boolean; readonly italic?: boolean;
  readonly underline?: boolean; readonly strike?: boolean; readonly color?: string | null }): string {
  const pt = Math.round(look.px * 0.75 * 2) / 2;
  return `<font>${look.bold ? '<b/>' : ''}${look.italic ? '<i/>' : ''}${look.strike ? '<strike/>' : ''}`
    + `${look.underline ? '<u/>' : ''}<sz val="${pt}"/>${look.color ? `<color rgb="${look.color}"/>` : ''}`
    + `<name val="${esc(look.face)}"/></font>`;
}

const fillXml = (rgb: string | null): string => (rgb
  ? `<fill><patternFill patternType="solid"><fgColor rgb="${rgb}"/><bgColor indexed="64"/></patternFill></fill>`
  : '<fill><patternFill patternType="none"/></fill>');

/** What a value is in a workbook: its cell type, its content, and which number format it takes. */
function valueOf(v: Scalar, column: ExportColumn, code: string | null):
  { readonly kind: 'n' | 'b' | 's'; readonly content: string; readonly fmt: string | null } {
  if (typeof v === 'boolean') return { kind: 'b', content: v ? '1' : '0', fmt: null };
  if (!column.redacted && !column.tree && isNumeric(column.type)) {
    // A double IS what Excel holds, whatever its digits. An exact value -- a DECIMAL's text, an
    // integer past 2^53 (a bigint) -- beyond 15 significant digits stays text, never silently
    // rounded (a Float's 66677024.29998732 was caught by that rule and lost its format, 2026-09-30).
    if (typeof v === 'number') {
      return Number.isFinite(v) ? { kind: 'n', content: String(v), fmt: code } : { kind: 's', content: String(v), fmt: null };
    }
    const t = String(v);
    return Number.isFinite(Number(t)) && exactInExcel(t) ? { kind: 'n', content: t, fmt: code } : { kind: 's', content: t, fmt: null };
  }
  if (!column.redacted && isTemporal(column.type) && typeof v === 'string') {
    if (isTimeOfDay(column.type)) {
      const m = TIME.exec(v);
      if (m) {
        const secs = Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3] ?? 0) + Number(m[4] ?? 0);
        return { kind: 'n', content: String(secs / 86_400), fmt: 'hh:mm:ss' };
      }
    } else {
      const m = DATE.exec(v);
      if (m) {
        const n = serial(Number(m[1]), Number(m[2]), Number(m[3]), Number(m[4] ?? 0), Number(m[5] ?? 0),
          Number(m[6] ?? 0), Number(m[7] ?? 0));
        return { kind: 'n', content: String(n), fmt: m[4] !== undefined ? 'yyyy-mm-dd hh:mm:ss' : 'yyyy-mm-dd' };
      }
    }
  }
  return { kind: 's', content: String(v), fmt: null };
}

/**
 * The grid as an .xlsx file that LOOKS like the grid: each cell's font, colours, alignment and
 * decoration as the grid resolves them (export-model.ts), its grid lines as borders, its banded
 * and total rows, a plain gray header -- and values a spreadsheet can compute on.
 */
export function toXlsx(table: ExportTable, options: XlsxOptions = {}): Uint8Array {
  const look = table.look;
  const face = faceOf(look.fontFamily);
  const defaultFont = fontXml({ face, px: look.fontSize, color: argb(look.color) });
  const styles = new Styles(defaultFont);
  const line = argb(look.lineColor) ?? 'FFD4D4D4';
  // the grid's lines: vertical ones as each cell's right edge, horizontal ones as its bottom
  const edge = (tag: 'right' | 'bottom', on: boolean): string =>
    (on ? `<${tag} style="thin"><color rgb="${line}"/></${tag}>` : `<${tag}/>`);
  const cellBorder = `<border><left/>${edge('right', look.verticalLines)}<top/>${edge('bottom', look.horizontalLines)}<diagonal/></border>`;
  const headerBorder = '<border><left style="thin"><color rgb="FFE5E5E5"/></left><right style="thin"><color rgb="FFE5E5E5"/></right>'
    + '<top style="thin"><color rgb="FFE5E5E5"/></top><bottom style="thin"><color rgb="FFE5E5E5"/></bottom><diagonal/></border>';
  const headerStyle = styles.xf({
    numFmt: 0,
    font: fontXml({ face, px: look.fontSize, color: argb(look.headerColor) }),
    fill: fillXml(argb(look.headerBackground)),
    border: headerBorder,
    align: '<alignment horizontal="center" vertical="center"/>',
  });
  const codes = table.columns.map((c) => (c.redacted || c.tree ? null : formatCode(options.formats?.[c.name], c.type)));

  const headerRows = table.headerRows.length > 0 ? table.headerRows : [table.columns.map((c, i) => ({
    label: c.path.join(' '), colStart: i, colSpan: 1, rowSpan: 1,
  }))];
  const top = headerRows.length;
  /**
   * THE TABLE'S CELLS with its top-left at (row0, col0), zero-based: its header levels (spans as
   * merges), then each row styled as the grid styles it. The table sheet and the dashboard's grid
   * tile both place it with this, so the two can never look different.
   */
  const tableAt = (row0: number, col0: number): Placed => {
    const cells: PlacedCell[] = [];
    const merges: string[] = [];
    const outline = new Map<number, number>();
    headerRows.forEach((row, r) => {
      for (const cell of row) {
        const ref = `${columnLetters(col0 + cell.colStart)}${row0 + r + 1}`;
        if (cell.colSpan > 1 || cell.rowSpan > 1) {
          merges.push(`${ref}:${columnLetters(col0 + cell.colStart + cell.colSpan - 1)}${row0 + r + cell.rowSpan}`);
        }
        cells.push({ row: row0 + r, col: col0 + cell.colStart,
          xml: `<c r="${ref}" s="${headerStyle}" t="inlineStr"><is><t xml:space="preserve">${esc(cell.label)}</t></is></c>` });
      }
    });
    table.rows.forEach((row, r) => {
      const at = row0 + top + r;
      if (row.depth > 1) outline.set(at, Math.min(7, row.depth - 1));
      table.columns.forEach((c, i) => {
        const st = row.styles[i] ?? { align: 'left' as const };
        const v = row.cells[i] ?? null;
        const value = v === null ? null : valueOf(v, c, codes[i] ?? null);
        const indent = c.tree ? Math.max(0, row.depth - 1) : 0;
        const align = `<alignment horizontal="${st.align}"${indent > 0 ? ` indent="${indent}"` : ''}/>`;
        const id = styles.xf({
          numFmt: styles.fmt(value?.fmt ?? null),
          font: fontXml({
            face: st.fontFamily ? faceOf(st.fontFamily) : face,
            px: st.fontSize ?? look.fontSize,
            bold: st.bold === true,
            italic: st.italic === true,
            underline: st.underline !== undefined,
            strike: st.strike === true,
            color: argb(st.color) ?? argb(look.color),
          }),
          fill: fillXml(argb(st.background)),
          border: cellBorder,
          align,
        });
        const ref = `${columnLetters(col0 + i)}${at + 1}`;
        const xml = !value ? `<c r="${ref}" s="${id}"/>`
          : value.kind === 's'
            ? `<c r="${ref}" s="${id}" t="inlineStr"><is><t xml:space="preserve">${esc(cased(value.content, st.fontCase))}</t></is></c>`
            : `<c r="${ref}" s="${id}"${value.kind === 'b' ? ' t="b"' : ''}><v>${value.content}</v></c>`;
        cells.push({ row: at, col: col0 + i, xml });
      });
    });
    return { cells, merges, outline };
  };

  const widths = table.columns.map((c, i) => {
    let w = c.path.join(' ').length;
    for (const row of table.rows.slice(0, 200)) w = Math.max(w, String(row.cells[i] ?? '').length + (c.tree ? row.depth * 2 : 0));
    return Math.min(60, Math.max(8, w + 2));
  });
  const placed = tableAt(0, 0);
  const last = `${columnLetters(Math.max(0, table.columns.length - 1))}${top + table.rows.length}`;
  const freeze = table.grouped ? `xSplit="1" ySplit="${top}" topLeftCell="B${top + 1}"` : `ySplit="${top}" topLeftCell="A${top + 1}"`;
  const sheet = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    + '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    + '<sheetPr><outlinePr summaryBelow="0"/></sheetPr>'
    + `<dimension ref="A1:${last}"/>`
    + `<sheetViews><sheetView workbookViewId="0"><pane ${freeze} activePane="bottomRight" state="frozen"/></sheetView></sheetViews>`
    + '<sheetFormatPr defaultRowHeight="15"/>'
    + `<cols>${widths.map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w}" customWidth="1"/>`).join('')}</cols>`
    + `<sheetData>${sheetRows(placed.cells, placed.outline)}</sheetData>`
    + mergeXml(placed.merges)
    + '</worksheet>';

  const at = options.at ?? new Date();
  const about = [['Title', table.title], ['Exported', at.toISOString()], ['Rows', String(table.rows.length)],
    ...table.notes.map((n) => ['Note', n])];
  const aboutSheet = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    + '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    + '<cols><col min="1" max="1" width="12" customWidth="1"/><col min="2" max="2" width="100" customWidth="1"/></cols><sheetData>'
    + about.map(([k, v], r) => `<row r="${r + 1}"><c r="A${r + 1}" s="${headerStyle}" t="inlineStr"><is><t>${esc(k ?? '')}</t></is></c>`
      + `<c r="B${r + 1}" t="inlineStr"><is><t xml:space="preserve">${esc(v ?? '')}</t></is></c></row>`).join('')
    + '</sheetData></worksheet>';

  const name = sheetName(table.title);
  const aboutName = name === 'About' ? 'About this export' : 'About';
  const board = options.page && options.page.tiles.some((t) => t.kind === 'chart')
    ? dashboardSheet(options.page, headerStyle, { tableAt, widths, height: top + table.rows.length }) : null;
  const dashName = [name, aboutName].includes('Dashboard') ? 'Page' : 'Dashboard';

  const ns = 'http://schemas.openxmlformats.org';
  const rel = (id: string, type: string, target: string): string =>
    `<Relationship Id="${id}" Type="${ns}/officeDocument/2006/relationships/${type}" Target="${target}"/>`;
  const sheetType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml';
  // the dashboard, when there is one, is the first sheet a reader sees
  const sheets = [
    ...(board ? [{ name: dashName, file: 'sheet3.xml', rid: 'rId4' }] : []),
    { name, file: 'sheet1.xml', rid: 'rId1' },
    { name: aboutName, file: 'sheet2.xml', rid: 'rId2' },
  ];
  const files: Record<string, Uint8Array> = {
    '[Content_Types].xml': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
      + `<Types xmlns="${ns}/package/2006/content-types">`
      + `<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>`
      + '<Default Extension="xml" ContentType="application/xml"/>'
      + (board ? '<Default Extension="png" ContentType="image/png"/>' : '')
      + '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
      + sheets.map((sh) => `<Override PartName="/xl/worksheets/${sh.file}" ContentType="${sheetType}"/>`).join('')
      + (board ? '<Override PartName="/xl/drawings/drawing1.xml" ContentType="application/vnd.openxmlformats-officedocument.drawing+xml"/>' : '')
      + '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
      + '</Types>'),
    '_rels/.rels': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
      + `<Relationships xmlns="${ns}/package/2006/relationships">${rel('rId1', 'officeDocument', 'xl/workbook.xml')}</Relationships>`),
    'xl/workbook.xml': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
      + `<workbook xmlns="${ns}/spreadsheetml/2006/main" xmlns:r="${ns}/officeDocument/2006/relationships"><sheets>`
      + sheets.map((sh, i) => `<sheet name="${esc(sh.name)}" sheetId="${i + 1}" r:id="${sh.rid}"/>`).join('')
      + '</sheets></workbook>'),
    'xl/_rels/workbook.xml.rels': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
      + `<Relationships xmlns="${ns}/package/2006/relationships">`
      + rel('rId1', 'worksheet', 'worksheets/sheet1.xml') + rel('rId2', 'worksheet', 'worksheets/sheet2.xml')
      + rel('rId3', 'styles', 'styles.xml') + (board ? rel('rId4', 'worksheet', 'worksheets/sheet3.xml') : '')
      + '</Relationships>'),
    'xl/worksheets/sheet1.xml': strToU8(sheet),
    'xl/worksheets/sheet2.xml': strToU8(aboutSheet),
    ...(board ? {
      'xl/worksheets/sheet3.xml': strToU8(board.sheet),
      'xl/worksheets/_rels/sheet3.xml.rels': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        + `<Relationships xmlns="${ns}/package/2006/relationships">${rel('rId1', 'drawing', '../drawings/drawing1.xml')}</Relationships>`),
      'xl/drawings/drawing1.xml': strToU8(board.drawing),
      'xl/drawings/_rels/drawing1.xml.rels': strToU8('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        + `<Relationships xmlns="${ns}/package/2006/relationships">`
        + board.images.map((_, i) => rel(`rId${i + 1}`, 'image', `../media/image${i + 1}.png`)).join('')
        + '</Relationships>'),
      ...Object.fromEntries(board.images.map((png, i) => [`xl/media/image${i + 1}.png`, png])),
    } : {}),
    // written last: cells registered their styles while the sheets were built
    'xl/styles.xml': strToU8(styles.xml()),
  };
  return zipSync(files, { level: 6, mtime: at });
}

/** A cell placed on a sheet, zero-based, as its XML. */
interface PlacedCell { readonly row: number; readonly col: number; readonly xml: string }
interface Placed {
  readonly cells: readonly PlacedCell[];
  readonly merges: readonly string[];
  /** Each tree row's outline level, by its (zero-based) sheet row. */
  readonly outline: ReadonlyMap<number, number>;
}

/** Cells as a sheet's rows: sorted by row, each row's cells by column, as Excel requires. */
function sheetRows(cells: readonly PlacedCell[], outline: ReadonlyMap<number, number> = new Map()): string {
  const byRow = new Map<number, PlacedCell[]>();
  for (const c of cells) {
    const list = byRow.get(c.row) ?? [];
    list.push(c);
    byRow.set(c.row, list);
  }
  return [...byRow].sort((x, y) => x[0] - y[0]).map(([r, list]) => {
    const level = outline.get(r);
    return `<row r="${r + 1}"${level ? ` outlineLevel="${level}"` : ''}>${list.sort((x, y) => x.col - y.col).map((c) => c.xml).join('')}</row>`;
  }).join('');
}

const mergeXml = (merges: readonly string[]): string => (merges.length > 0
  ? `<mergeCells count="${merges.length}">${merges.map((m) => `<mergeCell ref="${m}"/>`).join('')}</mergeCells>` : '');

/** Board units on the sheet: each board column two cells of 64px, each board row two cells of 20px. */
const CELLS_PER_COL = 2;
const CELLS_PER_ROW = 2;
const CELL_W = 64;
const CELL_H = 20;
const EMU = 9525;
/** A column `width` (in characters) in pixels, as Excel draws it at its default font. */
const pxOf = (width: number): number => Math.round(width * 7 + 5);

/**
 * THE DASHBOARD SHEET: the board as the HTML page lays it out. Each tile's title in the cell at its
 * top-left, a chart as its picture (fitted to its tile, not distorted), and the TABLE ITSELF in the
 * grid's tile -- every row, styled as on the table sheet, its columns as wide as there. Like the
 * page, the table grows its tile: what is below it moves down past its last row, and what is
 * beside it moves right when it is wider than its tile.
 */
function dashboardSheet(page: ExportPage, titleStyle: number, grid: {
  readonly tableAt: (row0: number, col0: number) => Placed;
  readonly widths: readonly number[];
  /** The table's height in rows, header levels included. */
  readonly height: number;
}): { readonly sheet: string; readonly drawing: string; readonly images: readonly Uint8Array[] } {
  const images: Uint8Array[] = [];
  const anchors: string[] = [];
  const cells: PlacedCell[] = [];
  const put = (row: number, col: number, text: string, style = titleStyle): void => {
    cells.push({ row, col, xml: `<c r="${columnLetters(col)}${row + 1}" s="${style}" t="inlineStr"><is><t xml:space="preserve">${esc(text)}</t></is></c>` });
  };
  const g = page.tiles.find((t) => t.kind === 'grid');
  // where the table goes, and how far past its tile it reaches
  const gridCol = g ? g.x * CELLS_PER_COL : 0;
  const gridRow = g ? g.y * CELLS_PER_ROW : 0;
  const extraRows = g ? Math.max(0, 1 + grid.height - g.h * CELLS_PER_ROW) : 0;
  const tableW = grid.widths.reduce((sum, w) => sum + pxOf(w), 0);
  const extraW = g ? Math.max(0, tableW - g.w * CELLS_PER_COL * CELL_W) : 0;
  // the sheet's columns: the board's 64px cells, the table's own widths where the table is
  const colsPx: number[] = [];
  const colsWidth: number[] = [];
  const total = page.cols * CELLS_PER_COL + (g ? grid.widths.length : 0) + Math.ceil(extraW / CELL_W) + 2;
  for (let c = 0; c < total; c++) {
    const w = g && c >= gridCol && c < gridCol + grid.widths.length ? grid.widths[c - gridCol]! : (CELL_W - 5) / 7;
    colsWidth.push(w);
    colsPx.push(pxOf(w));
  }
  /** A pixel across the sheet as (column, offset in it). */
  const colAt = (px: number): { col: number; off: number } => {
    let col = 0;
    let left = 0;
    while (col < colsPx.length - 1 && left + colsPx[col]! <= px) left += colsPx[col++]!;
    return { col, off: px - left };
  };
  let merges: readonly string[] = [];
  if (g) {
    put(gridRow, gridCol, g.title);
    const placed = grid.tableAt(gridRow + 1, gridCol);
    cells.push(...placed.cells);
    merges = placed.merges;
  }
  for (const t of page.tiles) {
    if (t.kind !== 'chart') continue;
    const below = g !== undefined && t.y >= g.y + g.h;
    const beside = g !== undefined && !below && t.y + t.h > g.y && t.x >= g.x + g.w;
    // the board's pixel for this tile: left of the table as on the board; right of it past the
    // table (as wide as its tile or wider); below it, past its last row
    const gridLeft = gridCol * CELL_W;
    const px = !g || t.x <= g.x ? t.x * CELLS_PER_COL * CELL_W
      : beside ? gridLeft + Math.max(tableW, g.w * CELLS_PER_COL * CELL_W) + (t.x - g.x - g.w) * CELLS_PER_COL * CELL_W
        : gridLeft + (t.x - g.x) * CELLS_PER_COL * CELL_W;
    const { col, off } = colAt(px);
    const row = t.y * CELLS_PER_ROW + (below ? extraRows : 0);
    put(row, col, t.title);
    if (!t.picture) {
      put(row + 1, col, '(this chart had not drawn)', 0);
      continue;
    }
    const boxW = t.w * CELLS_PER_COL * CELL_W - 8;
    const boxH = (t.h * CELLS_PER_ROW - 1) * CELL_H - 8;
    const aspect = t.picture.width / t.picture.height;
    const w = Math.round(Math.min(boxW, boxH * aspect));
    const h = Math.round(w / aspect);
    images.push(t.picture.png);
    const id = images.length;
    anchors.push(`<xdr:oneCellAnchor><xdr:from><xdr:col>${col}</xdr:col><xdr:colOff>${off * EMU}</xdr:colOff><xdr:row>${row + 1}</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:from>`
      + `<xdr:ext cx="${w * EMU}" cy="${h * EMU}"/><xdr:pic><xdr:nvPicPr><xdr:cNvPr id="${id + 1}" name="${esc(t.title)}" descr="${esc(t.title)}"/>`
      + '<xdr:cNvPicPr><a:picLocks noChangeAspect="1"/></xdr:cNvPicPr></xdr:nvPicPr>'
      + `<xdr:blipFill><a:blip r:embed="rId${id}"/><a:stretch><a:fillRect/></a:stretch></xdr:blipFill>`
      + `<xdr:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="${w * EMU}" cy="${h * EMU}"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></xdr:spPr>`
      + '</xdr:pic><xdr:clientData/></xdr:oneCellAnchor>');
  }
  const sheet = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    + '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
    + '<sheetViews><sheetView workbookViewId="0" showGridLines="0"/></sheetViews>'
    + `<sheetFormatPr defaultRowHeight="${CELL_H * 0.75}" customHeight="1"/>`
    + `<cols>${colsWidth.map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w.toFixed(2)}" customWidth="1"/>`).join('')}</cols>`
    + `<sheetData>${sheetRows(cells)}</sheetData>`
    + mergeXml(merges)
    + '<drawing r:id="rId1"/></worksheet>';
  const drawing = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    + '<xdr:wsDr xmlns:xdr="http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing"'
    + ' xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"'
    + ' xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
    + anchors.join('') + '</xdr:wsDr>';
  return { sheet, drawing, images };
}

/** The .xlsx MIME type. */
export const XLSX_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
