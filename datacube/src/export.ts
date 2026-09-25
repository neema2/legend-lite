// Export: the grid as CSV or TSV.
//
// Two modes, and the distinction matters more than it looks. FORMATTED
// export writes what the screen shows -- "$1,234.50" -- which is right
// for a report someone reads. RAW export writes the underlying scalar
// -- 1234.5 -- which is right for anything that will be computed on
// again. Exporting formatted numbers into a spreadsheet and then
// summing them is a classic way to get a wrong total, so raw is the
// default and formatted is the deliberate choice.
//
// Excel compatibility costs two small things, both included because
// their absence is invisible until someone opens the file:
// CRLF line endings, and a BOM so Excel reads UTF-8 rather than
// mangling any non-ASCII name.

import type { FormatterCache, ColumnFormat } from './format.ts';
import type { ResultTable, Scalar } from './result.ts';

export interface ExportOptions {
  /** ',' for CSV, '\t' for TSV. */
  readonly delimiter?: string;
  /** Write formatted text instead of raw values. */
  readonly formatted?: boolean;
  /** Required when `formatted`. */
  readonly formatters?: FormatterCache;
  readonly formats?: Readonly<Record<string, ColumnFormat>>;
  /** Restrict and order the columns; defaults to all, in table order. */
  readonly columns?: readonly string[];
  /** Prepend a UTF-8 BOM so Excel does not mangle non-ASCII. */
  readonly bom?: boolean;
  /** CRLF by default, which every spreadsheet reads. */
  readonly newline?: string;
}

/** A value a spreadsheet will read as a number, not as a formula. */
const PLAIN_NUMBER = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

/**
 * Quote a field for CSV.
 *
 * A field is quoted when it contains the delimiter, a quote, or any
 * newline; embedded quotes are doubled. A leading `=` or `@` is also
 * prefixed with a quote character, because spreadsheets treat those as
 * the start of a FORMULA -- the CSV-injection problem -- and a cell
 * whose value came from a database should never execute.
 */
export function escapeField(
  value: string,
  delimiter: string,
): string {
  // '=' and '@' can only begin a formula. '+' and '-' usually begin a
  // NUMBER, and prefixing those would turn -42 into text -- destroying
  // the whole point of a raw export, which is that it can be summed
  // again. So they are only defused when the rest is not numeric.
  const leading = value.charAt(0);
  const risky =
    leading === '=' ||
    leading === '@' ||
    leading === '\t' ||
    leading === '\r' ||
    ((leading === '+' || leading === '-') && !PLAIN_NUMBER.test(value));
  const body = risky ? `'${value}` : value;
  const needsQuotes =
    body.includes(delimiter) ||
    body.includes('"') ||
    body.includes('\n') ||
    body.includes('\r');
  return needsQuotes ? `"${body.replace(/"/g, '""')}"` : body;
}

/** Raw scalar as text, with null distinct from the string "null". */
function rawText(v: Scalar): string {
  if (v === null || v === undefined) return '';
  if (v instanceof Date) return v.toISOString();
  return String(v);
}

export function toDelimited(
  table: ResultTable,
  options: ExportOptions = {},
): string {
  const delimiter = options.delimiter ?? ',';
  const newline = options.newline ?? '\r\n';
  const wanted = options.columns
    ? options.columns
        .map((n) => table.columns.find((c) => c.name === n))
        .filter((c): c is NonNullable<typeof c> => c !== undefined)
    : table.columns;

  if (options.formatted && !options.formatters) {
    throw new Error('formatted export needs a FormatterCache');
  }

  const lines: string[] = [];
  lines.push(
    wanted.map((c) => escapeField(c.name, delimiter)).join(delimiter),
  );

  for (let r = 0; r < table.rowCount; r++) {
    const cells = wanted.map((c) => {
      const v = c.values[r] ?? null;
      const text = options.formatted
        ? options.formatters!.format(v, options.formats?.[c.name])
        : rawText(v);
      return escapeField(text, delimiter);
    });
    lines.push(cells.join(delimiter));
  }

  const body = lines.join(newline) + newline;
  return options.bom === false ? body : `﻿${body}`;
}

export function toCsv(
  table: ResultTable,
  options: ExportOptions = {},
): string {
  return toDelimited(table, { ...options, delimiter: ',' });
}

/**
 * TSV, which is what a spreadsheet expects on the clipboard.
 *
 * No BOM here: a BOM pasted into a cell shows up as a stray character,
 * whereas a downloaded file needs it.
 */
export function toClipboard(
  table: ResultTable,
  options: ExportOptions = {},
): string {
  return toDelimited(table, {
    ...options,
    delimiter: '\t',
    bom: false,
    newline: '\n',
  });
}

// -- file names and email drafts, as upstream writes them --------------

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
  'Oct', 'Nov', 'Dec'];

/**
 * Upstream's export name: `<title> - EEE MMM dd yyyy HH_mm_ss`, local
 * time, so two exports of one cube never overwrite each other.
 */
export function exportFileName(title: string, at: Date): string {
  const two = (n: number): string => String(n).padStart(2, '0');
  return `${title} - ${DAYS[at.getDay()]} ${MONTHS[at.getMonth()]} ${two(at.getDate())}`
    + ` ${at.getFullYear()} ${two(at.getHours())}_${two(at.getMinutes())}_${two(at.getSeconds())}`;
}

/** Base64 of a string's UTF-8 bytes, wrapped at 76 as MIME wants. */
function base64Lines(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary).replace(/.{76}/g, '$&\n');
}

/**
 * An UNSENT email with the file attached -- upstream's Email, which
 * needs no mail host: the browser downloads the `.eml`, and opening it
 * gives a draft (`X-Unsent: 1`) in the user's own mail client. The
 * layout is upstream's, including no blank line before the headers,
 * which some clients (Outlook) will not read.
 */
export function toEml(attachment: {
  readonly name: string;
  readonly mime: string;
  readonly content: string;
}): string {
  const mixed = 'mixed_boundary';
  const alternative = 'alternative_boundary';
  return [
    'From:',
    'To:',
    'Subject:',
    'X-Unsent: 1',
    `Content-Type: multipart/mixed; boundary="${mixed}"`,
    '',
    `--${mixed}`,
    `Content-Type: multipart/alternative; boundary="${alternative}"`,
    '',
    `--${alternative}`,
    'Content-Type: text/plain; charset="UTF-8"',
    'Content-Transfer-Encoding: 7bit',
    '',
    '',
    '',
    `--${alternative}`,
    'Content-Type: text/html; charset="UTF-8"',
    'Content-Transfer-Encoding: 7bit',
    '',
    '<html><body><p></p><body></html>',
    '',
    `--${alternative}--`,
    '',
    `--${mixed}`,
    `Content-Type: ${attachment.mime}; name="${attachment.name}"`,
    'Content-Transfer-Encoding: base64',
    `Content-Disposition: attachment; filename="${attachment.name}"`,
    '',
    base64Lines(attachment.content),
    '',
    `--${mixed}--`,
  ].join('\n');
}
