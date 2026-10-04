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
import { flatHeader, type ExportTable } from './export-model.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';

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
        ? options.formatters!.format(v, options.formats?.[c.name], c.type)
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

/** Base64 of bytes (a string is taken as its UTF-8), wrapped at 76 as MIME wants, CRLF between lines. */
function base64Lines(content: string | Uint8Array): string {
  const bytes = typeof content === 'string' ? new TextEncoder().encode(content) : content;
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return (btoa(binary).match(/.{1,76}/g) ?? ['']).join('\r\n');
}

/** A header value as RFC 2047 encoded-words when it is not plain ASCII (each word within 75 characters). */
export function encodedWords(text: string): string {
  // eslint-disable-next-line no-control-regex
  if (/^[\x20-\x7e]*$/.test(text)) return text;
  const words: string[] = [];
  let chunk = '';
  const flush = (): void => {
    if (chunk) words.push(`=?UTF-8?B?${base64Lines(chunk).replace(/\r\n/g, '')}?=`);
    chunk = '';
  };
  for (const ch of text) {
    // 45 bytes of UTF-8 is 60 of base64: the word stays under 75 with its markers
    if (new TextEncoder().encode(chunk + ch).length > 45) flush();
    chunk += ch;
  }
  flush();
  return words.join('\r\n ');
}

/** A filename as a MIME parameter: an ASCII fallback, and RFC 2231's exact UTF-8 form beside it. */
function fileParam(key: 'name' | 'filename', name: string): string {
  const ascii = name.replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
  // eslint-disable-next-line no-control-regex
  if (/^[\x20-\x7e]*$/.test(name) && !/["\\]/.test(name)) return `${key}="${name}"`;
  const exact = encodeURIComponent(name).replace(/['()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
  return `${key}="${ascii}"; ${key}*=UTF-8''${exact}`;
}

const escapeHtml = (s: string): string =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

/**
 * An UNSENT email with the file attached -- upstream's Email, which needs no mail host: the
 * browser downloads the `.eml`, and opening it gives a draft (`X-Unsent: 1`) in the user's own
 * mail client.
 *
 * Written to the letter (the 2026-09-29 audit found the upstream layout breaking it): the
 * `MIME-Version` header, CRLF line endings, the subject in RFC 2047 when it is not ASCII,
 * the filename in RFC 2231's form beside an ASCII fallback, a text and an HTML body that say
 * what is attached, and the attachment's real BYTES (a PDF or a workbook is binary) in base64.
 */
export function toEml(message: {
  readonly subject: string;
  /** What the body says; the HTML body says the same. */
  readonly text: string;
  readonly attachment: {
    readonly name: string;
    readonly mime: string;
    readonly content: string | Uint8Array;
  };
}): string {
  const mixed = '=_mixed_datacube';
  const alternative = '=_alternative_datacube';
  const { attachment } = message;
  const html = `<html><body><p>${escapeHtml(message.text).replace(/\n/g, '<br>')}</p></body></html>`;
  return [
    'MIME-Version: 1.0',
    'From:',
    'To:',
    `Subject: ${encodedWords(message.subject)}`,
    'X-Unsent: 1',
    `Content-Type: multipart/mixed; boundary="${mixed}"`,
    '',
    `--${mixed}`,
    `Content-Type: multipart/alternative; boundary="${alternative}"`,
    '',
    `--${alternative}`,
    'Content-Type: text/plain; charset="UTF-8"',
    'Content-Transfer-Encoding: base64',
    '',
    base64Lines(message.text),
    `--${alternative}`,
    'Content-Type: text/html; charset="UTF-8"',
    'Content-Transfer-Encoding: base64',
    '',
    base64Lines(html),
    `--${alternative}--`,
    '',
    `--${mixed}`,
    `Content-Type: ${attachment.mime}; ${fileParam('name', attachment.name)}`,
    'Content-Transfer-Encoding: base64',
    `Content-Disposition: attachment; ${fileParam('filename', attachment.name)}`,
    '',
    base64Lines(attachment.content),
    `--${mixed}--`,
    '',
  ].join('\r\n');
}

// -- CSV of the grid as shown ------------------------------------------------------------

/**
 * The grid AS SHOWN as CSV (export-model.ts): its columns in its order under its headers,
 * blurred ones REDACTED, raw values so the file can be computed on again. A grouped cube gets
 * a leading `Level` column (1 = top), because a CSV has no other way to say which rows are
 * groups and which are their children.
 */
export function exportCsv(table: ExportTable): string {
  const d = ',';
  const header = [
    ...(table.grouped ? ['Level'] : []),
    ...table.columns.map((c) => flatHeader(c)),
  ].map((h) => escapeField(h, d)).join(d);
  const lines = [header];
  for (const row of table.rows) {
    const cells = row.cells.map((v) => escapeField(rawText(v), d));
    lines.push([...(table.grouped ? [String(row.depth)] : []), ...cells].join(d));
  }
  return `\ufeff${lines.join('\r\n')}\r\n`;
}
