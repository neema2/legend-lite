// EXCEL'S NUMBER FORMAT CODES (docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md P3): a column's format written as Excel writes
// one -- `#,##0.0,,"M"`, `0.0%`, `$#,##0;[Red]($#,##0)`, `yyyy-mm-dd`, `dd mmm yyyy hh:mm` -- read once into its sections
// (positive; negative; zero; text) and each section's parts: its colour, its literal text, its digits, its grouping,
// the thousands its trailing commas divide by, a percent, a date's or time's fields. A cell's text is the section its
// value takes, its colour the section's (`[Red]`); the xlsx export writes the code itself, so the file shows what the
// grid does. A code this does not read -- a condition (`[>100]`), a fraction (`# ?/?`), an exponent -- is refused in
// words when it is set, never shown half-right. Presentation only: the value is the database's.

import type { Scalar } from '../../engine-client/src/result.ts';
import { CubeRefusal } from './snapshot.ts';

/** Excel's colour names, as a section's `[Red]` names them. */
const COLOURS: Readonly<Record<string, string>> = {
  black: '#000000', blue: '#0000ff', cyan: '#00ffff', green: '#008000', magenta: '#ff00ff', red: '#ff0000',
  white: '#ffffff', yellow: '#ffff00',
};

type Part =
  | { readonly kind: 'text'; readonly text: string }
  | { readonly kind: 'number' }
  | { readonly kind: 'date'; readonly field: DateField };

type DateField = 'yyyy' | 'yy' | 'mmmm' | 'mmm' | 'mm' | 'm' | 'dddd' | 'ddd' | 'dd' | 'd' | 'hh' | 'h' | 'MM' | 'M' | 'ss' | 's' | 'ampm';

/** One section of a code: what it draws and how. */
interface Section {
  readonly colour?: string;
  readonly parts: readonly Part[];
  /** Its number: the integer digits it always shows, the decimals it always and may show, grouping, its divisor. */
  readonly integers: number;
  readonly minDecimals: number;
  readonly maxDecimals: number;
  readonly grouping: boolean;
  readonly divide: number;
  readonly percent: boolean;
  /** A date's or a time's fields, not a number. */
  readonly date: boolean;
  /** The text section's `@`: the value itself. */
  readonly text: boolean;
}

/** A code read: its sections, by what each is for. */
export interface FormatCode {
  readonly code: string;
  readonly positive: Section;
  readonly negative?: Section;
  readonly zero?: Section;
  readonly text?: Section;
}

const read = new Map<string, FormatCode>();

/** A code read into its sections, or refused in words: what of it this does not read. Read once per code. */
export function readFormatCode(code: string): FormatCode {
  const had = read.get(code);
  if (had) return had;
  if (code.trim() === '') throw new CubeRefusal('a format code is some code, `#,##0.00` or `yyyy-mm-dd`, not nothing');
  const sections = splitSections(code).map((s) => readSection(s, code));
  if (sections.length > 4) throw new CubeRefusal(`a format code has at most four sections (positive;negative;zero;text), and "${code}" has ${sections.length}`);
  const [positive, negative, zero, text] = sections as [Section, Section?, Section?, Section?];
  const made: FormatCode = { code, positive, ...(negative ? { negative } : {}), ...(zero ? { zero } : {}), ...(text ? { text } : {}) };
  read.set(code, made);
  return made;
}

/** A code's sections, split at the semicolons outside its quotes and brackets. */
function splitSections(code: string): string[] {
  const out: string[] = [];
  let at = 0;
  let quoted = false;
  let bracket = false;
  for (let i = 0; i < code.length; i++) {
    const c = code[i]!;
    if (c === '\\' && !quoted) { i += 1; continue; }
    if (c === '"') quoted = !quoted;
    else if (!quoted && c === '[') bracket = true;
    else if (!quoted && c === ']') bracket = false;
    else if (!quoted && !bracket && c === ';') { out.push(code.slice(at, i)); at = i + 1; }
  }
  out.push(code.slice(at));
  return out;
}

const DATE_TOKENS = /^(yyyy|yy|mmmm|mmm|mm|m|dddd|ddd|dd|d|hh|h|ss|s|am\/pm|a\/p)/i;

function readSection(s: string, code: string): Section {
  const parts: Part[] = [];
  let colour: string | undefined;
  let text = '';
  const flush = (): void => {
    if (text !== '') parts.push({ kind: 'text', text });
    text = '';
  };
  let integers = 0;
  let minDecimals = 0;
  let maxDecimals = 0;
  let grouping = false;
  let trailingCommas = 0;
  let percent = false;
  let date = false;
  let isText = false;
  let inNumber = false;
  let afterPoint = false;
  let numberDone = false;
  const said = (what: string): never => {
    throw new CubeRefusal(`the format code "${code}" has ${what}, which DataCube does not read`);
  };
  // a fraction (`# ?/?`): digits either side of a slash, outside quotes
  if (/[0#?]\s*\/\s*[0#?\d]/.test(s.replace(/"[^"]*"/g, ''))) said('a fraction');
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    const rest = s.slice(i);
    if (c === '"') {
      const end = s.indexOf('"', i + 1);
      if (end < 0) said('a quote that is never closed');
      text += s.slice(i + 1, end);
      i = end;
      continue;
    }
    if (c === '\\') { text += s[i + 1] ?? ''; i += 1; continue; }
    // `_x` a space the width of x; `*x` x repeated to fill the cell: a space, and nothing
    if (c === '_') { text += ' '; i += 1; continue; }
    if (c === '*') { i += 1; continue; }
    if (c === '[') {
      const end = s.indexOf(']', i);
      if (end < 0) said('a bracket that is never closed');
      const inside = s.slice(i + 1, end);
      const named = COLOURS[inside.toLowerCase()];
      if (named) colour = named;
      // [$€-407]: a currency's symbol, its locale after the dash
      else if (inside.startsWith('$')) text += inside.slice(1).split('-')[0]!;
      else said(`[${inside}] (a condition or an elapsed time)`);
      i = end;
      continue;
    }
    if (c === '@') { flush(); isText = true; parts.push({ kind: 'text', text: '\u0000' }); continue; }
    if (c === '0' || c === '#' || c === '?') {
      if (numberDone) said('two numbers in one section');
      if (!inNumber) { flush(); parts.push({ kind: 'number' }); inNumber = true; }
      if (afterPoint) { maxDecimals += 1; if (c === '0') minDecimals += 1; }
      else if (c === '0') integers += 1;
      // a comma between digits groups the thousands
      if (trailingCommas > 0) { grouping = true; trailingCommas = 0; }
      continue;
    }
    if (inNumber && c === '.') { afterPoint = true; continue; }
    if (inNumber && c === ',') { trailingCommas += 1; continue; }
    if (inNumber) { inNumber = false; numberDone = true; }
    if (c === '%') { percent = true; text += '%'; continue; }
    if ((c === 'E' || c === 'e') && /^[Ee][+-]/.test(rest)) said('an exponent');
    const token = DATE_TOKENS.exec(rest);
    if (token) {
      flush();
      date = true;
      const t = token[1]!.toLowerCase();
      parts.push({ kind: 'date', field: t === 'am/pm' || t === 'a/p' ? 'ampm' : (t as DateField) });
      i += token[1]!.length - 1;
      continue;
    }
    text += c;
  }
  flush();
  if (date && parts.some((p) => p.kind === 'number')) said('a date and a number in one section');
  // `m` and `mm` are minutes after an hour or before a second, else months (Excel's rule)
  const fixed = parts.map((p, i): Part => {
    if (p.kind !== 'date' || (p.field !== 'm' && p.field !== 'mm')) return p;
    const before = parts.slice(0, i).reverse().find((q) => q.kind === 'date');
    const after = parts.slice(i + 1).find((q) => q.kind === 'date');
    const minutes = (before?.kind === 'date' && (before.field === 'h' || before.field === 'hh'))
      || (after?.kind === 'date' && (after.field === 's' || after.field === 'ss'));
    return minutes ? { kind: 'date', field: p.field === 'm' ? 'M' : 'MM' } : p;
  });
  return {
    ...(colour !== undefined ? { colour } : {}), parts: fixed, integers, minDecimals, maxDecimals, grouping,
    divide: 1000 ** trailingCommas, percent, date, text: isText,
  };
}

/** The section a value takes: a negative's, a zero's, a text's, else the first. */
function sectionOf(c: FormatCode, n: number | null): { section: Section; signed: boolean } | null {
  if (n === null) return c.text ? { section: c.text, signed: false } : null;
  if (n < 0 && c.negative) return { section: c.negative, signed: false };
  if (n === 0 && c.zero) return { section: c.zero, signed: false };
  return { section: c.positive, signed: n < 0 };
}

const MONTHS_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const DAYS_LONG = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

/** A date's or a timestamp's text as its fields, in UTC as stored; null for text that is not one. */
function fieldsOf(value: Scalar): Date | null {
  if (typeof value !== 'string') return null;
  const m = /^(-?\d{4,})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2}))?/.exec(value);
  if (!m) return null;
  const at = new Date(0);
  at.setUTCFullYear(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  at.setUTCHours(Number(m[4] ?? 0), Number(m[5] ?? 0), Number(m[6] ?? 0), 0);
  return at;
}

function dateText(section: Section, at: Date): string {
  const twelve = section.parts.some((p) => p.kind === 'date' && p.field === 'ampm');
  const pad = (n: number): string => String(n).padStart(2, '0');
  const hour = twelve ? ((at.getUTCHours() + 11) % 12) + 1 : at.getUTCHours();
  return section.parts.map((p) => {
    if (p.kind === 'text') return p.text;
    if (p.kind === 'number') return '';
    switch (p.field) {
      case 'yyyy': return String(at.getUTCFullYear());
      case 'yy': return pad(at.getUTCFullYear() % 100);
      case 'mmmm': return MONTHS_LONG[at.getUTCMonth()]!;
      case 'mmm': return MONTHS_LONG[at.getUTCMonth()]!.slice(0, 3);
      case 'mm': return pad(at.getUTCMonth() + 1);
      case 'm': return String(at.getUTCMonth() + 1);
      case 'dddd': return DAYS_LONG[at.getUTCDay()]!;
      case 'ddd': return DAYS_LONG[at.getUTCDay()]!.slice(0, 3);
      case 'dd': return pad(at.getUTCDate());
      case 'd': return String(at.getUTCDate());
      case 'hh': return pad(hour);
      case 'h': return String(hour);
      case 'MM': return pad(at.getUTCMinutes());
      case 'M': return String(at.getUTCMinutes());
      case 'ss': return pad(at.getUTCSeconds());
      case 's': return String(at.getUTCSeconds());
      case 'ampm': return at.getUTCHours() < 12 ? 'AM' : 'PM';
    }
  }).join('');
}

const numberFormats = new Map<string, Intl.NumberFormat>();

function numberText(section: Section, n: number, signed: boolean): string {
  const key = `${section.integers}|${section.minDecimals}|${section.maxDecimals}|${section.grouping}`;
  let intl = numberFormats.get(key);
  if (!intl) {
    intl = new Intl.NumberFormat('en-US', {
      minimumIntegerDigits: Math.max(1, section.integers), minimumFractionDigits: section.minDecimals,
      maximumFractionDigits: section.maxDecimals, useGrouping: section.grouping,
    });
    numberFormats.set(key, intl);
  }
  const scaled = Math.abs(n) * (section.percent ? 100 : 1) / section.divide;
  // `#` alone, its integer part zero: Excel writes nothing before the point (`.5`); `0` writes the zero
  let digits = intl.format(scaled);
  if (section.integers === 0 && digits.startsWith('0')) digits = digits.slice(1);
  const body = section.parts.map((p) => (p.kind === 'number' ? digits : p.kind === 'text' ? p.text : '')).join('');
  // a minus only on a value still negative at the places shown (as the grid's own numbers)
  return signed && /[1-9]/.test(digits) ? `-${body}` : body;
}

/**
 * A VALUE'S TEXT AND COLOUR BY A CODE: a number (its column numeric: `numeric`) by its section's digits and literals; a
 * date or a timestamp by its fields; a text by the text section's `@`. Null where the code has nothing for the value
 * (a text where it has no text section): the grid then writes the value as it is.
 */
export function formatByCode(code: FormatCode, value: Scalar, numeric: boolean): { text: string; colour?: string } | null {
  const n = !numeric ? null : typeof value === 'number' ? value : typeof value === 'bigint' ? Number(value)
    : typeof value === 'string' && /^-?\d+(\.\d+)?$/.test(value) ? Number(value) : null;
  if (code.positive.date) {
    const at = fieldsOf(value);
    if (at === null) return null;
    return { text: dateText(code.positive, at), ...(code.positive.colour ? { colour: code.positive.colour } : {}) };
  }
  const chosen = sectionOf(code, n);
  if (chosen === null) return null;
  const { section, signed } = chosen;
  const text = n === null
    ? section.parts.map((p) => (p.kind === 'text' ? (p.text === '\u0000' ? String(value) : p.text) : '')).join('')
    : section.parts.some((p) => p.kind === 'number') ? numberText(section, n, signed)
      : section.parts.map((p) => (p.kind === 'text' ? p.text : '')).join('');
  return { text, ...(section.colour ? { colour: section.colour } : {}) };
}
