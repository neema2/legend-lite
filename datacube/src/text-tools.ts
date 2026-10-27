// THE TEXT TOOLS (docs/DATACUBE_FLOW_MODE_2026_10_24.md M3: a flow's Text To Columns, RegEx and Data Cleansing,
// Alteryx's Parse and Preparation tools): each the calculated columns DataCube already has, as calc-presets.ts's
// presets are -- an expression over the row the planner plans and the database computes, never a value computed here.
// What a tool is set to is the expression it makes; what a box says it does is read back off that expression's shape
// (`textToolOf`), never off a column's name.
//
//   Text To Columns   a text column split on a delimiter into its first N parts: `splitPart`, a column a part -- or
//                     into rows: every part its own row (`split`, exploded: one column, each row once a part)
//   RegEx             a pattern over a text column: whether it matches (`regexpLike`), the text with each match
//                     replaced (`regexpReplace`), or its first match (`regexpExtract`)
//   Data Cleansing    text columns tidied, a column each: empties made blank (`coalesce`), punctuation taken out,
//                     repeated spaces made one (`regexpReplace`), the ends trimmed (`trim`), the case changed
//
// The columns each makes are text-tool-columns.ts's, fetched with the first tool set; what is here is what drawing a box
// needs (its words) and what its window checks.
//
// PURE: no DOM, no planner.

import type { ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { DerivedColumn } from './snapshot.ts';

/** How a RegEx tool reads its pattern: a true/false column, the text with its matches replaced, or its first match. */
export type RegexMode = 'match' | 'replace' | 'extract';

/** What Data Cleansing does to each column it is given. */
export interface Cleansing {
  /** An empty value made blank: `coalesce($x.c, '')`. */
  readonly blanks?: boolean;
  /** Every character but a letter, a digit or a space taken out. */
  readonly punctuation?: boolean;
  /** Repeated spaces (tabs, line breaks) made one space. */
  readonly spaces?: boolean;
  /** The spaces at its ends taken off. */
  readonly trim?: boolean;
  /** Its letters made upper or lower case. */
  readonly letterCase?: 'upper' | 'lower';
}

/** The tools, as their windows set them. */
export type TextTool =
  /** `rows`: each part its own row (one column, exploded), `parts` unread; else its first `parts` parts, a column each. */
  | { readonly kind: 'split'; readonly column: string; readonly delimiter: string; readonly parts: number; readonly name: string;
    readonly rows?: boolean }
  | { readonly kind: 'regex'; readonly column: string; readonly pattern: string; readonly mode: RegexMode; readonly replacement?: string;
    readonly name: string }
  /** `inPlace`: each cleansed column handed on by its column's name, the column hidden (box-columns.ts). */
  | { readonly kind: 'cleanse'; readonly columns: readonly string[]; readonly cleansing: Cleansing; readonly inPlace?: boolean };

/** The pattern Data Cleansing takes punctuation out with: anything but a letter, a digit or a space -- any script's
 *  (`\p{L}`, `\p{N}`: `é`, `ü`, `東` are letters, kept), never ASCII's alone. */
export const PUNCTUATION = '[^\\p{L}\\p{N}\\s]';
/** The pattern Data Cleansing makes repeated spaces one with. */
export const SPACES = '\\s+';
/** What a cleansed column is called: its column's name and this. */
export const CLEAN_SUFFIX = '_clean';

/** What a column made by a text tool does, read off its expression: the tool, and a few words of it. */
export interface TextToolSaid {
  readonly tool: 'textToColumns' | 'regex' | 'cleanse';
  readonly says: string;
}

/**
 * THE TOOL A COLUMN IS MADE BY, from its expression's shape -- whoever wrote it, the tool's window or a person typing
 * the same into a Formula: a part of a split, a pattern over a column, or a column cleansed (a chain of the cleansing
 * functions over it). Anything else, none.
 */
export function textToolOf(d: DerivedColumn): TextToolSaid | undefined {
  const param = d.lambda?.parameters[0]?.name;
  const body = d.lambda?.body;
  if (param === undefined || body?.length !== 1) return undefined;
  const top = body[0]!;
  if (top._type !== 'func') return undefined;
  const read = (v: ValueSpecification | undefined): string | undefined => {
    // `$x.c`, or `$x.c->toOne()`
    const p = v?._type === 'func' && v.function === 'toOne' && v.parameters.length === 1 ? v.parameters[0] : v;
    if (p?._type !== 'property' || p.parameters.length !== 1) return undefined;
    const owner = p.parameters[0]!;
    return owner._type === 'var' && owner.name === param ? p.property : undefined;
  };
  const text = (v: ValueSpecification | undefined): string | undefined => (v?._type === 'string' ? v.value : undefined);
  const [first, second] = top.parameters;
  if (top.function === 'splitPart') {
    const column = read(first);
    const on = text(second);
    return column !== undefined && on !== undefined ? { tool: 'textToColumns', says: `${column} split on '${on}'` } : undefined;
  }
  // each part its own row: an exploded split (of its text, an empty one blank)
  if (top.function === 'split' && d.unnest === true) {
    const blanked = first?._type === 'func' && first.function === 'coalesce' && first.parameters.length === 2
      && text(first.parameters[1]) === '' ? first.parameters[0] : first;
    const column = read(blanked);
    const on = text(second);
    return column !== undefined && on !== undefined ? { tool: 'textToColumns', says: `${column} split on '${on}' to rows` } : undefined;
  }
  // a pattern straight over a column
  const inner = top.function === 'first' && first?._type === 'func' && first.function === 'regexpExtract' ? first : top;
  const over = read(inner.parameters[0]);
  const pattern = text(inner.parameters[1]);
  if (over !== undefined && pattern !== undefined && !(inner.function === 'regexpReplace' && isCleansingPattern(pattern, inner))) {
    if (inner.function === 'regexpLike') return { tool: 'regex', says: `${over} matches /${pattern}/` };
    if (inner.function === 'regexpReplace') return { tool: 'regex', says: `/${pattern}/ replaced in ${over}` };
    if (inner.function === 'regexpExtract' && inner !== top) return { tool: 'regex', says: `/${pattern}/ of ${over}` };
  }
  // a chain of the cleansing functions, down to a column
  const cleaned = cleansedColumn(top, read);
  return cleaned === undefined ? undefined : { tool: 'cleanse', says: `${cleaned} cleansed` };
}

/** Whether a replacement is one of Data Cleansing's own: its punctuation out, its spaces one. */
function isCleansingPattern(pattern: string, call: Extract<ValueSpecification, { _type: 'func' }>): boolean {
  const by = call.parameters[2];
  const to = by?._type === 'string' ? by.value : undefined;
  return (pattern === PUNCTUATION && to === '') || (pattern === SPACES && to === ' ');
}

const CLEANSING = new Set(['trim', 'toUpper', 'toLower', 'regexpReplace', 'coalesce']);

/** The column a chain of cleansing functions starts from, if `v` is one: none when anything else is in it. */
function cleansedColumn(v: ValueSpecification, read: (v: ValueSpecification | undefined) => string | undefined): string | undefined {
  let at: ValueSpecification | undefined = v;
  let steps = 0;
  while (at?._type === 'func' && CLEANSING.has(at.function)) {
    const pattern = at.parameters[1];
    if (at.function === 'regexpReplace' && !isCleansingPattern(pattern?._type === 'string' ? pattern.value : '', at)) return undefined;
    if (at.function === 'coalesce' && (at.parameters.length !== 2 || pattern?._type !== 'string' || pattern.value !== '')) return undefined;
    steps += 1;
    at = at.parameters[0];
  }
  return steps === 0 ? undefined : read(at);
}
