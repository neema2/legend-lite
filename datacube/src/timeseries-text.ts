// A SERIES' OWN #TS TEXT, AND CORE'S TREE OF IT READ BACK (A3 step 3, docs/TS_A3_ONE_READER_PLAN_2026_11_03.md §6):
// what a tile stores of a series once it stores text -- the item a formula box holds, in the language's own words (the
// spec's §7, docs/TIMESERIES_LANGUAGE_2026_10_29.md) -- and the way back from core's reading of it (LitePlanner.tsItem,
// core's TsTree) to the tile's tree, so the tile's walkers keep working while each moves to core's facts.
//
// The text is the series' OWN: another series it reads is named (`A`, the tile's `$A`), never inlined, so editing the
// one moves the other; a column of a source other than the tile's first is read through its alias (`rates.yield`); a
// subscript picks an instrument (`close['SPX']`) or matches a column (`close[venue = 'NYSE']`); a period's aggregate of
// a column is written (`sum(volume['AAPL'])`, the tile's `.sum`); every parameter is written, defaults too (a window is
// always written: §7). What the text means is the language's; the tile's own per-point rules (a comparison 1 or 0, a
// bound held at none) are its query writer's until A3 step 5 (timeseries-ts.ts).
//
// NOTHING HERE PARSES. Text goes to core; core's tree comes back and is mapped, refusing what no tile tree is.

import type { TsItemAnswer, TsNode } from './lite-planner.ts';
import { CubeRefusal } from './snapshot.ts';
import {
  treeProblem, TS_SIGNATURES, type TsAggregate, type TsExpr, type TsFunctionName, type TsMatch, type TsOperator, type TsSeries,
} from './timeseries-spec.ts';

/** The words whose parameters the language takes by place (the platform's builder's own: TsToPure's), the rest by
 *  name (`w => 21`). */
const BY_PLACE: ReadonlySet<string> = new Set(['mean', 'sum', 'min', 'max', 'std', 'var', 'returns', 'diff', 'lag']);

/** A period's aggregate of a column as the language names it (R5), and back. */
const TAKEN: Readonly<Record<TsAggregate, string>> = { last: 'last', first: 'first', average: 'avg', sum: 'sum', min: 'min', max: 'max' };
const TAKEN_BACK: ReadonlyMap<string, TsAggregate> = new Map(Object.entries(TAKEN).map(([k, v]) => [v, k as TsAggregate]));

/** The tile's words the language spells otherwise (§7): gs-quant's natural log, its bounds, its whole line's first and
 *  last, its count to date. */
const SPELLED: Readonly<Partial<Record<TsFunctionName, string>>> = { log: 'ln', floor: 'greatest', ceil: 'least' };
const SPELLED_BACK: ReadonlyMap<string, TsFunctionName> = new Map(Object.entries(SPELLED).map(([k, v]) => [v, k as TsFunctionName]));
/** The words whose window of the whole line is written after the series: `first(x, all)`, `last(x, whole)`,
 *  `count(x, all)`. */
const WHOLE: Readonly<Partial<Record<TsFunctionName, 'all' | 'whole'>>> = { first: 'all', last: 'whole', count: 'all' };

/** gs-quant's functions this DataCube does not compute, each with why (gs-quant 2.2.3's timeseries package): refused by
 *  name where a series' text calls one. */
export const NOT_BUILT: Readonly<Record<string, string>> = {
  smooth_outliers: 'an iterative procedure (rounds of a centred rolling median until no new outlier), which no query states: smooth_spikes takes out a value beyond both its neighbours',
  repeat: 'it makes dates the data does not have: give the tile a frequency, or combine two series with its align',
  date_range: 'the tile\'s dates are its range: set it',
  filter_dates: 'the tile\'s dates are its range: set it',
  bucketize: 'a value per period is the tile\'s frequency: set it',
  align_calendar: 'a holiday calendar is none of the tile\'s sources',
  day_countdown: 'it makes dates the data does not have',
  generate_series: 'random data: read a grid of your own',
  corr_swap_correlation: 'it needs an index\'s constituents and their weights',
  mode: 'the most frequent value of a window: no aggregate every database shares',
};

/** A failure to read a series' text: core refused it, or its reading is no series a tile draws -- said where it is drawn;
 *  `at` its place in the text (line and column, from 1), where core gave one. */
export class TsTextRefusal extends CubeRefusal {
  readonly at: readonly [number, number] | undefined;

  constructor(message: string, at?: readonly [number, number]) {
    super(message);
    this.at = at;
  }
}

// ------------------------------------------------------------------ the readings

/** What reads a series' text: core's one reader (LitePlanner.tsItem: the tab's legend-lite module, Python's native
 *  library), never DataCube. */
export type TsReader = (text: string) => Promise<TsItemAnswer>;

/**
 * CORE'S READINGS OF SERIES' TEXTS, each a function of its text alone: its tree with every bare name left a column (which
 * one is another series is the tile's to say, treeOf's), or why it is no series. A text the tile writes of a tree it holds
 * (writtenText) is read back as that tree -- the round trip, proved over every example series and every form the tile
 * writes (timeseries-ts-parity.test.ts) -- so it is held as read without asking.
 */
const READINGS = new Map<string, TsExpr | TsTextRefusal>();

/** Every text of `series` read by core, each once (held for the tab: a reading is its text's). */
export async function readSeriesTexts(series: readonly TsSeries[], reader: TsReader): Promise<void> {
  const unread = [...new Set(series.map((s) => s.text))].filter((t) => !READINGS.has(t));
  await Promise.all(unread.map(async (text) => {
    READINGS.set(text, readingOf(await reader(text)));
  }));
}

/** Core's answer for a text, as the tile holds its reading. */
function readingOf(read: TsItemAnswer): TsExpr | TsTextRefusal {
  if (read.text === undefined) {
    return new TsTextRefusal(`${read.error.message} (line ${read.error.line}, column ${read.error.column})`, [read.error.line, read.error.column]);
  }
  if (read.item.alias !== null) {
    return new TsTextRefusal(`a series' text is its expression: its name is its id, not AS ${read.item.alias}`);
  }
  try {
    const tree = told(seriesExpr(read.item.expr!, []));
    const problem = treeProblem(tree, 'it', undefined);
    return problem === null ? tree : new TsTextRefusal(problem);
  } catch (e) {
    if (e instanceof TsTextRefusal) return e;
    throw e;
  }
}

/** Whether every text of `series` has its reading held: a tile drawn at once, no reading asked. */
export function allRead(series: readonly TsSeries[]): boolean {
  return series.every((s) => READINGS.has(s.text));
}

/** The reading held of `text`, as core read it or as the tile wrote it -- undefined where it was never read. */
export function heldReading(text: string): { readonly text: string } | { readonly refused: TsTextRefusal } | undefined {
  const r = READINGS.get(text);
  return r === undefined ? undefined : r instanceof TsTextRefusal ? { refused: r } : { text };
}

/** Core's canonical text of what a person typed, or why it is no series: what the tile keeps of it (each series' text
 *  as core prints it, so two texts of one meaning are one text). */
export async function readTyped(text: string, reader: TsReader): Promise<{ readonly text: string } | { readonly refused: TsTextRefusal }> {
  const read = await reader(text);
  const reading = readingOf(read);
  if (reading instanceof TsTextRefusal) return { refused: reading };
  READINGS.set(read.text!, reading);
  return { text: read.text! };
}

/** The text of a tree the tile holds, `series` the tile's (whose ids name the series it reads): its reading held. */
export function writtenText(tree: TsExpr, series: readonly TsSeries[]): string {
  const text = seriesText(tree, series.map((s) => s.id));
  if (!READINGS.has(text)) READINGS.set(text, told(unref(tree)));
  return text;
}

/** Why `s`'s text is no series, in words; null where it reads (it must have been read: readSeriesTexts). */
export function textProblem(s: TsSeries): string | null {
  const r = READINGS.get(s.text);
  if (r === undefined) throw new Error(`series ${s.id}'s text was never read (readSeriesTexts first): ${s.text}`);
  return r instanceof TsTextRefusal ? `Series ${s.id}: ${r.message}` : null;
}

/**
 * A SERIES' TREE, the one way every walker reads it: core's reading of its text, `series` the tile's, whose ids name the
 * series it reads (`A`). A text never read is a fault of the caller's (readSeriesTexts first); one core refused, its
 * refusal.
 */
export function treeOf(s: TsSeries, series: readonly TsSeries[]): TsExpr {
  const r = READINGS.get(s.text);
  if (r === undefined) throw new Error(`series ${s.id}'s text was never read (readSeriesTexts first): ${s.text}`);
  if (r instanceof TsTextRefusal) throw new TsTextRefusal(`series ${s.id}: ${r.message}`);
  const ids = series.map((x) => x.id);
  return ids.length === 0 ? r : reffed(r, ids);
}

/** `e`'s bare names of `ids` read as those series: a column of no source, subscript or aggregate named as one. */
function reffed(e: TsExpr, ids: readonly string[]): TsExpr {
  switch (e.kind) {
    case 'column':
      return e.source === undefined && e.fn === undefined && (e.where ?? []).length === 0 && ids.includes(e.column)
        ? { kind: 'ref', series: e.column } : e;
    case 'binary':
      return { ...e, left: reffed(e.left, ids), right: reffed(e.right, ids) };
    case 'call':
      return { ...e, args: e.args.map((a) => reffed(a, ids)) };
    default:
      return e;
  }
}

/** `e` with each series it reads a bare name: as its reading holds it, of its text alone. */
function unref(e: TsExpr): TsExpr {
  switch (e.kind) {
    case 'ref':
      return { kind: 'column', column: e.series };
    case 'binary':
      return { ...e, left: unref(e.left), right: unref(e.right) };
    case 'call':
      return { ...e, args: e.args.map(unref) };
    default:
      return e;
  }
}

// ------------------------------------------------------------------ the formula box's completions

/** A completion of the formula box: what it puts over the text it completes, what the list shows, and why. */
export interface TsCompletion {
  readonly insert: string;
  readonly label: string;
  readonly detail: string;
  readonly kind: 'word' | 'column' | 'series' | 'instrument';
}

/** What the box may offer (decision 9b): core's words the tile reads (core's catalog, each its name and why), the tile's
 *  sources' columns (each its source's alias), its other series, and -- in a column's subscript -- the instruments of
 *  that column's source, asked of the database (`instruments`, by the column the subscript is of). */
export interface TsOffer {
  readonly words: readonly { readonly name: string; readonly detail: string }[];
  readonly columns: readonly { readonly source: string; readonly name: string; readonly detail: string }[];
  readonly series: readonly { readonly id: string; readonly detail: string }[];
  readonly instruments?: { readonly of: string; readonly values: readonly string[] };
}

/** What the box completes at the caret: the text [from, to) the chosen one replaces, the completions, and -- in a
 *  subscript -- the column it is of (`subscript`: the source's alias and the column), whose instruments the box asks. */
export interface TsCompleting {
  readonly from: number;
  readonly to: number;
  readonly items: readonly TsCompletion[];
  readonly subscript?: { readonly source: string | undefined; readonly column: string };
}

/** Whether the tile reads `word` (a word of core's catalog): its own spelling's, or the language's of one of its own
 *  (ln, greatest, least) -- what the box offers. */
export function tileReads(word: string): boolean {
  return SPELLED_BACK.has(word) || (Object.hasOwn(TS_SIGNATURES, word) && !Object.hasOwn(SPELLED, word));
}

/** The words the box offers: core's catalog's the tile reads (its label, else its description), and the tile's words
 *  that are SQL's per point (the spec's R3: ln, greatest, sqrt, ...), which no library word is. */
export function offeredWords(catalog: Readonly<Record<string, { readonly doc: string | null; readonly facts: Readonly<Record<string, readonly string[]>> }>>):
  { readonly name: string; readonly detail: string }[] {
  const out = Object.entries(catalog).filter(([name]) => tileReads(name))
    .map(([name, w]) => ({ name, detail: w.facts['label']?.[0] ?? w.doc ?? '' }));
  for (const fn of Object.keys(TS_SIGNATURES) as TsFunctionName[]) {
    const name = SPELLED[fn] ?? fn;
    if (!Object.hasOwn(catalog, fn) && !Object.hasOwn(catalog, name) && tileReads(name)) out.push({ name, detail: `SQL's ${name}, per point` });
  }
  return out;
}

/**
 * THE COMPLETIONS AT `caret` IN `text` (A3 step 3, decision 9b): the name being written -- a series, a column (of the
 * source its alias names, `px.c`), a word -- or, in a column's subscript, an instrument. Found by the characters before
 * the caret alone: no reading of the text, which is core's.
 */
export function completionsAt(text: string, caret: number, offer: TsOffer): TsCompleting {
  const before = text.slice(0, caret);
  const after = /^[A-Za-z0-9_]*/.exec(text.slice(caret))![0].length;
  const none: TsCompleting = { from: caret, to: caret, items: [] };
  // in a subscript: close['SP| or close[venue = 'X', 'SP|
  const sub = /([A-Za-z_][A-Za-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))?\[(?:[^\][]*,\s*)?('?)([^',\][]*)$/.exec(before);
  if (sub !== null) {
    const [whole, a, b, quote, typed] = sub;
    const subscript = b === undefined ? { source: undefined, column: a! } : { source: a!, column: b };
    const from = caret - typed!.length - quote!.length;
    const values = offer.instruments !== undefined && offer.instruments.of === `${subscript.source ?? ''}.${subscript.column}`
      ? offer.instruments.values : [];
    void whole;
    const items = values.filter((v) => v.toLowerCase().startsWith(typed!.toLowerCase())).slice(0, 12)
      .map((v): TsCompletion => ({ insert: `'${v.replaceAll("'", "''")}'`, label: v, detail: `an instrument of ${subscript.column}`, kind: 'instrument' }));
    return { from, to: caret, items, subscript };
  }
  // a source's column: px.cl|
  const dotted = /([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)?$/.exec(before);
  if (dotted !== null) {
    const typed = dotted[2] ?? '';
    const items = offer.columns.filter((c) => c.source === dotted[1] && c.name.toLowerCase().startsWith(typed.toLowerCase())).slice(0, 12)
      .map((c): TsCompletion => ({ insert: ident(c.name), label: c.name, detail: c.detail, kind: 'column' }));
    return { from: caret - typed.length, to: caret + after, items };
  }
  const word = /[A-Za-z_][A-Za-z0-9_]*$/.exec(before);
  if (word === null) return none;
  // smart case: what is typed in lower case matches any case; a capital in it, its case (B: series B, not basket)
  const typed = word[0];
  const exact = /[A-Z]/.test(typed);
  const starts = (name: string): boolean => (exact ? name.startsWith(typed) : name.toLowerCase().startsWith(typed));
  const first = offer.columns[0]?.source;
  const items: TsCompletion[] = [
    ...offer.series.filter((s) => starts(s.id))
      .map((s): TsCompletion => ({ insert: s.id, label: s.id, detail: s.detail, kind: 'series' })),
    // a column of the tile's first source by its name, another's through its alias (the tile's tree: a column of no
    // source reads the first)
    ...offer.columns.filter((c) => starts(c.name))
      .map((c): TsCompletion => {
        const name = c.source === first ? ident(c.name) : `${ident(c.source)}.${ident(c.name)}`;
        return { insert: name, label: name, detail: c.detail, kind: 'column' };
      }),
    ...offer.words.filter((w) => starts(w.name))
      .map((w): TsCompletion => ({ insert: `${w.name}(`, label: w.name, detail: w.detail, kind: 'word' })),
  ];
  return { from: caret - word[0].length, to: caret + after, items: items.slice(0, 12) };
}

// ------------------------------------------------------------------ the writer

/** Precedences as core's printer has them (protocol/ts TsPrinter): a node in parentheses where it binds less tightly
 *  than its place asks. */
const OR = 1;
const AND = 2;
const NOT = 3;
const COMPARE = 5;
const ADD = 8;
const MULTIPLY = 9;
const SIGN = 11;
const ATOM = 12;

const PRECEDENCE: Readonly<Record<TsOperator, number>> = {
  or: OR, and: AND, '<': COMPARE, '<=': COMPARE, '>': COMPARE, '>=': COMPARE, '==': COMPARE, '!=': COMPARE,
  '+': ADD, '-': ADD, '*': MULTIPLY, '/': MULTIPLY,
};
const OPERATOR: Readonly<Record<TsOperator, string>> = {
  or: 'OR', and: 'AND', '<': '<', '<=': '<=', '>': '>', '>=': '>=', '==': '=', '!=': '<>', '+': '+', '-': '-', '*': '*', '/': '/',
};
const OPERATOR_BACK: Readonly<Record<string, TsOperator>> = {
  OR: 'or', AND: 'and', LT: '<', LE: '<=', GT: '>', GE: '>=', EQ: '==', NE: '!=', PLUS: '+', MINUS: '-', TIMES: '*', DIVIDE: '/',
};

/** A name as the language reads it: as it is where it is a plain lower-case word, else quoted. */
function ident(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}

/** A number as core prints it: plain, never in exponent form. */
function numberText(n: number): string {
  if (!Number.isFinite(n)) throw new TsTextRefusal(`${n} is no number a series is written with`);
  const plain = String(Math.abs(n));
  const text = /e/i.test(plain) ? Math.abs(n).toFixed(20).replace(/0+$/, '').replace(/\.$/, '') : plain;
  return n < 0 ? `-${text}` : text;
}

function literal(v: string | number | boolean): string {
  return typeof v === 'string' ? `'${v.replaceAll("'", "''")}'` : typeof v === 'boolean' ? (v ? 'TRUE' : 'FALSE') : numberText(v);
}

/** A window or a lag as the language writes it: observations, `all` for the tile's 0, a span (`3mo` for its `3m`). */
function span(v: number | string): string {
  if (typeof v === 'number') return v === 0 ? 'all' : numberText(v);
  const m = /^([1-9][0-9]*)([dwmy])$/.exec(v);
  if (!m) throw new TsTextRefusal(`a span is n then d, w, m or y: ${v}`);
  return `${m[1]}${m[2] === 'm' ? 'mo' : m[2]}`;
}

/** A parameter's value as the language takes it, by its kind. */
function paramText(fn: TsFunctionName, name: string, v: number | string): string {
  const p = TS_SIGNATURES[fn].params.find((x) => x.name === name)!;
  return p.kind === 'span' ? span(v) : p.kind === 'word' ? literal(String(v)) : typeof v === 'number' ? numberText(v) : String(v);
}

/**
 * THE OWN TEXT OF A SERIES' EXPRESSION, as core prints it back (the round trip, timeseries-ts-parity.test.ts): `ids`
 * the tile's series' ids -- a column named as one of them is refused unless read through its source, since it would
 * read back as that series. A column of no source named reads the tile's first, as the tile's tree says.
 */
export function seriesText(e: TsExpr, ids: readonly string[]): string {
  return written(e, 0, ids);
}

function written(e: TsExpr, min: number, ids: readonly string[]): string {
  const wrap = (p: number, text: string): string => (p < min ? `(${text})` : text);
  switch (e.kind) {
    case 'number':
      return wrap(e.value < 0 ? SIGN : ATOM, numberText(e.value));
    case 'ref':
      if (!/^[A-Z][A-Z0-9]*$/.test(e.series)) throw new TsTextRefusal(`a series' id is a capital letter: ${e.series}`);
      return e.series;
    case 'column': {
      if (e.source === undefined && ids.includes(e.column)) {
        throw new TsTextRefusal(`a column named ${e.column}, as a series is, is read through its source (px.${e.column})`);
      }
      const where = e.where ?? [];
      const col = `${e.source === undefined ? '' : `${ident(e.source)}.`}${ident(e.column)}`;
      const picked = where.length === 0 ? col : `${col}[${where.map(matchText).join(', ')}]`;
      return e.fn === undefined ? picked : `${TAKEN[e.fn]}(${picked})`;
    }
    case 'binary': {
      const p = PRECEDENCE[e.op];
      // left-associative, a comparison none: its left as its precedence allows, its right one above (core's printer)
      const chains = p !== COMPARE;
      return wrap(p, `${written(e.left, chains ? p : p + 1, ids)} ${OPERATOR[e.op]} ${written(e.right, p + 1, ids)}`);
    }
    case 'call': {
      if (e.fn === 'not') return wrap(NOT, `NOT ${written(e.args[0]!, NOT, ids)}`);
      const sig = TS_SIGNATURES[e.fn];
      const args = e.args.map((a) => written(a, 0, ids));
      const whole = WHOLE[e.fn];
      const value = (name: string): number | string => e.params?.[name] ?? sig.params.find((p) => p.name === name)!.default;
      const params = sig.params.map((p) => [p.name, paramText(e.fn, p.name, value(p.name))] as const);
      const by = BY_PLACE.has(e.fn) || e.fn === 'power' || e.fn === 'floor' || e.fn === 'ceil'
        ? params.map(([, v]) => v) : params.map(([n, v]) => `${n} => ${v}`);
      return `${SPELLED[e.fn] ?? e.fn}(${[...args, ...(whole === undefined ? [] : [whole]), ...by].join(', ')})`;
    }
  }
}

function matchText(m: TsMatch): string {
  return m.column === undefined ? literal(m.value) : `${ident(m.column)} = ${literal(m.value)}`;
}

// ------------------------------------------------------------------ core's tree, read back

/**
 * THE TILE'S TREE OF CORE'S READING of a series' own text: `ids` the tile's series' ids, a bare name of one of which
 * reads that series (`A`, the tile's `$A`). A form the tile has no tree for -- a CASE, a cast, a call of no word of the
 * tile's, a span of quarters -- is a {@link TsTextRefusal}, placed by its node.
 */
export function seriesExpr(node: TsNode, ids: readonly string[]): TsExpr {
  const no = (what: string, n: TsNode = node): never => {
    throw new TsTextRefusal(`${what} (line ${n.at[0]}, column ${n.at[1]})`, n.at);
  };
  const part = (name: string): TsNode => node[name] as TsNode;
  switch (node.k) {
    case 'number':
      return { kind: 'number', value: Number(node['text']) };
    case 'unary': {
      const operand = part('operand');
      if (node['op'] === 'NOT') return { kind: 'call', fn: 'not', args: [seriesExpr(operand, ids)] };
      if (node['op'] === 'MINUS' && operand.k === 'number') return { kind: 'number', value: -Number(operand['text']) };
      return no(`a sign before a ${operand.k} is no series a tile draws`);
    }
    case 'column':
    case 'pick':
      return columnOf(node, ids, undefined, no);
    case 'binary': {
      const op = OPERATOR_BACK[String(node['op'])];
      if (op === undefined) return no(`the operator ${String(node['op'])} is none a tile draws`);
      return { kind: 'binary', op, left: seriesExpr(part('left'), ids), right: seriesExpr(part('right'), ids) };
    }
    case 'call':
      return callOf(node, ids, no);
    default:
      return no(`a ${node.k} is no series a tile draws`);
  }
}

/** A column (`close`, `rates.yield`), picked (`close['SPX']`), taken by a period's aggregate where `fn` is given. */
function columnOf(node: TsNode, ids: readonly string[], fn: TsAggregate | undefined, no: (what: string, n?: TsNode) => never): TsExpr {
  const picked = node.k === 'pick' ? node['of'] as TsNode : node;
  if (picked.k !== 'column') return no(`a subscript of a ${picked.k} is no series a tile draws`, picked);
  const parts = picked['parts'] as readonly string[];
  if (parts.length > 2) return no(`the name ${parts.join('.')} is no column of a source (source.column)`, picked);
  if (node.k === 'column' && parts.length === 1 && fn === undefined && ids.includes(parts[0]!)) {
    return { kind: 'ref', series: parts[0]! };
  }
  const where = node.k === 'pick' ? (node['keys'] as readonly TsNode[]).map((k) => matchOf(k, no)) : [];
  return {
    kind: 'column', ...(parts.length === 2 ? { source: parts[0]! } : {}), column: parts[parts.length - 1]!,
    ...(fn === undefined ? {} : { fn }), ...(where.length === 0 ? {} : { where }),
  };
}

/** A subscript's key: a value (its source's instrument), or a column's match (`venue = 'NYSE'`). */
function matchOf(key: TsNode, no: (what: string, n?: TsNode) => never): TsMatch {
  if (key.k === 'binary' && key['op'] === 'EQ' && (key['left'] as TsNode).k === 'column') {
    const parts = (key['left'] as TsNode)['parts'] as readonly string[];
    if (parts.length !== 1) return no(`a match on ${parts.join('.')} is on no column of the source`, key);
    return { column: parts[0]!, value: valueOf(key['right'] as TsNode, no) };
  }
  return { value: valueOf(key, no) };
}

function valueOf(v: TsNode, no: (what: string, n?: TsNode) => never): string | number | boolean {
  if (v.k === 'string' || v.k === 'boolean') return v['value'] as string | boolean;
  if (v.k === 'number') return Number(v['text']);
  if (v.k === 'unary' && v['op'] === 'MINUS' && (v['operand'] as TsNode).k === 'number') return -Number((v['operand'] as TsNode)['text']);
  return no(`a ${v.k} is no value to match`, v);
}

function callOf(node: TsNode, ids: readonly string[], no: (what: string, n?: TsNode) => never): TsExpr {
  const name = (node['name'] as readonly string[]).join('.');
  const all = node['args'] as readonly TsNode[];
  // a period's aggregate of a column: the tile's `.sum`
  const taken = TAKEN_BACK.get(name);
  if (taken !== undefined && all.length === 1) {
    if (all[0]!.k === 'column' || all[0]!.k === 'pick') return columnOf(all[0]!, ids, taken, no);
    return no(`${name}(...) of a computed series is a period's: a tile takes a column's, ${name}(close)`);
  }
  // the tile's word, as the language spells it (ln, greatest, least); gs-quant's log, floor and ceil core refuses
  const fn = SPELLED_BACK.get(name) ?? (Object.hasOwn(TS_SIGNATURES, name) && !Object.hasOwn(SPELLED, name) ? name as TsFunctionName : undefined);
  if (fn === undefined) return no(NOT_BUILT[name.toLowerCase()] !== undefined ? `${name}: ${NOT_BUILT[name.toLowerCase()]}` : `there is no function ${name}`);
  const sig = TS_SIGNATURES[fn];
  const options = all.filter((a) => a.k === 'option');
  const placed = all.filter((a) => a.k !== 'option');
  if (options.length > 0 && all.indexOf(options[0]!) < placed.length) return no(`${name} takes no argument by place after one by name`);
  const whole = WHOLE[fn];
  // the series it reads: as its signature counts them (a weighted sum's, all it is given by place)
  const byPlace = BY_PLACE.has(fn) || fn === 'power' || fn === 'floor' || fn === 'ceil';
  const count = sig.series === 'many' ? placed.length : sig.series;
  const series = placed.slice(0, count);
  let rest = placed.slice(count);
  if (whole !== undefined) {
    if (rest[0]?.k !== whole) return no(`${name} is of the whole line: ${name}(x, ${whole})`);
    rest = rest.slice(1);
  }
  const params: Record<string, number | string> = {};
  if (byPlace) {
    if (rest.length > sig.params.length) return no(`${name} takes ${sig.params.length} parameters, not ${rest.length}`);
    rest.forEach((r, i) => { params[sig.params[i]!.name] = paramOf(fn, sig.params[i]!.name, r, no); });
  } else {
    if (rest.length > 0) return no(`${name} takes its parameters by name: ${sig.params.map((p) => `${p.name} =>`).join(', ')}`, rest[0]);
    for (const o of options) {
      const pname = String(o['name']);
      if (!sig.params.some((p) => p.name === pname)) return no(`${name} has no parameter ${pname}`, o);
      params[pname] = paramOf(fn, pname, o['value'] as TsNode, no);
    }
  }
  return {
    kind: 'call', fn, args: series.map((a) => seriesExpr(a, ids)),
    ...(Object.keys(params).length === 0 ? {} : { params }),
  };
}

/** A parameter's value, read by its kind: a span back as the tile's (`3mo` its `3m`, `all` its 0), a word, a number. */
function paramOf(fn: TsFunctionName, name: string, v: TsNode, no: (what: string, n?: TsNode) => never): number | string {
  const kind = TS_SIGNATURES[fn].params.find((p) => p.name === name)!.kind;
  if (kind === 'span' && v.k === 'all') return 0;
  if (kind === 'span' && v.k === 'span') {
    const unit = String(v['unit']);
    const tile = unit === 'mo' ? 'm' : unit;
    if (!['d', 'w', 'm', 'y'].includes(tile)) return no(`a span of ${unit} is none a tile reads: days, weeks, months or years`, v);
    return `${String(v['n'])}${tile}`;
  }
  if (kind === 'word') {
    if (v.k !== 'string') return no(`${fn}'s ${name} is a word, written as one: '...'`, v);
    return String(v['value']);
  }
  if (v.k === 'number') return Number(v['text']);
  if (v.k === 'unary' && v['op'] === 'MINUS' && (v['operand'] as TsNode).k === 'number') return -Number((v['operand'] as TsNode)['text']);
  return no(`${fn}'s ${name} is a number`, v);
}

/** `e` with every parameter told (its default where it was not): what reads back from its own text, compared. */
export function told(e: TsExpr): TsExpr {
  switch (e.kind) {
    case 'binary':
      return { ...e, left: told(e.left), right: told(e.right) };
    case 'call': {
      const sig = TS_SIGNATURES[e.fn];
      const params = Object.fromEntries(sig.params.map((p) => [p.name, e.params?.[p.name] ?? p.default]));
      const { params: _, ...rest } = e;
      void _;
      return { ...rest, args: e.args.map(told), ...(sig.params.length === 0 ? {} : { params }) };
    }
    case 'column': {
      const { where, ...rest } = e;
      return where === undefined || where.length === 0 ? rest : e;
    }
    default:
      return e;
  }
}
