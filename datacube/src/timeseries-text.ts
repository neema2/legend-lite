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

import type { TsNode } from './lite-planner.ts';
import { TS_SIGNATURES, type TsAggregate, type TsExpr, type TsFunctionName, type TsMatch, type TsOperator, type TsSeries } from './timeseries-spec.ts';

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

/**
 * A SERIES' TREE, the one way every walker reads it: `series` the tile's series, whose ids name the series it reads.
 * (Today the tree the tile holds; once the tile stores text, core's reading of that text.)
 */
export function treeOf(s: TsSeries, series: readonly TsSeries[]): TsExpr {
  void series;
  return s.expr;
}

/** A failure to read core's tree as a tile's: the text is the language's, but no series a tile draws. */
export class TsTextRefusal extends Error {}

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
    throw new TsTextRefusal(`${what} at line ${n.at[0]}, column ${n.at[1]} is no series a tile draws`);
  };
  const part = (name: string): TsNode => node[name] as TsNode;
  switch (node.k) {
    case 'number':
      return { kind: 'number', value: Number(node['text']) };
    case 'unary': {
      const operand = part('operand');
      if (node['op'] === 'NOT') return { kind: 'call', fn: 'not', args: [seriesExpr(operand, ids)] };
      if (node['op'] === 'MINUS' && operand.k === 'number') return { kind: 'number', value: -Number(operand['text']) };
      return no(`a sign before ${operand.k}`);
    }
    case 'column':
    case 'pick':
      return columnOf(node, ids, undefined, no);
    case 'binary': {
      const op = OPERATOR_BACK[String(node['op'])];
      if (op === undefined) return no(`the operator ${String(node['op'])}`);
      return { kind: 'binary', op, left: seriesExpr(part('left'), ids), right: seriesExpr(part('right'), ids) };
    }
    case 'call':
      return callOf(node, ids, no);
    default:
      return no(`a ${node.k}`);
  }
}

/** A column (`close`, `rates.yield`), picked (`close['SPX']`), taken by a period's aggregate where `fn` is given. */
function columnOf(node: TsNode, ids: readonly string[], fn: TsAggregate | undefined, no: (what: string, n?: TsNode) => never): TsExpr {
  const picked = node.k === 'pick' ? node['of'] as TsNode : node;
  if (picked.k !== 'column') return no(`a subscript of a ${picked.k}`, picked);
  const parts = picked['parts'] as readonly string[];
  if (parts.length > 2) return no(`the name ${parts.join('.')}`, picked);
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
    if (parts.length !== 1) return no(`a match on ${parts.join('.')}`, key);
    return { column: parts[0]!, value: valueOf(key['right'] as TsNode, no) };
  }
  return { value: valueOf(key, no) };
}

function valueOf(v: TsNode, no: (what: string, n?: TsNode) => never): string | number | boolean {
  if (v.k === 'string' || v.k === 'boolean') return v['value'] as string | boolean;
  if (v.k === 'number') return Number(v['text']);
  if (v.k === 'unary' && v['op'] === 'MINUS' && (v['operand'] as TsNode).k === 'number') return -Number((v['operand'] as TsNode)['text']);
  return no(`a ${v.k} to match`, v);
}

function callOf(node: TsNode, ids: readonly string[], no: (what: string, n?: TsNode) => never): TsExpr {
  const name = (node['name'] as readonly string[]).join('.');
  const all = node['args'] as readonly TsNode[];
  // a period's aggregate of a column: the tile's `.sum`
  const taken = TAKEN_BACK.get(name);
  if (taken !== undefined && all.length === 1) {
    if (all[0]!.k === 'column' || all[0]!.k === 'pick') return columnOf(all[0]!, ids, taken, no);
    return no(`${name}(...) of a computed series, a period's (a tile takes a column's: ${name}(close))`);
  }
  // the tile's word, as the language spells it (ln, greatest, least); gs-quant's log, floor and ceil core refuses
  const fn = SPELLED_BACK.get(name) ?? (Object.hasOwn(TS_SIGNATURES, name) && !Object.hasOwn(SPELLED, name) ? name as TsFunctionName : undefined);
  if (fn === undefined) return no(`${name}(...), no word of the tile's,`);
  const sig = TS_SIGNATURES[fn];
  const options = all.filter((a) => a.k === 'option');
  const placed = all.filter((a) => a.k !== 'option');
  if (options.length > 0 && all.indexOf(options[0]!) < placed.length) return no(`${name}'s argument after one by name`);
  const whole = WHOLE[fn];
  // the series it reads: as its signature counts them (a weighted sum's, all it is given by place)
  const byPlace = BY_PLACE.has(fn) || fn === 'power' || fn === 'floor' || fn === 'ceil';
  const count = sig.series === 'many' ? placed.length : sig.series;
  const series = placed.slice(0, count);
  let rest = placed.slice(count);
  if (whole !== undefined) {
    if (rest[0]?.k !== whole) return no(`${name} without its window of the whole line (${name}(x, ${whole}))`);
    rest = rest.slice(1);
  }
  const params: Record<string, number | string> = {};
  if (byPlace) {
    if (rest.length > sig.params.length) return no(`${name}'s ${rest.length} parameters (it takes ${sig.params.length})`);
    rest.forEach((r, i) => { params[sig.params[i]!.name] = paramOf(fn, sig.params[i]!.name, r, no); });
  } else {
    if (rest.length > 0) return no(`${name}'s parameter by place (by name: ${sig.params.map((p) => `${p.name} =>`).join(', ')})`, rest[0]);
    for (const o of options) {
      const pname = String(o['name']);
      if (!sig.params.some((p) => p.name === pname)) return no(`${name}'s ${pname}, no parameter of it,`, o);
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
    if (!['d', 'w', 'm', 'y'].includes(tile)) return no(`a span of ${unit} (a tile's is days, weeks, months or years)`, v);
    return `${String(v['n'])}${tile}`;
  }
  if (kind === 'word') {
    if (v.k !== 'string') return no(`${fn}'s ${name}, which is a word ('...'),`, v);
    return String(v['value']);
  }
  if (v.k === 'number') return Number(v['text']);
  if (v.k === 'unary' && v['op'] === 'MINUS' && (v['operand'] as TsNode).k === 'number') return -Number((v['operand'] as TsNode)['text']);
  return no(`${fn}'s ${name}, which is a number,`, v);
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
