// THE #TS BUILDER'S TERMS (docs/TS_DECISIONS_2026_11_07.md §4, the builders; the order's step 2): what a person writes
// to make a series in TypeScript, read as a gs-quant notebook reads -- each word a function of gs-quant 2.2.3's name,
// its parameters gs-quant's names, order and defaults (generated/ts-builder-words.ts, generated from core's catalog and
// gs-quant's own signatures), its keyword arguments a trailing object, its arithmetic methods (`spx.div(ndx)`: TypeScript
// has no operator overloading -- the one difference it cannot avoid). Thin and write-only: a term is the tile's tree
// (timeseries-spec.ts TsExpr), its text the tile's writer's (timeseries-text.ts seriesText), and core reads it back --
// the builder never parses. Every value gs-quant takes that the language has no way to say is refused in words, never
// dropped: a ramp apart from its window's size (item 13), a gs-quant parameter DataCube has not (assume_zero_mean)
// other than at gs-quant's default.

import type { TsAggregate, TsExpr, TsFunctionName, TsMatch, TsOperator, TsParamShape } from './timeseries-spec.ts';
import { TS_SIGNATURES } from './timeseries-spec.ts';
import { seriesText } from './timeseries-text.ts';

/** What the builder refuses: a value gs-quant takes that the language has no way to say, in words. */
export class BuilderRefusal extends Error {}

/** A SERIES the builder made: its tree, its #TS text (`text`), and gs-quant's arithmetic as pandas' methods. */
export class Term {
  readonly expr: TsExpr;

  constructor(expr: TsExpr) {
    this.expr = expr;
  }

  /** Its #TS text, the language's spelling, as core reads it back: `ids` the tile's series' ids (a column named as one
   *  is read through its source). */
  text(ids: readonly string[] = []): string {
    return seriesText(this.expr, ids);
  }

  toString(): string {
    return this.text();
  }

  /** pandas' `x.add(y)`: x + y. */
  add(other: Term | number): Term {
    return binary('+', this, other);
  }

  /** pandas' `x.sub(y)`: x - y. */
  sub(other: Term | number): Term {
    return binary('-', this, other);
  }

  /** pandas' `x.mul(y)`: x * y. */
  mul(other: Term | number): Term {
    return binary('*', this, other);
  }

  /** pandas' `x.div(y)`: x / y. */
  div(other: Term | number): Term {
    return binary('/', this, other);
  }

  /** pandas' `x.gt(y)`: 1 where x > y, else 0 -- a condition. */
  gt(other: Term | number): Term {
    return binary('>', this, other);
  }

  /** pandas' `x.ge(y)`: x >= y. */
  ge(other: Term | number): Term {
    return binary('>=', this, other);
  }

  /** pandas' `x.lt(y)`: x < y. */
  lt(other: Term | number): Term {
    return binary('<', this, other);
  }

  /** pandas' `x.le(y)`: x <= y. */
  le(other: Term | number): Term {
    return binary('<=', this, other);
  }

  /** pandas' `x.eq(y)`: x == y. */
  eq(other: Term | number): Term {
    return binary('==', this, other);
  }

  /** pandas' `x.ne(y)`: x != y. */
  ne(other: Term | number): Term {
    return binary('!=', this, other);
  }

  /** pandas' `x & y`: both conditions hold. */
  and(other: Term): Term {
    return binary('and', this, other);
  }

  /** pandas' `x | y`: either holds. */
  or(other: Term): Term {
    return binary('or', this, other);
  }
}

/** A column of a source: a Term whose period's value may be taken otherwise than its last (`volume.period('sum')`). */
export class ColumnTerm extends Term {
  /** Each period's `fn` of its rows under the tile's frequency (the language's `sum(volume)`); `last` unless told. */
  period(fn: TsAggregate): ColumnTerm {
    if (this.expr.kind !== 'column') throw new BuilderRefusal('a period is taken of a column');
    return new ColumnTerm({ ...this.expr, fn });
  }
}

function termOf(v: Term | number): TsExpr {
  if (typeof v === 'number') {
    if (!Number.isFinite(v)) throw new BuilderRefusal(`a number is finite, not ${v}`);
    return { kind: 'number', value: v };
  }
  return v.expr;
}

function binary(op: TsOperator, left: Term, right: Term | number): Term {
  return new Term({ kind: 'binary', op, left: left.expr, right: termOf(right) });
}

/**
 * A COLUMN of the tile's source along its date -- `column('close')`, `column('px.close')` (a source by its alias), its
 * rows of one instrument (`column('close', 'AAPL')`: the language's `close['AAPL']`) or of a column's value
 * (`column('close', { venue: 'NYSE' }, 'AAPL')`).
 */
export function column(name: string, ...where: readonly (string | number | boolean | Readonly<Record<string, string | number | boolean>>)[]): ColumnTerm {
  const dot = name.indexOf('.');
  const source = dot < 0 ? undefined : name.slice(0, dot);
  const col = dot < 0 ? name : name.slice(dot + 1);
  if (col === '' || source === '') throw new BuilderRefusal(`a column is named, or a source's alias and its name: ${JSON.stringify(name)}`);
  const matches: TsMatch[] = where.flatMap((w): TsMatch[] => (typeof w === 'object'
    ? Object.entries(w).map(([c, value]) => ({ column: c, value }))
    : [{ value: w }]));
  return new ColumnTerm({
    kind: 'column', column: col, ...(source !== undefined ? { source } : {}), ...(matches.length > 0 ? { where: matches } : {}),
  });
}

/** ANOTHER SERIES of the tile, by its id (`series('A')`: the language's `A`). */
export function series(id: string): Term {
  if (!/^[A-Z][A-Z0-9]*$/.test(id)) throw new BuilderRefusal(`a series' id is a capital letter: ${JSON.stringify(id)}`);
  return new Term({ kind: 'ref', series: id });
}

// ------------------------------------------------------------------ gs-quant's Window

/**
 * gs-quant's WINDOW (gs_quant.timeseries.helper.Window): a size -- observations, or a calendar period ('1m', '1y') --
 * and a ramp, the size unless told. The language says three of them: every observation to date (gs-quant's
 * `Window(None, 0)`, the language's `all`), a size whose ramp is the size (`Window(22)`, the language's `22`: drawn from
 * the observation its window first holds, one before gs-quant's -- the ramp difference every rolling word lists), and
 * a calendar period (`Window('3m')`). A ramp apart from its size is item 13's (B3), refused until then.
 */
export class Window {
  readonly w: number | string | null;
  readonly r: number | string | null;

  constructor(w: number | string | null = null, r?: number | string | null) {
    this.w = w;
    this.r = r === undefined || r === null ? w : r;
  }
}

/** What a window parameter takes: gs-quant's Window, a number of observations, or a calendar period. */
export type WindowLike = Window | number | string;

const TENOR = /^([1-9][0-9]*)([dwmy])$/;

/** A window as the tile holds it (timeseries-spec.ts: a span, 0 every observation, `3m` a period). */
function spanOf(fn: string, name: string, v: WindowLike): number | string {
  if (v instanceof Window) {
    if (v.w === null) {
      if (v.r === 0 || v.r === null) return 0;
      throw new BuilderRefusal(`${fn}'s ${name}: a window of every observation ramped by ${String(v.r)} is item 13's (a ramp apart from its size)`);
    }
    if (v.r !== v.w) {
      throw new BuilderRefusal(`${fn}'s ${name}: Window(${JSON.stringify(v.w)}, ${JSON.stringify(v.r)}) -- a ramp apart from its size is item 13's; Window(${JSON.stringify(v.w)}) is its size alone`);
    }
    return spanOf(fn, name, v.w);
  }
  if (typeof v === 'number') {
    if (!Number.isInteger(v) || v < 0) throw new BuilderRefusal(`${fn}'s ${name} is a whole number of observations, not ${v}`);
    return v;
  }
  if (!TENOR.test(v)) throw new BuilderRefusal(`${fn}'s ${name} is a number of observations or a calendar period ('3m', '1y'), not ${JSON.stringify(v)}`);
  return v;
}

// ------------------------------------------------------------------ a word's call

/** A call's parameters after its lines, as gs-quant takes them: by place, any after those by name in a trailing object
 *  (`volatility(spx, { returns_type: Returns.LOGARITHMIC })`). */
export type Args<P extends readonly unknown[], K> = Prefixes<P> | Named<P, K>;
type Prefixes<P extends readonly unknown[]> = P extends readonly [...infer H, unknown] ? P | Prefixes<H> : P;
type Named<P extends readonly unknown[], K> = P extends readonly [...infer H, unknown] ? [...P, K] | Named<H, K> : [K];

/** A line a word reads: a Term (a number where gs-quant takes one: a branch of if_, a compare's level). */
export type Line = Term;

/**
 * How a word's gs-quant parameters become the library's (generated, per word): gs-quant's parameters after its lines,
 * in order -- each either the library's parameter of that name or of `lib`, its gs-quant default, the words gs-quant's
 * values are where they differ (`values`), or `only`: a parameter DataCube has not, taken at gs-quant's default alone --
 * then the library's parameters gs-quant has not (`extra`), by name only.
 */
export interface WordShape {
  readonly word: TsFunctionName;
  /** gs-quant's function it is (`econometrics.volatility`); none, DataCube's own. */
  readonly gsFunction: string | null;
  /** Its lines' names, gs-quant's (a list's one: union's `x`, basket_series' `series`). */
  readonly lines: readonly string[];
  readonly gs: readonly GsParam[];
  readonly extra: readonly string[];
  /** Series and their weights given as gs-quant's two lists (weighted_sum, basket_series): its one line the list of
   *  series, its `weights` parameter theirs -- even where none and `evenWeights` (gs-quant's basket_series'). */
  readonly members?: { readonly evenWeights: boolean };
  /** Its lines given as gs-quant's one list of them (union's, append's), of so many. */
  readonly listOf?: number;
}

export interface GsParam {
  readonly name: string;
  /** The library's parameter it is, where its name differs. */
  readonly lib?: string;
  /** gs-quant's default: a value, a window (`{ window: [w, r] }`), or none (the language's own where left out). */
  readonly default?: number | string | boolean | null | { readonly window: readonly [number | string | null, number | string | null] };
  /** gs-quant's values the language spells otherwise (Returns.LOGARITHMIC: log). */
  readonly values?: Readonly<Record<string, string>>;
  /** No parameter of DataCube's: taken at gs-quant's default alone (or a value of `also`, which the word computes:
   *  lag's truncate), else refused. */
  readonly only?: true;
  readonly also?: readonly string[];
  /** The weights of a word of series and weights (weighted_sum's, basket_series'): the language's pairs' second. */
  readonly weights?: true;
  /** Given by name alone (gs-quant's keyword-only: excess_returns' day_count_convention). */
  readonly kwOnly?: true;
  /** A window gs-quant takes as the whole line (the library's gsWhole): its default (zscores', percentile's: written
   *  `whole`, the look-ahead seen), or whatever it is told (winsorize's, its window only a ramp: another refused). */
  readonly whole?: 'default' | 'always';
}

function isKeywords(v: unknown): v is Readonly<Record<string, unknown>> {
  return typeof v === 'object' && v !== null && !(v instanceof Term) && !(v instanceof Window) && !Array.isArray(v);
}

function defaultOf(p: GsParam): unknown {
  const d = p.default;
  return typeof d === 'object' && d !== null ? new Window(d.window[0], d.window[1]) : d;
}

function same(a: unknown, b: unknown): boolean {
  if (a instanceof Window && b instanceof Window) return a.w === b.w && a.r === b.r;
  return a === b;
}

/** A value as the library's parameter `p` takes it: a window its span, an option its word, a number itself. */
function libValue(fn: string, gsName: string, p: TsParamShape, g: GsParam | undefined, v: unknown): number | string {
  switch (p.kind) {
    case 'span':
      if (!(v instanceof Window) && typeof v !== 'number' && typeof v !== 'string') throw new BuilderRefusal(`${fn}'s ${gsName} is a window: Window(22), 22 or '3m'`);
      return spanOf(fn, gsName, v);
    case 'word': {
      const word = typeof v === 'string' ? (g?.values?.[v] ?? v) : undefined;
      if (word === undefined || !(p.words ?? []).includes(word)) {
        throw new BuilderRefusal(`${fn}'s ${gsName} is one of ${(p.words ?? []).map((w) => JSON.stringify(Object.entries(g?.values ?? {}).find(([, l]) => l === w)?.[0] ?? w)).join(', ')}, not ${JSON.stringify(v)}`);
      }
      return word;
    }
    case 'whole':
    case 'number':
      if (typeof v !== 'number' || !Number.isFinite(v)) throw new BuilderRefusal(`${fn}'s ${gsName} is a number, not ${JSON.stringify(v)}`);
      if (p.kind === 'whole' && !Number.isInteger(v)) throw new BuilderRefusal(`${fn}'s ${gsName} is a whole number, not ${v}`);
      return v;
  }
}

/**
 * A WORD CALLED as gs-quant calls it: its lines, then `args` -- its parameters by place, any after by name in a
 * trailing object -- each gs-quant's default where not given, written explicitly (the builders' rule); one of none
 * left out, the language's own where it has one (an annualization factor: the frequency's).
 */
export function word(shape: WordShape, lines: readonly (Term | number | readonly Term[])[], args: readonly unknown[]): Term {
  const fn = shape.word;
  const sig = TS_SIGNATURES[fn];
  const told = new Map<string, unknown>();
  const placed = args.length > 0 && isKeywords(args[args.length - 1]) ? args.slice(0, -1) : args;
  const named = placed.length === args.length ? {} : args[args.length - 1] as Readonly<Record<string, unknown>>;
  const byPlace = shape.gs.filter((g) => !g.kwOnly);
  if (placed.length > byPlace.length) throw new BuilderRefusal(`${fn} takes ${byPlace.length} parameters by place after its series, not ${placed.length}`);
  placed.forEach((v, i) => told.set(byPlace[i]!.name, v));
  for (const [k, v] of Object.entries(named)) {
    if (!shape.gs.some((g) => g.name === k) && !shape.extra.includes(k)) {
      throw new BuilderRefusal(`${fn} has no parameter ${k} (it has ${[...shape.gs.map((g) => g.name), ...shape.extra].join(', ') || 'none'})`);
    }
    if (told.has(k)) throw new BuilderRefusal(`${fn}'s ${k} is given twice: by place and by name`);
    told.set(k, v);
  }
  const params: Record<string, number | string> = {};
  let weights: unknown;
  for (const g of shape.gs) {
    const v = told.has(g.name) ? told.get(g.name) : defaultOf(g);
    if (g.weights) {
      weights = v;
      continue;
    }
    if (g.whole !== undefined) {
      const told = v !== undefined && v !== null && !same(v, defaultOf(g));
      if (told && g.whole === 'always') {
        throw new BuilderRefusal(`${fn}: gs-quant's clips by the whole series' mean and deviation whatever ${g.name} is -- its ${g.name} blanks the first points alone, a ramp (item 13)`);
      }
      if (!told) {
        params[g.lib ?? g.name] = 'whole';
        continue;
      }
    }
    if (g.only) {
      if (!same(v, defaultOf(g)) && !(g.also ?? []).some((a) => a === v)) {
        throw new BuilderRefusal(`${fn}: DataCube's has no ${g.name} -- gs-quant's ${[defaultOf(g) ?? null, ...(g.also ?? [])].map((x) => JSON.stringify(x)).join(' or ')} alone`);
      }
      continue;
    }
    if (v === undefined || v === null) continue;
    const libName = g.lib ?? g.name;
    const p = sig.params.find((q) => q.name === libName);
    if (p === undefined) throw new Error(`${fn}'s gs-quant parameter ${g.name} is the library's ${libName}, which ${fn} has not`);
    params[libName] = libValue(fn, g.name, p, g, v);
  }
  for (const name of shape.extra) {
    const v = told.get(name);
    if (v === undefined || v === null) continue;
    const p = sig.params.find((q) => q.name === name);
    if (p === undefined) throw new Error(`${fn} has no library parameter ${name}`);
    params[name] = libValue(fn, name, p, undefined, v);
  }
  const missing = sig.params.find((p) => p.required && params[p.name] === undefined);
  if (missing !== undefined) throw new BuilderRefusal(`${fn} is given its ${missing.name} in every call`);
  if (shape.listOf !== undefined && lines.length !== shape.listOf) {
    throw new BuilderRefusal(`${fn} reads a list of ${shape.listOf} series, not ${lines.length} (gs-quant's takes any number: DataCube's ${shape.listOf})`);
  }
  const exprs: TsExpr[] = shape.members ? membersOf(fn, lines[0], weights, shape.members.evenWeights) : lines.map((l) => {
    if (Array.isArray(l)) throw new BuilderRefusal(`${fn} reads a series, not a list`);
    return termOf(l as Term | number);
  });
  return new Term({ kind: 'call', fn, args: exprs, ...(Object.keys(params).length > 0 ? { params } : {}) });
}

/** gs-quant's two lists -- the series, their weights (even where none: gs-quant's) -- as the language's pairs. */
function membersOf(fn: string, list: unknown, weights: unknown, even: boolean): TsExpr[] {
  if (!Array.isArray(list) || list.length === 0 || !list.every((m) => m instanceof Term)) throw new BuilderRefusal(`${fn} reads a list of series`);
  const members = list as readonly Term[];
  if (weights !== undefined && weights !== null && (!Array.isArray(weights) || !weights.every((x) => typeof x === 'number'))) {
    throw new BuilderRefusal(`${fn}'s weights are a list of numbers`);
  }
  const w = (weights as readonly number[] | null | undefined) ?? (even ? members.map(() => 1 / members.length) : undefined);
  if (w === undefined) throw new BuilderRefusal(`${fn} reads a weight for each series`);
  if (w.length !== members.length) throw new BuilderRefusal(`${fn} reads one weight for each series: ${members.length} series, ${w.length} weights`);
  return members.flatMap((m, i) => [m.expr, termOf(w[i]!)]);
}

// ------------------------------------------------------------------ SQL's words per point, gs-quant's algebra

const scalar = (fn: TsFunctionName, x: Term, params?: Readonly<Record<string, number>>): Term =>
  new Term({ kind: 'call', fn, args: [x.expr], ...(params ? { params } : {}) });

/** gs-quant's algebra.log: the natural log (the language's ln). */
export function log(x: Term): Term {
  return scalar('log', x);
}

/** gs-quant's algebra.exp: e to the x. */
export function exp(x: Term): Term {
  return scalar('exp', x);
}

/** gs-quant's algebra.sqrt: the square root. */
export function sqrt(x: Term): Term {
  return scalar('sqrt', x);
}

/** gs-quant's algebra.abs_: the absolute value. */
export function abs_(x: Term): Term {
  return scalar('abs', x);
}

/** gs-quant's algebra.power: x to the power y (gs-quant's y 1). */
export function power(x: Term, y = 1): Term {
  return scalar('power', x, { y });
}

/** gs-quant's algebra.floor: never below value (gs-quant's 0; the language's greatest). */
export function floor(x: Term, value = 0): Term {
  return scalar('floor', x, { value });
}

/** gs-quant's algebra.ceil: never above value (gs-quant's 0; the language's least). */
export function ceil(x: Term, value = 0): Term {
  return scalar('ceil', x, { value });
}

/** gs-quant's algebra.not_: a condition turned over (the language's NOT). */
export function not_(series: Term): Term {
  return scalar('not', series);
}
