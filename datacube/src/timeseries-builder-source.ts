// A SERIES AS THE BUILDER'S CODE (docs/TS_DECISIONS_2026_11_07.md §4, the builders; B4's Show TypeScript and Show
// Python): a tile's tree written as a person writes it with the builder -- gs-quant's names, its required parameters by
// place and the rest by name, columns and series by name, each option gs-quant's enumeration's member (Returns.LOGARITHMIC)
// -- in TypeScript (src/timeseries-builder.ts: a trailing object of the rest, pandas' methods for arithmetic) or in Python
// (python/legend_lite/timeseries.py: keyword arguments, Python's operators). Run, it builds the same tree again
// (timeseries-builder.test.ts holds TypeScript's so, test_ts_builder.py Python's) -- but for a comparison with its number
// first, written the series' way round (1 < x as x > 1, the same meaning, its tree mirrored: no method of a series' puts
// the number first; the review of 2026-11-08), and a z-score's `all`, written as its call of no window (the same
// meaning: to date). Every series a tile draws is written (item 1 of 2026-11-09, the user: "everything has a real python
// typed representation"): where gs-quant has no call of the tree's meaning, DataCube's own names say it -- ToDate() (a
// z-score to date), Across() (a cross-section), rand (a number first of and), a value gs-quant refuses (vol_swap's
// n_days '3m', a currency gs-quant has no cash of) -- each one gs-quant has not, never a gs-quant spelling of another
// meaning. A tree no tile draws (a number alone) is a defect, thrown as one.

import type { TsComparison, TsExpr, TsOperator } from './timeseries-spec.ts';
import * as generated from './generated/ts-builder-words.ts';
import { BUILDER_WORDS } from './generated/ts-builder-words.ts';
import { cashOver, RISK_FREE_CURRENCIES, takesCash, type GsParam } from './timeseries-builder-core.ts';
import { told as filled } from './timeseries-text.ts';

/** The languages a series is written in as the builder's code. */
export type BuilderLanguage = 'typescript' | 'python';

/** A tree no tile draws (a number alone): a defect of the caller's, never a series written otherwise. */
export class CodeDefect extends Error {}

/** SQL's words per point, the builder's own (timeseries-builder-core.ts, _ts_terms.py), by the tile's word. */
const SCALAR_SOURCE: Readonly<Record<string, string>> = {
  log: 'log', exp: 'exp', sqrt: 'sqrt', abs: 'abs_', power: 'power', not: 'not_',
};

/** Each operator's method (pandas'), and the one that reads the other way round (`5 + x` is `x.radd(5)`). */
const METHOD: Readonly<Record<TsOperator, string>> = {
  '+': 'add', '-': 'sub', '*': 'mul', '/': 'div', '>': 'gt', '>=': 'ge', '<': 'lt', '<=': 'le', '==': 'eq', '!=': 'ne',
  and: 'and', or: 'or',
};
const REVERSED: Readonly<Partial<Record<TsOperator, string>>> = {
  '+': 'radd', '-': 'rsub', '*': 'rmul', '/': 'rdiv', and: 'rand', or: 'ror',
};
const MIRRORED: Readonly<Partial<Record<TsOperator, TsComparison>>> = { '<': '>', '<=': '>=', '>': '<', '>=': '<=', '==': '==', '!=': '!=' };

/** Python's operator for each, and how tightly it binds: a comparison least (below & and |, unlike the language's). */
const PY_OPERATOR: Readonly<Record<TsOperator, string>> = {
  '+': '+', '-': '-', '*': '*', '/': '/', '>': '>', '>=': '>=', '<': '<', '<=': '<=', '==': '==', '!=': '!=', and: '&', or: '|',
};
const PY_PRECEDENCE: Readonly<Record<TsOperator, number>> = {
  '>': 1, '>=': 1, '<': 1, '<=': 1, '==': 1, '!=': 1, or: 2, and: 3, '+': 4, '-': 4, '*': 5, '/': 5,
};
const PY_NOT = 6;
const PY_ATOM = 9;

const quote = (v: string): string => `'${v.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;

/** The builder's code of `e` in `language`: each parameter as the tile's tree means it (its default written where it was
 *  left out), refused in words where gs-quant has no call of that meaning. */
export function builderSource(e: TsExpr, language: BuilderLanguage = 'typescript'): string {
  return builderSourceOf(e, language).text;
}

/** The builder's code of `e`, and the builder's names it calls (its words, column, series, Window, an enumeration): what
 *  a program imports -- the printer's own record of them, never its text read back (a column named `Price (USD)` is no
 *  call); and DataCube's own spellings it uses, none gs-quant's (ToDate(), Across(), rand, a value gs-quant refuses): a
 *  program of any is no gs-quant program. */
export function builderSourceOf(e: TsExpr, language: BuilderLanguage = 'typescript'): {
  readonly text: string; readonly names: readonly string[]; readonly extensions: readonly string[];
} {
  const p = new Printer(language);
  const text = p.source(filled(e)).text;
  return { text, names: [...p.names].sort(), extensions: [...p.extensions] };
}

interface Code {
  readonly text: string;
  /** How tightly it binds, Python's (an atom: a call, a column, a number). */
  readonly at: number;
}

class Printer {
  readonly #py: boolean;
  /** The builder's names the code calls. */
  readonly names = new Set<string>();
  /** DataCube's own spellings the code uses, gs-quant's none. */
  readonly extensions = new Set<string>();

  constructor(language: BuilderLanguage) {
    this.#py = language === 'python';
  }

  /** A value as the language writes it: a string quoted, true or True, null or None. */
  literal(v: string | number | boolean | null): string {
    if (v === null) return this.#py ? 'None' : 'null';
    if (typeof v === 'boolean') return this.#py ? (v ? 'True' : 'False') : String(v);
    return typeof v === 'string' ? quote(v) : String(v);
  }

  /** `name: value` in an object, `name=value` in a call. */
  named(name: string, value: string): string {
    return this.#py ? `${name}=${value}` : `${name}: ${value}`;
  }

  /** `items`, a trailing object of what is told by name (TypeScript's) or keyword arguments (Python's). */
  call(fn: string, placed: readonly string[], byName: readonly string[]): string {
    this.names.add(fn);
    const rest = byName.length === 0 ? [] : this.#py ? byName : [`{ ${byName.join(', ')} }`];
    return `${fn}(${[...placed, ...rest].join(', ')})`;
  }

  /** gs-quant's Window(w, r), the builder's. */
  window(w: string, r: string): string {
    this.names.add('Window');
    return `${this.#py ? '' : 'new '}Window(${w}, ${r})`;
  }

  /** DataCube's own window, the builder's (`ToDate()`, `new Across()`): a name gs-quant has not. */
  own(name: 'ToDate' | 'Across'): string {
    this.names.add(name);
    this.extensions.add(`${name}()`);
    return `${this.#py ? '' : 'new '}${name}()`;
  }

  /** An enumeration's member, the builder's (Returns.LOGARITHMIC). */
  member(enumeration: string, name: string): string {
    this.names.add(enumeration);
    return /^[A-Za-z_$][\w$]*$/.test(name) ? `${enumeration}.${name}` : `${enumeration}[${quote(name)}]`;
  }

  source(e: TsExpr): Code {
    switch (e.kind) {
      case 'number':
        throw new CodeDefect('a number alone is no series a tile draws: it is an argument');
      case 'ref':
        this.names.add('series');
        return { text: `series(${quote(e.series)})`, at: PY_ATOM };
      case 'column': {
        this.names.add('column');
        const name = e.source === undefined ? e.column : `${e.source}.${e.column}`;
        const where = (e.where ?? []).map((m) => (m.column === undefined ? this.literal(m.value)
          : this.#py ? `{${quote(m.column)}: ${this.literal(m.value)}}`
            : `{ ${/^[A-Za-z_$][\w$]*$/.test(m.column) ? m.column : quote(m.column)}: ${this.literal(m.value)} }`));
        const col = `column(${[quote(name), ...where].join(', ')})`;
        return { text: e.fn === undefined || e.fn === 'last' ? col : `${col}.period(${quote(e.fn)})`, at: PY_ATOM };
      }
      case 'binary':
        return this.#py ? this.pyBinary(e) : { text: this.tsBinary(e), at: PY_ATOM };
      case 'call':
        return this.callSource(e);
    }
  }

  /** A binary's number first, which reads the other way round (5 + x is x.radd(5), 1 < x is x > 1; 1 & x is x.rand(1),
   *  DataCube's name, as pandas reflects it): of two numbers, no series a tile draws. */
  numberFirst(e: Extract<TsExpr, { kind: 'binary' }>): void {
    if (e.left.kind === 'number' && e.right.kind === 'number') throw new CodeDefect('arithmetic of two numbers is no series a tile draws');
    if (e.left.kind === 'number' && (e.op === 'and' || e.op === 'or')) this.extensions.add(`r${e.op}`);
  }

  /** TypeScript's arithmetic: pandas' methods. */
  tsBinary(e: Extract<TsExpr, { kind: 'binary' }>): string {
    this.numberFirst(e);
    if (e.left.kind === 'number') {
      const reversed = REVERSED[e.op];
      if (reversed !== undefined) return `${this.source(e.right).text}.${reversed}(${e.left.value})`;
      // a comparison with its number first: the series' side compared the other way (1 < x is x > 1)
      return `${this.source(e.right).text}.${METHOD[MIRRORED[e.op]!]}(${e.left.value})`;
    }
    const right = e.right.kind === 'number' ? String(e.right.value) : this.source(e.right).text;
    return `${this.source(e.left).text}.${METHOD[e.op]}(${right})`;
  }

  /** Python's arithmetic: its operators, in parentheses where Python binds otherwise than the tree. */
  pyBinary(e: Extract<TsExpr, { kind: 'binary' }>): Code {
    this.numberFirst(e);
    // a comparison with its number first: the series' side compared the other way (1 < x is x > 1), as Python reflects it
    const flipped = e.left.kind === 'number' && REVERSED[e.op] === undefined;
    const op = flipped ? MIRRORED[e.op]! : e.op;
    const [left, right] = flipped ? [e.right, e.left] : [e.left, e.right];
    const at = PY_PRECEDENCE[op];
    const comparison = at === PY_PRECEDENCE['>'];
    const side = (x: TsExpr, first: boolean): string => {
      if (x.kind === 'number') return String(x.value);
      const c = this.source(x);
      // a comparison's sides, and an operand's right side of the same binding, in parentheses (Python chains a > b > c)
      const wrap = comparison ? c.at <= at : first ? c.at < at : c.at <= at;
      return wrap ? `(${c.text})` : c.text;
    };
    return { text: `${side(left, true)} ${PY_OPERATOR[op]} ${side(right, false)}`, at };
  }

  callSource(e: Extract<TsExpr, { kind: 'call' }>): Code {
    const scalar = SCALAR_SOURCE[e.fn];
    const arg = (a: TsExpr): string => (a.kind === 'number' ? String(a.value) : this.source(a).text);
    if (scalar !== undefined) {
      if (e.fn === 'not' && this.#py) {
        this.names.add(scalar);
        const c = this.source(e.args[0]!);
        return { text: `~${c.at < PY_NOT ? `(${c.text})` : c.text}`, at: PY_NOT };
      }
      const told = Object.values(e.params ?? {});
      this.names.add(scalar);
      return { text: `${scalar}(${[arg(e.args[0]!), ...told.map((v) => this.literal(v))].join(', ')})`, at: PY_ATOM };
    }
    const entry = Object.entries(BUILDER_WORDS).find(([, s]) => s.word === e.fn);
    // every word a tile draws is one of the builder's (generated from the tile's words, timeseries-words.ts)
    if (entry === undefined) throw new CodeDefect(`${e.fn} is no word of the builder's`);
    const [tsName, shape] = entry;
    // Python's name gs-quant's own (var, not TypeScript's var_); excess returns over another line's levels gs-quant's
    // excess_returns_pure, in both (its excess_returns reads an asset, a currency or a rate, never a line: the review of
    // 2026-11-08, gs-quant run over Show Python's calls, tools/ts-oracle/calls.py)
    const overLine = shape.gsFunction === 'econometrics.excess_returns' && e.args[1] !== undefined && e.args[1].kind !== 'number';
    const name = overLine ? 'excess_returns_pure'
      : this.#py && shape.gsFunction !== null ? shape.gsFunction.slice(shape.gsFunction.lastIndexOf('.') + 1) : tsName;
    const lines: string[] = [];
    const kw: string[] = [];
    const placed: string[] = [];
    // gs-quant's risk-free cash (item 13b): a line's excess returns over a currency's cash is gs-quant's PRICES of that
    // currency, one of its seven; any other line, its EXCESS_RETURNS
    let args = e.args;
    let excess = false;
    if (takesCash(shape)) {
      const over = e.args.length > 0 ? cashOver(e.args[0]!) : undefined;
      if (over === undefined) {
        excess = true;
      } else {
        args = [over.x, ...e.args.slice(1)];
        if (RISK_FREE_CURRENCIES.includes(over.currency)) {
          placed.push(this.member('RiskFreeRateCurrency', over.currency));
        } else {
          // a currency gs-quant has no cash of, the page's cash line of it: its code, a value gs-quant refuses
          this.extensions.add(`currency ${quote(over.currency)}`);
          placed.push(quote(over.currency));
        }
      }
    }
    if (shape.members !== undefined) {
      // series and their weights: gs-quant's two lists
      const series = e.args.filter((_, i) => i % 2 === 0).map(arg);
      const weights = e.args.filter((_, i) => i % 2 === 1).map(arg);
      lines.push(`[${series.join(', ')}]`);
      const w = shape.gs.find((g) => g.weights === true)!;
      if (w.default === undefined) placed.push(`[${weights.join(', ')}]`);
      else kw.push(this.named(w.name, `[${weights.join(', ')}]`));
    } else if (shape.listOf !== undefined) {
      lines.push(`[${e.args.map(arg).join(', ')}]`);
    } else {
      lines.push(...args.map(arg));
    }
    const told = { ...(e.params ?? {}) };
    // gs-quant's filter_ of no operator and no value removes the missing values: the language's operator 'empty'
    if (shape.gsFunction === 'algebra.filter_' && told['operator'] === 'empty') {
      // a value beside it is refused as the tile reads it (timeseries-spec.ts: gs-quant's filter_ raises on one)
      if (told['value'] !== undefined && told['value'] !== 0) throw new CodeDefect('filter\'s empty operator reads no value');
      delete told['operator'];
      delete told['value'];
    }
    // a parameter by place where gs-quant has no default for it, else by name
    const put = (g: GsParam, value: string): void => {
      if (g.default === undefined) placed.push(value);
      else kw.push(this.named(g.name, value));
    };
    for (const g of shape.gs) {
      if (g.only || g.weights) continue;
      const lib = g.lib ?? g.name;
      const v = told[lib];
      delete told[lib];
      // a cross-section (item 13c): gs-quant reads none -- DataCube's Across()
      if (v === 'across') {
        put(g, this.own('Across'));
        continue;
      }
      // vol_swap_volatility's n_days to date: gs-quant's None is one figure of the whole series at its last date --
      // DataCube's ToDate(); over a calendar period, its value as the language's ('3m': gs-quant's raises on it)
      if (g.name === 'n_days' && v === 0) {
        put(g, this.own('ToDate'));
        continue;
      }
      if (g.name === 'n_days' && typeof v === 'string') this.extensions.add(`${g.name} ${quote(v)}`);
      if (g.whole !== undefined) {
        // gs-quant's default here is the whole line: written as its call without it
        if (v === 'whole') {
          // a window gs-quant reads for its ramp alone (winsorize's): Window(r, r), its first r points dropped (item 13)
          const ramp = g.whole === 'always' ? told['r'] : undefined;
          if (ramp !== undefined) {
            delete told['r'];
            kw.push(this.named(g.name, String(ramp)));
          }
          continue;
        }
        // to date, the language's call of no window (and zscores' all, the same): gs-quant's no window is the whole
        // series -- DataCube's ToDate(); percentile's all is gs-quant's Window(None, 0), below
        if (v === undefined || (v === 0 && g.whole !== 'default')) {
          put(g, this.own('ToDate'));
          continue;
        }
      }
      // left to the language, whose reading is gs-quant's default (compare's method: step, S145) -- the builder's call
      // without it says the same (timeseries-builder-core.ts word)
      if (v === undefined) continue;
      // a window's ramp apart from its size: gs-quant's Window(w, r) (item 13), the library's w and r
      const ramp = lib === 'w' ? told['r'] : undefined;
      if (ramp !== undefined) delete told['r'];
      const value = ramp !== undefined ? this.window(v === 0 ? this.literal(null) : this.literal(v), String(ramp))
        : lib === 'w' && v === 0 && shape.gsFunction !== null ? this.window(this.literal(null), '0') : this.gsValue(g, v);
      put(g, value);
    }
    for (const [lib, v] of Object.entries(told)) kw.push(this.named(lib, typeof v === 'string' && /^[1-9][0-9]*[dwmy]$/.test(v) ? quote(v) : this.literal(v)));
    if (excess) kw.push(this.named('curve_type', this.member('CurveType', 'EXCESS_RETURNS')));
    return { text: this.call(name, [...lines, ...placed], kw), at: PY_ATOM };
  }

  /** A tile's parameter value as gs-quant's argument: a window its size, an option gs-quant's enumeration's member (log
   *  is Returns.LOGARITHMIC). */
  gsValue(g: GsParam, v: number | string): string {
    if (typeof v !== 'string') return String(v);
    const gs = Object.entries(g.values ?? {}).find(([, lib]) => lib === v)?.[0] ?? v;
    const members = g.enum === undefined ? undefined : (generated as unknown as Record<string, Readonly<Record<string, string>> | undefined>)[g.enum];
    const member = members === undefined ? undefined : Object.entries(members).find(([, x]) => x === gs)?.[0];
    if (member === undefined) return quote(gs);
    return this.member(g.enum!, member);
  }
}
