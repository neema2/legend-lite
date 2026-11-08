// A SERIES AS THE BUILDER'S CODE (docs/TS_DECISIONS_2026_11_07.md §4, the builders; B4's Show TypeScript and Show
// Python): a tile's tree written as a person writes it with the builder -- gs-quant's names, its required parameters by
// place and the rest by name, columns and series by name, each option gs-quant's enumeration's member (Returns.LOGARITHMIC)
// -- in TypeScript (src/timeseries-builder.ts: a trailing object of the rest, pandas' methods for arithmetic) or in Python
// (python/legend_lite/timeseries.py: keyword arguments, Python's operators). Run, it builds the same tree again
// (timeseries-builder.test.ts holds TypeScript's so, test_ts_builder.py Python's); where gs-quant has no call of the
// tree's meaning it is refused in words (a z-score to date).

import type { TsComparison, TsExpr, TsOperator } from './timeseries-spec.ts';
import * as generated from './generated/ts-builder-words.ts';
import { BUILDER_WORDS } from './generated/ts-builder-words.ts';
import { cashOver, takesCash, type GsParam } from './timeseries-builder-core.ts';
import { told as filled } from './timeseries-text.ts';

/** The languages a series is written in as the builder's code. */
export type BuilderLanguage = 'typescript' | 'python';

/** SQL's words per point, the builder's own (timeseries-builder-core.ts, _ts_terms.py), by the tile's word. */
const SCALAR_SOURCE: Readonly<Record<string, string>> = {
  log: 'log', exp: 'exp', sqrt: 'sqrt', abs: 'abs_', power: 'power', not: 'not_',
};

/** Each operator's method (pandas'), and the one that reads the other way round (`5 + x` is `x.radd(5)`). */
const METHOD: Readonly<Record<TsOperator, string>> = {
  '+': 'add', '-': 'sub', '*': 'mul', '/': 'div', '>': 'gt', '>=': 'ge', '<': 'lt', '<=': 'le', '==': 'eq', '!=': 'ne',
  and: 'and', or: 'or',
};
const REVERSED: Readonly<Partial<Record<TsOperator, string>>> = { '+': 'radd', '-': 'rsub', '*': 'rmul', '/': 'rdiv' };
const MIRRORED: Readonly<Record<TsComparison, TsComparison>> = { '<': '>', '<=': '>=', '>': '<', '>=': '<=', '==': '==', '!=': '!=' };

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
  return new Printer(language).source(filled(e)).text;
}

interface Code {
  readonly text: string;
  /** How tightly it binds, Python's (an atom: a call, a column, a number). */
  readonly at: number;
}

class Printer {
  readonly #py: boolean;

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
    const rest = byName.length === 0 ? [] : this.#py ? byName : [`{ ${byName.join(', ')} }`];
    return `${fn}(${[...placed, ...rest].join(', ')})`;
  }

  source(e: TsExpr): Code {
    switch (e.kind) {
      case 'number':
        throw new Error('a number alone is no series of the builder\'s: it is an argument');
      case 'ref':
        return { text: `series(${quote(e.series)})`, at: PY_ATOM };
      case 'column': {
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

  /** TypeScript's arithmetic: pandas' methods. */
  tsBinary(e: Extract<TsExpr, { kind: 'binary' }>): string {
    if (e.left.kind === 'number' && e.right.kind === 'number') throw new Error('arithmetic of two numbers is no series');
    if (e.left.kind === 'number') {
      const reversed = REVERSED[e.op];
      if (reversed !== undefined) return `${this.source(e.right).text}.${reversed}(${e.left.value})`;
      // a comparison with its number first: the series' side compared the other way (1 < x is x > 1)
      return `${this.source(e.right).text}.${METHOD[MIRRORED[e.op as TsComparison]]}(${e.left.value})`;
    }
    const right = e.right.kind === 'number' ? String(e.right.value) : this.source(e.right).text;
    return `${this.source(e.left).text}.${METHOD[e.op]}(${right})`;
  }

  /** Python's arithmetic: its operators, in parentheses where Python binds otherwise than the tree. */
  pyBinary(e: Extract<TsExpr, { kind: 'binary' }>): Code {
    if (e.left.kind === 'number' && e.right.kind === 'number') throw new Error('arithmetic of two numbers is no series');
    // a comparison with its number first: the series' side compared the other way (1 < x is x > 1), as Python reflects it
    const flipped = e.left.kind === 'number' && REVERSED[e.op] === undefined;
    const op = flipped ? MIRRORED[e.op as TsComparison] : e.op;
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
        const c = this.source(e.args[0]!);
        return { text: `~${c.at < PY_NOT ? `(${c.text})` : c.text}`, at: PY_NOT };
      }
      const told = Object.values(e.params ?? {});
      return { text: `${scalar}(${[arg(e.args[0]!), ...told.map((v) => this.literal(v))].join(', ')})`, at: PY_ATOM };
    }
    const entry = Object.entries(BUILDER_WORDS).find(([, s]) => s.word === e.fn);
    if (entry === undefined) throw new Error(`${e.fn} is no word of the builder's`);
    const [tsName, shape] = entry;
    // Python's name gs-quant's own (var, not TypeScript's var_)
    const name = this.#py && shape.gsFunction !== null ? shape.gsFunction.slice(shape.gsFunction.lastIndexOf('.') + 1) : tsName;
    const lines: string[] = [];
    const kw: string[] = [];
    const placed: string[] = [];
    // gs-quant's risk-free cash (item 13b): a line's excess returns over a currency's cash is gs-quant's PRICES of that
    // currency; any other line, its EXCESS_RETURNS
    let args = e.args;
    let excess = false;
    if (takesCash(shape)) {
      const over = e.args.length > 0 ? cashOver(e.args[0]!) : undefined;
      if (over === undefined) {
        excess = true;
      } else {
        args = [over.x, ...e.args.slice(1)];
        placed.push(`RiskFreeRateCurrency.${over.currency}`);
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
    for (const g of shape.gs) {
      if (g.only || g.weights) continue;
      const lib = g.lib ?? g.name;
      const v = told[lib];
      delete told[lib];
      if (v === 'across') {
        throw new Error(`${e.fn}'s ${g.name} across a date's instruments is no call of gs-quant's: none reads a cross-section (write the language's #TS)`);
      }
      if (g.whole !== undefined) {
        // gs-quant's default here is the whole line: written as its call without it; to date is none of its calls
        if (v === 'whole') {
          // a window gs-quant reads for its ramp alone (winsorize's): Window(r, r), its first r points dropped (item 13)
          const ramp = g.whole === 'always' ? told['r'] : undefined;
          if (ramp !== undefined) {
            delete told['r'];
            kw.push(this.named(g.name, String(ramp)));
          }
          continue;
        }
        if (v === undefined || v === 0) throw new Error(`${e.fn}'s ${g.name} to date is no call of gs-quant's: its ${g.name} of no window is the whole series' (write the language's #TS)`);
      }
      if (v === undefined) {
        // left to the tile where gs-quant always has one (a compare's method, the tile's align): no call of gs-quant's says it
        if (g.default !== undefined && g.default !== null && typeof g.default !== 'object') {
          throw new Error(`${e.fn}'s ${g.name} is the tile's own here: gs-quant's ${name} takes ${String(g.default)} unless told -- no call of gs-quant's says it (write the language's #TS)`);
        }
        continue;
      }
      // a window's ramp apart from its size: gs-quant's Window(w, r) (item 13), the library's w and r
      const ramp = typeof g.default === 'object' && g.default !== null ? told['r'] : undefined;
      if (ramp !== undefined) delete told['r'];
      const value = ramp === undefined ? this.gsValue(g, v)
        : `${this.#py ? '' : 'new '}Window(${v === 0 ? this.literal(null) : this.literal(v)}, ${String(ramp)})`;
      if (g.default === undefined) placed.push(value);
      else kw.push(this.named(g.name, value));
    }
    for (const [lib, v] of Object.entries(told)) kw.push(this.named(lib, typeof v === 'string' && /^[1-9][0-9]*[dwmy]$/.test(v) ? quote(v) : this.literal(v)));
    if (excess) kw.push(this.named('curve_type', 'CurveType.EXCESS_RETURNS'));
    return { text: this.call(name, [...lines, ...placed], kw), at: PY_ATOM };
  }

  /** A tile's parameter value as gs-quant's argument: a window its size, `all` every observation (Window(None, 0)), an
   *  option gs-quant's enumeration's member (log is Returns.LOGARITHMIC). */
  gsValue(g: GsParam, v: number | string): string {
    if (v === 0 && g.default !== undefined && typeof g.default === 'object' && g.default !== null) return this.#py ? 'Window(None, 0)' : 'new Window(null, 0)';
    if (typeof v !== 'string') return String(v);
    const gs = Object.entries(g.values ?? {}).find(([, lib]) => lib === v)?.[0] ?? v;
    const members = g.enum === undefined ? undefined : (generated as unknown as Record<string, Readonly<Record<string, string>> | undefined>)[g.enum];
    const member = members === undefined ? undefined : Object.entries(members).find(([, x]) => x === gs)?.[0];
    if (member === undefined) return quote(gs);
    return /^[A-Za-z_$][\w$]*$/.test(member) ? `${g.enum}.${member}` : `${g.enum}[${quote(member)}]`;
  }
}
