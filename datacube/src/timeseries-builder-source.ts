// A SERIES AS THE BUILDER'S CODE (docs/TS_DECISIONS_2026_11_07.md §4, the builders; B4's Show TypeScript, begun with the
// order's step 3): a tile's tree written as a person writes it with src/timeseries-builder.ts -- gs-quant's names, its
// required parameters by place and the rest by name in a trailing object, columns and series by name, arithmetic as
// pandas' methods -- which, run, builds the same tree again (timeseries-builder.test.ts holds it so).

import type { TsComparison, TsExpr, TsOperator } from './timeseries-spec.ts';
import { BUILDER_WORDS } from './generated/ts-builder-words.ts';
import type { GsParam } from './timeseries-builder-core.ts';
import { told as filled } from './timeseries-text.ts';

/** SQL's words per point, the builder's own (timeseries-builder-core.ts), by the tile's word. */
const SCALAR_SOURCE: Readonly<Record<string, string>> = {
  log: 'log', exp: 'exp', sqrt: 'sqrt', abs: 'abs_', power: 'power', floor: 'floor', ceil: 'ceil', not: 'not_',
};

/** Each operator's method (pandas'), and the one that reads the other way round (`5 + x` is `x.radd(5)`). */
const METHOD: Readonly<Record<TsOperator, string>> = {
  '+': 'add', '-': 'sub', '*': 'mul', '/': 'div', '>': 'gt', '>=': 'ge', '<': 'lt', '<=': 'le', '==': 'eq', '!=': 'ne',
  and: 'and', or: 'or',
};
const REVERSED: Readonly<Partial<Record<TsOperator, string>>> = { '+': 'radd', '-': 'rsub', '*': 'rmul', '/': 'rdiv' };
const MIRRORED: Readonly<Record<TsComparison, TsComparison>> = { '<': '>', '<=': '>=', '>': '<', '>=': '<=', '==': '==', '!=': '!=' };

const quote = (v: string): string => `'${v.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
const literal = (v: string | number | boolean): string => (typeof v === 'string' ? quote(v) : String(v));

/** The builder's code of `e`: each parameter as the tile's tree means it (its default written where it was left out),
 *  refused in words where gs-quant has no call of that meaning (a z-score to date: gs-quant's of no window is the whole
 *  series'). */
export function builderSource(e: TsExpr): string {
  return source(filled(e));
}

function source(e: TsExpr): string {
  switch (e.kind) {
    case 'number':
      throw new Error('a number alone is no series of the builder\'s: it is an argument');
    case 'ref':
      return `series(${quote(e.series)})`;
    case 'column': {
      const name = e.source === undefined ? e.column : `${e.source}.${e.column}`;
      const where = (e.where ?? []).map((m) => (m.column === undefined ? literal(m.value) : `{ ${/^[A-Za-z_$][\w$]*$/.test(m.column) ? m.column : quote(m.column)}: ${literal(m.value)} }`));
      const col = `column(${[quote(name), ...where].join(', ')})`;
      return e.fn === undefined || e.fn === 'last' ? col : `${col}.period(${quote(e.fn)})`;
    }
    case 'binary': {
      if (e.left.kind === 'number' && e.right.kind === 'number') throw new Error('arithmetic of two numbers is no series');
      if (e.left.kind === 'number') {
        const reversed = REVERSED[e.op];
        if (reversed !== undefined) return `${source(e.right)}.${reversed}(${e.left.value})`;
        // a comparison with its number first: the series' side compared the other way (1 < x is x > 1)
        return `${source(e.right)}.${METHOD[MIRRORED[e.op as TsComparison]]}(${e.left.value})`;
      }
      const right = e.right.kind === 'number' ? String(e.right.value) : source(e.right);
      return `${source(e.left)}.${METHOD[e.op]}(${right})`;
    }
    case 'call':
      return callSource(e);
  }
}

function callSource(e: Extract<TsExpr, { kind: 'call' }>): string {
  const scalar = SCALAR_SOURCE[e.fn];
  const arg = (a: TsExpr): string => (a.kind === 'number' ? String(a.value) : source(a));
  if (scalar !== undefined) {
    const told = Object.values(e.params ?? {});
    return `${scalar}(${[arg(e.args[0]!), ...told.map((v) => literal(v))].join(', ')})`;
  }
  const entry = Object.entries(BUILDER_WORDS).find(([, s]) => s.word === e.fn);
  if (entry === undefined) throw new Error(`${e.fn} is no word of the builder's`);
  const [name, shape] = entry;
  const lines: string[] = [];
  const kw: string[] = [];
  const placed: string[] = [];
  if (shape.members !== undefined) {
    // series and their weights: gs-quant's two lists
    const series = e.args.filter((_, i) => i % 2 === 0).map(arg);
    const weights = e.args.filter((_, i) => i % 2 === 1).map(arg);
    lines.push(`[${series.join(', ')}]`);
    const w = shape.gs.find((g) => g.weights === true)!;
    if (w.default === undefined) placed.push(`[${weights.join(', ')}]`);
    else kw.push(`${w.name}: [${weights.join(', ')}]`);
  } else if (shape.listOf !== undefined) {
    lines.push(`[${e.args.map(arg).join(', ')}]`);
  } else {
    lines.push(...e.args.map(arg));
  }
  const told = { ...(e.params ?? {}) };
  for (const g of shape.gs) {
    if (g.only || g.weights) continue;
    const lib = g.lib ?? g.name;
    const v = told[lib];
    delete told[lib];
    if (g.whole !== undefined) {
      // gs-quant's default here is the whole line: written as its call without it; to date is none of its calls
      if (v === 'whole') continue;
      if (v === undefined || v === 0) throw new Error(`${e.fn}'s ${g.name} to date is no call of gs-quant's: its ${g.name} of no window is the whole series' (write the language's #TS)`);
    }
    if (v === undefined) continue;
    const value = gsValueOf(e.fn, g, v);
    if (g.default === undefined) placed.push(value);
    else kw.push(`${g.name}: ${value}`);
  }
  for (const [lib, v] of Object.entries(told)) kw.push(`${lib}: ${typeof v === 'string' && /^[1-9][0-9]*[dwmy]$/.test(v) ? quote(v) : literal(v)}`);
  const args = [...lines, ...placed, ...(kw.length > 0 ? [`{ ${kw.join(', ')} }`] : [])];
  return `${name}(${args.join(', ')})`;
}

/** A tile's parameter value as gs-quant's argument: a window its size, `all` every observation (Window(None, 0)), the
 *  whole line none (the builder's gs-quant default writes it), an option gs-quant's value (log is LOGARITHMIC's). */
function gsValueOf(fn: string, g: GsParam, v: number | string): string {
  void fn;
  if (v === 0 && g.default !== undefined && typeof g.default === 'object' && g.default !== null) return 'new Window(null, 0)';
  if (typeof v === 'string') {
    const gs = Object.entries(g.values ?? {}).find(([, lib]) => lib === v)?.[0];
    return quote(gs ?? v);
  }
  return String(v);
}
