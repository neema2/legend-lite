// A BUILDER TERM AS PYTHON TELLS IT (B3's Python twin, docs/TS_DECISIONS_2026_11_07.md §4, the builders): Python's DSL
// (python/legend_lite/timeseries.py) is gs-quant's syntax alone -- its words by name, its operators, its Window and
// enumerations -- each call told as JSON and made here by this builder's own functions, so a word's way to the tile's
// tree is written once (timeseries-builder-core.ts word, the generated shapes), whichever language called it.
//
//   {term: 'column', name, picks?, period?}   column(name, ...picks).period(period)
//   {term: 'series', id}                      series(id)
//   {term: 'word', name, lines, kw}           a generated word, its lines by place and the rest by name
//   {term: 'scalar', name, x, args?}          log, exp, sqrt, abs_, power, not_
//   {term: 'method', name, x, y}              x.add(y), x.radd(y), x.gt(y), x.and(y), ...
//   {term: 'window', w, r}                    new Window(w, r), a word's argument
//
// PURE: no DOM, no planner.

import { BUILDER_WORDS } from './generated/ts-builder-words.ts';
import { abs_, BuilderRefusal, column, ColumnTerm, exp, log, not_, power, series, sqrt, Term, Window, word } from './timeseries-builder-core.ts';
import type { TsAggregate } from './timeseries-spec.ts';

/** The methods of a term Python's operators and methods call (pandas' names). */
const METHODS = new Set(['add', 'sub', 'mul', 'div', 'radd', 'rsub', 'rmul', 'rdiv', 'gt', 'ge', 'lt', 'le', 'eq', 'ne', 'and', 'or']);
const SCALARS: Readonly<Record<string, (x: Term, ...a: number[]) => Term>> = {
  log, exp, sqrt, abs_, not_, power: (x, y) => power(x, y),
};

const isRecord = (v: unknown): v is Readonly<Record<string, unknown>> => typeof v === 'object' && v !== null && !Array.isArray(v);

/** The term `w` tells, refused in the builder's words where it is none. */
export function termOf(w: unknown): Term {
  const v = argOf(w);
  if (!(v instanceof Term)) throw new BuilderRefusal(`${JSON.stringify(w)} is no series: a column, a series, a word, a scalar or arithmetic of them`);
  return v;
}

/** An argument as the builder's functions take it: a term, a window, a list of them, or a value as it is. */
function argOf(w: unknown): unknown {
  if (Array.isArray(w)) return w.map(argOf);
  if (!isRecord(w)) return w;
  switch (w['term']) {
    case 'column': {
      const picks = Array.isArray(w['picks']) ? (w['picks'] as never[]) : [];
      const c: ColumnTerm = column(String(w['name']), ...picks);
      return w['period'] === undefined || w['period'] === null ? c : c.period(w['period'] as TsAggregate);
    }
    case 'series':
      return series(String(w['id']));
    case 'window':
      return new Window(w['w'] as number | string | null, w['r'] as number | string | null | undefined);
    case 'scalar': {
      const fn = SCALARS[String(w['name'])];
      if (fn === undefined) throw new BuilderRefusal(`${String(w['name'])} is no word of SQL's per point the builder has`);
      return fn(termOf(w['x']), ...((Array.isArray(w['args']) ? w['args'] : []) as number[]));
    }
    case 'method': {
      const name = String(w['name']);
      if (!METHODS.has(name)) throw new BuilderRefusal(`${name} is no method of a series`);
      const y = argOf(w['y']);
      return (termOf(w['x']) as unknown as Record<string, (o: unknown) => Term>)[name]!(y);
    }
    case 'word': {
      // the word's shape, its parameters by name (word() takes every one so, a required one too)
      const name = String(w['name']);
      const shape = BUILDER_WORDS[name];
      if (shape === undefined) throw new BuilderRefusal(`${name} is no word of the builder's`);
      const told = (Array.isArray(w['lines']) ? w['lines'] : []).map(argOf);
      // a word of one list of series (union's, append's) reads the list's series as its lines
      const lines = shape.listOf !== undefined && told.length === 1 && Array.isArray(told[0]) ? told[0] as Term[] : told;
      const kw = isRecord(w['kw']) ? Object.fromEntries(Object.entries(w['kw']).map(([k, x]) => [k, argOf(x)])) : {};
      return word(shape, lines as never, Object.keys(kw).length > 0 ? [kw] : []);
    }
    default:
      throw new BuilderRefusal(`${JSON.stringify(w)} is no term of the builder's`);
  }
}
