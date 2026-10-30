// A TILE'S SERIES AS THE TIMESERIES LANGUAGE (docs/TS_LANGUAGE_BUILD_2026_10_30.md P5; the language,
// docs/TIMESERIES_LANGUAGE_2026_10_29.md): the #TS{...}# query that answers what the tile's own writer
// (timeseries-query.ts seriesQuery) answers -- each grid a source of the query, bound by `let` and read as `$alias`;
// the tile's frequency its SAMPLE BY, its alignment its FILL; each function the language's of the same name and
// parameters (the platform's TsFunctions, the tile's catalog), the tile's per-point rules spelled in SQL (a division
// none where its divisor is 0, a comparison 1 or 0). The tile's vocabulary -- gs-quant's, its Python DSL's -- stays the
// surface (ruling 7); this is what it means underneath. It writes words: the platform's #TS builder writes the Pure.

import { fn, lambda, lit, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { sourceWithDerived } from './query.ts';
import { CubeRefusal } from './snapshot.ts';
import { inlined, sourceAlias, sourcesOf } from './timeseries-expr.ts';
import type { TsInputs } from './timeseries-query.ts';
import { TS_SIGNATURES, type TimeseriesSpec, type TsAggregate, type TsExpr, type TsMatch, type TsSeries } from './timeseries-spec.ts';
import type { TimeGrain } from './chart-spec.ts';

/** The answer's column of a series' value; a struct's fields are `ts_value_<field>` (R10). */
export const TS_ITEM = 'ts_value';

/** The language's name of each of the tile's functions where they differ (§6: a name SQL reserves, a period's
 *  aggregate's). */
const RENAMED: Readonly<Record<string, string>> = {
  range: 'range_of', first: 'first_of', last: 'last_of', filter: 'filter_values', union: 'union_of',
  day: 'day_of', month: 'month_of', year: 'year_of', quarter: 'quarter_of', weekday: 'weekday_of',
};

/** The functions the builder writes itself (TsToPure's along): their window, lag and type by place. */
const OWN: ReadonlySet<string> = new Set(['mean', 'sum', 'min', 'max', 'std', 'var', 'returns', 'diff', 'lag']);

const SAMPLE: Readonly<Record<TimeGrain, string>> = { day: '1d', week: '1w', month: '1mo', quarter: '1q', year: '1y' };

const FILL: Readonly<Record<string, string | undefined>> = {
  intersect: undefined, nan: 'NULL', zero: '0', time: 'LINEAR', step: 'PREV',
};

/** A period's aggregate, as the language names it (R5). */
const TAKEN: Readonly<Record<TsAggregate, string>> = { last: 'last', first: 'first', average: 'avg', sum: 'sum', min: 'min', max: 'max' };

/** How a period takes each series a candle or a technical reads, by its role (the tile's ROLE_TAKEN, CANDLE). */
const ROLE: Readonly<Record<string, TsAggregate>> = { open: 'first', high: 'max', low: 'min', close: 'last', price: 'last', volume: 'sum', dividends: 'sum' };
const CANDLE = ['open', 'high', 'low', 'close'] as const;

/** A name as SQL writes it: as it is where it is a plain lower-case word, else quoted. */
function ident(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}

function literal(v: string | number | boolean): string {
  return typeof v === 'string' ? `'${v.replaceAll("'", "''")}'` : typeof v === 'boolean' ? (v ? 'TRUE' : 'FALSE') : String(v);
}

/** A window or a lag as the language writes it: observations, `all` for the tile's 0, a span (`3mo` for its `3m`). */
function span(v: number | string): string {
  if (typeof v === 'number') return v === 0 ? 'all' : String(v);
  const m = /^([1-9][0-9]*)([dwmy])$/.exec(v);
  if (!m) throw new CubeRefusal(`a span the tile writes is n then d, w, m or y: ${v}`);
  return `${m[1]}${m[2] === 'm' ? 'mo' : m[2]}`;
}

/** The #TS text of `series` of the tile `ts`, each source read as `$<alias>`: its one item `ts_value`. */
export function tsQueryText(ts: TimeseriesSpec, series: TsSeries): string {
  const expr = inlined(series.expr, ts.series);
  const read = sourcesOf(ts, expr);
  const many = read.length > 1;
  const grain = ts.grain;
  const split = series.split;
  // each source's BY: the split's column where the series splits on it (its first source), else none -- a match on
  // its instrument written as a condition, so a line of no subscript is its column's rows, as the tile's
  const by = (alias: string): string | undefined => (alias === read[0] ? split : undefined);
  const column = (e: Extract<TsExpr, { kind: 'column' }>, role?: TsAggregate): string => {
    const alias = sourceAlias(ts, e)!;
    const src = ts.sources.find((s) => s.alias === alias)!;
    const matches = (e.where ?? []).map((m: TsMatch) => {
      const c = m.column ?? src.key;
      if (c === undefined) throw new CubeRefusal(`${alias} has no instrument column to match ${String(m.value)}`);
      return c === by(alias) ? literal(m.value) : `${ident(c)} = ${literal(m.value)}`;
    });
    const col = `${many ? `${ident(alias)}.` : ''}${ident(e.column)}${matches.length === 0 ? '' : `[${matches.join(', ')}]`}`;
    return grain === undefined ? col : `${TAKEN[e.fn ?? role ?? 'last']}(${col})`;
  };
  const text = (e: TsExpr, role?: TsAggregate): string => {
    switch (e.kind) {
      case 'number':
        return String(e.value);
      case 'ref':
        throw new Error('a series is read inlined');
      case 'column':
        return column(e, role);
      case 'binary': {
        const l = text(e.left);
        const r = text(e.right);
        switch (e.op) {
          case '+': case '-': case '*':
            return `(${l} ${e.op} ${r})`;
          case '/':
            return `(${l} / nullif(${r}, 0))`;
          case 'and': case 'or':
            return `(CASE WHEN ${l} IS NULL OR ${r} IS NULL THEN NULL WHEN (${l} <> 0) ${e.op.toUpperCase()} (${r} <> 0) THEN 1 ELSE 0 END)`;
          default: {
            const op = e.op === '==' ? '=' : e.op === '!=' ? '<>' : e.op;
            return `(CASE WHEN ${l} IS NULL OR ${r} IS NULL THEN NULL WHEN ${l} ${op} ${r} THEN 1 ELSE 0 END)`;
          }
        }
      }
      case 'call': {
        const sig = TS_SIGNATURES[e.fn];
        const roles: readonly (string | undefined)[] = e.fn === 'ohlc' ? CANDLE : (sig.reads ?? []);
        const args = e.args.map((a, i) => text(a, roles[i] === undefined ? undefined : ROLE[roles[i]!]));
        const param = (name: string): number | string => {
          const p = sig.params.find((x) => x.name === name)!;
          return e.params?.[name] ?? p.default;
        };
        switch (e.fn) {
          // the tile's row maths, SQL's (R3: per point)
          case 'log': return `ln(${args[0]})`;
          case 'exp': case 'sqrt': case 'abs': return `${e.fn}(${args[0]})`;
          case 'power': return `power(${args[0]}, ${param('y')})`;
          case 'floor': return `greatest(${args[0]}, ${param('value')})`;
          case 'ceil': return `least(${args[0]}, ${param('value')})`;
          case 'not': return `(CASE WHEN ${args[0]} IS NULL THEN NULL WHEN ${args[0]} = 0 THEN 1 ELSE 0 END)`;
          // a running count from the line's first (the tile's count)
          case 'count': return `count(${args[0]}, all)`;
          default: break;
        }
        const written = sig.params.map((p) => {
          const v = e.params?.[p.name] ?? p.default;
          return [p.name, p.kind === 'span' ? span(v as number | string) : p.kind === 'word' ? literal(String(v)) : String(v)] as const;
        });
        // the builder's own by place; the catalog's by name (R10)
        const params = OWN.has(e.fn) ? written.map(([, v]) => v) : written.map(([n, v]) => `${n} => ${v}`);
        return `${RENAMED[e.fn] ?? e.fn}(${[...args, ...params].join(', ')})`;
      }
    }
  };
  const item = text(expr);
  const sources = read.map((alias) => {
    const src = ts.sources.find((s) => s.alias === alias)!;
    const b = by(alias);
    return `$${alias} ${ident(alias)} ON ${ident(src.date)}${b === undefined ? '' : ` BY ${ident(b)}`}`;
  });
  const fill = FILL[ts.align ?? 'intersect'];
  return `SELECT ${item} AS ${TS_ITEM} FROM ${sources.join(', ')}${grain === undefined ? '' : ` SAMPLE BY ${SAMPLE[grain]}`}`
    + `${fill === undefined ? '' : ` FILL(${fill})`}`;
}

/** The #TS query of `series`: each source it reads bound by `let` to its grid's rows (its derived columns too), then the
 *  island over them. */
export function tsSeriesQuery(inputs: TsInputs, ts: TimeseriesSpec, series: TsSeries): Lambda {
  const read = sourcesOf(ts, inlined(series.expr, ts.series));
  const lets: ValueSpecification[] = read.map((alias) => {
    const input = inputs.get(alias);
    if (!input) throw new CubeRefusal(`source ${alias}'s grid is not open`);
    return fn('letFunction', lit.string(alias), sourceWithDerived(input.snapshot).node);
  });
  const island: ValueSpecification = { _type: 'classInstance', type: 'TS', value: { ts: tsQueryText(ts, series) } };
  return lambda([], ...lets, island);
}
