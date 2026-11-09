// THE #TS BUILDER (docs/TS_DECISIONS_2026_11_07.md §4, the builders; the order's step 2): each word a function of
// gs-quant 2.2.3's name, its parameters gs-quant's names, order and defaults -- held here to gs-quant's own signatures
// (projects/timeseries/gs-quant-signatures.json, read from gs-quant by tools/ts-oracle/signatures.py), every difference a
// row of the ledger below with its reason; each term's text core's to read, and read back as the term's own tree.

import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import * as builder from '../src/timeseries-builder.ts';
import {
  basket_series, BUILDER_WORDS, column, CurveType, Interpolate, LagMode, not_, percentile, power, RebalFreq, Returns, RiskFreeRateCurrency, series,
  union, volatility, Window, zscores,
  type Term, type WordShape,
} from '../src/timeseries-builder.ts';
import { termOf } from '../src/timeseries-builder-wire.ts';
import { TS_FUNCTION_NAMES, TS_SIGNATURES, type TsExpr } from '../src/timeseries-spec.ts';
import { SCALARS } from '../src/timeseries-words.ts';
import { seriesExpr, seriesText, told, treeOf } from '../src/timeseries-text.ts';
import { timeseriesGalleryPage } from '../src/examples/timeseries-gallery.ts';
import { pageToJson, readPage } from '../src/page-document.ts';
import { builderSource } from '../src/timeseries-builder-source.ts';
import { readPageTexts, readText } from './catalog-builder.ts';

type GsDefault = null | { value: unknown } | { window: [unknown, unknown] } | { enum: string; member: string };
interface Gs {
  readonly words: Readonly<Record<string, readonly { function: string; params: readonly { name: string; kind: string; default: GsDefault }[] }[]>>;
  readonly enums: Readonly<Record<string, Readonly<Record<string, string>>>>;
}
const GS = JSON.parse(readFileSync(runfileFromEnv('GS_QUANT_SIGNATURES'), 'utf8')) as Gs;
const functions = builder as unknown as Readonly<Record<string, (...args: unknown[]) => Term>>;

/** gs-quant's name for a function TypeScript reserves, as the builder spells it. */
const spelled = (gsName: string): string => (gsName === 'var' ? 'var_' : gsName);

/**
 * THE LEDGER: every way a builder word's signature differs from gs-quant's, with why. A parameter gs-quant has that the
 * word computes none of is taken at gs-quant's value alone (`only`); one the library has that gs-quant has not is the
 * builder's by name (`+`); a list of exactly so many series where gs-quant's takes any number; a name TypeScript
 * reserves. A row added or gone fails until this ledger says so.
 */
const LEDGER: Readonly<Record<string, string>> = {
  'var_ spelled': 'TypeScript reserves var: gs-quant\'s own way with Python\'s keywords (sum_, if_), a trailing underscore',
  'mean.mean_type only': 'an arithmetic mean alone: the language has no quadratic one',
  'volatility.assume_zero_mean only': 'deviations about the mean alone (the language has no zero-mean form)',
  'sharpe_ratio.currency only': 'the page\'s cash line, cash.index[currency] (the risk-free data model, item 13b; S155 retired): the line\'s excess returns over it, written as the language\'s excess_returns',
  'sharpe_ratio.curve_type only': 'PRICES writes the line\'s excess returns over the currency\'s cash; EXCESS_RETURNS the line itself (the language\'s sharpe_ratio reads excess-return levels)',
  'sharpe_ratio.method only': 'no risk-free curve to align',
  'correlation.type_ only': 'prices alone: their returns taken first, as gs-quant\'s PRICES',
  'correlation.returns_type only': 'simple returns alone, as the language\'s correlation reads them',
  'correlation.assume_zero_mean only': 'deviations about the mean alone',
  'beta.prices only': 'prices alone: their returns taken first, as gs-quant\'s prices=True',
  'percentiles.y only': 'x\'s own values alone: no second series to rank among',
  'excess_returns.day_count_convention only': 'actual/360 alone, gs-quant\'s default',
  'vol_swap_volatility.annualization_factor only': '252 alone, gs-quant\'s default',
  'vol_swap_volatility.assume_zero_mean only': 'a zero mean alone, gs-quant\'s default',
  'basket_series.costs only': 'no execution costs: the language\'s basket trades free',
  'basket_series.return_type only': 'gs-quant\'s one return type, its default',
  'annualize +annualization_factor': 'the periods a year told, where gs-quant infers them alone',
  'sharpe_ratio +annualization_factor': 'the periods a year told, where gs-quant infers them alone',
  'exponential_volatility +annualization_factor': 'the periods a year told, where gs-quant infers them alone',
  'exponential_spread_volatility +annualization_factor': 'the periods a year told, where gs-quant infers them alone',
  'zscores.w whole': 'gs-quant\'s default is its two-sided z-score of the whole series: written whole, the future read in the text, where the language\'s default is to date (S150; the user, 2026-11-08, kdb\'s discipline)',
  'percentile.w whole': 'gs-quant\'s default is one number of the whole series: written whole, that number on every date, where the language\'s default is to date (S150; the user, 2026-11-08)',
  'winsorize.w whole': 'gs-quant\'s clips by the whole series\' mean and deviation whatever its window, which only drops the first points: written whole, a window its ramp (item 13, r)',
  'union list': 'two series: the language\'s union reads two lines',
  'append list': 'two series: the language\'s append reads two lines',
  'prepend list': 'two series: the language\'s prepend reads two lines',
  'smoothed_moving_average.w default':'the library\'s 14, Wilder\'s, as rsi\'s and atr\'s: gs-quant\'s default smooths by the whole series\' length, each past value changed by data to come (S150\'s look-ahead rule; the user, 2026-11-08)',
};

/** The ledger's rows the generated builder has. */
function differences(): string[] {
  const out: string[] = [];
  for (const [fn, s] of Object.entries(BUILDER_WORDS)) {
    if (s.gsFunction === null) continue;
    const gsName = s.gsFunction.slice(s.gsFunction.lastIndexOf('.') + 1);
    if (fn !== gsName) out.push(`${fn} spelled`);
    for (const g of s.gs) if (g.only) out.push(`${fn}.${g.name} only`);
    for (const g of s.gs) if (g.whole !== undefined) out.push(`${fn}.${g.name} whole`);
    // a parameter told where gs-quant's has a default, or defaulted otherwise
    const gs = GS.words[s.word]?.[0];
    for (const g of s.gs) {
      const p = gs?.params.find((x) => x.name === g.name);
      if (p === undefined || g.only || g.weights) continue;
      if (g.default === undefined && p.default !== null) out.push(`${fn}.${g.name} required`);
      else if (g.default !== undefined && JSON.stringify(g.default) !== JSON.stringify(gsDefault(p.default, GS.enums))) out.push(`${fn}.${g.name} default`);
    }
    // the library's r is gs-quant's own Window's ramp (item 13), told inside the window argument: no difference
    const windowed = s.gs.some((g) => (g.lib ?? g.name) === 'w');
    for (const e of s.extra) if (!(e === 'r' && windowed)) out.push(`${fn} +${e}`);
    if (s.listOf !== undefined) out.push(`${fn} list`);
  }
  return out.sort();
}

/** gs-quant's default as the builder's shape holds it. */
function gsDefault(d: GsDefault, enums: Gs['enums']): unknown {
  if (d === null) return undefined;
  if ('window' in d) return { window: d.window };
  if ('enum' in d) return enums[d.enum]?.[d.member] ?? d.member;
  return d.value;
}

describe('the builder\'s words are gs-quant\'s', () => {
  it('every word a tile draws is a builder function: gs-quant\'s name where gs-quant has one, else the library\'s', () => {
    const made = new Map(Object.values(BUILDER_WORDS).map((s) => [s.word, s]));
    for (const name of TS_FUNCTION_NAMES) {
      if (SCALARS[name] !== undefined) continue;
      const s = made.get(name);
      assert.ok(s !== undefined, `${name} has a builder function`);
      const gs = GS.words[name]?.[0];
      assert.equal(s.gsFunction, gs?.function ?? null, `${name}'s gs-quant function`);
    }
    for (const [fn, s] of Object.entries(BUILDER_WORDS)) assert.equal(typeof functions[fn], 'function', `${fn} (${s.word}) is exported`);
    // SQL's per point, the core's: gs-quant's algebra's names
    for (const fn of ['log', 'exp', 'sqrt', 'abs_', 'power', 'floor', 'ceil', 'not_']) assert.equal(typeof functions[fn], 'function', fn);
  });

  it('each word\'s lines and parameters are gs-quant\'s, by name and in order, each default gs-quant\'s', () => {
    const wrong: string[] = [];
    for (const [word, sigs] of Object.entries(GS.words)) {
      const gs = sigs[0]!;
      const fn = spelled(gs.function.slice(gs.function.lastIndexOf('.') + 1));
      const s: WordShape | undefined = BUILDER_WORDS[fn];
      if (s === undefined) {
        wrong.push(`${word}: no builder function ${fn}`);
        continue;
      }
      const names = [...s.lines, ...s.gs.map((g) => g.name)];
      const theirs = gs.params.map((p) => p.name);
      if (JSON.stringify(names) !== JSON.stringify(theirs)) wrong.push(`${fn}(${names.join(', ')}) where gs-quant's is (${theirs.join(', ')})`);
      for (const g of s.gs) {
        const p = gs.params.find((x) => x.name === g.name);
        if (p === undefined) continue;
        const want = gsDefault(p.default, GS.enums);
        if (LEDGER[`${fn}.${g.name} required`] !== undefined || LEDGER[`${fn}.${g.name} default`] !== undefined) continue;
        if (JSON.stringify(g.default) !== JSON.stringify(want)) wrong.push(`${fn}'s ${g.name} defaults to ${JSON.stringify(g.default)} where gs-quant's is ${JSON.stringify(want)}`);
        if ((p.kind === 'KEYWORD_ONLY') !== (g.kwOnly === true)) wrong.push(`${fn}'s ${g.name} by name alone where gs-quant's is ${p.kind}`);
      }
    }
    assert.deepEqual(wrong, []);
  });

  it('every difference from gs-quant\'s signatures is a row of the ledger, with its reason', () => {
    assert.deepEqual(differences(), Object.keys(LEDGER).sort());
    for (const [row, why] of Object.entries(LEDGER)) assert.ok(why.length > 20, `${row} says why`);
  });
});

describe('a builder term is its #TS text', () => {
  const spx = column('close', 'SPX');
  const ndx = column('close', 'NDX');
  const { volatility, mean, compare, basket_series, sharpe_ratio, lag, max_, if_, smooth_spikes, append, log, power } = functions;

  it('writes gs-quant\'s defaults explicitly, in the language\'s spelling', () => {
    assert.equal(volatility!(spx).text(), "volatility(close['SPX'], w => all, type => 'simple')");
    assert.equal(volatility!(spx, 22, Returns.LOGARITHMIC).text(), "volatility(close['SPX'], w => 22, type => 'log')");
    assert.equal(volatility!(spx, { returns_type: Returns.ABSOLUTE, annualization_factor: 52 }).text(),
      "volatility(close['SPX'], w => all, type => 'absolute', annualization_factor => 52)");
    assert.equal(mean!(spx, '3m').text(), "mean(close['SPX'], 3mo)");
    assert.equal(mean!(spx, new Window(22)).text(), "mean(close['SPX'], 22)");
    assert.equal(mean!(spx, new Window(null, 0)).text(), "mean(close['SPX'], all)");
    assert.equal(compare!(spx, ndx).text(), "compare(close['SPX'], close['NDX'], method => 'step')");
    assert.equal(basket_series!([spx, ndx]).text(), "basket(close['SPX'], 0.5, close['NDX'], 0.5, rebalance => 'daily')");
    assert.equal(basket_series!([spx, ndx], [0.6, 0.4], null, 'monthly').text(),
      "basket(close['SPX'], 0.6, close['NDX'], 0.4, rebalance => 'monthly')");
    assert.equal(smooth_spikes!(spx, 0.3).text(), "smooth_spikes(close['SPX'], threshold => 0.3, type => 'percentage')");
    assert.equal(lag!(spx, 2, LagMode.TRUNCATE).text(), "lag(close['SPX'], 2, 'truncate')");
    assert.equal(lag!(spx).text(), "lag(close['SPX'], 1, 'extend')");
    // gs-quant's PRICES over a currency's cash: the page's cash line (the risk-free data model, item 13b)
    assert.equal(sharpe_ratio!(spx).text(), "sharpe_ratio(excess_returns(close['SPX'], cash.index['USD']), w => all)");
    assert.equal(sharpe_ratio!(spx, RiskFreeRateCurrency.EUR, 252).text(), "sharpe_ratio(excess_returns(close['SPX'], cash.index['EUR']), w => 252)");
    assert.equal(sharpe_ratio!(spx, { w: 252, curve_type: CurveType.EXCESS_RETURNS }).text(), "sharpe_ratio(close['SPX'], w => 252)");
    assert.equal(builderSource(sharpe_ratio!(spx, RiskFreeRateCurrency.EUR, 252).expr, 'python'),
      "sharpe_ratio(column('close', 'SPX'), RiskFreeRateCurrency.EUR, w=252)");
    assert.equal(builderSource(sharpe_ratio!(spx, { w: 252, curve_type: CurveType.EXCESS_RETURNS }).expr, 'python'),
      "sharpe_ratio(column('close', 'SPX'), w=252, curve_type=CurveType.EXCESS_RETURNS)");
    assert.throws(() => sharpe_ratio!(spx, { curve_type: 'levels' as never }), /curve_type is CurveType.PRICES or CurveType.EXCESS_RETURNS/);
    assert.equal(append!([spx, ndx]).text(), "append(close['SPX'], close['NDX'])");
    // gs-quant's whole-series figures written whole, the future read seen (the user, 2026-11-08)
    assert.equal(functions['zscores']!(spx).text(), "zscores(close['SPX'], w => whole)");
    assert.equal(functions['zscores']!(spx, 22).text(), "zscores(close['SPX'], w => 22)");
    assert.equal(functions['percentile']!(spx, 90).text(), "percentile(close['SPX'], n => 90, w => whole)");
    assert.equal(functions['winsorize']!(spx).text(), "winsorize(close['SPX'], limit => 2.5, w => whole)");
  });

  it('arithmetic as pandas\' methods; columns, series and SQL\'s words per point', () => {
    assert.equal(max_!(spx, 10).div(spx).sub(1).text(), "max(close['SPX'], 10) / close['SPX'] - 1");
    assert.equal(if_!(spx.gt(mean!(spx, 50)), spx, 0).text(), "if(close['SPX'] > mean(close['SPX'], 50), close['SPX'], 0)");
    assert.equal(log!(power!(spx, 2)).text(), "ln(power(close['SPX'], 2))");
    assert.equal(series('A').add(1).mul(column('px.volume', 'AAPL').period('sum')).text(), "(A + 1) * sum(px.volume['AAPL'])");
    assert.equal(column('close', { venue: 'NYSE' }, 'AAPL').text(), "close[venue = 'NYSE', 'AAPL']");
  });

  it('a window\'s ramp apart from its size is gs-quant\'s Window(w, r): the library\'s r (item 13)', () => {
    assert.equal(volatility!(spx, new Window(22, 0)).text(), "volatility(close['SPX'], w => 22, type => 'simple', r => 0)");
    assert.equal(mean!(spx, new Window(50, 10)).text(), "mean(close['SPX'], 50, 10)");
    assert.equal(mean!(spx, new Window(null, 5)).text(), "mean(close['SPX'], all, 5)");
    assert.equal(mean!(spx, new Window(50, 50)).text(), "mean(close['SPX'], 50)", 'a ramp the size is the window alone');
    assert.throws(() => mean!(spx, new Window('3m', 10)), /a calendar window's ramp apart from its period is none of the language's yet/);
    assert.equal(functions['bollinger_bands']!(spx, new Window(20, 5)).text(), "bollinger_bands(close['SPX'], w => 20, k => 2, r => 5)");
    assert.throws(() => functions['smoothed_moving_average']!(spx, new Window(20, 5)), /smoothed_moving_average takes no ramp apart from its window's size/);
    // winsorize: gs-quant clips by the whole series whatever its window, which drops its first points alone
    assert.equal(functions['winsorize']!(spx, 2.5, 22).text(), "winsorize(close['SPX'], limit => 2.5, w => whole, r => 22)");
    assert.equal(functions['winsorize']!(spx, 2.5, new Window(null, 5)).text(), "winsorize(close['SPX'], limit => 2.5, w => whole, r => 5)");
    assert.equal(builderSource(functions['winsorize']!(spx, 2.5, 22).expr, 'python'), "winsorize(column('close', 'SPX'), limit=2.5, w=22)");
    assert.throws(() => functions['winsorize']!(spx, 2.5, new Window(30, 5)), /reads its window's size only to give nothing where the line is shorter/);
    assert.throws(() => functions['winsorize']!(spx, 2.5, 0), /refuses a window of 0/);
  });

  it('refuses in words what the language has no way to say, never drops it', () => {
    assert.throws(() => volatility!(spx, 22, Returns.SIMPLE, null, true), /DataCube's has no assume_zero_mean -- gs-quant's false alone/);
    assert.throws(() => compare!(spx, ndx, Interpolate.TIME), /compare's method is one of .*not "time"/);
    assert.throws(() => append!([spx]), /append reads a list of 2 series, not 1/);
    assert.throws(() => volatility!(spx, { window: 22 }), /volatility has no parameter window/);
    assert.throws(() => volatility!(spx, 22, { w: 63 }), /volatility's w is given twice/);
    assert.throws(() => series('a'), /a series' id is a capital letter/);
  });
});

/** A call of builder function `fn` with sample lines and each required parameter: its lines columns. */
function sampleCall(fn: string, s: WordShape): Term {
  const sig = TS_SIGNATURES[s.word];
  const cols = ['close', 'open', 'high', 'low'];
  const lines: unknown[] = s.members !== undefined || s.listOf !== undefined
    ? [Array.from({ length: s.listOf ?? 2 }, (_, i) => column(cols[i]!))]
    : s.lines.map((_, i) => column(cols[i] ?? 'close'));
  const required = s.gs.filter((g) => g.default === undefined && !g.only && !g.kwOnly).map((g): unknown => {
    if (g.weights) return [0.5, 0.5];
    const p = sig.params.find((q) => q.name === (g.lib ?? g.name))!;
    return p.kind === 'word' ? p.words![0] : p.kind === 'number' ? (p.max ?? 50) / 2 : 5;
  });
  return functions[fn]!(...lines, ...required);
}

/** A tree as text, its keys in order: two trees the same are the same text. */
function canonical(x: TsExpr): string {
  return JSON.stringify(x, (_, v: unknown) => (v !== null && typeof v === 'object' && !Array.isArray(v)
    ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : v));
}

it('every builder word\'s default call is text core reads back as the term\'s own tree', async () => {
  const wrong: string[] = [];
  for (const [fn, s] of Object.entries(BUILDER_WORDS)) {
    const term = sampleCall(fn, s);
    const own = term.text();
    const read = await readText(own);
    if (read.text === undefined) {
      wrong.push(`${fn}: ${own}\n    refused: ${read.error.message}`);
      continue;
    }
    if (read.text !== own) wrong.push(`${fn}: ${own}\n    printed by core as ${read.text}`);
    const back = canonical(told(seriesExpr(read.item.expr!, [])));
    if (back !== canonical(told(term.expr))) wrong.push(`${fn}: ${own}\n    read back as ${back}`);
  }
  assert.deepEqual(wrong, []);
});

it('a number is written with every digit it has, never in exponent form, and core reads it back so', async () => {
  // the review of 2026-11-08: 1e-25 was written 0, 1e21 in exponent form, a 17-digit fraction cut to 14
  const cases: [number, string][] = [
    [1e-25, '0.0000000000000000000000001'], [1e21, '1000000000000000000000'],
    [1.2345678901234566e-7, '0.00000012345678901234566'], [-2.5e-8, '-0.000000025'], [0.5, '0.5'], [42, '42'],
  ];
  for (const [n, text] of cases) {
    const own = column('close').mul(n).text();
    assert.equal(own, `close * ${text}`, String(n));
    const read = await readText(own);
    assert.equal(read.text, own, `${n}: core prints it back as written`);
  }
});

it('every series is the builder\'s code, which run builds the same tree (B4\'s Show TypeScript, begun)', () => {
  const names = Object.keys(functions);
  const run = (code: string): Term => new Function(...names, `return ${code};`)(...names.map((n) => functions[n])) as Term;
  const { volatility, mean, compare, basket_series, percentile, zscores, append, if_, Returns: R } = functions as unknown as Record<string, (...a: unknown[]) => Term> & { Returns: typeof Returns };
  const spx = column('close', 'SPX');
  const ndx = column('px.close', { venue: 'NYSE' }, 'NDX');
  const terms: Term[] = [
    ...Object.entries(BUILDER_WORDS).map(([fn, s]) => sampleCall(fn, s)),
    volatility!(spx, 22, R.LOGARITHMIC).div(ndx).sub(1), mean!(series('A'), '3m').rmul(100), spx.gt(mean!(spx, 50)).and(spx.lt(200)),
    compare!(spx, ndx), basket_series!([spx, ndx], [0.6, 0.4], null, 'monthly'), percentile!(spx, 90), percentile!(spx, 90, 22),
    zscores!(spx), zscores!(spx, '1y'), append!([spx, ndx]), if_!(spx.gt(1), spx, 0), column('volume', 'SPX').period('sum').rdiv(1),
  ];
  for (const t of terms) {
    const code = builderSource(t.expr);
    assert.equal(canonical(told(run(code).expr)), canonical(told(t.expr)), code);
  }
});

it('a term as Python tells it is the builder\'s own (B3\'s Python twin: python/legend_lite/timeseries.py)', () => {
  const spx = { term: 'column', name: 'close', picks: ['SPX'] };
  const cases: readonly (readonly [unknown, Term])[] = [
    [{ term: 'word', name: 'volatility', lines: [spx], kw: { w: 22, returns_type: 'logarithmic' } }, volatility(column('close', 'SPX'), 22, Returns.LOGARITHMIC)],
    [{ term: 'word', name: 'percentile', lines: [spx], kw: { n: 90, w: { term: 'window', w: '1y', r: '1y' } } }, percentile(column('close', 'SPX'), 90, new Window('1y'))],
    [{ term: 'word', name: 'zscores', lines: [spx], kw: {} }, zscores(column('close', 'SPX'))],
    [{ term: 'word', name: 'union', lines: [[spx, { term: 'series', id: 'A' }]], kw: {} }, union([column('close', 'SPX'), series('A')])],
    [{ term: 'word', name: 'basket_series', lines: [[spx, { term: 'column', name: 'close', picks: ['NDX'] }]], kw: { weights: [0.6, 0.4], rebal_freq: 'monthly' } },
      basket_series([column('close', 'SPX'), column('close', 'NDX')], [0.6, 0.4], null, RebalFreq.MONTHLY)],
    [{ term: 'method', name: 'sub', x: { term: 'method', name: 'div', x: spx, y: { term: 'series', id: 'B' } }, y: 1 }, column('close', 'SPX').div(series('B')).sub(1)],
    [{ term: 'method', name: 'and', x: { term: 'method', name: 'gt', x: spx, y: 100 }, y: { term: 'scalar', name: 'not_', x: { term: 'method', name: 'lt', x: spx, y: 50 } } },
      column('close', 'SPX').gt(100).and(not_(column('close', 'SPX').lt(50)))],
    [{ term: 'scalar', name: 'power', x: spx, args: [2] }, power(column('close', 'SPX'), 2)],
    [{ term: 'method', name: 'radd', x: { term: 'column', name: 'px.volume', picks: [{ venue: 'NYSE' }], period: 'sum' }, y: 5 },
      column('px.volume', { venue: 'NYSE' }).period('sum').radd(5)],
  ];
  for (const [wire, want] of cases) assert.deepEqual(termOf(wire).expr, want.expr, JSON.stringify(wire));
  assert.throws(() => termOf({ term: 'word', name: 'vola', lines: [spx], kw: {} }), /vola is no word of the builder's/);
  assert.throws(() => termOf({ term: 'word', name: 'volatility', lines: [spx], kw: { width: 3 } }), /volatility has no parameter width/);
  assert.throws(() => termOf(42), /is no series/);
  // a period's value one of the language's (the review of 2026-11-08: 'median' was written as a call of nothing)
  assert.throws(() => termOf({ term: 'column', name: 'close', period: 'median' }), /a period's value is one of last, first, average, sum, min, max, not median/);
});

/**
 * B4'S SHOW PYTHON: every gallery series, and every word's own call, as Python's builder code with the text it writes --
 * held to test/fixtures/timeseries/builder-python.json, which //python:ts_builder_test runs in Python (each source a
 * term, its text DataCube's builder's, read by core). A series gs-quant has no call for is listed with why. A change on
 * purpose rewrites the fixture:
 *   bazel test //datacube:timeseries_builder_test --strategy=TestRunner=local \
 *     --test_env=BUILDER_PYTHON_UPDATE=$PWD/datacube/test/fixtures/timeseries/builder-python.json
 */
it('every gallery series and every word as Python\'s builder code, with its text (held for Python to run)', async () => {
  const page = await readPageTexts(readPage(pageToJson(await timeseriesGalleryPage())));
  const rows: { readonly series: string; readonly python?: string; readonly refused?: string; readonly text: string }[] = [];
  for (const [fn, s] of Object.entries(BUILDER_WORDS)) {
    const expr = told(sampleCall(fn, s).expr);
    rows.push({ series: `word ${fn}`, python: builderSource(expr, 'python'), text: seriesText(expr, []) });
  }
  for (const v of page.views) {
    if (v.kind !== 'timeseries') continue;
    for (const s of v.spec.series) {
      const tree = treeOf(s, v.spec.series);
      try {
        rows.push({ series: `${v.id} ${s.id}`, python: builderSource(tree, 'python'), text: s.text });
      } catch (e) {
        rows.push({ series: `${v.id} ${s.id}`, refused: (e as Error).message, text: s.text });
      }
    }
  }
  const text = `${JSON.stringify(rows, null, 1)}\n`;
  const update = process.env['BUILDER_PYTHON_UPDATE'];
  if (update) {
    writeFileSync(update, text);
    return;
  }
  assert.equal(readFileSync(runfileFromEnv('BUILDER_PYTHON'), 'utf8'), text,
    'every series as Python\'s builder code as held (a change on purpose rewrites the fixture: the comment above says how)');
});

/** A YAML flow sequence's scalars as the oracle's mapping writes them ("..." with \" and \\, or plain, split at ", "):
 *  tools/gs-quant-oracle/snap.py's flow_scalars and TsGsquantParityTest's flowScalars. */
function flowScalars(text: string): string[] {
  const out: string[] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] === '"') {
      let j = i + 1;
      let buf = '';
      while (text[j] !== '"') {
        if (text[j] === '\\') j++;
        buf += text[j];
        j++;
      }
      out.push(buf);
      i = j + 1;
    } else {
      let j = text.indexOf(', ', i);
      j = j < 0 ? text.length : j;
      out.push(text.slice(i, j));
      i = j;
    }
    if (text.startsWith(', ', i)) i += 2;
  }
  return out;
}

/**
 * THE GS-QUANT ORACLE'S CALLS (docs/TS_DECISIONS_2026_11_07.md item 14f, step 2: one oracle, its gs-quant side the
 * Show Python printer's): each case of tools/gs-quant-oracle/mapping.yaml, its #TS item read by core and printed as
 * Python's builder code -- the gs-quant call snap.py runs for it, `column('close', 'NVDA')` the case's x and 'SPX' its y
 * (over a fixture, its trap symbol and that symbol's y). A whole statement (WITH ...) is no series the printer prints:
 * STATEMENT, its gs-quant call the mapping's. Held to tools/gs-quant-oracle/calls.tsv; a change on purpose rewrites it:
 *   bazel test //datacube:timeseries_builder_test --strategy=TestRunner=local \
 *     --test_env=GS_ORACLE_CALLS_UPDATE=$PWD/tools/gs-quant-oracle/calls.tsv
 */
it('every case of the gs-quant oracle as Show Python prints it: the call gs-quant answers', async () => {
  const lines = ['# THE GS-QUANT ORACLE\'S CALLS (generated by //datacube:timeseries_builder_test from mapping.yaml): each case\'s #TS'
    + ' item as Show Python prints it -- the call snap.py runs. GENERATED: see the test for how to rewrite it.', 'case\tpython'];
  let word: string | undefined;
  for (const line of readFileSync(runfileFromEnv('GS_ORACLE_MAPPING'), 'utf8').split('\n')) {
    const w = /^ {2}- word: (\S+)$/.exec(line);
    if (w) {
      word = w[1];
      continue;
    }
    if (/^ {2}- \{word: /.test(line)) {
      word = undefined;
      continue;
    }
    const c = /^ {6}- \[(.*)\]$/.exec(line);
    if (!c || word === undefined) continue;
    const [args, ts] = flowScalars(c[1]!) as [string, string];
    const key = `${word} ${args}`;
    if (ts.startsWith('WITH')) {
      lines.push(`${key}\tSTATEMENT`);
      continue;
    }
    const text = ts.replaceAll('{x}', 'close[\'NVDA\']').replaceAll('{y}', 'close[\'SPX\']');
    const read = await readText(text);
    if (read.text === undefined) {
      lines.push(`${key}\tREFUSED: core reads none: ${read.error.message.split('\n')[0]}`);
      continue;
    }
    try {
      lines.push(`${key}\t${builderSource(told(seriesExpr(read.item.expr!, [])), 'python')}`);
    } catch (e) {
      lines.push(`${key}\tREFUSED: ${(e as Error).message.split('\n')[0]}`);
    }
  }
  const text = `${lines.join('\n')}\n`;
  const update = process.env['GS_ORACLE_CALLS_UPDATE'];
  if (update) {
    writeFileSync(update, text);
    return;
  }
  assert.equal(readFileSync(runfileFromEnv('GS_ORACLE_CALLS'), 'utf8'), text,
    'every oracle case as Show Python prints it, as held (a change on purpose rewrites the file: the comment above says how)');
});
