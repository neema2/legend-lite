// A TIMESERIES TILE'S SPEC (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §3, §4; src/timeseries-spec.ts): the
// expression language read and printed back to the same tree, what it refuses and where, the saved spec's strict reader,
// the problems against the sources' columns, and a first tile of a grid.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { treeProblem, tsProblem, type TimeseriesSpec, type TsAnnotation, type TsExpr } from '../src/timeseries-spec.ts';
import {
  aliasFor, candleOf, columnsOf, defaultTimeseries, exprSource, exprText, functionNamed, inlined, nextSeriesId, parseExpr, refsOf, sourcesOf,
  levelOf, timeseriesProblems, TsSyntaxError, unitOf, type TsColumn, TS_FUNCTIONS, WORDS,
} from '../src/timeseries-expr.ts';
import { TS_WORDS } from '../src/generated/ts-words.ts';
import { completionsAt, offeredWords, treeOf, writtenText, type TsOffer } from '../src/timeseries-text.ts';

const ROUND_TRIPS = [
  'close',
  'close[AAPL]',
  'close[venue=NYSE, AAPL]',
  'volume[AAPL].sum',
  'px:close[SPX]',
  'rates:yield[10Y]',
  '`total pnl`',
  'close["BRK.B"]',
  'close["10"]',
  '$A',
  'returns(close[AAPL])',
  'returns(close[AAPL], 5)',
  'returns(close[AAPL], 1, log)',
  'volatility($A, 63)',
  'volatility($A, 63, log)',
  'zscores($A, 252)',
  // calendar periods: a window of dates, a lag as of a date (gs-quant's tenors)
  'mean(close, 3m)',
  'returns(close[AAPL], 1y)',
  'correlation($A, $B, 6m)',
  'volatility($A, 2w, log)',
  // conditions: comparisons, and, or -- 1 where they hold
  'close > mean(close, 50)',
  'close[AAPL] >= 100 and close[MSFT] < 300',
  'rates:yield[10Y] - rates:yield[2Y] < 0 or $A == 1',
  '(close > 100) * 2',
  'returns(close) != 0',
  'correlation(px:close[SPX], rates:yield[10Y], 126)',
  'bollinger_bands(close[AAPL], 20, 2.5)',
  'close[AAPL] / close[MSFT]',
  '($A + $B) / 2',
  '$A - ($B - $C)',
  '$A / ($B * 2)',
  '100 * returns($A)',
  '-1 * $A',
  'rebase(close[AAPL])',
  'rebase(close[AAPL], 1)',
  'floor(close[AAPL], -5)',
];

describe('the expression language', () => {
  for (const text of ROUND_TRIPS) {
    it(`reads and prints back ${text}`, () => {
      const tree = parseExpr(text);
      assert.equal(exprText(tree), text);
      assert.deepEqual(parseExpr(exprText(tree)), tree);
    });
  }

  it('a parameter left at its default is left off; one told by name is placed', () => {
    assert.equal(exprText(parseExpr('returns(close, 1, simple)')), 'returns(close)');
    assert.equal(exprText(parseExpr('volatility(close, type=log)')), 'volatility(close, 22, log)');
    assert.equal(exprText(parseExpr('returns(close, type=absolute)')), 'returns(close, 1, absolute)');
    // a function of many series' by name: by its place, a word after the weights read as one more series (it was
    // written basket(x, 0.6, y, 0.4, monthly), which no reader read back)
    assert.equal(exprText(parseExpr('basket(x, 0.6, y, 0.4, rebalance=monthly)')), 'basket(x, 0.6, y, 0.4, rebalance=monthly)');
    assert.deepEqual(parseExpr(exprText(parseExpr('basket(x, 0.6, y, 0.4, rebalance=monthly)'))), parseExpr('basket(x, 0.6, y, 0.4, rebalance=monthly)'));
  });

  // the code that builds a tile (timeseries-code.ts): its text reads back as the very expression, each parameter as told
  it('exprSource: each parameter as it was told -- by its place while each before it is, else by its name', () => {
    for (const [text, source] of [
      ['returns(close, type=log)', 'returns(close, type=log)'],
      ['returns(close, 1, simple)', 'returns(close, 1, simple)'],
      ['returns(close)', 'returns(close)'],
      ['volatility(close, 63)', 'volatility(close, 63)'],
      ['basket(x, 0.6, y, 0.4, rebalance=monthly)', 'basket(x, 0.6, y, 0.4, rebalance=monthly)'],
      ['mean(close[AAPL], 3m) / lag($A, 1y)', 'mean(close[AAPL], 3m) / lag($A, 1y)'],
    ] as const) {
      const tree = parseExpr(text);
      assert.equal(exprSource(tree), source);
      assert.deepEqual(parseExpr(exprSource(tree)), tree, `${text}: read back as itself`);
    }
  });

  it('aliases are gs-quant\'s other names, and its own spellings', () => {
    for (const [alias, name] of [['vol', 'volatility'], ['sma', 'mean'], ['moving_average', 'mean'], ['corr', 'correlation'],
      ['zscore', 'zscores'], ['relative_strength_index', 'rsi'], ['abs_', 'abs'], ['sum_', 'sum']] as const) {
      const e = parseExpr(name === 'correlation' ? `${alias}(close, open)` : `${alias}(close)`);
      assert.equal(e.kind === 'call' && e.fn, name, alias);
    }
  });

  it('a tree as the spec keeps it', () => {
    assert.deepEqual(parseExpr('px:volume[AAPL, venue=NYSE].sum'), {
      kind: 'column', source: 'px', column: 'volume', fn: 'sum',
      where: [{ value: 'AAPL' }, { column: 'venue', value: 'NYSE' }],
    });
    assert.deepEqual(parseExpr('volatility($A, w=63)'), {
      kind: 'call', fn: 'volatility', args: [{ kind: 'ref', series: 'A' }], params: { w: 63 },
    });
    assert.deepEqual(parseExpr('-2'), { kind: 'number', value: -2 });
    assert.deepEqual(parseExpr('close[10Y, 2]'), { kind: 'column', column: 'close', where: [{ value: '10Y' }, { value: 2 }] });
  });

  const refused = (text: string, words: RegExp): void => {
    assert.throws(() => parseExpr(text), (e: unknown) => e instanceof TsSyntaxError && words.test(e.message), text);
  };

  it('refuses what it cannot read, saying where', () => {
    refused('returns(close', /"\)" was expected/);
    refused('close +', /stops before its end/);
    refused('close close', /goes on past its end/);
    refused('foo(close)', /there is no function foo/);
    refused('returns(close, 1.5)', /obs is a whole number/);
    refused('returns(close, 1, cubic)', /type is one of simple, log, absolute/);
    refused('volatility(close, w=-1)', /w is at least 0/);
    refused('correlation(close)', /reads two series/);
    refused('returns(close, 1, log, 3)', /takes one series and obs, type/);
    refused('volatility(close, k=2)', /has no parameter k/);
    refused('close.median', /a period's value is one of/);
    refused('close[', /a value was expected/);
    refused('#', /means nothing here/);
    // a calendar period is days, weeks, months or years
    refused('mean(close, 3q)', /w is a whole number of observations or a calendar period \(3m, 1y\)/);
    refused('returns(close, 0y)', /obs is a whole number of observations or a calendar period/);
    refused('mean(close, 2.5)', /w is a whole number/);
    refused('close < 1 < 2', /a comparison compares two sides/);
    const at = ((): number => {
      try {
        parseExpr('returns(close, x)');
      } catch (e) {
        return (e as TsSyntaxError).at;
      }
      return -1;
    })();
    assert.equal(at, 15, 'the refusal says where it is');
  });

  it('refuses gs-quant\'s functions DataCube does not compute, saying why', () => {
    refused('smooth_outliers(close)', /an iterative procedure/);
    refused('repeat(close)', /dates the data does not have/);
    // built since (the next phases' 1, 2026-10-26): read as typed
    assert.equal(exprText(parseExpr('seasonally_adjusted(close)')), 'seasonally_adjusted(close)');
    refused('percentile(close, 101)', /n is at most 100/);
  });

  // the expert functions (2026-10-26): gs-quant's recursions, read with its defaults; a weight of 1 or more refused
  it('reads the exponential averages, MACD and Wilder\'s, with gs-quant\'s defaults', () => {
    assert.deepEqual(parseExpr('ema(close)'), { kind: 'call', fn: 'exponential_moving_average', args: [{ kind: 'column', column: 'close' }] });
    assert.equal(exprText(parseExpr('exponential_volatility(close, 0.94)')), 'exponential_volatility(close, 0.94)');
    assert.equal(exprText(parseExpr('macd(close, 12, 26, 9)')), 'macd(close, 12, 26, 9)');
    assert.equal(exprText(parseExpr('macd(close, 12, 26, 1)')), 'macd(close)');
    refused('exponential_moving_average(close, 1)', /beta is below 1/);
    refused('exponential_std(close, -0.1)', /beta is above 0/);
    // the E9 review's 2: at beta 0 each value weighs alone, which has no deviation (the database's log of 0 refused it)
    refused('exponential_std(close, 0)', /beta is above 0/);
    refused('exponential_volatility(close, 0)', /beta is above 0/);
    refused('exponential_spread_volatility(close, 0)', /beta is above 0/);
    refused('exponential_moving_average(close, -0.1)', /beta is at least 0/);
    refused('smoothed_moving_average(close, 3m)', /w is a number|whole/);
    refused('rsi(close, 1m)', /w is a number|whole/);
  });

  it('every function in the catalog says what it is, and what of gs-quant it is', () => {
    for (const f of TS_FUNCTIONS) {
      assert.ok(f.about.length > 0 && f.gsQuant.length > 0, f.name);
      // a weighted sum's series each with its weight
      const args = f.series === 'many' ? 'close, 0.6, close, 0.4' : Array.from({ length: f.series }, () => 'close').join(', ');
      assert.equal(exprText(parseExpr(`${f.name}(${args})`)), `${f.name}(${args})`, f.name);
    }
  });

  it('every word\'s facts are the library\'s: the catalog says what the tile\'s own table says (A3 step 1)', () => {
    // the scalars become words in A4; not is the language's own form (if is the library's if_, read as if)
    const notWords = new Set(['abs', 'ceil', 'exp', 'floor', 'log', 'not', 'power', 'sqrt']);
    for (const [name, tile] of Object.entries(WORDS)) {
      const lib = TS_WORDS[name];
      if (notWords.has(name)) {
        assert.equal(lib, undefined, `${name} is no word of the library yet`);
        continue;
      }
      assert.ok(lib !== undefined, `${name} is the library's`);
      const facts = lib.facts;
      const one = (tag: string): string | undefined => {
        const values = facts[tag];
        if (values !== undefined && values.length !== 1) assert.fail(`${name}'s ${tag} said ${values.length} times`);
        return values?.[0];
      };
      const said = {
        label: one('label'), family: one('family'), unit: one('unit'), level: one('level'),
        bounds: one('bounds')?.split(', ').map(Number), gsQuant: one('gsQuant'), about: one('about'), differs: one('differs'),
        params: facts['param'] === undefined ? undefined
          : Object.fromEntries(facts['param'].map((p) => [p.slice(0, p.indexOf(': ')), p.slice(p.indexOf(': ') + 2)])),
        aliases: facts['search'], band: lib.marks.includes('band') || undefined,
        candles: lib.marks.includes('candles') || undefined, anchored: lib.marks.includes('anchored') || undefined,
      };
      const told = {
        label: tile.label, family: tile.family, unit: tile.unit, level: tile.level, bounds: tile.bounds, gsQuant: tile.gsQuant,
        about: tile.about, differs: tile.differs, params: tile.params, aliases: tile.aliases, band: tile.band,
        candles: tile.candles, anchored: tile.anchored,
      };
      assert.deepEqual(said, told, name);
    }
  });
});

// the Phase 1 review's 7 and 8 (2026-10-26)
describe('a weighted sum\'s weights, and the catalog\'s words', () => {
  it('weights summing to nothing refused, said so -- a sum of floats near 0 as much as one of 0', () => {
    assert.throws(() => parseExpr('weighted_sum(close[A], 1, close[B], -1)'), /its weights sum to 0/);
    assert.throws(() => parseExpr('weighted_sum(close[A], 0.1, close[A], 0.2, close[A], -0.3)'), /its weights sum to 0/);
    assert.throws(() => parseExpr('weighted_sum(close[A], 0.5, close[B])'), /reads series and their weights, in pairs/);
    const saved: TsExpr = { kind: 'call', fn: 'weighted_sum',
      args: [{ kind: 'column', column: 'c' }, { kind: 'number', value: 0.1 }, { kind: 'column', column: 'c' }, { kind: 'number', value: 0.2 },
        { kind: 'column', column: 'c' }, { kind: 'number', value: -0.3 }] };
    assert.match(treeProblem(saved, 'series A', ['px']) ?? '', /its weights sum to 0/);
  });

  it('vol_swap_volatility\'s first value is gs-quant\'s: no ramp said to differ', () => {
    assert.doesNotMatch(functionNamed('vol_swap_volatility')!.differs ?? '', /ramp/);
    assert.match(functionNamed('mean')!.differs ?? '', /ramp starts one observation later/);
  });
});

describe('what an expression reads, and is', () => {
  const TS: TimeseriesSpec = {
    sources: [{ alias: 'px', cube: 'g1', date: 'day', key: 'symbol' }, { alias: 'rates', cube: 'g2', date: 'd', key: 'tenor' }],
    series: [
      { id: 'A', text: writtenText(parseExpr('px:close[SPX]'), []) },
      { id: 'B', text: writtenText(parseExpr('rates:yield[10Y]'), []) },
      { id: 'C', text: writtenText(parseExpr('correlation($A, $B, 63)'), []) },
    ],
  };

  it('its refs, inlined; its sources in the order it reads them; their columns', () => {
    const c = TS.series[2]!;
    assert.deepEqual(refsOf(treeOf(c, TS.series)), ['A', 'B']);
    const whole = inlined(treeOf(c, TS.series), TS.series);
    assert.deepEqual(sourcesOf(TS, whole), ['px', 'rates']);
    assert.deepEqual([...columnsOf(TS, whole)], [['px', ['close', 'symbol']], ['rates', ['yield', 'tenor']]]);
    assert.deepEqual(sourcesOf(TS, parseExpr('close')), ['px'], 'a column of no source reads the first');
  });

  it('a ref round in a circle is said', () => {
    const series = [{ id: 'A', text: writtenText(parseExpr('$B'), []) }, { id: 'B', text: writtenText(parseExpr('$A + 1'), []) }];
    assert.throws(() => inlined(treeOf(series[0]!, series), series), /series B reads itself: B reads A reads B/);
  });

  it('its unit: a level, a ratio, a percent, a number, an index', () => {
    const u = (t: string): string => unitOf(parseExpr(t));
    assert.equal(u('close'), 'same');
    assert.equal(u('mean(close, 20)'), 'same');
    assert.equal(u('returns(close)'), 'ratio');
    assert.equal(u('returns(close, 1, absolute)'), 'same');
    assert.equal(u('volatility(close)'), 'percent');
    assert.equal(u('zscores(close)'), 'number');
    assert.equal(u('rebase(close)'), 'index');
    assert.equal(u('close[A] / close[B]'), 'number');
    assert.equal(u('close * 2'), 'same');
    assert.equal(u('drawdown(close)'), 'ratio');
    // a level compounded from its returns is a level, as an index is -- not its returns' ratio
    assert.equal(u('prices(returns(close), 100)'), 'index');
    // the review's Q4: a ratio times 100 is a percent (`100 * returns(x)`, drawn a hundred times too large as a ratio), a
    // percent over 100 a ratio; another number keeps the unit; a number over a series is a number
    assert.deepEqual(['100 * returns(close)', 'returns(close) * 100', 'volatility(close) / 100', '2 * returns(close)', '1 / close']
      .map(u), ['percent', 'percent', 'ratio', 'ratio', 'number']);
    // the second audit's N5: by a hundredth too, and a variance in its input's units squared -- a number
    assert.deepEqual(['volatility(close) * 0.01', 'returns(close) / 0.01', 'var(returns(close))'].map(u), ['ratio', 'percent', 'number']);
  });

  // the review's Q2: what compare and the level statistics take -- a price, an index, a ratio or product of prices --
  // and what they do not, though it is in its input's units
  it('a level: a price and what keeps one; a change, a difference, a deviation, a spread none', () => {
    const level = (t: string): boolean => levelOf(parseExpr(t));
    assert.deepEqual(['close', 'mean(close, 5)', 'close[A] / close[B]', 'close * 2', 'rebase(close)', 'prices(returns(close))', 'ohlc(open, high, low, close)']
      .map(level), [true, true, true, true, true, true, true]);
    assert.deepEqual(['change(close)', 'diff(close)', 'std(close, 5)', 'var(close, 5)', 'range(close, 5)', 'close[A] - close[B]', 'returns(close)', 'rebase(close[A]) - rebase(close[B])']
      .map(level), [false, false, false, false, false, false, false, false]);
  });

  // the second audit's B1 (docs/TIMESERIES_REVIEW_2026_10_25.md): a level shifted by a number was one -- index(close) - 1,
  // gs-quant's cumulative return, compared from a start of zero (every value none) and given a level's statistics
  it('a level shifted, turned negative or spread is none; a basket, a scale and a ratio of levels are', () => {
    const level = (t: string): boolean => levelOf(parseExpr(t));
    assert.deepEqual(['index(close) - 1', 'rebase(close) - 100', 'close - 200', 'close + 5', '5 + close', '-close', 'close * -2', '100 / close']
      .map(level), [false, false, false, false, false, false, false, false]);
    assert.deepEqual(['(close[A] + close[B]) / 2', 'close[A] * close[B]', 'close / 100', '0.5 * close', 'mean(close[A] + close[B], 5)']
      .map(level), [true, true, true, true, true]);
  });

  it('series ids run A to Z, then A2', () => {
    assert.equal(nextSeriesId([]), 'A');
    assert.equal(nextSeriesId([{ id: 'A', text: writtenText(parseExpr('close'), []) }, { id: 'C', text: writtenText(parseExpr('close'), []) }]), 'B');
    const all = Array.from({ length: 26 }, (_, i) => ({ id: String.fromCharCode(65 + i), text: writtenText(parseExpr('close') as TsExpr, []) }));
    assert.equal(nextSeriesId(all), 'A2');
  });

  it('an alias from a grid\'s title: its first word, made unique', () => {
    assert.equal(aliasFor('Prices', []), 'prices');
    assert.equal(aliasFor('US Rates (daily)', ['us']), 'us2');
    assert.equal(aliasFor(undefined, []), 'grid');
    assert.equal(aliasFor('2024 file', []), 'grid');
    assert.equal(aliasFor('returns', []), 'returns', 'a function\'s name too: an alias is read before a colon');
  });
});

describe('the saved spec\'s reader', () => {
  const OK: TimeseriesSpec = {
    sources: [{ alias: 'px', cube: 'grid-1', date: 'day', key: 'symbol' }],
    grain: 'day',
    series: [{ id: 'A', text: writtenText(parseExpr('rebase(close[AAPL])'), []), name: 'Apple', style: { kind: 'line', color: 2, width: 2, dash: 'dashed' }, pane: 0, axis: 'left' }],
    range: { last: '5y' },
    panes: [{ height: 3, log: true, invert: true, min: 50, max: 400 }],
    annotations: [{ kind: 'level', value: 100, label: 'par' }, { kind: 'event', date: '2024-03-20', label: 'FOMC' }, { kind: 'period', from: '2020-02-19', to: '2020-04-07' },
      { kind: 'marks', source: 'px', where: [{ value: 'AAPL' }, { column: 'side', value: 'BUY' }], at: 'price', text: 'quantity', shape: 'up', pane: 0, label: 'Bought', color: 2 },
      { kind: 'marks', source: 'px', text: 'headline', label: 'Earnings' },
      // a person's own drawings (Phase 5): a line from a point to a point, a note pinned at one
      { kind: 'line', from: { date: '2024-01-02', value: 180.5 }, to: { date: '2024-06-28', value: 210 }, pane: 0, label: 'Trend', color: 1 },
      { kind: 'note', at: { date: '2024-03-20', value: 175 }, text: 'Bought here', pane: 0 },
      // a trend projected (Phase 5): of a series of the tile, its window and how far ahead
      { kind: 'projection', series: 'A', window: 126, ahead: '6m', label: 'Trend' }],
    view: { compare: 'rebase', legend: true, lastValues: true, navigator: true, stats: false, crosshair: true, subtitle: 'Since 2020', note: 'Source: the exchange' },
  };
  const with_ = (change: Record<string, unknown>): unknown => ({ ...JSON.parse(JSON.stringify(OK)), ...change });

  it('reads a spec of every slot', () => {
    assert.equal(tsProblem(JSON.parse(JSON.stringify(OK))), null);
    // a window of a calendar period, and a lag of one
    assert.equal(tsProblem(with_({ series: [{ id: 'A', text: writtenText(parseExpr('returns(mean(close, 3m), 1y)'), []) }] })), null);
    // the next phases' 3: a view of its series, and each view's settings
    assert.equal(tsProblem(with_({ view: { mode: 'curve', of: ['A'], dates: ['latest', '1m', '2024-03-29'], years: 10, bins: 40 } })), null);
  });

  it('a series\' tree as core reads its text, judged by the tile\'s words: each refusal by name (treeProblem)', () => {
    const cases: [TsExpr, RegExp][] = [
      [{ kind: 'call', fn: 'smooth_outliers', args: [] } as unknown as TsExpr, /calls "smooth_outliers", which this DataCube does not compute/],
      [{ kind: 'call', fn: 'percentile', args: [{ kind: 'column', column: 'c' }], params: { n: 120 } }, /n 120 is above 100/],
      [{ kind: 'call', fn: 'exponential_moving_average', args: [{ kind: 'column', column: 'c' }], params: { beta: 1 } }, /beta 1 is not below 1/],
      [{ kind: 'call', fn: 'returns', args: [{ kind: 'column', column: 'c' }], params: { obs: 0 } }, /obs 0 is below 1/],
      [{ kind: 'call', fn: 'exponential_std', args: [{ kind: 'column', column: 'c' }], params: { beta: 0 } }, /beta 0 is not above 0/],
      [{ kind: 'column', source: 'fx', column: 'c' }, /reads fx.<column>, and fx is none of its sources \(px\)/],
      [{ kind: 'call', fn: 'mean', args: [{ kind: 'column', column: 'c' }], params: { w: '3q' } }, /w "3q" is neither a whole number nor a calendar period/],
      [{ kind: 'call', fn: 'mean', args: [{ kind: 'column', column: 'c' }], params: { w: 2.5 } }, /w 2.5 is not a whole number/],
    ];
    for (const [tree, said] of cases) assert.match(treeProblem(tree, 'series A', ['px']) ?? '', said, JSON.stringify(tree));
    assert.equal(treeProblem({ kind: 'column', source: 'px', column: 'c' }, 'series A', ['px']), null);
  });

  it('refuses a word, a key or a number it has no place for, by name', () => {
    const cases: [unknown, RegExp][] = [
      [with_({ colour: 1 }), /no "colour" in its timeseries/],
      [with_({ sources: [] }), /reads no grid/],
      [with_({ sources: [{ alias: 'p x', cube: 'g', date: 'd' }] }), /alias "p x" is not a word/],
      [with_({ sources: [{ alias: 'px', cube: 'g', date: 'd' }, { alias: 'px', cube: 'h', date: 'd' }] }), /two sources are called px/],
      [with_({ grain: 'hour' }), /frequency "hour"/],
      [with_({ series: [{ id: 'a', text: '1' }] }), /not a capital letter/],
      [with_({ series: [{ id: 'A', text: '  ' }] }), /series A has no text/],
      [with_({ series: [{ id: 'A', expr: { kind: 'column', column: 'c' } }] }), /no "expr" in series 1/],
      [with_({ series: [{ id: 'A', text: writtenText({ kind: 'column', column: 'c' }, []), pane: 4 }] }), /pane 4 is not one of 0 to 3/],
      [with_({ series: [{ id: 'A', text: writtenText({ kind: 'column', column: 'c' }, []), style: { kind: 'candles' } }] }), /style "candles"/],
      [with_({ range: { last: '7y' } }), /range "7y"/],
      [with_({ range: { from: '2024-02-01', to: '2024-01-01' } }), /ends before it starts/],
      [with_({ range: { last: '1y', from: '2024-01-01' } }), /not both/],
      [with_({ annotations: [{ kind: 'event', date: 'March' }] }), /is not a date/],
      // marks from a grid's rows (Phase 6's D4): of a source of the tile, each column a word, a shape it draws
      [with_({ annotations: [{ kind: 'marks', source: 'tr' }] }), /annotation 1's source "tr" is none of the tile's \(px\)/],
      [with_({ annotations: [{ kind: 'marks', source: 'px', at: '' }] }), /annotation 1's at is not a column/],
      [with_({ annotations: [{ kind: 'marks', source: 'px', at: 'price', shape: 'star' }] }), /shape "star" is not one of up, down, dot/],
      [with_({ annotations: [{ kind: 'marks', source: 'px', pane: 1 }] }), /drawn in a pane at a value: it names no column to draw them at/],
      [with_({ annotations: [{ kind: 'marks', source: 'px', where: [{ column: 'side' }] }] }), /annotation 1's match 1 is not a value/],
      [with_({ annotations: [{ kind: 'marks', source: 'px', size: 3 }] }), /no "size" in annotation 1 \(a marks\)/],
      // a person's drawings (Phase 5): each point a date and a value, a note its text, a pane of the tile's
      [with_({ annotations: [{ kind: 'line', from: { date: '2024-01-02', value: 1 }, to: { date: 'June', value: 2 } }] }), /annotation 1's to is not a point: a date \(2024-01-31\) and a value/],
      [with_({ annotations: [{ kind: 'line', from: { date: '2024-01-02', value: 1 } }] }), /annotation 1's to is not a point/],
      [with_({ annotations: [{ kind: 'note', at: { date: '2024-01-02', value: null }, text: 'x' }] }), /annotation 1's at is not a point/],
      [with_({ annotations: [{ kind: 'note', at: { date: '2024-01-02', value: 1, pane: 1 }, text: 'x' }] }), /annotation 1's at is not a point/],
      [with_({ annotations: [{ kind: 'note', at: { date: '2024-01-02', value: 1 }, text: '  ' }] }), /annotation 1 says nothing: a note has its text/],
      [with_({ annotations: [{ kind: 'note', at: { date: '2024-01-02', value: 1 }, text: 'x', label: 'y' }] }), /no "label" in annotation 1 \(a note\)/],
      [with_({ annotations: [{ kind: 'line', from: { date: '2024-01-02', value: 1 }, to: { date: '2024-01-03', value: 2 }, pane: 9 }] }), /annotation 1's pane is not one of 0 to/],
      [with_({ annotations: [{ kind: 'line', from: { date: '2024-01-02', value: 1 }, to: { date: '2024-01-03', value: 2 }, color: -1 }] }), /annotation 1's colour -1 is not a slot of the palette/],
      [with_({ annotations: [{ kind: 'arrow' }] }), /annotation 1 is not a level, an event, a period, marks, a line, a note or a projection/],
      [with_({ annotations: [{ kind: 'projection', series: 'Z' }] }), /annotation 1 projects "Z", which is none of the tile's series/],
      [with_({ annotations: [{ kind: 'projection', series: 'A', window: 2 }] }), /annotation 1's window 2 is not a whole number of observations from 3/],
      [with_({ annotations: [{ kind: 'projection', series: 'A', ahead: '3q' }] }), /annotation 1's ahead "3q" is not a calendar period/],
      [with_({ annotations: [{ kind: 'projection', series: 'A', pane: 1 }] }), /no "pane" in annotation 1 \(a projection\)/],
      [with_({ view: { compare: 'log' } }), /compare "log"/],
      // the next phases' 2: a pane upside down, its ends by hand; a subtitle and a note in words
      [with_({ panes: [{ invert: 'yes' }] }), /pane 0's invert is not true or false/],
      [with_({ panes: [{ min: 'low' }] }), /pane 0's min "low" is not a number/],
      [with_({ panes: [{ min: 5, max: 5 }] }), /pane 0's min 5 is not below its max 5/],
      // the Phase 2 review's 2: a log scale's ends above 0, never clamped without a word
      [with_({ panes: [{ log: true, min: 0 }] }), /pane 0's min 0 is not above 0: a log scale starts above it/],
      [with_({ panes: [{ log: true, max: -1 }] }), /pane 0's max -1 is not above 0: a log scale starts above it/],
      [with_({ view: { subtitle: 3 } }), /subtitle is not text/],
      [with_({ panes: [{ flip: true }] }), /flip/],
      // the next phases' 3: a view's mode, its series, its settings
      [with_({ view: { mode: 'pie' } }), /mode "pie" is not one of scatter, seasonality, histogram, curve/],
      [with_({ view: { of: [] } }), /view reads one series or two, by id/],
      [with_({ view: { of: ['A', 'A', 'A'] } }), /view reads one series or two, by id/],
      [with_({ view: { of: ['Q'] } }), /view reads series Q, which it has not/],
      [with_({ view: { dates: ['yesterday'] } }), /curve date "yesterday" is not latest, a period back \(1m, 1y\) or a day/],
      [with_({ view: { dates: ['latest', '1m', '2m', '3m', '6m', '1y', '2y'] } }), /curve dates are 1 to 6/],
      // the Phase 3 review's 10: a day no calendar has, and a date asked twice
      [with_({ view: { dates: ['2024-02-30'] } }), /curve date "2024-02-30" is not latest, a period back \(1m, 1y\) or a day/],
      [with_({ view: { dates: ['latest', '1m', 'latest'] } }), /curve date "latest" is asked twice/],
      [with_({ view: { years: 0 } }), /years 0 is not a whole number of 1 to 20/],
      [with_({ view: { bins: 2.5 } }), /bins 2.5 is not a whole number of 5 to 100/],
    ];
    for (const [spec, words] of cases) {
      const p = tsProblem(spec);
      assert.ok(p !== null && words.test(p), `${JSON.stringify(spec).slice(0, 80)}: ${p}`);
    }
  });
});

describe('a timeseries against its sources\' columns', () => {
  const PX: TsColumn[] = [
    { name: 'symbol', type: 'String' }, { name: 'day', type: 'StrictDate' }, { name: 'close', type: 'Float' },
    { name: 'volume', type: 'Integer' }, { name: 'sector', type: 'String' },
  ];
  const RATES: TsColumn[] = [{ name: 'tenor', type: 'String' }, { name: 'obs', type: 'StrictDate' }, { name: 'yield', type: 'Float' }];
  const TS = (series: Record<string, string>, more: Partial<TimeseriesSpec> = {}): TimeseriesSpec => ({
    sources: [{ alias: 'px', cube: 'g1', date: 'day', key: 'symbol' }, { alias: 'rates', cube: 'g2', date: 'obs', key: 'tenor' }],
    series: Object.entries(series).map(([id, text]) => ({ id, text: writtenText(parseExpr(text), []) })),
    ...more,
  });
  const cols = new Map([['px', PX], ['rates', RATES]]);
  const problems = (ts: TimeseriesSpec, c: ReadonlyMap<string, readonly TsColumn[] | undefined> = cols): string[] => timeseriesProblems(ts, c);

  it('draws what reads what is there', () => {
    assert.deepEqual(problems(TS({ A: 'px:close[SPX]', B: 'rates:yield[10Y]', C: 'correlation($A, $B, 63)' })), []);
  });

  it('says each thing that stops it', () => {
    const has = (ts: TimeseriesSpec, words: RegExp, c?: ReadonlyMap<string, readonly TsColumn[] | undefined>): void => {
      const p = problems(ts, c);
      assert.ok(p.some((x) => words.test(x)), `${words}: ${p.join(' | ')}`);
    };
    has(TS({ A: 'px:price' }), /reads 'price', which source px has not/);
    has(TS({ A: 'px:sector' }), /plots 'sector', which is not a number/);
    has(TS({ A: '$Z' }), /reads 'Z', which source px has not/);
    has(TS({}), /Add a series/);
    has(TS({ A: 'px:close' }), /Source rates's grid is not on the page/, new Map([['px', PX], ['rates', undefined]]));
    has({ ...TS({ A: 'close' }), sources: [{ alias: 'px', cube: 'g1', date: 'close' }] }, /is not a date/);
    has({ ...TS({ A: 'close[AAPL]' }), sources: [{ alias: 'px', cube: 'g1', date: 'day' }] }, /choose source px's instrument column/);
    has({ ...TS({ A: 'bollinger_bands(px:close, 20)', B: 'returns($A)' }) }, /a band is drawn, never read/);
    has({ ...TS({ A: 'ohlc(px:close, px:close, px:close, px:close)', B: 'returns($A)' }) }, /reads series A, candles: candles are drawn, never read/);
    has(TS({ A: 'mean(ohlc(px:close, px:close, px:close, px:close), 5)' }), /reads candles: candles are drawn, never read/);
    const split: TimeseriesSpec = { ...TS({}), series: [{ id: 'A', text: writtenText(parseExpr('px:close'), []), split: 'symbol' }, { id: 'B', text: writtenText(parseExpr('returns($A)'), []) }] };
    has(split, /split by 'symbol': a series reads only series split as it is/);
    const wrongSplit: TimeseriesSpec = { ...TS({}), series: [{ id: 'A', text: writtenText(parseExpr('rates:yield'), []), split: 'symbol' }] };
    has(wrongSplit, /which source rates -- the one it reads first -- has not/);
  });

  // marks from a grid's rows (Phase 6's D4): each column they read their source's, the one they are drawn at a number
  it('says what stops marks from a grid\'s rows', () => {
    const marked = (marks: Record<string, unknown>, sources?: TimeseriesSpec['sources']): TimeseriesSpec =>
      ({ ...TS({ A: 'px:close' }), ...(sources ? { sources } : {}), annotations: [{ kind: 'marks', source: 'px', ...marks } as TsAnnotation] });
    assert.deepEqual(problems(marked({ where: [{ value: 'AAPL' }, { column: 'sector', value: 'IT' }], at: 'close', text: 'volume', label: 'Trades' })), []);
    assert.deepEqual(problems(marked({ at: 'price', label: 'Trades' })), ["Marks \"Trades\" read 'price', which source px has not."]);
    assert.deepEqual(problems(marked({ at: 'sector' })), ["Marks 1 are drawn at 'sector', which is not a number."]);
    assert.deepEqual(problems(marked({ where: [{ column: 'side', value: 'BUY' }] })), ["Marks 1 read 'side', which source px has not."]);
    assert.deepEqual(problems(marked({ where: [{ value: 'AAPL' }] }, [{ alias: 'px', cube: 'g1', date: 'day' }])),
      ["Marks 1 match a value with no column ([AAPL]): choose source px's instrument column (Key)."]);
  });
});

describe('candles', () => {
  it('ohlc reads four series, written back as it was read; three are refused in words', () => {
    const e = parseExpr('ohlc(open[AAPL], high[AAPL], low[AAPL], close[AAPL])');
    assert.equal(exprText(e), 'ohlc(open[AAPL], high[AAPL], low[AAPL], close[AAPL])');
    assert.equal(unitOf(e), 'same');
    assert.equal(exprText(parseExpr('candles(o, h, l, c)')), 'ohlc(o, h, l, c)', 'by its other names');
    assert.throws(() => parseExpr('ohlc(open, high, low)'), /ohlc reads four series \(its open, high, low and close\)/);
    assert.match(treeProblem({ kind: 'call', fn: 'ohlc', args: [{ kind: 'column', column: 'c' }] }, 'series A', ['px']) ?? '',
      /ohlc reads four series/);
  });

  it('a candle of a column: its source\'s open, high and low beside its close, by their names', () => {
    const cols = (...names: string[]) => names.map((name) => ({ name, type: 'Float' }));
    assert.equal(exprText(candleOf(parseExpr('px:close[AAPL]'), cols('open', 'high', 'low', 'close'))!),
      'ohlc(px:open[AAPL], px:high[AAPL], px:low[AAPL], px:close[AAPL])');
    assert.equal(exprText(candleOf(parseExpr('close_px'), cols('Open_px', 'High_px', 'Low_px', 'close_px'))!), 'ohlc(Open_px, High_px, Low_px, close_px)');
    assert.equal(candleOf(parseExpr('close'), cols('close', 'volume')), undefined, 'no open, high and low: none');
    assert.equal(candleOf(parseExpr('returns(close)'), cols('open', 'high', 'low', 'close')), undefined, 'no column: none');
  });
});

describe('a first timeseries of a grid', () => {
  it('its date, its instruments, its price, one line per instrument -- each from 100, so any prices compare', () => {
    const ts = defaultTimeseries([
      { name: 'id', type: 'Integer' }, { name: 'trade_date', type: 'StrictDate' }, { name: 'ticker', type: 'String' },
      { name: 'volume', type: 'Integer' }, { name: 'close_px', type: 'Float' },
    ], [], 'grid-1', 'prices');
    assert.deepEqual(ts, {
      sources: [{ alias: 'prices', cube: 'grid-1', date: 'trade_date', key: 'ticker' }],
      series: [{ id: 'A', text: writtenText({ kind: 'column', column: 'close_px' }, []), split: 'ticker' }],
      view: { compare: 'rebase' },
    });
  });

  it('a rate per tenor drawn as it is; one line of no instrument as it is', () => {
    const curve = defaultTimeseries([{ name: 'tenor', type: 'String' }, { name: 'day', type: 'StrictDate' }, { name: 'yield', type: 'Float' }], [], 'g', 'rates');
    assert.deepEqual([curve?.series[0]?.split, curve?.view], ['tenor', undefined]);
    const one = defaultTimeseries([{ name: 'day', type: 'StrictDate' }, { name: 'close', type: 'Float' }], [], 'g', 'px');
    assert.deepEqual([one?.series[0]?.split, one?.view], [undefined, undefined]);
  });

  it('none of a grid with no date, or no number', () => {
    assert.equal(defaultTimeseries([{ name: 'a', type: 'String' }, { name: 'n', type: 'Float' }], [], 'g', 'a'), null);
    assert.equal(defaultTimeseries([{ name: 'd', type: 'StrictDate' }, { name: 's', type: 'String' }], [], 'g', 'a'), null);
  });

  it('a grid grouped by a text column takes it as the instruments', () => {
    const ts = defaultTimeseries([{ name: 'd', type: 'DateTime' }, { name: 'desk', type: 'String' }, { name: 'pnl', type: 'Float' }], ['desk'], 'g', 'pnl');
    assert.equal(ts?.sources[0]?.key, 'desk');
  });
});

describe('the formula box\'s completions (A3 step 3, decision 9b): found by the characters before the caret, never a reading', () => {
  const offer: TsOffer = {
    words: offeredWords(TS_WORDS),
    columns: [{ source: 'px', name: 'close', detail: '' }, { source: 'px', name: 'volume', detail: '' }, { source: 'rates', name: 'yield', detail: '' }],
    series: [{ id: 'A', detail: 'S&P 500' }, { id: 'B', detail: 'Its volatility' }],
  };
  const strip = (text: string): string => text.replace('|', '');
  const labels = (text: string): string[] => completionsAt(strip(text), text.indexOf('|'), offer).items.map((c) => c.label);

  it('a word of core\'s catalog the tile reads, its call opened; a series; a column of the first source by its name', () => {
    assert.ok(labels('vol|').includes('volatility'), labels('vol|').join());
    assert.ok(!labels('vol|').includes('log'), 'gs-quant\'s log is no word the tile writes: ln is');
    assert.deepEqual(completionsAt('vol', 3, offer).items.find((c) => c.label === 'volatility')?.insert, 'volatility(');
    assert.deepEqual(labels('mean(clo|'), ['close']);
    assert.deepEqual(labels('A + |'), []);
    assert.deepEqual(labels('mean(|'), []);
    assert.deepEqual(labels('B|'), ['B']);
    assert.deepEqual(labels('yi|'), ['rates.yield'], 'another source\'s column through its alias');
    assert.deepEqual(labels('rates.y|'), ['yield']);
    assert.ok(labels('l|').includes('ln') && !labels('l|').includes('log'));
  });

  it('what it replaces: the word at the caret, its rest after the caret too', () => {
    const c = completionsAt('mean(clo, 5)', 8, offer);
    assert.deepEqual([c.from, c.to], [5, 8]);
    const inside = completionsAt('mean(clxx, 5)', 7, offer);
    assert.deepEqual([inside.from, inside.to], [5, 9]);
  });

  it('in a subscript, the instruments of its column\'s source, asked of the database; a match on another column too', () => {
    const asked = completionsAt("close['SP", 9, offer);
    assert.deepEqual(asked.subscript, { source: undefined, column: 'close' });
    assert.deepEqual(asked.items, [], 'none until the database answers');
    const answered = completionsAt("close['SP", 9, { ...offer, instruments: { of: '.close', values: ['SPX', 'SPY', 'TLT'] } });
    assert.deepEqual(answered.items.map((c) => c.insert), ["'SPX'", "'SPY'"]);
    assert.deepEqual([answered.from, answered.to], [6, 9]);
    assert.deepEqual(completionsAt("rates.yield[venue = 'X', '1", 27, offer).subscript, { source: 'rates', column: 'yield' });
    assert.deepEqual(completionsAt("close['SPX'] + ", 15, offer).subscript, undefined, 'a subscript closed');
  });
});
