// A TIMESERIES TILE'S SPEC (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §3, §4; src/timeseries-spec.ts): the
// expression language read and printed back to the same tree, what it refuses and where, the saved spec's strict reader,
// the problems against the sources' columns, and a first tile of a grid.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { tsProblem, type TimeseriesSpec, type TsExpr } from '../src/timeseries-spec.ts';
import {
  aliasFor, candleOf, columnsOf, defaultTimeseries, exprText, inlined, nextSeriesId, parseExpr, refsOf, sourcesOf,
  levelOf, timeseriesProblems, TsSyntaxError, unitOf, type TsColumn, TS_FUNCTIONS,
} from '../src/timeseries-expr.ts';

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
    refused('median(close)', /rolling median/);
    refused('seasonally_adjusted(close)', /seasonal decomposition/);
  });

  // the expert functions (2026-10-26): gs-quant's recursions, read with its defaults; a weight of 1 or more refused
  it('reads the exponential averages, MACD and Wilder\'s, with gs-quant\'s defaults', () => {
    assert.deepEqual(parseExpr('ema(close)'), { kind: 'call', fn: 'exponential_moving_average', args: [{ kind: 'column', column: 'close' }] });
    assert.equal(exprText(parseExpr('exponential_volatility(close, 0.94)')), 'exponential_volatility(close, 0.94)');
    assert.equal(exprText(parseExpr('macd(close, 12, 26, 9)')), 'macd(close, 12, 26, 9)');
    assert.equal(exprText(parseExpr('macd(close, 12, 26, 1)')), 'macd(close)');
    refused('exponential_moving_average(close, 1)', /beta is below 1/);
    refused('exponential_std(close, -0.1)', /beta is at least 0/);
    refused('smoothed_moving_average(close, 3m)', /w is a number|whole/);
    refused('rsi(close, 1m)', /w is a number|whole/);
  });

  it('every function in the catalog says what it is, and what of gs-quant it is', () => {
    for (const f of TS_FUNCTIONS) {
      assert.ok(f.about.length > 0 && f.gsQuant.length > 0, f.name);
      const args = Array.from({ length: f.series }, () => 'close').join(', ');
      assert.equal(exprText(parseExpr(`${f.name}(${args})`)), `${f.name}(${args})`, f.name);
    }
  });
});

describe('what an expression reads, and is', () => {
  const TS: TimeseriesSpec = {
    sources: [{ alias: 'px', cube: 'g1', date: 'day', key: 'symbol' }, { alias: 'rates', cube: 'g2', date: 'd', key: 'tenor' }],
    series: [
      { id: 'A', expr: parseExpr('px:close[SPX]') },
      { id: 'B', expr: parseExpr('rates:yield[10Y]') },
      { id: 'C', expr: parseExpr('correlation($A, $B, 63)') },
    ],
  };

  it('its refs, inlined; its sources in the order it reads them; their columns', () => {
    const c = TS.series[2]!;
    assert.deepEqual(refsOf(c.expr), ['A', 'B']);
    const whole = inlined(c.expr, TS.series);
    assert.deepEqual(sourcesOf(TS, whole), ['px', 'rates']);
    assert.deepEqual([...columnsOf(TS, whole)], [['px', ['close', 'symbol']], ['rates', ['yield', 'tenor']]]);
    assert.deepEqual(sourcesOf(TS, parseExpr('close')), ['px'], 'a column of no source reads the first');
  });

  it('a ref round in a circle is said', () => {
    const series = [{ id: 'A', expr: parseExpr('$B') }, { id: 'B', expr: parseExpr('$A + 1') }];
    assert.throws(() => inlined(series[0]!.expr, series), /series B reads itself: \$B reads \$A reads \$B/);
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
    assert.equal(nextSeriesId([{ id: 'A', expr: parseExpr('close') }, { id: 'C', expr: parseExpr('close') }]), 'B');
    const all = Array.from({ length: 26 }, (_, i) => ({ id: String.fromCharCode(65 + i), expr: parseExpr('close') as TsExpr }));
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
    series: [{ id: 'A', expr: parseExpr('rebase(close[AAPL])'), name: 'Apple', style: { kind: 'line', color: 2, width: 2, dash: 'dashed' }, pane: 0, axis: 'left' }],
    range: { last: '5y' },
    panes: [{ height: 3, log: true }],
    annotations: [{ kind: 'level', value: 100, label: 'par' }, { kind: 'event', date: '2024-03-20', label: 'FOMC' }, { kind: 'period', from: '2020-02-19', to: '2020-04-07' }],
    view: { compare: 'rebase', legend: true, lastValues: true, navigator: true, stats: false, crosshair: true },
  };
  const with_ = (change: Record<string, unknown>): unknown => ({ ...JSON.parse(JSON.stringify(OK)), ...change });

  it('reads a spec of every slot', () => {
    assert.equal(tsProblem(JSON.parse(JSON.stringify(OK))), null);
    // a window of a calendar period, and a lag of one
    assert.equal(tsProblem(with_({ series: [{ id: 'A', expr: parseExpr('returns(mean(close, 3m), 1y)') }] })), null);
  });

  it('refuses a word, a key or a number it has no place for, by name', () => {
    const cases: [unknown, RegExp][] = [
      [with_({ colour: 1 }), /no "colour" in its timeseries/],
      [with_({ sources: [] }), /reads no grid/],
      [with_({ sources: [{ alias: 'p x', cube: 'g', date: 'd' }] }), /alias "p x" is not a word/],
      [with_({ sources: [{ alias: 'px', cube: 'g', date: 'd' }, { alias: 'px', cube: 'h', date: 'd' }] }), /two sources are called px/],
      [with_({ grain: 'hour' }), /frequency "hour"/],
      [with_({ series: [{ id: 'a', expr: { kind: 'number', value: 1 } }] }), /not a capital letter/],
      [with_({ series: [{ id: 'A', expr: { kind: 'call', fn: 'median', args: [] } }] }), /calls "median", which this DataCube does not compute/],
      [with_({ series: [{ id: 'A', expr: { kind: 'call', fn: 'exponential_moving_average', args: [{ kind: 'column', column: 'c' }], params: { beta: 1 } } }] }), /beta 1 is not below 1/],
      [with_({ series: [{ id: 'A', expr: { kind: 'call', fn: 'returns', args: [{ kind: 'column', column: 'c' }], params: { obs: 0 } } }] }), /obs 0 is below 1/],
      [with_({ series: [{ id: 'A', expr: { kind: 'column', source: 'fx', column: 'c' } }] }), /reads fx:..., which is none of its sources/],
      [with_({ series: [{ id: 'A', expr: { kind: 'column', column: 'c' }, pane: 4 }] }), /pane 4 is not one of 0 to 3/],
      [with_({ series: [{ id: 'A', expr: { kind: 'column', column: 'c' }, style: { kind: 'candles' } }] }), /style "candles"/],
      [with_({ series: [{ id: 'A', expr: { kind: 'call', fn: 'mean', args: [{ kind: 'column', column: 'c' }], params: { w: '3q' } } }] }), /w "3q" is neither a whole number nor a calendar period/],
      [with_({ series: [{ id: 'A', expr: { kind: 'call', fn: 'mean', args: [{ kind: 'column', column: 'c' }], params: { w: 2.5 } } }] }), /w 2.5 is not a whole number/],
      [with_({ range: { last: '7y' } }), /range "7y"/],
      [with_({ range: { from: '2024-02-01', to: '2024-01-01' } }), /ends before it starts/],
      [with_({ range: { last: '1y', from: '2024-01-01' } }), /not both/],
      [with_({ annotations: [{ kind: 'event', date: 'March' }] }), /is not a date/],
      [with_({ view: { compare: 'log' } }), /compare "log"/],
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
    series: Object.entries(series).map(([id, text]) => ({ id, expr: parseExpr(text) })),
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
    has(TS({ A: '$Z' }), /reads \$Z, which there is not/);
    has(TS({}), /Add a series/);
    has(TS({ A: 'px:close' }), /Source rates's grid is not on the page/, new Map([['px', PX], ['rates', undefined]]));
    has({ ...TS({ A: 'close' }), sources: [{ alias: 'px', cube: 'g1', date: 'close' }] }, /is not a date/);
    has({ ...TS({ A: 'close[AAPL]' }), sources: [{ alias: 'px', cube: 'g1', date: 'day' }] }, /choose source px's instrument column/);
    has({ ...TS({ A: 'bollinger_bands(px:close, 20)', B: 'returns($A)' }) }, /a band is drawn, never read/);
    has({ ...TS({ A: 'ohlc(px:close, px:close, px:close, px:close)', B: 'returns($A)' }) }, /reads \$A, candles: candles are drawn, never read/);
    has(TS({ A: 'mean(ohlc(px:close, px:close, px:close, px:close), 5)' }), /reads candles: candles are drawn, never read/);
    const split: TimeseriesSpec = { ...TS({}), series: [{ id: 'A', expr: parseExpr('px:close'), split: 'symbol' }, { id: 'B', expr: parseExpr('returns($A)') }] };
    has(split, /split by 'symbol': a series reads only series split as it is/);
    const wrongSplit: TimeseriesSpec = { ...TS({}), series: [{ id: 'A', expr: parseExpr('rates:yield'), split: 'symbol' }] };
    has(wrongSplit, /which source rates -- the one it reads first -- has not/);
  });
});

describe('candles', () => {
  it('ohlc reads four series, written back as it was read; three are refused in words', () => {
    const e = parseExpr('ohlc(open[AAPL], high[AAPL], low[AAPL], close[AAPL])');
    assert.equal(exprText(e), 'ohlc(open[AAPL], high[AAPL], low[AAPL], close[AAPL])');
    assert.equal(unitOf(e), 'same');
    assert.equal(exprText(parseExpr('candles(o, h, l, c)')), 'ohlc(o, h, l, c)', 'by its other names');
    assert.throws(() => parseExpr('ohlc(open, high, low)'), /ohlc reads four series \(its open, high, low and close\)/);
    assert.match(tsProblem({ sources: [{ alias: 'px', cube: 'g', date: 'd' }], series: [{ id: 'A', expr: { kind: 'call', fn: 'ohlc', args: [{ kind: 'column', column: 'c' }] } }] }) ?? '',
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
      series: [{ id: 'A', expr: { kind: 'column', column: 'close_px' }, split: 'ticker' }],
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
