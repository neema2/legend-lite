// REAL EXAMPLES OF THE TIMESERIES LANGUAGE OVER CLASS MODELS (the user, 2026-10-30: "Show me real examples";
// docs/TS_LANGUAGE_BUILD_2026_10_30.md): each a #TS{...}# query written over the markets project's classes
// (marketdata::MarketQuote, trading::Trade through stress::AllMapping), beside the tile formulas that draw the same
// chart today. #TS is not built yet: its text is the design. What runs is the tile's writer over the page's real
// sources (each grid's saved query, a Class projected to a relation), planned by the lite compiler and answered by
// DuckDB over the project's own Data -- the Pure the #TS builder is to write the same (P5's parity). `bazel test
// //datacube:ts_examples_measure --test_output=all` prints each query's Pure and its last rows.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { after, before, it } from 'node:test';
import { openDatabase } from '#test-host';
import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { MARKETS_PROJECT, SAVED_GRIDS, savedQueryTree } from '../src/examples/markets-classes.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { seriesQuery, TS_DATE, TS_SPLIT, TS_VALUE, type TsInputs } from '../src/timeseries-query.ts';
import type { TimeGrain } from '../src/chart-spec.ts';
import type { TimeseriesSpec, TsSource } from '../src/timeseries-spec.ts';
import { plannerFor } from './catalog-builder.ts';

const CONFIGURED = (JSON.parse(readFileSync(runfileFromEnv('DEMO_CONFIG'), 'utf8')) as {
  projects: { groupId: string; artifactId: string; versionId: string; models: string[] }[];
}).projects.find((p) => p.groupId === MARKETS_PROJECT.groupId && p.artifactId === MARKETS_PROJECT.artifactId);
const SERVED = runfileFromEnv('MARKETS_PROJECT');
const PROJECT = (CONFIGURED?.models ?? [])
  .map((url) => readFileSync(join(SERVED, url.slice('./projects/markets/'.length)), 'utf8')).join('\n');

const QUOTES: TsSource = { alias: 'px', cube: 'quotes', date: 'quoteTime', key: 'ticker' };
const TRADES: TsSource = { alias: 'tr', cube: 'trades', date: 'tradeDate', key: 'ticker' };

/** One example: the #TS query (the design), and the tile's series that draw it today. */
interface Example {
  readonly title: string;
  readonly ts: string;
  readonly sources: readonly TsSource[];
  readonly grain?: TimeGrain;
  readonly series: readonly { readonly name: string; readonly expr: string; readonly split?: string }[];
}

const EXAMPLES: readonly Example[] = [
  {
    title: '1. Each instrument\'s 20-day average close, straight from the class',
    ts: `SELECT closePrice AS close, avg(closePrice, 20) AS ma20
FROM   marketdata::MarketQuote ON quoteTime BY instrument.ticker
FILTER ticker IN ('AAPL', 'MSFT')`,
    sources: [QUOTES],
    series: [{ name: 'ma20', expr: 'mean(close, 20)', split: 'ticker' }],
  },
  {
    title: '2. Two instruments\' 63-day volatility, side by side',
    ts: `SELECT volatility(closePrice['AAPL'], 63) AS aapl_vol,
       volatility(closePrice['MSFT'], 63) AS msft_vol
FROM   marketdata::MarketQuote ON quoteTime BY instrument.ticker`,
    sources: [QUOTES],
    series: [
      { name: 'aapl_vol', expr: 'volatility(close[AAPL], 63)' },
      { name: 'msft_vol', expr: 'volatility(close[MSFT], 63)' },
    ],
  },
  {
    title: '3. A sector through the association, every instrument rebased to 100',
    ts: `SELECT rebase(closePrice) AS indexed
FROM   (marketdata::MarketQuote WHERE instrument.sector.name = 'Information Technology')
       ON quoteTime BY instrument.ticker`,
    sources: [QUOTES],
    series: [{ name: 'indexed', expr: 'rebase(close[sector="Information Technology"])', split: 'ticker' }],
  },
  {
    title: '4. Weekly: Nvidia\'s week-end close and its weekly return',
    ts: `SELECT last(closePrice['NVDA']) AS wk_close,
       returns(wk_close)            AS wk_ret
FROM   marketdata::MarketQuote ON quoteTime BY instrument.ticker
SAMPLE BY 1w`,
    sources: [QUOTES],
    grain: 'week',
    series: [
      { name: 'wk_close', expr: 'close[NVDA]' },
      { name: 'wk_ret', expr: 'returns(close[NVDA])' },
    ],
  },
  {
    title: '5. Trades beside prices: Apple\'s close, and the shares bought and sold each day',
    ts: `SELECT last(px.closePrice['AAPL'])             AS close,
       sum(tr.quantity['AAPL', side = 'BUY'])   AS bought,
       sum(tr.quantity['AAPL', side = 'SELL'])  AS sold
FROM   marketdata::MarketQuote px ON quoteTime BY instrument.ticker,
       trading::Trade          tr ON tradeDate BY instrument.ticker
SAMPLE BY 1d`,
    sources: [QUOTES, TRADES],
    series: [
      { name: 'close', expr: 'px:close[AAPL]' },
      // the tile matches a string against the enumeration's STORED code ('B', the mapping's), as legend-engine compares
      // an enum column with a string: side="BUY" matches no row. #TS is to read 'BUY' as trading::Side.BUY (proposed)
      { name: 'bought', expr: 'tr:quantity[AAPL, side="B"].sum' },
      { name: 'sold', expr: 'tr:quantity[AAPL, side="S"].sum' },
    ],
  },
];

let planner: LitePlanner;
let engine: DuckDbEngine;
let inputs: TsInputs;

before(async () => {
  planner = plannerFor(PROJECT, 'stress::RT');
  engine = new DuckDbEngine((await openDatabase()).connection);
  const project = await planner.projectData(PROJECT, engine.databaseType);
  for (const sql of [...project.open, ...project.use, ...project.fill]) await engine.run(sql, 0);
  // each source the page's grid: its saved query, the class projected to a relation through stress::AllMapping
  const snapshot = (id: string): CubeSnapshot => {
    const g = SAVED_GRIDS.find((x) => x.id === id)!;
    return { source: { query: savedQueryTree(g) }, columns: g.columns.map((c) => ({ name: c.name, type: c.type })),
      derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1 };
  };
  inputs = new Map([['px', { snapshot: snapshot('quotes'), filter: undefined }], ['tr', { snapshot: snapshot('trades'), filter: undefined }]]);
});

after(async () => {
  await engine?.close();
});

const fmt = (v: unknown): string => v === null || v === undefined ? '-' : typeof v === 'number' || typeof v === 'bigint'
  ? Number(v).toFixed(Math.abs(Number(v)) < 10 ? 4 : 2) : String(v).slice(0, 10);

it('the examples, each planned and answered over the markets project', async () => {
  const printed: string[] = [];
  for (const ex of EXAMPLES) {
    const out: string[] = [`\n=== ${ex.title}`, '--- #TS (the design)', ex.ts, '--- the tile today: the same chart'];
    const spec: TimeseriesSpec = {
      sources: [...ex.sources],
      series: ex.series.map((s, i) => ({ id: String.fromCharCode(65 + i), expr: parseExpr(s.expr), ...(s.split ? { split: s.split } : {}) })),
      grain: ex.grain ?? 'day',
    };
    for (const [i, s] of ex.series.entries()) {
      const q = seriesQuery(inputs, spec, spec.series[i]!);
      const t = await engine.execute(await planner.plan(q.query), 0);
      const col = (n: string): unknown[] => (t.columns.find((c) => c.name === n)?.values ?? []) as unknown[];
      const dates = col(TS_DATE);
      const values = col(TS_VALUE);
      const keys = s.split ? col(TS_SPLIT) : dates.map(() => '');
      out.push(`--- series ${s.name}: the tile's ${s.expr}${s.split ? `, a line per ${s.split}` : ''}`);
      if (process.env['TS_EXAMPLES_PURE'] === ex.title.slice(0, 1) && i === 0) {
        out.push(await planner.print(q.query as never));
      }
      // the latest rows of each line, as the database answers them: AAPL's and MSFT's where the lines are split
      const byKey = new Map<string, string[]>();
      dates.forEach((d, j) => {
        const k = String(keys[j] ?? '');
        const l = byKey.get(k) ?? [];
        byKey.set(k, l);
        l.push(`${fmt(d)} ${fmt(values[j])}`);
      });
      const shown = s.split ? [...byKey.keys()].filter((k) => ['AAPL', 'MSFT'].includes(k)).sort() : [''];
      out.push(`    (${byKey.size} line${byKey.size === 1 ? '' : 's'}${s.split ? `: ${[...byKey.keys()].sort().join(', ')}` : ''})`);
      for (const [k, l] of shown.map((k) => [k, byKey.get(k) ?? []] as const)) {
        const sorted = [...l].sort();
        out.push(`--- ${s.name}${k ? ` [${k}]` : ''}: ${sorted.length} rows, ${sorted.filter((r) => !r.endsWith(' -')).length} with a value; the last 4:`);
        out.push(...sorted.slice(-4).map((r) => `    ${r}`));
      }
    }
    printed.push(...out);
  }
  console.log(printed.join('\n'));
});
