// A MEASUREMENT, not a test (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.6, "the SQL's size"): what the SQL of a
// timeseries tile's queries weighs, as legend-lite's own planner writes it, and what DuckDB takes to run it. Each query
// is planned and run over six years of weekdays of three instruments; its numbers print, and are written to the test's
// undeclared outputs as timeseries-sql.json beside each query's SQL. Manual: `bazel test
// //datacube:timeseries_sql_measure --test_output=all`.
//
// Per query: the SQL's bytes; its CASE expressions (all, and distinct); the CASE whose copies past its first weigh most
// (its copies, its bytes, and those extra copies' share of the SQL: what saying it once would save); its windows
// (`OVER (`); DuckDB's time to run it, the median of RUNS after one not timed.

import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { after, before, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { ingestFile } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import type { TimeseriesSpec, TsSeries } from '../src/timeseries-spec.ts';
import { parseExpr } from '../src/timeseries-expr.ts';
import { seriesQuery, statsQuery, type TsInputs, type TsQuery } from '../src/timeseries-query.ts';

const RUNS = 5;
const DAYS = 1560;

function weekdays(n: number): string[] {
  const out: string[] = [];
  for (let d = new Date(Date.UTC(2019, 0, 1)); out.length < n; d = new Date(d.getTime() + 86400000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

const dates = weekdays(DAYS);
const price = (i: number, base: number, a: number, b: number): number =>
  Math.round(base * (1 + a * Math.sin(i / 9) + b * i / DAYS + 0.01 * Math.cos(i / 2.3)) * 100) / 100;
const CSV = [
  'symbol,day,close,volume',
  ...dates.flatMap((d, i) => [
    `AAA,${d},${price(i, 100, 0.08, 0.6)},${1000 + (i % 37) * 11}`,
    `BBB,${d},${price(i, 50, 0.05, 0.3)},${2000 + (i % 29) * 7}`,
    // CCC trades on every third day only, so an alignment has gaps to fill
    ...(i % 3 === 0 ? [`CCC,${d},${price(i, 20, 0.03, 0.2)},${500 + (i % 13) * 5}`] : []),
  ]),
].join('\n');

/** The queries measured: a name, the series' formula, and the tile's settings beside it. */
const QUERIES: { name: string; expr: string; more?: Partial<TimeseriesSpec>; stats?: true }[] = [
  { name: 'level', expr: 'close[AAA]' },
  { name: 'mean_50', expr: 'mean(close[AAA], 50)' },
  { name: 'volatility_63', expr: 'volatility(close[AAA], 63)' },
  { name: 'volatility_3m', expr: 'volatility(close[AAA], 3m)' },
  { name: 'returns_1y', expr: 'returns(close[AAA], 1y)' },
  { name: 'beta_63', expr: 'beta(close[AAA], close[BBB], 63)' },
  { name: 'drawdown', expr: 'drawdown(close[AAA])' },
  { name: 'rsi_14', expr: 'rsi(close[AAA], 14)' },
  { name: 'bollinger_20', expr: 'bollinger_bands(close[AAA], 20, 2)' },
  { name: 'zscores_of_volatility', expr: 'zscores(volatility(close[AAA], 21), 252)' },
  { name: 'ratio_step', expr: 'close[AAA] / close[CCC]', more: { align: 'step' } as Partial<TimeseriesSpec> },
  { name: 'statistics', expr: 'close[AAA]', stats: true },
];

/** Every CASE expression of `sql`, nested ones included, each from its CASE to its matching END. */
function cases(sql: string): string[] {
  const out: string[] = [];
  const token = /\bCASE\b|\bEND\b/g;
  const open: number[] = [];
  for (let m = token.exec(sql); m; m = token.exec(sql)) {
    if (m[0] === 'CASE') open.push(m.index);
    else if (open.length) out.push(sql.slice(open.pop()!, m.index + 3));
  }
  return out;
}

interface Measured {
  name: string;
  bytes: number;
  cases: number;
  distinctCases: number;
  topCaseCopies: number;
  topCaseBytes: number;
  topCaseShare: number;
  windows: number;
  runMs: number;
}

let engine: DuckDbEngine;
let planner: LitePlanner;
let inputs: TsInputs;
const results: Measured[] = [];
const out = process.env['TEST_UNDECLARED_OUTPUTS_DIR'];

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  const writer = plannerFor('', 'local::RT');
  const r = await ingestFile(engine, db.files, new File([CSV], 'prices.csv'), writer);
  planner = plannerFor(r.model, r.runtime);
  const snapshot: CubeSnapshot = {
    source: { query: r.source },
    columns: [
      { name: 'symbol', type: 'String' },
      { name: 'day', type: 'StrictDate' },
      { name: 'close', type: 'Float' },
      { name: 'volume', type: 'Integer', kind: 'measure' },
    ],
    derived: [],
    rows: ['symbol'],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };
  inputs = new Map([['px', { snapshot, filter: undefined }]]);
});

after(async () => {
  await engine?.close();
  const rows = results.map((m) => [m.name, m.bytes, m.cases, m.distinctCases, m.topCaseCopies, m.topCaseBytes,
    `${Math.round(m.topCaseShare * 100)}%`, m.windows, m.runMs]);
  const head = ['query', 'bytes', 'CASEs', 'distinct', 'top copies', 'top bytes', 'extra share', 'windows', 'run ms'];
  const widths = head.map((h, i) => Math.max(h.length, ...rows.map((r) => String(r[i]).length)));
  const line = (r: unknown[]): string => r.map((v, i) => String(v).padStart(widths[i]!)).join('  ');
  console.log(['', line(head), ...rows.map(line), ''].join('\n'));
  if (out) writeFileSync(path.join(out, 'timeseries-sql.json'), JSON.stringify(results, null, 2));
});

for (const q of QUERIES) {
  it(q.name, async () => {
    const ts: TimeseriesSpec = {
      sources: [{ alias: 'px', cube: 'prices', date: 'day', key: 'symbol' }],
      series: [],
      ...q.more,
    };
    const series: TsSeries = { id: 'A', expr: parseExpr(q.expr) };
    const query: TsQuery = q.stats ? statsQuery(inputs, { ...ts, series: [series] }, series)
      : seriesQuery(inputs, { ...ts, series: [series] }, series);
    const plan = await planner.plan(query.query as Parameters<LitePlanner['plan']>[0]);
    const sql = plan.sql;
    const all = cases(sql);
    const counts = new Map<string, number>();
    for (const c of all) counts.set(c, (counts.get(c) ?? 0) + 1);
    // the CASE whose copies past the first weigh most (a nested CASE is counted inside each copy of the one holding it)
    let top = '';
    const extra = (c: string): number => ((counts.get(c) ?? 1) - 1) * c.length;
    for (const c of counts.keys()) if (extra(c) > extra(top)) top = c;
    const copies = counts.get(top) ?? 0;
    await engine.execute(plan, 0);
    const times: number[] = [];
    for (let i = 0; i < RUNS; i++) {
      const t0 = performance.now();
      const t = await engine.execute(plan, 0);
      times.push(performance.now() - t0);
      assert.ok(t.columns.length > 0, `${q.name} answers`);
    }
    times.sort((a, b) => a - b);
    results.push({
      name: q.name,
      bytes: sql.length,
      cases: all.length,
      distinctCases: counts.size,
      topCaseCopies: copies,
      topCaseBytes: top.length,
      topCaseShare: extra(top) / sql.length,
      windows: sql.split('OVER (').length - 1,
      runMs: Math.round(times[Math.floor(RUNS / 2)]!),
    });
    if (out) {
      mkdirSync(path.join(out, 'sql'), { recursive: true });
      writeFileSync(path.join(out, 'sql', `${q.name}.sql`), sql);
    }
  });
}
