// Does the REAL legend-engine compile everything this cube emits?
//
// Not "does a query work" -- every operation the serialiser can
// produce: each aggregate function, each filter operator, the pivot's
// two-stage cast, a level scope's parent conditions, the grand total,
// the measureless group. One case each, sent to a running upstream
// engine, and the answer recorded per case.
//
// This is the compatibility claim the whole clean-room rewrite rests
// on: that the Pure we emit is Pure that upstream accepts. It was an
// assumption until this file existed.
//
//   ENGINE=http://127.0.0.1:6300 npm run verify:engine
//
// Start an engine first -- see the shaded-jar recipe: no JDK, Maven
// or Docker install is needed on this Mac.

import { readFile } from 'node:fs/promises';

import { LegendEngineExecutor } from '../src/engine-remote.ts';
import { serialize } from '../src/serialize.ts';

const ENGINE = (process.env.ENGINE ?? 'http://127.0.0.1:6300')
  .replace(/\/$/, '');
const API = `${ENGINE}/api/pure/v1`;
const ONLY = process.env.ONLY;
const RUNTIME = 'trades::RT';

/** The demo model's table, as the cube sees it. */
const COLUMNS = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'book', type: 'String' },
  { name: 'year', type: 'Integer', kind: 'dimension' },
  { name: 'qtr', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'pnl', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];

const BASE = {
  source: { expression: '#>{trades::DB.TRADES}#' },
  columns: COLUMNS,
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};

const cube = (over = {}) => ({ ...BASE, ...over });
const SUM = { name: 'notional', column: 'notional', fn: 'sum' };

/** A filter case: one operator, with a value that suits it. */
const filterCase = (operator, extra = {}) => ({
  name: `filter: ${operator}`,
  snapshot: cube({
    measures: [SUM],
    rows: ['region'],
    filter: { kind: 'condition', column: 'region', operator, ...extra },
  }),
});

const AGGREGATES = ['sum', 'count', 'average', 'min', 'max', 'median',
  'stdDevSample', 'stdDevPopulation', 'varianceSample',
  'variancePopulation', 'joinStrings', 'wavg', 'unique'];

const CASES = [
  // -- the shapes ----------------------------------------------------
  { name: 'detail rows', snapshot: cube({}) },
  {
    name: 'sort ascending',
    snapshot: cube({ sorts: [{ column: 'region', direction: 'asc' }] }),
  },
  {
    name: 'sort descending, two columns',
    snapshot: cube({
      sorts: [
        { column: 'region', direction: 'desc' },
        { column: 'desk', direction: 'asc' },
      ],
    }),
  },
  {
    name: 'group by one dimension',
    snapshot: cube({ rows: ['region'], measures: [SUM] }),
  },
  {
    name: 'group by three dimensions',
    snapshot: cube({ rows: ['region', 'desk', 'book'], measures: [SUM] }),
  },
  {
    name: 'group by with NO measures',
    snapshot: cube({ rows: ['region'] }),
  },
  {
    name: 'the grand total (no keys)',
    snapshot: cube({ measures: [SUM] }),
  },
  {
    name: 'a level scope, with parent conditions',
    snapshot: cube({ rows: ['region', 'desk'], measures: [SUM] }),
    scope: { level: 2, parent: ['EMEA'] },
  },
  {
    name: 'column pivot',
    snapshot: cube({ pivotOn: ['year'], measures: [SUM] }),
  },
  {
    name: 'pivot AND group by, through the cast',
    snapshot: cube({
      rows: ['region'],
      pivotOn: ['year'],
      measures: [SUM],
      pivotCast: [
        { name: '2021__|__notional', measure: 'notional' },
        { name: '2022__|__notional', measure: 'notional' },
      ],
    }),
  },
  {
    name: 'a derived column',
    snapshot: cube({
      rows: ['region'],
      measures: [SUM],
      derived: [{ name: 'big', expression: '$x.notional > 100' }],
    }),
  },
  {
    name: 'a row window (offset and limit)',
    snapshot: cube({ window: { offset: 10, limit: 20 } }),
  },
  {
    name: 'a string value with a quote in it',
    snapshot: cube({
      rows: ['region'],
      measures: [SUM],
      filter: {
        kind: 'condition', column: 'desk', operator: 'equal',
        value: "O'Brien's desk",
      },
    }),
  },
  {
    name: 'and / or / not, nested',
    snapshot: cube({
      rows: ['region'],
      measures: [SUM],
      filter: {
        kind: 'and',
        children: [
          {
            kind: 'or',
            children: [
              { kind: 'condition', column: 'region', operator: 'equal',
                value: 'EMEA' },
              { kind: 'condition', column: 'region', operator: 'equal',
                value: 'AMER' },
            ],
          },
          {
            kind: 'not',
            child: { kind: 'condition', column: 'qty', operator: 'lessThan',
              value: 10 },
          },
        ],
      },
    }),
  },
  // -- every aggregate ----------------------------------------------
  ...AGGREGATES.map((fn) => ({
    name: `aggregate: ${fn}`,
    snapshot: cube({
      rows: ['region'],
      measures: [{
        name: 'agg',
        column: fn === 'joinStrings' || fn === 'unique' ? 'desk' : 'notional',
        fn,
        ...(fn === 'wavg' ? { weight: 'qty' } : {}),
      }],
    }),
  })),
  // -- every filter operator ----------------------------------------
  filterCase('equal', { value: 'EMEA' }),
  filterCase('notEqual', { value: 'EMEA' }),
  filterCase('lessThan', { value: 'EMEA' }),
  filterCase('lessThanEqual', { value: 'EMEA' }),
  filterCase('greaterThan', { value: 'EMEA' }),
  filterCase('greaterThanEqual', { value: 'EMEA' }),
  filterCase('isEmpty'),
  filterCase('isNotEmpty'),
  filterCase('contains', { value: 'EM' }),
  filterCase('notContains', { value: 'EM' }),
  filterCase('startsWith', { value: 'E' }),
  filterCase('notStartsWith', { value: 'E' }),
  filterCase('endsWith', { value: 'A' }),
  filterCase('notEndsWith', { value: 'A' }),
  filterCase('in', { value: ['EMEA', 'AMER'] }),
  filterCase('notIn', { value: ['EMEA', 'AMER'] }),
  filterCase('equalCaseInsensitive', { value: 'emea' }),
  filterCase('notEqualCaseInsensitive', { value: 'emea' }),
  filterCase('containsCaseInsensitive', { value: 'em' }),
  filterCase('startsWithCaseInsensitive', { value: 'e' }),
  filterCase('endsWithCaseInsensitive', { value: 'a' }),
  filterCase('inCaseInsensitive', { value: ['emea', 'amer'] }),
  filterCase('notInCaseInsensitive', { value: ['emea', 'amer'] }),
  filterCase('equalColumn', { rightColumn: 'desk' }),
  filterCase('equalCaseInsensitiveColumn', { rightColumn: 'desk' }),
  filterCase('notEqualColumn', { rightColumn: 'desk' }),
  filterCase('notEqualCaseInsensitiveColumn', { rightColumn: 'desk' }),
  filterCase('lessThanColumn', { rightColumn: 'desk' }),
  filterCase('lessThanEqualColumn', { rightColumn: 'desk' }),
  filterCase('greaterThanColumn', { rightColumn: 'desk' }),
  filterCase('greaterThanEqualColumn', { rightColumn: 'desk' }),
];

async function post(path, body, text = false) {
  const response = await fetch(`${API}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': text ? 'text/plain' : 'application/json' },
    body: text ? body : JSON.stringify(body),
    signal: AbortSignal.timeout(120_000),
  });
  const raw = await response.text();
  if (!response.ok) {
    let message = raw.slice(0, 300);
    try {
      message = JSON.parse(raw).message ?? message;
    } catch {
      // keep the raw text
    }
    throw new Error(message.replace(/\s+/g, ' ').slice(0, 220));
  }
  return JSON.parse(raw);
}

/** Every SQL an execution plan carries, in order. */
function sqlOf(node, out = []) {
  if (Array.isArray(node)) {
    for (const v of node) sqlOf(v, out);
  } else if (node && typeof node === 'object') {
    if (typeof node.sqlQuery === 'string') out.push(node.sqlQuery);
    for (const v of Object.values(node)) sqlOf(v, out);
  }
  return out;
}

const CONTEXT = {
  _type: 'BaseExecutionContext',
  queryTimeOutInSeconds: 60,
  enableConstraints: true,
};

let model;
const results = [];

try {
  const grammar = await readFile(
    new URL('./trades.pure', import.meta.url), 'utf8');
  model = await post('/grammar/grammarToJson/model', grammar, true);
  console.log(`\nmodel compiled by the engine at ${ENGINE}`);
} catch (e) {
  console.log(`\ncould not reach an engine at ${ENGINE}: ${e.message}`);
  console.log('start one first (the shaded jar needs no install), then'
    + ' re-run.');
  process.exit(2);
}

for (const { name, snapshot, scope } of CASES) {
  if (ONLY && !name.toLowerCase().includes(ONLY.toLowerCase())) continue;
  let pure;
  try {
    // THE RUNTIME NAMED IN THE QUERY. Our planners take it
    // out-of-band; a relation query sent to the engine carries it.
    pure = `${serialize(snapshot, scope)}->from(${RUNTIME})`;
  } catch (e) {
    results.push({ name, ok: false, where: 'our serialiser',
      detail: String(e.message ?? e).slice(0, 200) });
    continue;
  }
  try {
    const lambda = await post('/grammar/grammarToJson/lambda', pure, true);
    const plan = await post('/execution/generatePlan', {
      clientVersion: 'vX_X_X',
      function: lambda,
      model,
      context: CONTEXT,
    });
    const sql = sqlOf(plan);
    if (sql.length === 0) {
      results.push({ name, ok: false, where: 'the plan', pure,
        detail: 'the plan carries no SQL' });
    } else {
      results.push({ name, ok: true, pure, sql: sql.at(-1) });
    }
  } catch (e) {
    results.push({ name, ok: false, where: 'the engine', pure,
      detail: String(e.message ?? e) });
  }
}

// -- SERVER MODE: the engine runs it ---------------------------------
//
// Compiling is not answering. This is the other half of the claim:
// the engine EXECUTES our Pure against a database the browser cannot
// reach -- the H2 it embeds -- and hands back rows, which is what
// upstream's uncached path does (`_runQuery` posts to
// `execution/execute` and renders the TDS).
//
// The figures are asserted, not printed. A query that runs and
// returns the wrong sums is the failure that looks like success.
if (!ONLY || 'server mode'.includes(ONLY.toLowerCase())) {
  try {
    const h2Model = await readFile(
      new URL('./trades-h2.pure', import.meta.url), 'utf8');
    const executor = new LegendEngineExecutor({
      baseUrl: ENGINE,
      model: h2Model,
      runtime: 'trades::h2::RT',
    });
    const snapshot = {
      source: { expression: '#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#' },
      columns: COLUMNS,
      derived: [],
      rows: ['region'],
      pivotOn: [],
      measures: [{ name: 'notional', column: 'notional', fn: 'sum' }],
      sorts: [{ column: 'region', direction: 'asc' }],
      epoch: 3,
    };
    const out = await executor.execute(serialize(snapshot), snapshot);
    const by = Object.fromEntries(out.rows.columns.map((c) => [c.name, c]));
    const seeded = { AMER: 300, APAC: 400.25, EMEA: 301 };
    const regions = by['region']?.values ?? [];
    const notional = by['notional']?.values ?? [];
    const wrong = regions
      .map((r, i) => [r, notional[i], seeded[r]])
      .filter(([, got, want]) => Math.abs(Number(got) - want) > 0.001);
    if (out.rows.rowCount !== 3) {
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the engine', detail: `${out.rows.rowCount} rows, expected 3` });
    } else if (wrong.length > 0) {
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the figures',
        detail: wrong.map(([r, got, want]) =>
          `${r}: ${got} not ${want}`).join(', ') });
    } else if (!/^select /i.test(out.sql)) {
      // The SQL comes back as an execution ACTIVITY -- reported, not
      // run here. Without it the SQL pane has nothing true to show.
      results.push({ name: 'server mode: the engine executes', ok: false,
        where: 'the activity', detail: `no SQL reported: ${out.sql}` });
    } else {
      // AND THE `unique` AGGREGATE MEANS THE SAME THING THERE. The
      // engine lowers it to `case when count(distinct x) = 1 then
      // max(x) else null end`, which is exactly what this cube
      // documents it as -- so a group with two desks reads null and
      // one with a single desk reads the desk.
      const desks = Object.fromEntries(
        regions.map((r, i) => [r, (by['desk']?.values ?? [])[i]]));
      const agreed = desks['AMER'] === 'Rates' && desks['EMEA'] === null;
      results.push(agreed
        ? { name: 'server mode: the engine executes', ok: true,
            detail: `3 rows, sums agree, unique agrees` }
        : { name: 'server mode: the engine executes', ok: false,
            where: 'the unique aggregate',
            detail: `AMER desk ${JSON.stringify(desks['AMER'])},`
              + ` EMEA desk ${JSON.stringify(desks['EMEA'])}` });
    }
  } catch (e) {
    results.push({ name: 'server mode: the engine executes', ok: false,
      where: 'the executor', detail: String(e.message ?? e).slice(0, 220) });
  }
}

const ok = results.filter((r) => r.ok);
const bad = results.filter((r) => !r.ok);
for (const r of results) {
  console.log(r.ok
    ? `  ok   ${r.name}`
    : `  BAD  ${r.name} — ${r.where}: ${r.detail}`);
}
console.log(`\n${ok.length}/${results.length} operations compile on the`
  + ' real engine');
if (bad.length) {
  console.log(`\nNOT ACCEPTED (${bad.length}):`);
  for (const r of bad) {
    console.log(`\n  ${r.name}\n    ${r.where}: ${r.detail}`);
    if (r.pure) console.log(`    pure: ${r.pure.slice(0, 200)}`);
  }
}
console.log(bad.length === 0
  ? '\n*** the engine accepts every operation this cube emits ***'
  : `\n!!! ${bad.length} operations the engine does not accept !!!`);
process.exit(bad.length === 0 ? 0 : 1);
