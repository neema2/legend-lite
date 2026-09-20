// End-to-end: DataCube's own serialiser -> the WASM planner -> SQL,
// checked against the JVM over the same grammar.
//
// The unit tests exercise the seam with a fake module, and
// research/wasm/differential.mjs exercises the planner over a
// hand-written corpus. Neither proves the thing that actually
// matters for shipping: that the Pure DataCube EMITS -- from real
// snapshots, through serialize(), with pivots and tree levels and
// filters -- plans identically in the browser and on the server.
//
// This closes that loop. The grammar is not written by hand here; it
// comes out of the product's serialiser, which is the only way to
// catch a construct DataCube emits that the corpus never thought to.
//
//   npm run planner:vendor && npm run verify:wasm
//
// Needs a JVM and the installed core jar for the reference side; set
// LEGEND_CORE_JAR to override where it looks.

import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { serialize } from '../src/serialize.ts';
import type { LevelScope } from '../src/serialize.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';

const HOME = process.env.HOME ?? '';
const CORE = process.env.LEGEND_CORE_JAR
  ?? `${HOME}/.m2/repository/com/legend/legend-lite-core/1.0.0-SNAPSHOT`
    + '/legend-lite-core-1.0.0-SNAPSHOT.jar';
const CLASSES = new URL('../../research/wasm/target/classes', import.meta.url)
  .pathname;
const JAVA = `${process.env.JAVA_HOME ?? `${HOME}/jdk/jdk-21.0.11+10/Contents/Home`}/bin/java`;

const MODEL = `###Relational
Database trades::DB
(
    Table TRADES
    (
        region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32),
        year INTEGER, qtr VARCHAR(8),
        notional DOUBLE, pnl DOUBLE, qty INTEGER
    )
)

###Connection
RelationalDatabaseConnection trades::Conn
{
    type: DuckDB;
    specification: DuckDB { };
    auth: Test;
}

###Runtime
Runtime trades::RT
{
    mappings: [];
    connections:
    [
        trades::DB: [ c1: trades::Conn ]
    ];
}
`;
const RUNTIME = 'trades::RT';

const COLUMNS: CubeSnapshot['columns'] = [
  { name: 'region', type: 'String' },
  { name: 'desk', type: 'String' },
  { name: 'book', type: 'String' },
  { name: 'year', type: 'Integer', kind: 'dimension' },
  { name: 'qtr', type: 'String' },
  { name: 'notional', type: 'Float' },
  { name: 'pnl', type: 'Float' },
  { name: 'qty', type: 'Integer' },
];

function snap(over: Partial<CubeSnapshot>): CubeSnapshot {
  return {
    source: { expression: '#>{trades::DB.TRADES}#' },
    columns: COLUMNS,
    derived: [],
    rows: [],
    pivotOn: [],
    measures: [],
    sorts: [],
    epoch: 1,
    ...over,
  };
}

const SUM_NOTIONAL: CubeSnapshot['measures'] = [
  { name: 'notional', column: 'notional', fn: 'sum' },
];

/**
 * Cube shapes a user actually reaches.
 *
 * No `as` casts anywhere below: every case is checked against the
 * real snapshot types, which is the only reason this file catches a
 * shape DataCube would reject.
 */
const CASES: { name: string; snapshot: CubeSnapshot; scope?: LevelScope }[] = [
  { name: 'flat', snapshot: snap({}) },
  { name: 'measures-only', snapshot: snap({ measures: SUM_NOTIONAL }) },
  {
    name: 'one-dimension',
    snapshot: snap({ rows: ['region'], measures: SUM_NOTIONAL }),
  },
  {
    name: 'three-dimensions',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
  },
  {
    name: 'pivot',
    snapshot: snap({
      rows: ['region', 'desk'],
      pivotOn: ['year'],
      measures: SUM_NOTIONAL,
    }),
  },
  {
    name: 'pivot-two-measures',
    snapshot: snap({
      rows: ['region'],
      pivotOn: ['year', 'qtr'],
      measures: [
        { name: 'notional', column: 'notional', fn: 'sum' },
        { name: 'qty', column: 'qty', fn: 'count' },
      ],
    }),
  },
  {
    name: 'filtered',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'condition', column: 'region', operator: 'equal',
                value: 'EMEA' },
    }),
  },
  {
    name: 'filter-injection',
    snapshot: snap({
      rows: ['desk'],
      measures: SUM_NOTIONAL,
      // A quote and a backslash: the pair that turns a literal into
      // grammar if escapePure gets the order wrong.
      filter: { kind: 'condition', column: 'book', operator: 'equal',
                value: "O'Brien\\Co" },
    }),
  },
  {
    name: 'filter-in',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'condition', column: 'region', operator: 'in',
                value: ['EMEA', 'APAC'] },
    }),
  },
  {
    name: 'filter-and-or',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: {
        kind: 'and',
        children: [
          { kind: 'condition', column: 'qty', operator: 'greaterThan',
            value: 10 },
          { kind: 'or', children: [
            { kind: 'condition', column: 'region', operator: 'equal',
              value: 'EMEA' },
            { kind: 'condition', column: 'desk', operator: 'notEqual',
              value: 'FX' },
          ] },
        ],
      },
    }),
  },
  {
    name: 'filter-not-empty',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      filter: { kind: 'not', child: {
        kind: 'condition', column: 'book', operator: 'isEmpty' } },
    }),
  },
  {
    name: 'sorted',
    snapshot: snap({
      rows: ['region'],
      measures: SUM_NOTIONAL,
      sorts: [{ column: 'region', direction: 'desc' }],
    }),
  },
  {
    name: 'derived',
    snapshot: snap({
      rows: ['region'],
      derived: [{ name: 'double_qty', expression: '$x.qty * 2' }],
      measures: SUM_NOTIONAL,
    }),
  },
  // Tree levels: what an expanded row actually asks for. A RowPath is
  // the parent's dimension VALUES, in row order.
  {
    name: 'tree-level-1',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 1, parent: [] },
  },
  {
    name: 'tree-level-2',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 2, parent: ['EMEA'] },
  },
  {
    name: 'tree-level-3-capped',
    snapshot: snap({
      rows: ['region', 'desk', 'book'],
      measures: SUM_NOTIONAL,
    }),
    scope: { level: 3, parent: ['EMEA', 'Rates'], limit: 501 },
  },
];

function jvmPlan(grammars: string[]): string[] {
  // One JVM per run, not one per case: boot is ~350ms and would
  // otherwise dominate a 12-case check.
  const dir = mkdtempSync(join(tmpdir(), 'dc-wasm-'));
  const modelFile = join(dir, 'model.pure');
  const tsvFile = join(dir, 'queries.tsv');
  const outFile = join(dir, 'out.txt');
  writeFileSync(modelFile, MODEL);
  writeFileSync(
    tsvFile,
    grammars.map((g, i) => `q${i}\t${g}`).join('\n') + '\n',
  );
  execFileSync(
    JAVA,
    ['-cp', `${CORE}:${CLASSES}`, 'planner.JvmMain',
      modelFile, tsvFile, RUNTIME, outFile],
    { stdio: ['ignore', 'inherit', 'ignore'] },
  );
  const text = readFileSync(outFile, 'utf8');
  const out: string[] = [];
  const re = /<<<q(\d+)>>>\n([\s\S]*?)\n<<<END>>>\n/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) out[Number(m[1])] = m[2]!;
  return out;
}

async function main(): Promise<void> {
  const planner = new WasmPlanner({
    model: MODEL,
    runtime: RUNTIME,
    assetBaseUrl: new URL('./vendor/', import.meta.url).href,
    cache: false,
  });

  const grammars = CASES.map((c) => serialize(c.snapshot, c.scope));

  console.log(`${CASES.length} cube shapes, serialised by DataCube itself\n`);
  const jvm = jvmPlan(grammars);

  let mismatches = 0;
  for (const [i, c] of CASES.entries()) {
    const expected = jvm[i] ?? '<<missing>>';
    let actual: string;
    try {
      actual = `OK\n${await planner.plan(grammars[i]!, c.snapshot, c.scope)}`;
    } catch (e) {
      // planOrError's ERR text, reassembled the way the JVM half
      // writes it, so a refusal compares as a refusal rather than
      // as a crash.
      actual = `ERR\n?\n${(e as Error).message}`;
    }
    // The JVM side keeps the exception class; the TS planner drops it
    // by design. Compare on the parts both sides carry.
    const norm = (s: string) => {
      const [tag, ...rest] = s.split('\n');
      return tag === 'ERR'
        ? `ERR\n${rest.slice(1).join('\n')}`
        : s;
    };
    const ok = norm(expected) === norm(actual);
    if (!ok) mismatches++;
    console.log(`${ok ? 'MATCH ' : 'DIFFER'}  ${c.name}`);
    if (!ok) {
      console.log(`  grammar: ${grammars[i]}`);
      console.log(`  jvm  : ${JSON.stringify(expected)}`);
      console.log(`  wasm : ${JSON.stringify(actual)}`);
    }
  }

  console.log(
    mismatches === 0
      ? `\n*** ${CASES.length}/${CASES.length} identical: DataCube's own`
        + ' grammar plans the same in WASM and on the JVM ***'
      : `\n!!! ${mismatches} of ${CASES.length} differ !!!`,
  );
  process.exit(mismatches === 0 ? 0 : 1);
}

await main();
