// Adversarial end-to-end: weird data, weird shapes, weird scale.
//
// Runs against the REAL planner and a REAL DuckDB, because the
// point is the whole path -- serialiser, compiler, engine, column
// model -- and every bug found so far lived in a seam between two
// of those rather than inside one.
//
// Distinct from verify.mjs: that asserts the product does what it
// claims for a normal cube. This tries to break it. A failure here
// is either a bug or a wall that should exist and does not.
//
// Run: node demo/torture.mjs   (needs `npm run engine`)
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import { serialize } from '../src/serialize.ts';
import { buildColumnModel } from '../src/grid/columns.ts';
import { CubeRefusal } from '../src/snapshot.ts';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const MODEL = readFileSync(`${ROOT}demo/torture.pure`, 'utf8');
const ENGINE = 'http://localhost:8080';

let failed = false;

// DEFECTS IN legend-lite, NOT IN DATACUBE.
//
// Each of these is a query DataCube builds correctly and the engine
// cannot plan. They are listed rather than deleted because a suite
// that quietly drops what it cannot pass stops being evidence -- and
// because the day one starts working, we want to know. A known
// failure that PASSES fails this run: the note is then stale and the
// case belongs back in the normal set.
//
// All of them are one bug wearing two faces: a column DECLARED with
// quotes in the Database grammar ("with space", "quo'te", "Ünïcødé",
// "select") resolves fine through select, filter and extend, and
// fails through groupBy, sort and pivot -- the three paths that
// ISOLATE the table scan before resolving. groupBy and sort report
// it as an IllegalStateException; pivot dereferences null.
//
//   reproduce: #>{torture::DB.WEIRD}#->groupBy(~['with space'],
//       ~[m:x|$x.n_float:y|$y->sum()])
const KNOWN_ENGINE_BUGS = new Map([
  ['group by a name with a space', 'quoted identifier unresolvable in groupBy'],
  ['group by a name with an apostrophe', 'quoted identifier unresolvable in groupBy'],
  ['group by a unicode name', 'quoted identifier unresolvable in groupBy'],
  ['group by a RESERVED word', 'quoted identifier unresolvable in groupBy'],
  ['sort on a weird name, descending', 'quoted identifier unresolvable in sort'],
  ['pivot on a weird name', 'NPE pivoting on a quoted identifier'],
  ['pivot on a date', 'NPE pivoting on a quoted identifier'],
]);

const check = (name, ok, detail = '') => {
  const known = KNOWN_ENGINE_BUGS.get(name);
  if (known) {
    if (ok) {
      console.log(
        `FIXED ${name} — the engine bug is gone; drop it from`
          + ' KNOWN_ENGINE_BUGS so this case asserts normally',
      );
      failed = true;
    } else {
      console.log(`KNOWN ${name} — engine: ${known}`);
    }
    return;
  }
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

async function plan(pure) {
  const r = await fetch(`${ENGINE}/engine/plan`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code: `${MODEL}\n\n${pure}`, runtime: 'torture::RT' }),
  });
  const body = await r.json().catch(() => ({}));
  return { sql: body.sql, error: body.error };
}

/** Every column the torture table has, with its declared type. */
const COLUMNS = [
  { name: 'plain', type: 'String' },
  { name: 'with space', type: 'String' },
  { name: "quo'te", type: 'String' },
  { name: 'Ünïcødé', type: 'String' },
  { name: 'select', type: 'String' },
  { name: 'nulls', type: 'String' },
  { name: 'n_int', type: 'Integer' },
  { name: 'n_float', type: 'Float' },
  { name: 'n_neg', type: 'Float' },
  { name: 'when', type: 'Date' },
  { name: 'flag', type: 'Boolean' },
];

const SRC = '#>{torture::DB.WEIRD}#';
const base = {
  source: { expression: SRC },
  columns: COLUMNS,
  derived: [],
  rows: [],
  pivotOn: [],
  measures: [],
  sorts: [],
  epoch: 1,
};
const sum = (name, column) => ({ name, column, fn: 'sum' });

const cases = [
  ['detail over every weird column', { ...base }],
  [
    'group by a name with a space',
    { ...base, rows: ['with space'], measures: [sum('m', 'n_float')] },
  ],
  [
    'group by a name with an apostrophe',
    { ...base, rows: ["quo'te"], measures: [sum('m', 'n_float')] },
  ],
  [
    'group by a unicode name',
    { ...base, rows: ['Ünïcødé'], measures: [sum('m', 'n_float')] },
  ],
  [
    'group by a RESERVED word',
    { ...base, rows: ['select'], measures: [sum('m', 'n_float')] },
  ],
  [
    'group by a column full of NULLs',
    { ...base, rows: ['nulls'], measures: [sum('m', 'n_float')] },
  ],
  [
    'pivot on a weird name',
    { ...base, rows: ['plain'], pivotOn: ['with space'], measures: [sum('m', 'n_float')] },
  ],
  [
    'pivot on NULLs',
    { ...base, rows: ['plain'], pivotOn: ['nulls'], measures: [sum('m', 'n_float')] },
  ],
  [
    'pivot on a boolean',
    { ...base, rows: ['plain'], pivotOn: ['flag'], measures: [sum('m', 'n_float')] },
  ],
  [
    'pivot on a date',
    { ...base, rows: ['plain'], pivotOn: ['when'], measures: [sum('m', 'n_float')] },
  ],
  [
    'three row dimensions and two pivots',
    {
      ...base,
      rows: ['plain', 'with space', 'select'],
      pivotOn: ['flag', 'nulls'],
      measures: [sum('m', 'n_float'), sum('m2', 'n_int')],
    },
  ],
  [
    'filter on an apostrophe value',
    {
      ...base,
      rows: ['plain'],
      measures: [sum('m', 'n_float')],
      filter: { kind: 'condition', column: "quo'te", operator: 'equal', value: "it's" },
    },
  ],
  [
    'filter on an empty string',
    {
      ...base,
      rows: ['plain'],
      measures: [sum('m', 'n_float')],
      filter: { kind: 'condition', column: 'plain', operator: 'equal', value: '' },
    },
  ],
  [
    'filter is-null on a null column',
    {
      ...base,
      rows: ['plain'],
      measures: [sum('m', 'n_float')],
      filter: { kind: 'condition', column: 'nulls', operator: 'isEmpty' },
    },
  ],
  [
    'filter with a backslash',
    {
      ...base,
      rows: ['plain'],
      measures: [sum('m', 'n_float')],
      filter: {
        kind: 'condition',
        column: 'plain',
        operator: 'contains',
        value: 'back\\slash',
      },
    },
  ],
  [
    'every aggregate at once',
    {
      ...base,
      rows: ['plain'],
      measures: [
        { name: 'a', column: 'n_float', fn: 'sum' },
        { name: 'b', column: 'n_float', fn: 'average' },
        { name: 'c', column: 'n_float', fn: 'min' },
        { name: 'd', column: 'n_float', fn: 'max' },
        { name: 'e', column: 'n_float', fn: 'count' },
        { name: 'f', column: 'n_float', fn: 'stdDevSample' },
        { name: 'g', column: 'n_float', fn: 'varianceSample' },
        { name: 'h', column: 'n_float', fn: 'wavg', weight: 'n_int' },
      ],
    },
  ],
  [
    'sort on a weird name, descending',
    {
      ...base,
      rows: ['with space'],
      measures: [sum('m', 'n_float')],
      sorts: [{ column: 'with space', direction: 'desc' }],
    },
  ],
  [
    'derived column before aggregation',
    {
      ...base,
      rows: ['plain'],
      derived: [{ name: 'doubled', expression: '$x.n_float * 2' }],
      measures: [sum('m', 'doubled')],
    },
  ],
];

console.log('--- shapes ---');
for (const [name, snap] of cases) {
  let pure;
  try {
    pure = serialize(snap, { level: Math.max(1, snap.rows.length), parent: [], limit: 50 });
  } catch (e) {
    check(name, e instanceof CubeRefusal, `refused: ${e.message}`);
    continue;
  }
  const { sql, error } = await plan(pure);
  check(name, Boolean(sql), error ?? pure.slice(0, 110));
}

// -- every LEVEL of a deep cube, which is what the tree actually asks
console.log('\n--- every level of a deep cube ---');
const deep = {
  ...base,
  rows: ['plain', 'with space', 'select', 'nulls'],
  pivotOn: ['flag'],
  measures: [sum('m', 'n_float')],
};
for (let level = 0; level <= deep.rows.length; level++) {
  const parent = ['a', 'b', 'c', 'd'].slice(0, Math.max(0, level - 1));
  const pure = serialize(deep, { level, parent, limit: 50 });
  const { sql, error } = await plan(pure);
  check(`level ${level} of ${deep.rows.length}`, Boolean(sql), error ?? '');
}

// -- WIDE: many measures, and the column model that comes back
console.log('\n--- wide ---');
for (const n of [1, 10, 40]) {
  const measures = Array.from({ length: n }, (_, i) => sum(`m${i}`, 'n_float'));
  const wide = { ...base, rows: ['plain'], pivotOn: ['flag'], measures };
  const pure = serialize(wide, { level: 1, parent: [], limit: 50 });
  const { sql, error } = await plan(pure);
  check(`${n} measures under a pivot`, Boolean(sql), error ?? `${sql?.length} chars of SQL`);
}

// -- the column model must survive whatever names come back
console.log('\n--- column model on hostile names ---');
{
  const names = [
    '__tree',
    'with space',
    "quo'te",
    'Ünïcødé',
    'true__|__m',
    'false__|__m',
    // A dimension VALUE that contains the separator: the case that
    // makes a naive split produce the wrong header path.
    'a__|__b__|__m',
  ];
  const table = {
    columns: names.map((name) => ({ name, type: 'String', values: [null] })),
    rowCount: 1,
    epoch: 1,
    elapsedMs: 0,
  };
  let model;
  try {
    model = buildColumnModel(table, ['plain'], ['m']);
    check('builds a model over hostile column names', true, `${model.leaves.length} leaves`);
    check(
      'and one leaf per column, none lost',
      model.leaves.length === names.length,
      `${model.leaves.length} of ${names.length}`,
    );
    const overrun = model.headerRows.some((row) =>
      row.some((c) => c.colStart + c.colSpan > model.leaves.length),
    );
    check('and no header cell overruns the leaves', !overrun);
  } catch (e) {
    check('builds a model over hostile column names', false, e.message);
  }
}

console.log(
  failed
    ? '\nSOME CHECKS FAILED'
    : `\nall clear (${KNOWN_ENGINE_BUGS.size} known engine bugs skipped)`,
);
process.exit(failed ? 1 : 0);
