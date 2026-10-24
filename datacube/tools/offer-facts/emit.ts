// A BUILD ACTION (datacube/BUILD.bazel, offer_queries): the queries T5's facts are made of
// (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T5). For one column of every type a cube
// column can have, EXACTLY the queries DataCube sends -- built by the product's own query
// builder (src/query.ts), never written by hand -- each aggregate as a level's measure; and
// each filter operator as a condition; and the calculated-column editor's curated
// functions with the example that proves each.
// OfferFacts.java compiles them with legend-lite and writes what the compiler said.
//
// Output: the model, and a TSV of `kind<TAB>column<TAB>name<TAB>payload` lines:
//   column  <probe>  <declared type>  -          a probe column, and the type it was built as
//   type    -        -                <q>        the cube's columns, typed (checked against the above)
//   agg     <probe>  <aggregate>      <q>        a level measuring the probe with the aggregate
//   op      <probe>  <operator>       <q>        a level filtering the probe with the operator
//   calc    -        <function>       <example>  the example a person reads, over the table TRADES
//
// Usage: node --experimental-strip-types emit.ts <model-out> <queries-out>

import { writeFileSync } from 'node:fs';

import { accessor, col, fn, lambda, lit, toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import { CALC_FUNCTIONS } from '../../src/calc.ts';
import { levelLambda } from '../../src/query.ts';
import {
  AGGREGATE_FNS, FILTER_OPERATORS, SECOND_COLUMN,
  type CubeSnapshot, type DerivedColumn, type FilterNode, type FilterOperator, type FilterValue,
} from '../../src/snapshot.ts';

/** A probe: a column of one type, stored (its DDL) or calculated (its expression). */
interface Probe {
  readonly name: string;
  /** The type it is built as; OfferFacts checks the compiler agrees. */
  readonly type: string;
  readonly ddl?: string;
  readonly lambda?: Lambda;
}

const x = (c: string) => col('x', c);

/**
 * Every relational type legend-lite's catalog declares a DuckDB column as (Wasm
 * databaseFromCatalog), and the types only a calculated column reaches.
 */
const PROBES: readonly Probe[] = [
  { name: 'text', type: 'String', ddl: 'VARCHAR(64)' },
  { name: 'integer', type: 'Integer', ddl: 'INTEGER' },
  { name: 'bigint', type: 'Integer', ddl: 'BIGINT' },
  { name: 'double', type: 'Float', ddl: 'DOUBLE' },
  { name: 'decimal', type: 'Decimal', ddl: 'DECIMAL(20,4)' },
  { name: 'bit', type: 'Boolean', ddl: 'BIT' },
  { name: 'date', type: 'StrictDate', ddl: 'DATE' },
  { name: 'timestamp', type: 'DateTime', ddl: 'TIMESTAMP' },
  { name: 'json', type: 'Variant', ddl: 'SEMISTRUCTURED' },
  // a number that is either kind: Pure's common supertype
  { name: 'number', type: 'Number', lambda: lambda(['x'], fn('if', lit.boolean(true), lambda([], x('integer')), lambda([], x('double')))) },
  // a date that may carry a time
  { name: 'either_date', type: 'Date', lambda: lambda(['x'], fn('if', lit.boolean(true), lambda([], x('date')), lambda([], x('timestamp')))) },
  { name: 'time', type: 'StrictTime', lambda: lambda(['x'], lit.strictTime('12:00:00')) },
  // values turned into text (the user, 2026-09-28: such a column offers joinStrings)
  { name: 'as_text', type: 'String', lambda: lambda(['x'], fn('toString', fn('toOne', x('integer')))) },
];

/** A value of each type, as the filter editor hands it over (exact text). */
const SAMPLE: Readonly<Record<string, FilterValue>> = {
  String: 'a',
  Integer: '1',
  Float: '1.5',
  Decimal: '1.5',
  Number: '1',
  Boolean: true,
  StrictDate: '2024-01-02',
  DateTime: '2024-01-02T03:04:05',
  Date: '2024-01-02',
  StrictTime: '03:04:05',
  Variant: { json: '1' },
};

/** The column a column operator compares the probe with: its twin, of the same type. */
const twin = (name: string): string => `${name}_2`;
const GROUP = 'g';
const WEIGHT = 'w';

const MODEL = `###Relational
Database offer::DB
(
    Table T
    (
        ${GROUP} VARCHAR(64), ${WEIGHT} INTEGER,
        ${PROBES.filter((p) => p.ddl).map((p) => `${p.name} ${p.ddl}, ${twin(p.name)} ${p.ddl}`).join(',\n        ')}
    )
    // the columns the editor's examples are written over (demo/trades.pure)
    Table TRADES
    (
        region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32), year INTEGER, qtr VARCHAR(8),
        notional DOUBLE, pnl DOUBLE, qty INTEGER
    )
)
`;

const LISTS = new Set<FilterOperator>(['in', 'notIn', 'inCaseInsensitive', 'notInCaseInsensitive']);

function condition(p: Probe, operator: FilterOperator): FilterNode {
  const value = SAMPLE[p.type];
  if (value === undefined) throw new Error(`no sample value of type ${p.type}`);
  return {
    kind: 'condition',
    column: p.name,
    operator,
    value: LISTS.has(operator) ? [value] : value,
    ...(operator.endsWith('Column') ? { rightColumn: twin(p.name) } : {}),
  } as FilterNode;
}

function cube(): CubeSnapshot {
  const derived: DerivedColumn[] = PROBES.filter((p) => p.lambda).flatMap((p) => [
    { name: p.name, lambda: p.lambda!, type: p.type },
    { name: twin(p.name), lambda: p.lambda!, type: p.type },
  ]);
  return {
    source: { query: accessor('offer::DB', 'T') },
    columns: [
      { name: GROUP, type: 'String' },
      { name: WEIGHT, type: 'Integer' },
      ...PROBES.filter((p) => p.ddl).flatMap((p) => [
        { name: p.name, type: p.type },
        { name: twin(p.name), type: p.type },
      ]),
    ],
    derived,
    rows: [],
    pivotOn: [],
    sorts: [],
    epoch: 1,
  };
}

/** Every line of the TSV. */
function lines(): string[] {
  const s = cube();
  const out: string[] = PROBES.map((p) => `column\t${p.name}\t${p.type}\t-`);
  out.push(`type\t-\t-\t${toJson(levelLambda(s))}`);
  for (const p of PROBES) {
    for (const aggregate of AGGREGATE_FNS) {
      // an aggregate of two takes the integer column W as its second (snapshot.ts SECOND_COLUMN: a number each)
      const m = { name: 'm', column: p.name, fn: aggregate, ...(SECOND_COLUMN[aggregate]?.required ? { weight: WEIGHT } : {}) };
      out.push(`agg\t${p.name}\t${aggregate}\t${toJson(levelLambda({ ...s, rows: [GROUP], measures: [m] }))}`);
    }
    for (const operator of FILTER_OPERATORS) {
      out.push(`op\t${p.name}\t${operator}\t${toJson(levelLambda({ ...s, filter: condition(p, operator) }))}`);
    }
  }
  for (const f of CALC_FUNCTIONS) {
    if (/[\t\n]/.test(f.example)) throw new Error(`${f.name}'s example must be one line`);
    out.push(`calc\t-\t${f.name}\t${f.example}`);
  }
  return out;
}

const [modelOut, queriesOut] = process.argv.slice(2);
if (modelOut === undefined || queriesOut === undefined) {
  throw new Error('usage: emit.ts <model-out> <queries-out>');
}
writeFileSync(modelOut, MODEL);
// compact JSON escapes every tab and newline inside a string, so a line is one query
writeFileSync(queriesOut, lines().map((l) => `${l}\n`).join(''));
