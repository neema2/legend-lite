// Getting a picked file into DuckDB, and out again as a schema.
//
// Kept apart from `infer.ts` on purpose: this half touches the
// duckdb-wasm handle and the browser's File object, the other half is
// pure text-in text-out and is therefore the half worth unit testing.

import type { QueryEngine } from './engine.ts';
import type { ResultTable, Scalar } from './result.ts';
import {
  inferModel,
  isNestedType,
  type DescribedColumn,
  type InferredModel,
} from './infer.ts';

/**
 * The duckdb-wasm surface used here.
 *
 * Declared structurally rather than imported so that this file, and
 * the tests around it, do not drag in the 36 MB package.
 */
export interface DuckDbFiles {
  registerFileText(name: string, text: string): Promise<void>;
  registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>;
}

export type UploadFormat = 'csv' | 'parquet' | 'json';

export interface UploadResult extends InferredModel {
  readonly table: string;
  readonly rowCount: number;
  readonly fileName: string;
}

/** DuckDB's identifier quoting: the doubled quote, not a backslash. */
function dq(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Guess by extension; the picker allows only these. JSON covers both a
 * document holding an array of records and newline-delimited records
 * -- DuckDB's reader tells them apart itself.
 */
export function formatOf(fileName: string): UploadFormat {
  if (/\.parquet$/i.test(fileName)) return 'parquet';
  if (/\.(json|jsonl|ndjson)$/i.test(fileName)) return 'json';
  return 'csv';
}

/**
 * A table name derived from the file, safe to interpolate.
 *
 * The name reaches both SQL and a Pure Database declaration, and a
 * file can be called anything at all, so everything outside
 * [A-Za-z0-9_] goes. A leading digit gets a prefix because a bare
 * `2024_trades` is not an identifier.
 */
export function tableNameOf(fileName: string): string {
  const stem = fileName.replace(/\.[^.]*$/, '');
  const cleaned = stem.replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'data';
  return /^[0-9]/.test(cleaned) ? `t_${cleaned}` : cleaned;
}

/**
 * Read a picked file into DuckDB and describe what arrived.
 *
 * CSV goes in as text with `read_csv(..., AUTO_DETECT)` so DuckDB
 * sniffs the header and types; Parquet goes in as bytes and carries
 * its own schema. Either way the result is a real table, and from
 * there `DESCRIBE` is the only thing that says what the columns are
 * -- guessing from the file would be a second, worse sniffer.
 */
export async function ingestFile(
  engine: QueryEngine,
  db: DuckDbFiles,
  file: { name: string; text(): Promise<string>;
    arrayBuffer(): Promise<ArrayBuffer> },
): Promise<UploadResult> {
  const format = formatOf(file.name);
  const table = tableNameOf(file.name);
  // A fixed virtual filename per table: re-picking a file replaces
  // the registration rather than accumulating them.
  const virtualName = `upload_${table}.${format}`;

  if (format === 'parquet') {
    await db.registerFileBuffer(virtualName,
      new Uint8Array(await file.arrayBuffer()));
  } else {
    await db.registerFileText(virtualName, await file.text());
  }

  const reader = format === 'parquet'
    ? `read_parquet('${virtualName}')`
    : format === 'json'
      // Each record's top-level keys become columns; anything nested
      // below them arrives as a STRUCT or a LIST, converted below.
      ? `read_json('${virtualName}', auto_detect=true)`
      // AUTO_DETECT sniffs delimiter, quoting and types. Upstream's
      // DataCube requires a header row and comma delimiters; DuckDB's
      // sniffer handles more than that, so there is no reason to
      // impose the narrower rule.
      : `read_csv('${virtualName}', AUTO_DETECT=TRUE, HEADER=TRUE)`;

  // The user's own column names, unchanged. An earlier version
  // renamed anything that was not a plain identifier, because a
  // quote in a header broke the model and a space broke groupBy and
  // sort. Both were core defects -- the lexer's escape is a
  // backslash, and name resolution compared a quoted wire name
  // against a bare reference -- and both are fixed there now, so
  // mangling the user's headers to route around them would be
  // keeping a workaround that has outlived its bug.
  // Quote the table name: it comes from a FILENAME, and `pivot.csv`
  // produced `CREATE OR REPLACE TABLE pivot AS …`, which is a syntax
  // error because pivot is reserved in DuckDB. tableNameOf already
  // strips it to [A-Za-z0-9_], so quoting is all that is left.
  const qt = dq(table);
  await engine.execute(
    `CREATE OR REPLACE TABLE ${qt} AS SELECT * FROM ${reader}`, 0);

  // Nested columns become JSON. legend-lite declares them
  // SEMISTRUCTURED and navigates them with DuckDB's JSON operators,
  // which do not apply to a STRUCT or a LIST -- so the table has to
  // hold what the model says it holds. Parquet carries nested columns
  // too, which is why this is not JSON-only.
  let described = await describeTable(engine, qt);
  const nested = described.filter((c) => isNestedType(c.type));
  if (nested.length > 0) {
    const replaced = nested
      .map((c) => `to_json(${dq(c.name)}) AS ${dq(c.name)}`).join(', ');
    await engine.execute(
      `CREATE OR REPLACE TABLE ${qt} AS SELECT * REPLACE (${replaced}) `
        + `FROM ${qt}`, 0);
    described = await describeTable(engine, qt);
  }

  const counted = await engine.execute(
    `SELECT count(*) AS n FROM ${qt}`, 0);
  const rowCount = Number(counted.columns[0]?.values[0] ?? 0);

  return {
    ...inferModel(described, { table }),
    table,
    rowCount,
    fileName: file.name,
  };
}

/**
 * The table's columns and DuckDB types. A ResultTable is COLUMNAR, so
 * DESCRIBE's answer is read by picking the two columns out and zipping
 * them, not row by row.
 */
async function describeTable(
  engine: QueryEngine,
  qt: string,
): Promise<DescribedColumn[]> {
  const describe = await engine.execute(`DESCRIBE ${qt}`, 0);
  const names = columnOf(describe, 'column_name');
  const types = columnOf(describe, 'column_type');
  return names.map((n, i) => ({
    name: String(n),
    type: String(types[i] ?? 'VARCHAR'),
  }));
}

/** One column of a DESCRIBE result, by name. */
function columnOf(t: ResultTable, name: string): readonly Scalar[] {
  const col = t.columns.find((c) => c.name === name);
  if (!col) {
    throw new Error(`DESCRIBE did not return ${name} — got `
      + t.columns.map((c) => c.name).join(', '));
  }
  return col.values;
}
