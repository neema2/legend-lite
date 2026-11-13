// A RESULT'S ROWS AS ONE ARROW BATCH (the engine contract's query answer, Q9: RecordBatch objects in-process): what the
// tab's in-thread engine (datacube's engines/tab-engine.ts, step 2, 2026-11-13) hands DataCube until step 4, when it hands
// DuckDB-WASM's own batches on. Each column by its values' kind -- a whole number as Int64 (an Integer column's), a
// fraction as Float64, a boolean as Bool, text as Utf8; the Pure type rides in the answer's schema. A column of mixed
// kinds is refused by name: no value is guessed at (G7).

import { Bool, Float64, Int64, Table, Utf8, vectorFromArray, type RecordBatch, type Vector } from 'apache-arrow';
import { RunError } from './engine-contract.ts';
import type { ResultTable, Scalar } from './result.ts';

export function recordBatchOf(rows: ResultTable, integers: readonly string[]): RecordBatch {
  const vectors: Record<string, Vector> = {};
  for (const c of rows.columns) vectors[c.name] = vectorOf(c.name, c.values, integers.includes(c.name));
  const [batch, ...more] = new Table(vectors).batches;
  if (batch === undefined || more.length > 0) throw new RunError(`the rows came as ${more.length + (batch ? 1 : 0)} batches, not one`, 'query');
  return batch;
}

function vectorOf(name: string, values: readonly Scalar[], integer: boolean): Vector {
  const kinds = new Set(values.filter((v) => v !== null).map((v) => typeof v));
  if (kinds.size > 1 && !(kinds.size === 2 && kinds.has('number') && kinds.has('bigint'))) {
    throw new RunError(`the column '${name}' holds values of ${[...kinds].join(' and ')}: not carried as one Arrow column`, 'query');
  }
  if (kinds.has('bigint') || (integer && kinds.has('number'))) {
    return vectorFromArray(values.map((v) => (v === null ? null : BigInt(v as number | bigint))), new Int64());
  }
  if (kinds.has('number')) return vectorFromArray(values as (number | null)[], new Float64());
  if (kinds.has('boolean')) return vectorFromArray(values as (boolean | null)[], new Bool());
  return vectorFromArray(values as (string | null)[], new Utf8());
}
