// A DEEPHAVEN SERVER'S ROWS (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23): the Arrow IPC stream a session reads a result's
// rows as (deephaven.ts `rows`), as a planned query's result. Each column's values are read by the tab's own Arrow
// decoders (duckdb.ts `decoderFor`), so a value reads the same whichever database answered (values.ts) -- but for what
// Deephaven writes in a form of its own, named by the column's `deephaven:type`: a BigDecimal (its scale, a 32-bit
// little-endian integer, then its unscaled value, big-endian two's complement) and a BigInteger (big-endian two's
// complement). The plan names and types the columns BY POSITION: the target writes each result column under its own
// name where Deephaven holds that name, else `__c<i>` (com.legend.sql.dialect.DeephavenBatch).

import { tableFromIPC } from 'apache-arrow';

import { decoderFor, type ArrowishVector, type Decode } from './duckdb.ts';
import { QueryError, type RawTable } from './engine.ts';
import type { Plan } from './relation-type.ts';
import type { ResultTable, Scalar } from './result.ts';
import { decimalText, exactInteger } from './values.ts';

/** An Arrow IPC stream of a Deephaven result as its columns and values, untyped: the plan types them (`byPosition`). */
export function deephavenRows(stream: Uint8Array, epoch: number, elapsedMs: number): RawTable {
  const table = tableFromIPC(stream);
  const columns = table.schema.fields.map((field, c) => {
    const vector = table.getChildAt(c) as unknown as ArrowishVector | null;
    const decode = decoderOf(field.name, String(field.type), field.metadata.get('deephaven:type'), field.type);
    const values: Scalar[] = new Array<Scalar>(table.numRows);
    for (let r = 0; r < table.numRows; r++) values[r] = vector ? decode(vector, r) : null;
    return { name: field.name, values };
  });
  return { columns, rowCount: table.numRows, epoch, elapsedMs };
}

/** A column's decoder: Deephaven's own forms by their Java type, every other by its Arrow type. */
function decoderOf(name: string, arrowType: string, javaType: string | undefined, type: unknown): Decode {
  const bytes = (v: ArrowishVector, r: number): Uint8Array | null => {
    const b = v.get(r);
    return b === null || b === undefined ? null : b as Uint8Array;
  };
  if (javaType === 'java.math.BigDecimal') {
    return (v, r) => {
      const b = bytes(v, r);
      if (b === null) return null;
      if (b.length < 5) throw new Error(`a Deephaven BigDecimal in '${name}' is ${b.length} bytes`);
      const scale = new DataView(b.buffer, b.byteOffset, 4).getInt32(0, true);
      return decimalText(twosComplement(b.subarray(4)), scale);
    };
  }
  if (javaType === 'java.math.BigInteger') {
    return (v, r) => {
      const b = bytes(v, r);
      return b === null ? null : exactInteger(twosComplement(b));
    };
  }
  // bytes in a form this does not read are refused, never shown as their numbers
  if (/^(Binary|LargeBinary|FixedSizeBinary)/.test(arrowType)) {
    throw new Error(`the Deephaven column '${name}' is ${javaType ?? 'binary'}, which the tab does not read`);
  }
  return decoderFor(type);
}

/** Big-endian two's complement bytes as the integer they hold. */
function twosComplement(b: Uint8Array): bigint {
  let n = 0n;
  for (const byte of b) n = (n << 8n) | BigInt(byte);
  return b.length > 0 && ((b[0] ?? 0) & 0x80) ? n - (1n << BigInt(8 * b.length)) : n;
}

/**
 * A Deephaven result typed by its plan, column by column in order: the plan's name and type for each. A column count
 * that differs, or a column under neither the plan's name nor its place's (`__c<i>`), is refused: the result is not
 * the plan's, a bug to surface, never a gap to fill.
 */
export function byPosition(raw: RawTable, plan: Plan): ResultTable {
  if (raw.columns.length !== plan.columns.length) {
    throw new QueryError(`the Deephaven result has ${raw.columns.length} columns, the plan ${plan.columns.length}`,
      plan.sql);
  }
  return {
    ...raw,
    columns: raw.columns.map((c, i) => {
      const p = plan.columns[i];
      if (p === undefined || (c.name !== p.name && c.name !== `__c${i}`)) {
        throw new QueryError(`the Deephaven result's column ${i} is '${c.name}', where the plan has '${p?.name}'`,
          plan.sql);
      }
      return { name: p.name, type: p.type, values: c.values };
    }),
  };
}
