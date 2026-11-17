// A DEEPHAVEN PLAN'S STEPS, AS A RUNTIME READS THEM (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §4; the planner's side is
// core's com.legend.deephaven.DeephavenSteps). The planner wrote every byte a session sends: each step's request, and
// the session's own requests, each with the places listed where the session's numbers go -- an export's id, four bytes
// at a fixed place, so filling one moves nothing. This fills those places and reads the server's answers; it encodes
// nothing, plans nothing, and reads no step's readable form (a person's and an error message's alone).

import { tableFromIPC } from 'apache-arrow';

/** The format a step plan names first: a plan of another is refused by name. */
export const STEPS_FORMAT = 'legend-deephaven-steps';

/** A store's table as its store declares it, read by a step's operation (DH16): each column's name in the made table,
 *  its name on the server, the Java type the server must hold it as, and the type its store declares. */
export interface DeclaredTable {
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly column: string; readonly type: string; readonly declared: string }[];
}

/** A table a step exports: the place its id is written, its operation, and the store's table it reads, if one. */
export interface StepExport {
  readonly at: number;
  readonly op: number;
  readonly declared?: DeclaredTable;
}

/** One step: its request's bytes (base64), the tables it exports (the last its result), its operations' count, and the
 *  name a session holds its result by. */
export interface Step {
  readonly identity: string;
  readonly kept: boolean;
  readonly request: string;
  readonly exports: readonly StepExport[];
  readonly operations: number;
  /** Where it reads the table of a step before it: that step's place in the plan, and the place its export's id is
   *  written. */
  readonly reads?: readonly { readonly step: number; readonly at: number }[];
  readonly readable: string;
}

/** A request of the session's own, its bytes and the place an export's id is written. */
export interface SessionRequest {
  readonly request: string;
  readonly export: number;
}

/** The session's own requests, as the plan declares them: release an export, the ticket a row read names, and
 *  publish an export under a shared name (its `name` the place of the name's characters). */
export interface SessionRequests {
  readonly release: SessionRequest;
  readonly fetch: { readonly ticket: string; readonly export: number };
  readonly publish: SessionRequest & { readonly name: number };
}

/** A Deephaven plan: its steps, in order, and the session's requests. */
export interface StepPlan {
  readonly steps: readonly Step[];
  readonly requests: SessionRequests;
}

/** What a published name begins with, and how many characters follow (DeephavenSteps.NAME_PREFIX, NAME_LENGTH). */
export const NAME_PREFIX = 'legend-';
export const NAME_LENGTH = 24;

/** `text`, a Deephaven plan's statement, as its steps; anything else refused by name. */
export function stepPlan(text: string): StepPlan {
  let doc: unknown;
  try {
    doc = JSON.parse(text);
  } catch {
    throw new Error('a Deephaven plan is its steps, as the planner writes them; this is no JSON');
  }
  const d = doc as { format?: unknown; steps?: unknown; requests?: unknown };
  if (d.format !== STEPS_FORMAT || !Array.isArray(d.steps) || d.steps.length === 0 || typeof d.requests !== 'object') {
    throw new Error(`a Deephaven plan is its steps (${STEPS_FORMAT}), as the planner writes them`);
  }
  return d as StepPlan;
}

/** `bytes` (base64) with each `at` place holding its id: a little-endian 32-bit number, as the planner placed it. */
export function filled(base64: string, places: readonly { readonly at: number; readonly id: number }[]): Uint8Array {
  const bytes = fromBase64(base64);
  for (const { at, id } of places) {
    if (!Number.isInteger(id) || id <= 0 || id > 0x7fffffff) throw new Error(`a Deephaven export id is positive, not ${id}`);
    if (at < 0 || at + 4 > bytes.length) throw new Error(`a place at ${at} is outside the plan's ${bytes.length} bytes`);
    new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).setInt32(at, id, true);
  }
  return bytes;
}

/** The publish request for `exportId` under `NAME_PREFIX + name`, `name` the plan's NAME_LENGTH characters. */
export function publishRequest(requests: SessionRequests, exportId: number, name: string): Uint8Array {
  if (!/^[0-9a-z]+$/.test(name) || name.length !== NAME_LENGTH) {
    throw new Error(`a published name's characters are ${NAME_LENGTH} of [0-9a-z], not '${name}'`);
  }
  const bytes = filled(requests.publish.request, [{ at: requests.publish.export, id: exportId }]);
  bytes.set(new TextEncoder().encode(name), requests.publish.name);
  return bytes;
}

/** A column of a table the server made, as its schema names it: its name and its Deephaven type (null: none). */
export interface MadeColumn {
  readonly name: string;
  readonly type: string | null;
}

/** What the server answered for one export of a batch: its id (0 when it names none), whether it was made, why not,
 *  and its columns. */
export interface Created {
  readonly exportId: number;
  readonly success: boolean;
  readonly errorInfo: string;
  readonly columns: readonly MadeColumn[];
}

/** One message of a batch's answer (`ExportedTableCreationResponse`: result_id 1, success 2, error_info 3,
 *  schema_header 4; the rest skipped by protobuf's own rule). */
export function created(message: Uint8Array): Created {
  let exportId = 0;
  let success = false;
  let errorInfo = '';
  let columns: readonly MadeColumn[] = [];
  for (const f of fields(message)) {
    if (f.number === 1 && f.bytes) exportId = exportOf(f.bytes);
    else if (f.number === 2 && f.value !== undefined) success = f.value !== 0n;
    else if (f.number === 3 && f.bytes) errorInfo = new TextDecoder().decode(f.bytes);
    else if (f.number === 4 && f.bytes && f.bytes.length > 0) columns = schemaColumns(f.bytes);
  }
  return { exportId, success, errorInfo, columns };
}

/** A batch's whole answer read against its step (DH16): why it failed (null when every table was made as declared),
 *  and each export's columns by its id. */
export function made(answers: readonly Uint8Array[], step: Step, ids: readonly number[]): {
  readonly refused: string | null;
  readonly mismatch: string | null;
  readonly columns: ReadonlyMap<number, readonly MadeColumn[]>;
} {
  let refused: string | null = answers.length === step.exports.length ? null
    : `the server answered ${answers.length} of the step's ${step.exports.length} exports`;
  let mismatch: string | null = null;
  const byId = new Map(ids.map((id, i) => [id, step.exports[i]!]));
  const columns = new Map<number, readonly MadeColumn[]>();
  for (const answer of answers) {
    const c = created(answer);
    if (!c.success) {
      refused ??= c.errorInfo;
      continue;
    }
    columns.set(c.exportId, c.columns);
    const e = byId.get(c.exportId);
    if (e === undefined) {
      refused ??= `the server answered for export ${c.exportId}, which the step did not make`;
      continue;
    }
    if (e.declared !== undefined) mismatch ??= mismatchOf(e.declared, c.columns);
  }
  for (const id of ids) if (!columns.has(id)) refused ??= `the server did not answer for export ${id}`;
  return { refused, mismatch, columns };
}

/** Why `held`, the columns the server made a store's table's read as, are not what `declared` says; null if they are. */
function mismatchOf(declared: DeclaredTable, held: readonly MadeColumn[]): string | null {
  const byName = new Map(held.map((c) => [c.name, c]));
  const wrong: string[] = [];
  for (const d of declared.columns) {
    const c = byName.get(d.name);
    if (c === undefined) wrong.push(`${d.column} is not in the table the server made`);
    else if (c.type !== d.type) {
      wrong.push(`${d.column} is held as ${c.type ?? 'no Deephaven type'}; its store declares ${d.declared} (${d.type}`
        + ' on Deephaven)');
    }
  }
  return wrong.length === 0 ? null
    : `the server's table ${declared.table} is not what its store declares: ${wrong.join('; ')}`;
}

/** The export a `TableReference` names: its ticket's id, 0 when it names none (a batch offset, a scope ticket). */
function exportOf(reference: Uint8Array): number {
  for (const f of fields(reference)) {
    if (f.number !== 1 || !f.bytes) continue;
    for (const t of fields(f.bytes)) {
      if (t.number !== 1 || !t.bytes) continue;
      const b = t.bytes;
      if (b.length !== 5 || b[0] !== 0x65) return 0;
      return new DataView(b.buffer, b.byteOffset + 1, 4).getInt32(0, true);
    }
  }
  return 0;
}

/** A schema message's columns: each field's name and its `deephaven:type`, read by Arrow's own reader. */
function schemaColumns(header: Uint8Array): readonly MadeColumn[] {
  const framed = header.length >= 8 && new DataView(header.buffer, header.byteOffset, 4).getInt32(0, true) === -1;
  const prefix = framed ? new Uint8Array() : (() => {
    const p = new Uint8Array(8);
    new DataView(p.buffer).setUint32(0, 0xffffffff, true);
    new DataView(p.buffer).setInt32(4, header.length, true);
    return p;
  })();
  const end = new Uint8Array(8);
  new DataView(end.buffer).setUint32(0, 0xffffffff, true);
  const stream = new Uint8Array(prefix.length + header.length + end.length);
  stream.set(prefix, 0);
  stream.set(header, prefix.length);
  stream.set(end, prefix.length + header.length);
  return tableFromIPC(stream).schema.fields.map((f) => ({ name: f.name, type: f.metadata.get('deephaven:type') ?? null }));
}

/** A protobuf message's fields, each its number and either its varint value or its bytes. */
function fields(message: Uint8Array): { readonly number: number; readonly value?: bigint; readonly bytes?: Uint8Array }[] {
  const out: { number: number; value?: bigint; bytes?: Uint8Array }[] = [];
  for (let at = 0; at < message.length;) {
    const [key, afterKey] = varint(message, at);
    const number = Number(key >> 3n);
    const wire = Number(key & 7n);
    if (wire === 0) {
      const [value, next] = varint(message, afterKey);
      out.push({ number, value });
      at = next;
    } else if (wire === 2) {
      const [length, start] = varint(message, afterKey);
      const end = start + Number(length);
      if (end > message.length) throw new Error('a Deephaven answer ended inside a field');
      out.push({ number, bytes: message.subarray(start, end) });
      at = end;
    } else if (wire === 1) {
      at = afterKey + 8;
    } else if (wire === 5) {
      at = afterKey + 4;
    } else {
      throw new Error(`a Deephaven answer holds wire type ${wire}, which no message lite reads uses`);
    }
  }
  return out;
}

/** A base-128 varint at `at`: its value, and where the next field starts. */
function varint(bytes: Uint8Array, at: number): [bigint, number] {
  let value = 0n;
  for (let shift = 0n, i = at; shift < 64n; shift += 7n, i++) {
    const b = bytes[i];
    if (b === undefined) throw new Error('a Deephaven answer ended inside a number');
    value |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) return [value, i + 1];
  }
  throw new Error('a Deephaven answer holds a number longer than 64 bits');
}

function fromBase64(text: string): Uint8Array {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
