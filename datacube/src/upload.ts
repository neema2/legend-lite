// Getting a picked file into DuckDB, and out again as a schema.
//
// This half touches the duckdb-wasm handle and the browser's File object; the model is written
// from the table's catalog by legend-lite's one writer (`TableModels`, infer.ts).

import type { QueryEngine } from '../../engine-client/src/engine.ts';
import type { RawTable } from '../../engine-client/src/engine.ts';
import type { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { Scalar } from '../../engine-client/src/result.ts';
import type { CatalogColumn, InferredModel, TableModels } from './infer.ts';
import { mountRemote, type S3Credentials } from './remote.ts';
import { runHostOperations } from '../../engine-client/src/host-operations.ts';
import { isIdentifier } from '../../pure-protocol/src/index.ts';

/**
 * The duckdb-wasm surface used here.
 *
 * Declared structurally rather than imported so that this file, and
 * the tests around it, do not drag in the 36 MB package.
 */
export interface DuckDbFiles {
  registerFileText(name: string, text: string): Promise<void>;
  registerFileBuffer(name: string, buffer: Uint8Array): Promise<void>;
  /** Let go of a registered file's bytes (DuckDB-WASM's `dropFile`). */
  dropFile?(name: string): Promise<unknown>;
}

export type UploadFormat = 'csv' | 'parquet' | 'json';

export interface UploadResult extends InferredModel {
  readonly table: string;
  readonly rowCount: number;
  readonly fileName: string;
}

/**
 * Guess by extension; the picker allows only these. JSON covers both a
 * document holding an array of records and newline-delimited records
 * -- DuckDB's reader tells them apart itself.
 */
export function formatOf(fileName: string): UploadFormat {
  // a gzipped file's format is its name's within (`trades.csv.gz`): its bytes are unpacked as they are read (fileParts)
  const name = fileName.replace(/\.gz$/i, '');
  if (/\.parquet$/i.test(name)) return 'parquet';
  if (/\.(json|jsonl|ndjson)$/i.test(name)) return 'json';
  return 'csv';
}

/**
 * A table name derived from the file, safe to interpolate.
 *
 * The name reaches both SQL and a Pure Database declaration, and a
 * file can be called anything at all, so everything outside
 * [A-Za-z0-9_] goes. A leading digit gets a prefix because a bare
 * `2024_trades` is not an identifier, and a word the grammar reads as
 * a literal (`true.csv`) a suffix, as the compiler's own writer gives
 * one (ModelChanges.identifier: `true_`) -- one identifier rule,
 * pure-protocol's isIdentifier (the phase 8 audit).
 */
export function tableNameOf(fileName: string): string {
  const stem = fileName.replace(/\.gz$/i, '').replace(/\.[^.]*$/, '');
  const cleaned = stem.replace(/[^A-Za-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'data';
  const led = /^[0-9]/.test(cleaned) ? `t_${cleaned}` : cleaned;
  return isIdentifier(led) ? led : `${led}_`;
}

/**
 * Read a picked file into DuckDB and describe what arrived.
 *
 * The file goes in as its bytes, whatever its format: a CSV is read with
 * `read_csv(..., AUTO_DETECT)` so DuckDB sniffs the header and types; Parquet
 * carries its own schema. Either way the result is a real table, and from
 * there DuckDB's catalog is the only thing that says what the columns are
 * -- guessing from the file would be a second, worse sniffer.
 */
/**
 * An upload that was overtaken before its cube was built (a newer open won): its table and its
 * file's bytes go, so an open abandoned half-way leaves nothing held in the tab. The caller
 * makes sure no cube reads a table of the same name.
 */
export async function forgetUpload(engine: QueryEngine & { readonly databaseType: string }, models: TableModels,
  db: DuckDbFiles, fileName: string,
  // the table it was read into, when not the file's own name (`ingestFile`'s `table`)
  table = tableNameOf(fileName)): Promise<void> {
  await dropFileTable(engine, models, table);
  await db.dropFile?.(uploadFileName(table, fileName));
}

/** A file as `fileParts` reads it: its name (its format) and its bytes as a stream, as a picked `File` gives them. */
export interface PickedFile {
  readonly name: string;
  stream(): ReadableStream<Uint8Array>;
}

/** The size of a part `fileParts` hands over: small enough for any channel a host's calls travel (base64 in JSON). */
export const PART_BYTES = 4 * 1024 * 1024;

/**
 * A FILE'S BYTES AS DUCKDB IS GIVEN THEM, on every host (the tab's DuckDB-WASM, Python's): Parquet as it is, and a text
 * file -- CSV, JSON, JSON Lines -- decoded in the page and written out again as UTF-8, so every host's DuckDB reads the
 * same text (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md G4):
 * - a gzipped file (its first bytes gzip's) unpacked first, whatever its format;
 * - UTF-16 read by its byte-order mark, either order;
 * - UTF-8 when its first 64 KB are UTF-8, or hold a UTF-8 character at least (a mark taken off; a stray byte replaced,
 *   as a File's text() replaces it);
 * - else Latin-1 (the web's windows-1252): bytes above ASCII and no UTF-8 character among them, each byte its own.
 *
 * A part at a time, each `size` bytes but the last, read as the file streams: no part of the file is ever one string,
 * so no file is too long for the page's strings, and a host is sent the parts as they come (Python's file-part.json).
 * The one place it is done: the tab registers these bytes whole (`fileBytes`).
 */
export async function* fileParts(file: PickedFile, size = PART_BYTES): AsyncGenerator<Uint8Array> {
  let raw = await peeked(file.stream(), 2);
  if (raw.head[0] === 0x1f && raw.head[1] === 0x8b) {
    // gzip: unpacked, then read as what is inside
    raw = await peeked(raw.stream.pipeThrough(new DecompressionStream('gzip') as unknown as ReadableWritablePair<Uint8Array, Uint8Array>), 2);
  }
  let bytes: ReadableStream<Uint8Array>;
  if (formatOf(file.name) === 'parquet') {
    bytes = raw.stream;
  } else {
    const sampled = await peeked(raw.stream, SNIFF_BYTES);
    // a character split between two of the stream's chunks is decoded whole: the stream decoder keeps its state between
    // them (a TextDecoderStream takes any BufferSource, a Uint8Array among them; TypeScript's own types will not pipe
    // one in)
    bytes = sampled.stream
      .pipeThrough(new TextDecoderStream(encodingOf(sampled.head)) as unknown as ReadableWritablePair<string, Uint8Array>)
      .pipeThrough(new TextEncoderStream());
  }
  const reader = bytes.getReader();
  let part = new Uint8Array(size);
  let filled = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (let at = 0; at < value.length;) {
        const taken = Math.min(size - filled, value.length - at);
        part.set(value.subarray(at, at + taken), filled);
        filled += taken;
        at += taken;
        if (filled === size) {
          yield part;
          part = new Uint8Array(size);
          filled = 0;
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
  if (filled > 0) yield part.subarray(0, filled);
}

/** How much of a text file is looked at to tell UTF-8 from Latin-1. */
const SNIFF_BYTES = 64 * 1024;

/**
 * A text file's encoding, from its first bytes (`fileParts`): its mark's; else UTF-8 when its first bytes hold no byte
 * above ASCII that is not part of a UTF-8 character, or hold one UTF-8 character at least (a stray byte in a UTF-8
 * file is then replaced, as a File's text() replaces it, never the whole file read as Latin-1); else Latin-1.
 */
function encodingOf(head: Uint8Array): 'utf-8' | 'utf-16le' | 'utf-16be' | 'windows-1252' {
  if (head[0] === 0xff && head[1] === 0xfe) return 'utf-16le';
  if (head[0] === 0xfe && head[1] === 0xff) return 'utf-16be';
  if (head[0] === 0xef && head[1] === 0xbb && head[2] === 0xbf) return 'utf-8';
  let characters = 0;
  let strays = 0;
  for (let at = 0; at < head.length;) {
    const b = head[at]!;
    if (b < 0x80) {
      at += 1;
      continue;
    }
    // a UTF-8 character's length by its lead byte, and its continuation bytes 10xxxxxx
    const length = b >= 0xc2 && b <= 0xdf ? 2 : b >= 0xe0 && b <= 0xef ? 3 : b >= 0xf0 && b <= 0xf4 ? 4 : 0;
    if (length === 0) {
      strays += 1;
      at += 1;
      continue;
    }
    if (at + length > head.length) break; // a character cut off at the sample's end is not a fault of the file's
    let whole = true;
    for (let k = 1; k < length; k++) if ((head[at + k]! & 0xc0) !== 0x80) whole = false;
    if (whole) {
      characters += 1;
      at += length;
    } else {
      strays += 1;
      at += 1;
    }
  }
  return strays === 0 || characters > 0 ? 'utf-8' : 'windows-1252';
}

/** A stream's first `n` bytes (fewer when it is shorter), and the whole stream again from its start. */
async function peeked(stream: ReadableStream<Uint8Array>, n: number): Promise<{ readonly head: Uint8Array; readonly stream: ReadableStream<Uint8Array> }> {
  const reader = stream.getReader();
  const read: Uint8Array[] = [];
  let length = 0;
  let done = false;
  while (length < n) {
    const next = await reader.read();
    if (next.done) {
      done = true;
      break;
    }
    read.push(next.value);
    length += next.value.length;
  }
  const head = new Uint8Array(Math.min(n, length));
  let at = 0;
  for (const chunk of read) {
    if (at >= head.length) break;
    const taken = Math.min(chunk.length, head.length - at);
    head.set(chunk.subarray(0, taken), at);
    at += taken;
  }
  const again = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of read) controller.enqueue(chunk);
      if (done) controller.close();
    },
    async pull(controller) {
      const next = await reader.read();
      if (next.done) controller.close();
      else controller.enqueue(next.value);
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
  return { head, stream: again };
}

/** A file's bytes as every host's DuckDB is given them (`fileParts`), whole: what the tab registers. */
export async function fileBytes(file: PickedFile): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let length = 0;
  for await (const part of fileParts(file)) {
    parts.push(part);
    length += part.length;
  }
  const whole = new Uint8Array(length);
  let at = 0;
  for (const part of parts) {
    whole.set(part, at);
    at += part.length;
  }
  return whole;
}

export async function ingestFile(
  // the tab's own DuckDB: what it reads the file into, and the database type its model declares
  engine: DuckDbEngine,
  db: DuckDbFiles,
  file: PickedFile,
  // legend-lite's model writer (infer.ts): it reads the catalog and writes the model
  models: TableModels,
  options: {
    /**
     * The table to read it into, when not the file's own name: a SECOND source on the page
     * (New ▸ Source…) must not replace the table another grid reads.
     */
    readonly table?: string;
  } = {},
): Promise<UploadResult> {
  const table = options.table ?? tableNameOf(file.name);
  // A fixed virtual filename per table: re-picking a file replaces
  // the registration rather than accumulating them.
  const virtualName = uploadFileName(table, file.name);
  // its bytes as every host's DuckDB is given them (fileBytes): a text file decoded by its encoding (fileParts)
  await db.registerFileBuffer(virtualName, await fileBytes(file));
  try {
    return await readFileInto(engine, { at: virtualName, fileName: file.name, table }, models);
  } catch (e) {
    // not read: its bytes let go of, as an upload overtaken is (forgetUpload)
    await db.dropFile?.(virtualName);
    throw e;
  }
}

/** Where a file read into `table` is registered: one name per table, its format's extension. */
export function uploadFileName(table: string, fileName: string): string {
  return `upload_${table}.${formatOf(fileName)}`;
}

/**
 * A FILE THE DATABASE CAN READ, READ INTO A TABLE OF ITS OWN -- the one implementation of what reading a picked file
 * is, on every host: the tab's DuckDB-WASM reads it by the name it was registered under, a host's DuckDB at its path
 * (headless/data-in.ts). Its format is its name's (`fileName`); the table is a copy, so the file is not needed after.
 */
export async function readFileInto(
  engine: DuckDbEngine,
  file: {
    /** What the database reads it by: a registered name, or a path (named in the SQL as a string literal). */
    readonly at: string;
    /** The file's own name, as the person picked it: its format, and what the answer says arrived. */
    readonly fileName: string;
    readonly table: string;
  },
  models: TableModels,
): Promise<UploadResult> {
  const { table } = file;
  const format = formatOf(file.fileName);

  // The user's own column names, unchanged. An earlier version
  // renamed anything that was not a plain identifier, because a
  // quote in a header broke the model and a space broke groupBy and
  // sort. Both were core defects -- the lexer's escape is a
  // backslash, and name resolution compared a quoted wire name
  // against a bare reference -- and both are fixed there now, so
  // mangling the user's headers to route around them would be
  // keeping a workaround that has outlived its bug.
  // The table's name is a person's (from a FILENAME: `pivot.csv` is a reserved word in DuckDB), so the planner quotes
  // it; the file read by its format -- a CSV sniffed with a header, Parquet, JSON with each record's keys its columns
  // (anything nested below them a STRUCT or a LIST, kept: a Variant).
  const target = { name: table };
  await runHostOperations(models, engine, [{
    op: 'relation', kind: 'table', target, replace: true,
    from: { file: { at: file.at, format: format === 'parquet' ? 'parquet' : format === 'json' ? 'json' : 'csv' } },
  }]);

  const inferred = await modelLanded(engine, models, table);

  const [count] = await models.hostOperations(engine.databaseType, [{ op: 'count', from: { relation: target } }]);
  const counted = await engine.run(count![0]!, 0);
  const rowCount = Number(counted.columns[0]?.values[0] ?? 0);

  return {
    ...inferred,
    table,
    rowCount,
    fileName: file.fileName,
  };
}

/**
 * A TABLE OF ROWS JUST LANDED IN THE HOST'S DATABASE, made to hold what its model declares, and its model -- a file read
 * in, and a Snap's copy of a grid's base query (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3). legend-lite's writer
 * reads the catalog's types and says which columns the table must convert to hold what the model declares: a
 * TIMESTAMPTZ its UTC timestamp, a UBIGINT an exact DECIMAL(20,0), a UUID or TIME its text. A nested STRUCT or LIST needs
 * none: it is a Variant as stored (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md). The table is ours to rewrite, so it is
 * rewritten in place.
 */
export async function modelLanded(engine: QueryEngine & { readonly databaseType: string }, models: TableModels,
  table: string): Promise<InferredModel> {
  const target = { name: table };
  const inferred = await models.tableModel(await catalogColumns(engine, models, table),
    { table, convertible: true, databaseType: engine.databaseType });
  if (inferred.conversions.length > 0) {
    // the writer's own select list: each conversion under its column's name, written by the copy's own database
    // (SqlDialect.copySelectList); a table rewritten in place, so the writer always writes one
    const selectList = inferred.copySelectList;
    if (selectList === undefined) throw new Error(`the model of ${table} came back with no copy select list`);
    await runHostOperations(models, engine, [{
      op: 'relation', kind: 'table', target, replace: true, selectList, from: { relation: target },
    }]);
  }
  return inferred;
}

/**
 * A REMOTE FILE MOUNTED AS A VIEW named `name`, and its model written from what the catalog found -- the one
 * implementation of reading a remote Parquet, CSV or Iceberg file, on every host: the tab's DuckDB-WASM, a host's DuckDB
 * (headless/data-in.ts). A view of a remote file cannot be rewritten: a column that needs a conversion is left out.
 */
export async function mountRemoteView(engine: DuckDbEngine, models: TableModels,
  file: { readonly url: string; readonly s3?: S3Credentials; readonly name: string }): Promise<InferredModel> {
  await mountRemote(engine, models, { sources: [{ name: file.name, url: file.url }], ...(file.s3 ? { s3: file.s3 } : {}) });
  return models.tableModel(await catalogColumns(engine, models, file.name), {
    table: file.name, convertible: false, databaseType: engine.databaseType,
  });
}

/** A file's table dropped: its name is free again (the caller lets go of the file itself, where it keeps one). */
export async function dropFileTable(engine: QueryEngine & { readonly databaseType: string }, models: TableModels,
  table: string, view = false): Promise<void> {
  // a remote file's is a view (mountRemoteView)
  await runHostOperations(models, engine, [{ op: 'drop', kind: view ? 'view' : 'table', target: { name: table } }]);
}

/**
 * The table's columns, as DuckDB's catalog reports them, STRUCTURED: each one's canonical
 * type and a DECIMAL's precision and scale as data (the catalog question, infer.ts), no type string parsed.
 * A RawTable is COLUMNAR, so the answer is read by picking its columns out and zipping them.
 */
export async function catalogColumns(engine: QueryEngine, models: TableModels, table: string): Promise<CatalogColumn[]> {
  const answer = await engine.run(await models.catalogColumnsSql('main', table), 0);
  const names = columnOf(answer, 'column_name');
  const types = columnOf(answer, 'data_type');
  const logical = columnOf(answer, 'logical_type');
  const precision = columnOf(answer, 'numeric_precision');
  const scale = columnOf(answer, 'numeric_scale');
  const notNull = columnOf(answer, 'not_null');
  const num = (v: Scalar | undefined): number | null => (v === null || v === undefined ? null : Number(v));
  return names.map((n, i) => ({
    name: String(n),
    dataType: String(types[i]),
    logicalType: logical[i] === null || logical[i] === undefined ? null : String(logical[i]),
    precision: num(precision[i]),
    scale: num(scale[i]),
    notNull: notNull[i] === true,
  }));
}

/** One column of the catalog's answer, by name. */
function columnOf(t: RawTable, name: string): readonly Scalar[] {
  const col = t.columns.find((c) => c.name === name);
  if (!col) {
    throw new Error(`the catalog did not return ${name} — got `
      + t.columns.map((c) => c.name).join(', '));
  }
  return col.values;
}
