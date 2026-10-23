// DATA IN, HEADLESS (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §4.2, §4.3; step 2b): a file read into the host's
// database by DataCube's own code -- upload.ts's readFileInto, the tab's -- so Python and the tab take a file the same
// way: DuckDB reading the file's own bytes, a table of its own, the model legend-lite's writer writes from what the
// catalog found, and the conversions that table then applies. Python has written the file's bytes where its DuckDB
// reads them, and hands in only that path, the file's name and the table Python chose: the bytes never cross into the
// engine, and the SQL is DataCube's, never the page's (R4).

import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import { LitePlanner } from '../lite-planner.ts';
import { accessRemote, inferFormat, scanUrl, type S3Credentials } from '../remote.ts';
import { lastSegment } from '../cube-document.ts';
import { dropFileTable, modelLanded, mountRemoteView, readFileInto, tableNameOf } from '../upload.ts';
import { hostCalls, hostDatabase, hostWorker } from './host.ts';
import type { HostLeaf } from './host-tables.ts';

/** A file as Python hands it in: where its DuckDB reads it, its name as picked (its format), the table to read it into
 *  -- and, as the host read its bytes, their size and lower-case hex SHA-256, by which a page names the file. */
export interface IngestSpec {
  readonly path: string;
  readonly name: string;
  readonly table: string;
  readonly size: number;
  readonly sha256: string;
}

/** The host's database and legend-lite's model writer over the host's planner, for one call. */
async function using<T>(use: (o: { engine: DuckDbEngine; writer: LitePlanner }) => Promise<T>): Promise<T> {
  const calls = hostCalls();
  // the model writer needs no model of its own: it writes one (the worker's tableModel)
  const writer = new LitePlanner({ model: '', runtime: '', worker: () => hostWorker(calls) });
  try {
    return await use({ engine: new DuckDbEngine(hostDatabase(calls).connection), writer });
  } finally {
    writer.dispose();
  }
}

/** The file read into its table: what arrived, as upload.ts says it (its model, runtime, source and row count), JSON. */
export async function ingest(spec: string): Promise<string> {
  const s = JSON.parse(spec) as IngestSpec;
  return using(async ({ engine, writer }) => {
    const read = await readFileInto(engine, { at: s.path, fileName: s.name, table: s.table }, writer);
    // the file as a page's cube names it (host-tables.ts HostLeaf): what the host serves the table by
    const leaf: HostLeaf = { _type: 'file', name: s.name, size: s.size, sha256: s.sha256 };
    return JSON.stringify({ ...read, leaf });
  });
}

/**
 * THE TABLE A HOST READS A PAGE'S FILE OR REMOTE FILE INTO, asked for by the tab's rule (upload.ts tableNameOf; a remote
 * file's view `remote`): `name` the file's, or null for a remote file. The host takes it when it is free, else numbers it.
 */
export function tableFor(name: string | null): string {
  return name === null ? 'remote' : tableNameOf(name);
}

/**
 * A SNAP'S COPY, landed in the table Python named (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3): made to hold what its
 * model declares and modelled, as the tab models its own copy (upload.ts modelLanded) -- its model, runtime and source,
 * JSON.
 */
export async function landed(spec: string): Promise<string> {
  const s = JSON.parse(spec) as { readonly table: string };
  return using(async ({ engine, writer }) => JSON.stringify(await modelLanded(engine, writer, s.table)));
}

/** A remote file mounted as the view Python named, through the httpfs extension Python's DuckDB carries: its model,
 *  as the tab's mount says it, JSON. Python checked its URL is remote before anything ran. */
export async function mount(spec: string): Promise<string> {
  const s = JSON.parse(spec) as { readonly url: string; readonly name: string; readonly s3?: S3Credentials };
  return using(async ({ engine, writer }) => {
    const mounted = await mountRemoteView(engine, writer, { url: s.url, name: s.name, ...(s.s3 ? { s3: s.s3 } : {}) });
    // the remote file as a page's cube names it (host-tables.ts HostLeaf): what the host serves the view by
    const leaf: HostLeaf = { _type: 'remoteFile', name: lastSegment(s.url), url: s.url };
    return JSON.stringify({ ...mounted, leaf });
  });
}

/** A remote file's extensions and keys put in place, no view yet (remote.ts `accessOperations`), and what it is read as
 *  and from where, JSON (`{"format", "url"}`): an Iceberg table's URL is its table's, whose metadata names every file
 *  it reads by path, which Python checks are remote too before it mounts the table (R4). */
export async function remoteAccess(spec: string): Promise<string> {
  const s = JSON.parse(spec) as { readonly url: string; readonly name: string; readonly s3?: S3Credentials };
  return using(async ({ engine, writer }) => {
    await accessRemote(engine, writer, { sources: [{ name: s.name, url: s.url }], ...(s.s3 ? { s3: s.s3 } : {}) });
    return JSON.stringify({ format: inferFormat(s.url), url: scanUrl(s.url) });
  });
}

/** A file's table dropped (its last grid gone, or an open overtaken) -- a remote file's view, `view` -- the name free
 *  again. */
export async function forget(spec: string): Promise<void> {
  const s = JSON.parse(spec) as { readonly table: string; readonly view?: boolean };
  await using(async ({ engine, writer }) => dropFileTable(engine, writer, s.table, s.view === true));
}
