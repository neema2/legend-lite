// DATA IN, HEADLESS (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §4.2, §4.3; step 2b): a file read into the host's
// database by DataCube's own code -- upload.ts's readFileInto, the tab's -- so Python and the tab take a file the same
// way: DuckDB reading the file's own bytes, a table of its own, the model legend-lite's writer writes from what the
// catalog found, and the conversions that table then applies. Python has written the file's bytes where its DuckDB
// reads them, and hands in only that path, the file's name and the table Python chose: the bytes never cross into the
// engine, and the SQL is DataCube's, never the page's (R4).

import { LitePlanner } from '../lite-planner.ts';
import { accessRemote, bearerName, inferFormat, isBearer, scanUrl, type S3Credentials } from '../remote.ts';
import { lastSegment } from '../cube-document.ts';
import type { DataHost } from '../data-host.ts';
import { dropFileTable, modelLanded, mountRemoteView, readFileInto, tableNameOf, uploadFileName } from '../upload.ts';
import { hostCalls, hostData, hostWorker } from './host.ts';
import type { HostLeaf } from './host-tables.ts';

/** A file as Python hands it in: the handle Python issued for it (a name in its registry of the files it handed out,
 *  never a path: the orchestrator's ruling of 2026-11-12), its name as picked (its format), the table to read it into
 *  -- and, as the host read its bytes, their size and lower-case hex SHA-256, by which a page names the file. */
export interface IngestSpec {
  readonly file: string;
  readonly name: string;
  readonly table: string;
  readonly size: number;
  readonly sha256: string;
}

/** The host's database, as DataCube's data flows ask of it (each ask planned and run by the host: data-host.ts), and
 *  legend-lite's model writer over the host's planner, for one call. */
async function using<T>(use: (o: { host: DataHost; writer: LitePlanner }) => Promise<T>): Promise<T> {
  const calls = hostCalls();
  // the model writer needs no model of its own: it writes one (the worker's tableModel)
  const writer = new LitePlanner({ model: '', runtime: '', worker: () => hostWorker(calls) });
  try {
    return await use({ host: hostData(calls), writer });
  } finally {
    writer.dispose();
  }
}

/** The file read into its table: what arrived, as upload.ts says it (its model, runtime, source and row count), JSON. */
export async function ingest(spec: string): Promise<string> {
  const s = JSON.parse(spec) as IngestSpec;
  return using(async ({ host, writer }) => {
    // the plan names the file by the table's upload name, Python's SHA-256 and size of the bytes it wrote; the load holds
    // it by the handle Python issued, which Python alone resolves to what its DuckDB reads, bound to the statement's ?
    // (B3: a planned load, the file a pointer), Python's planner planning the load and its runner running it (the #6
    // bridge)
    const read = await readFileInto(host, {
      handle: s.file, name: uploadFileName(s.table, s.name), digest: s.sha256, size: s.size, fileName: s.name, table: s.table,
    }, writer);
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
  return using(async ({ host, writer }) => JSON.stringify(await modelLanded(host, writer, s.table)));
}

/** A remote file mounted as the view Python named, through the httpfs extension Python's DuckDB carries, by its planned
 *  load (B3's Q3), Python's planner planning it and its runner running it (the #6 bridge: the settings the mount made on
 *  the connection, a bearer URL's session variable, kept there, which Python replays on each read's cursor): its model,
 *  as the tab's mount says it, and -- a bearer URL -- the name it is held under (`bearer`: Python binds the URL to that
 *  held bind alone, the tenets audit's LOW 6), JSON. Python checked its URL is remote before anything ran. */
export async function mount(spec: string): Promise<string> {
  const s = JSON.parse(spec) as { readonly url: string; readonly name: string; readonly s3?: S3Credentials };
  return using(async ({ host, writer }) => {
    const model = await mountRemoteView(host, writer, { url: s.url, name: s.name, ...(s.s3 ? { s3: s.s3 } : {}) });
    // the remote file as a page's cube names it (host-tables.ts HostLeaf): what the host serves the view by
    const leaf: HostLeaf = { _type: 'remoteFile', name: lastSegment(s.url), url: s.url };
    return JSON.stringify({ ...model, leaf, ...(isBearer(s.url) ? { bearer: bearerName(s.name) } : {}) });
  });
}

/** A remote file's extensions and keys put in place, no view yet (its planned load's access alone), and what it is read as
 *  and from where, JSON (`{"format", "url"}`): an Iceberg table's URL is its table's, whose metadata names every file
 *  it reads by path, which Python checks are remote too before it mounts the table (R4). */
export async function remoteAccess(spec: string): Promise<string> {
  const s = JSON.parse(spec) as { readonly url: string; readonly name: string; readonly s3?: S3Credentials };
  return using(async ({ host }) => {
    await accessRemote(host, { sources: [{ name: s.name, url: s.url }], ...(s.s3 ? { s3: s.s3 } : {}) });
    return JSON.stringify({ format: inferFormat(s.url), url: scanUrl(s.url) });
  });
}

/** A file's table dropped (its last grid gone, or an open overtaken) -- a remote file's view, `view` -- the name free
 *  again. */
export async function forget(spec: string): Promise<void> {
  const s = JSON.parse(spec) as { readonly table: string; readonly view?: boolean };
  await using(async ({ host }) => dropFileTable(host, s.table, s.view === true));
}
