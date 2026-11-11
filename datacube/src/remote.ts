// Reading a cube's data from object storage.
//
// The engine already ships what this needs: the duckdb-wasm binary
// has httpfs, parquet and iceberg compiled in, and `CREATE SECRET
// (TYPE S3, ...)`, `SET s3_*` and `CREATE OR REPLACE VIEW` all work.
// What was missing was a way to point a cube at a URL.
//
// THE REMOTE FILE BECOMES A VIEW under the name the model already
// uses. That is the whole design, and it is what keeps this from
// being a second plane: `#>{trades::DB.TRADES}#` still compiles to
// `SELECT ... FROM TRADES`, the planner is untouched, the tree
// assembly is untouched, and DuckDB resolves TRADES to a scan of a
// remote Parquet instead of a local table. Only the connection
// differs -- literally, and not as a figure of speech.
//
// It also means predicate and projection pushdown come free: DuckDB
// reads Parquet row-group statistics over HTTP range requests, so a
// filtered cube pulls the bytes it needs rather than the file.
//
// WHAT IS NOT HERE. Delta is absent on purpose: `delta_scan` exists
// as a symbol but the extension is not bundled, so it tries to
// autoload and fails. Offering it would be offering an action that
// cannot work -- the same rule the menu follows elsewhere.

import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import {
  SecretStore, type RemoteMountRequest, type RemoteMountSource, type SecretField, type SecretValue,
} from '../../engine-client/src/host-operations.ts';
import { heldTexts, type ConnectionSettings, type KeptSetting } from '../../engine-client/src/connection-settings.ts';
import type { PlanValue } from '../../engine-client/src/plan-runner.ts';

export type RemoteFormat = 'parquet' | 'csv' | 'iceberg';

export interface RemoteSource {
  /**
   * The name the Pure model refers to.
   *
   * This is the point of the whole module: the view takes the table's
   * name, so nothing above the connection has to know the data is
   * remote.
   */
  readonly name: string;
  /** https://…, s3://…, or any URL httpfs understands. */
  readonly url: string;
  /** Inferred from the URL when omitted. */
  readonly format?: RemoteFormat;
}

export interface S3Credentials {
  readonly region?: string;
  readonly endpoint?: string;
  readonly accessKeyId?: string;
  readonly secretAccessKey?: string;
  readonly sessionToken?: string;
  /** MinIO and most S3-compatibles need 'path'. */
  readonly urlStyle?: 'vhost' | 'path';
  readonly useSsl?: boolean;
}

export interface RemoteOptions {
  readonly sources: readonly RemoteSource[];
  readonly s3?: S3Credentials;
}

/** Guess the format from the URL, so the common case needs no field. */
export function inferFormat(url: string): RemoteFormat {
  const path = url.split('?')[0] ?? url;
  if (/\.csv(\.gz)?$/i.test(path)) return 'csv';
  // An Iceberg table is a DIRECTORY with metadata in it, not a file,
  // so it is recognised by shape rather than extension.
  if (/\/metadata\/?$/i.test(path) || /#iceberg$/i.test(url)) return 'iceberg';
  return 'parquet';
}

/** The URL a source's data is read from: as given, but an Iceberg table's without the mark that names it one. */
export function scanUrl(url: string): string {
  return url.replace(/#iceberg$/i, '');
}

/**
 * The name a source's values are held under in the host's store, as a plan names it: `prefix` and the source's name,
 * each character but a letter, a digit or '_' written `_<hex>_` -- a word (the plan names a store's entry by a word:
 * LoadStep.Bind.Held). A source's name is an identifier already wherever a host makes one (a table's name), and kept
 * as it is.
 */
export function storeName(prefix: string, source: string): string {
  return prefix + [...source].map((c) => (/[A-Za-z0-9_]/.test(c) ? c : `_${c.codePointAt(0)!.toString(16)}_`)).join('');
}

/** The secret a source's credentials are read with, and held under: named by the source (`datacube_s3_<name>`). */
export function secretName(source: string): string {
  return storeName('datacube_s3_', source);
}

/** The name a source's bearer URL is held under (`datacube_url_<name>`): the plan's held bind, the dialect's variable. */
export function bearerName(source: string): string {
  return storeName('datacube_url_', source);
}

/** Whether a URL is a BEARER URL: its query string carries its token (a presigned link's signature), so it is held by
 *  the host and bound, never written into a statement or a plan. */
export function isBearer(url: string): boolean {
  return scanUrl(url).includes('?');
}

/** Whether a URL names a user or a password in its address (`https://user:password@host/...`): a credential written in
 *  it, which no plan or statement holds. An '@' past the address (a path's) is no user. */
export function carriesUserinfo(url: string): boolean {
  const scheme = url.indexOf('://');
  if (scheme < 0) return false;
  const rest = url.slice(scheme + 3);
  const end = rest.search(/[/?#]/);
  return (end < 0 ? rest : rest.slice(0, end)).includes('@');
}

/** A URL's address without its query string: what a receipt names and a secret's scope is cut from, never the token. */
export function shownAddress(url: string): string {
  const at = scanUrl(url);
  const query = at.indexOf('?');
  return query < 0 ? at : at.slice(0, query);
}

/** A source's S3 values, by field (none for none given). */
function credentialValues(creds: S3Credentials | undefined): Map<SecretField, SecretValue> {
  const values = new Map<SecretField, SecretValue>();
  if (!creds) return values;
  if (creds.accessKeyId) values.set('KEY_ID', creds.accessKeyId);
  if (creds.secretAccessKey) values.set('SECRET', creds.secretAccessKey);
  if (creds.sessionToken) values.set('SESSION_TOKEN', creds.sessionToken);
  if (creds.region) values.set('REGION', creds.region);
  if (creds.endpoint) values.set('ENDPOINT', creds.endpoint);
  if (creds.urlStyle) values.set('URL_STYLE', creds.urlStyle);
  if (creds.useSsl !== undefined) values.set('USE_SSL', creds.useSsl);
  return values;
}

/**
 * ONE SOURCE'S MOUNT AS THE PLANNER IS ASKED FOR IT (B3's Q3, 2026-11-11, the user's ruling: "Yes do option 2"):
 * NAMES AND FIELDS ONLY -- an address that carries no token as it is, a bearer URL by the name this host holds it under
 * and its address without the query string; the S3 secret by the name it is held under and the fields given. `view`:
 * the view it is read through under the source's name, or none (its access alone).
 */
export function mountRequest(databaseType: string, source: RemoteSource, creds: S3Credentials | undefined,
  view: boolean): RemoteMountRequest {
  // a user or a password in the address is a credential written in it: refused by name before the planner is asked,
  // the address not repeated (the tenets audit's LOW 5, 2026-11-11; the planner refuses it too)
  if (carriesUserinfo(source.url)) {
    throw new Error(`${source.name}'s URL names a user or a password (user:password@host): a credential is never written`
      + " in an address -- give the file's keys as its credentials");
  }
  const fields = [...credentialValues(creds).keys()];
  return {
    databaseType,
    ...(view ? { view: source.name } : {}),
    location: isBearer(source.url) ? { bearer: bearerName(source.name), shown: shownAddress(source.url) } : { url: scanUrl(source.url) },
    format: source.format ?? inferFormat(source.url),
    ...(fields.length > 0 ? { credentials: { name: secretName(source.name), fields } } : {}),
  };
}

/** The host's store for one source: its S3 values under its secret's name, its bearer URL under its own -- what the
 *  plan's held binds name, bound by the hosts' runner, never written. */
export function sourceStore(source: RemoteSource, creds: S3Credentials | undefined): SecretStore {
  const store = new SecretStore();
  const values = credentialValues(creds);
  if (values.size > 0) store.hold(secretName(source.name), values);
  if (isBearer(source.url)) store.hold(bearerName(source.name), new Map([['URL', scanUrl(source.url)]]));
  return store;
}

/**
 * Remove credentials from text before it can be shown or logged.
 *
 * No statement and no plan holds a credential or a bearer URL's token (each is bound: B3), but a database may still
 * echo a bound value in its own error -- a failed read repeats the whole URL it read (measured, B3's Q3). The values
 * are redacted by VALUE rather than by parsing the message, so a secret is caught whatever words carry it.
 */
export function redactSecrets(
  text: string,
  creds: S3Credentials | undefined,
  urls: readonly string[] = [],
): string {
  let out = text;
  const secrets: string[] = [];
  for (const secret of [creds?.secretAccessKey, creds?.sessionToken, creds?.accessKeyId]) {
    if (secret) secrets.push(secret);
  }
  for (const url of urls) {
    if (!isBearer(url)) continue;
    // the whole URL, its query string alone, each parameter's value (a message may quote any: heldTexts, M2)
    secrets.push(...heldTexts(scanUrl(url)));
  }
  // the longest first, so a part never leaves the rest of a longer one behind
  for (const secret of secrets.sort((a, b) => b.length - a.length)) {
    if (secret.length > 0) out = out.split(secret).join('***');
  }
  return out;
}

/** The engine a remote source is mounted on: its database's type, its queue's run (a statement that binds nothing),
 *  its connection held for a statement that binds (DuckDbEngine.exclusive), and where it keeps a mount's connection
 *  settings for the connections its host opens (the tab's: DuckDbEngine.settings), if anywhere. */
export interface RemoteEngine {
  readonly databaseType: string;
  run(sql: string, epoch: number): Promise<unknown>;
  exclusive<T>(work: (held: Pick<HeldConnection, 'rows'>) => Promise<T>): Promise<T>;
  readonly settings?: ConnectionSettings | undefined;
}

/** One source mounted: its name, and the settings its mount made on the connection, which a host that opens other
 *  connections to its database replays on each (a bearer URL's session variable; the plan's held binds, no value). */
export interface Mounted {
  readonly name: string;
  readonly settings: readonly KeptSetting[];
}

/**
 * Point the engine's connection at remote data, each source by its PLANNED LOAD (B3's Q3: the planner's remoteMount,
 * run by the hosts' runner): its extensions, its S3 secret and its bearer URL bound from the host's store by name, its
 * view -- no credential and no token in any statement or in the plan. Its connection settings are kept where the
 * engine keeps them (the tab's), and answered.
 *
 * Runs before the first query. Failures are rethrown with the credentials and the bearer URL stripped, and name the
 * source rather than the statement, because "could not mount TRADES" is actionable where a redacted CREATE SECRET is not.
 */
export async function mountRemote(
  engine: RemoteEngine,
  planner: RemoteMountSource,
  options: RemoteOptions,
): Promise<Mounted[]> {
  const mounted: Mounted[] = [];
  for (const source of options.sources) {
    const settings = await runMount(engine, planner, source, options.s3, true);
    engine.settings?.keep(source.name, settings, sourceStore(source, options.s3));
    mounted.push({ name: source.name, settings });
  }
  return mounted;
}

/** The engine's connection given the sources' extensions and credentials, no view yet: its access alone (Python's,
 *  before it checks an Iceberg table's files: R4). */
export async function accessRemote(engine: RemoteEngine, planner: RemoteMountSource,
  options: RemoteOptions): Promise<void> {
  for (const source of options.sources) await runMount(engine, planner, source, options.s3, false);
}

/** One source's mount planned and run, its failures redacted: its connection settings. */
async function runMount(engine: RemoteEngine, planner: RemoteMountSource, source: RemoteSource,
  creds: S3Credentials | undefined, view: boolean): Promise<readonly KeptSetting[]> {
  const redacted = (cause: unknown): Error =>
    new Error(`could not mount remote data: ${redactSecrets(cause instanceof Error ? cause.message : String(cause), creds, [source.url])}`);
  try {
    const plan = await planner.remoteMount(mountRequest(engine.databaseType, source, creds, view));
    // the hosts' plan runner, fetched with the first mount (a grid-only page carries none of it)
    const { runLoad } = await import('../../engine-client/src/plan-runner.ts');
    // a statement that binds nothing runs as every query runs (DuckDbEngine.run: B3.1's fix, the demo's cube over a
    // remote Parquet); one that binds, on the held connection
    const run = (sql: string, params: readonly PlanValue[]): Promise<unknown> =>
      (params.length === 0 ? engine.run(sql, 0) : engine.exclusive((held) => held.rows(sql, params)));
    return (await runLoad(plan, run, { secrets: sourceStore(source, creds) })).settings;
  } catch (cause) {
    throw redacted(cause);
  }
}
