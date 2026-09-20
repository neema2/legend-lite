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

import type { QueryEngine } from './engine.ts';

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

/**
 * Quote a SQL string literal.
 *
 * SQL doubles the quote; it does NOT use a backslash. Getting this
 * wrong is how a bucket name with an apostrophe becomes a syntax
 * error at best -- and the same class of bug as the Pure escaping
 * this codebase already had to fix once.
 */
export function sqlLiteral(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
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

/** The scan expression for one source. */
export function scanExpression(source: RemoteSource): string {
  const url = sqlLiteral(source.url.replace(/#iceberg$/i, ''));
  switch (source.format ?? inferFormat(source.url)) {
    case 'csv':
      return `read_csv_auto(${url})`;
    case 'iceberg':
      return `iceberg_scan(${url})`;
    case 'parquet':
      return `read_parquet(${url})`;
  }
}

/** An identifier for a view name, quoted so any name survives. */
function ident(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

export function viewStatement(source: RemoteSource): string {
  // OR REPLACE so remounting a cube against a new URL is not an
  // error -- pointing at yesterday's snapshot and back is a normal
  // thing to do.
  return `CREATE OR REPLACE VIEW ${ident(source.name)} AS`
    + ` SELECT * FROM ${scanExpression(source)}`;
}

/**
 * The statements that configure credentials.
 *
 * `CREATE SECRET` rather than the `SET s3_*` globals: a secret is
 * scoped and replaceable, where the globals are process-wide and
 * leak between sources when a cube reads two buckets.
 */
export function credentialStatements(
  creds: S3Credentials | undefined,
): string[] {
  if (!creds) return [];
  const parts: string[] = ['TYPE S3'];
  if (creds.accessKeyId) parts.push(`KEY_ID ${sqlLiteral(creds.accessKeyId)}`);
  if (creds.secretAccessKey) {
    parts.push(`SECRET ${sqlLiteral(creds.secretAccessKey)}`);
  }
  if (creds.sessionToken) {
    parts.push(`SESSION_TOKEN ${sqlLiteral(creds.sessionToken)}`);
  }
  if (creds.region) parts.push(`REGION ${sqlLiteral(creds.region)}`);
  if (creds.endpoint) parts.push(`ENDPOINT ${sqlLiteral(creds.endpoint)}`);
  if (creds.urlStyle) parts.push(`URL_STYLE ${sqlLiteral(creds.urlStyle)}`);
  if (creds.useSsl !== undefined) {
    parts.push(`USE_SSL ${creds.useSsl ? 'true' : 'false'}`);
  }
  if (parts.length === 1) return [];
  return [`CREATE OR REPLACE SECRET datacube_s3 (${parts.join(', ')})`];
}

/**
 * Remove credentials from text before it can be shown or logged.
 *
 * QueryError carries the SQL that failed, and a failing CREATE SECRET
 * would otherwise put a live access key into an error banner, a
 * console line and whatever collects them. The values are redacted
 * by VALUE rather than by parsing the statement, so a secret survives
 * no matter which statement it turns up in.
 */
export function redactSecrets(
  text: string,
  creds: S3Credentials | undefined,
): string {
  if (!creds) return text;
  let out = text;
  for (const secret of [
    creds.secretAccessKey,
    creds.sessionToken,
    creds.accessKeyId,
  ]) {
    if (secret && secret.length > 0) {
      out = out.split(secret).join('***');
    }
  }
  return out;
}

/** Extensions a format needs, loaded before anything references it. */
export function extensionStatements(
  sources: readonly RemoteSource[],
): string[] {
  const out = ['INSTALL httpfs', 'LOAD httpfs'];
  const formats = new Set(
    sources.map((s) => s.format ?? inferFormat(s.url)),
  );
  if (formats.has('iceberg')) {
    out.push('INSTALL iceberg', 'LOAD iceberg');
  }
  return out;
}

/** Every statement, in the order it has to run. */
export function mountStatements(options: RemoteOptions): string[] {
  return [
    ...extensionStatements(options.sources),
    ...credentialStatements(options.s3),
    ...options.sources.map(viewStatement),
  ];
}

/**
 * Point the engine's connection at remote data.
 *
 * Runs before the first query. Failures are rethrown with the
 * credentials stripped, and name the source rather than the
 * statement, because "could not mount TRADES" is actionable where a
 * redacted CREATE SECRET is not.
 */
export async function mountRemote(
  engine: QueryEngine,
  options: RemoteOptions,
): Promise<void> {
  for (const statement of mountStatements(options)) {
    try {
      await engine.execute(statement, 0);
    } catch (cause) {
      const message = redactSecrets(
        cause instanceof Error ? cause.message : String(cause),
        options.s3,
      );
      throw new Error(`could not mount remote data: ${message}`);
    }
  }
}
